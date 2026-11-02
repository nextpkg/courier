# Kafka 容量规划

## CPU 配额与并发配置

客户端 `PublishWorkers` 控制并发发送请求的协程数；Kafka 适配器的 `Workers` 控制每个请求 topic 的处理协程数，包括执行 handler 和等待回复发布确认，并非 Kafka Broker 的配置，也不是拉取线程数。两端都存在同步发布等待，默认 10 ms 攒批期间 worker 仍占用名额；增加名额可以提高批次利用率，直到 CPU、网络或下游成为新的瓶颈。

2026-11-02 在版本 `74671c0` 上测量，**三个客户端、一个服务端及测试驱动共享应用容器配额，Kafka 独立运行**；以下是小消息 `Orders.Get(id, 0)` 的起始配置，不是任意业务的默认值。

| 配置项 | 1C2G | 2C4G |
| --- | ---: | ---: |
| 每 Client `PublishWorkers` | 128 | 256 |
| 每 Client `MaxInFlight` | 512 | 1024 |
| 每 Client `DefaultTimeout` | 8 s | 8 s |
| Kafka 每请求 topic `Workers` | 512 | 1024 |
| Kafka `Window` | 1024 | 2048 |
| 每 transport `PublishBuffers` | 512 | 1024 |
| 核心 Server `MaxConcurrent` | 512 | 1024 |
| 应用 `GOMAXPROCS` | 1 | 2 |

配套提高窗口、缓冲与核心并发，避免只调整 worker 后被其他名额限制；默认处理与操作超时下，这两档自动计算的 `RebalanceTimeout` 均为 80 秒，需满足 Broker 消费组超时约束。

### 实测结果

每组连续三轮，每轮发起请求 20 秒后排空；吞吐、CPU、p99 为三轮中位数，内存为容器生命周期峰值。

| 应用配额 | 调用并发 | 成功 RPC/s | CPU 配额利用率 | p99 | 峰值内存 |
| --- | ---: | ---: | ---: | ---: | ---: |
| 1C2G | 1024 | 29,077 | 82.5% | 57.07 ms | 66.55 MiB |
| 2C4G | 1536 | 54,512 | 85.6% | 47.09 ms | 105.15 MiB |

全部调用成功且返回值匹配，无 OOM；1C2G 未发生 CPU 配额节流，2C4G 累计 4 个节流周期。CPU 以每轮 cgroup 使用量增量除以实际耗时及 CPU 配额计算，2C4G 的 85.6% 约等于 1.71 核，不能理解为 0.856 核。

短时扫描每档三轮、每轮 5 秒：1C2G 的最高中位数约 32,913 RPC/s（PublishWorkers=256、Workers=1024、调用并发 2048、CPU 99%、p99 99 ms）；2C4G 约 56,939 RPC/s（256/1024、调用并发 2048、CPU 94%、p99 58 ms）。继续增至 512/2048 后吞吐分别降至约 30,033/55,019 RPC/s，因此不建议单纯堆协程追求满核。

![固定 1024 调用并发下的吞吐、CPU 与尾延迟](images/kafka-cpu-capacity.png)

图中固定 1024 调用并发，点和误差线为三轮中位数及最小至最大值；2C4G 的最高吞吐来自 2048 并发，不在该图中。完整 [78 轮数据](benchmarks/kafka-cpu-capacity.csv) 共记录 13,033,590 次成功调用、0 失败或不匹配。Window、核心并发与缓冲随 worker 配套调整，这是联合配置扫描，不是单变量实验。

测量环境为 Apple M4 / OrbStack ARM64（虚拟机 8 vCPU、约 8 GiB，共享宿主机）、Go 1.27.3；应用分别限制 1 CPU/2 GiB、2 CPU/4 GiB，禁止额外 swap。Kafka 4.4.0 单节点独立容器限制 2 CPU/1 GiB，JVM 256–512 MiB，副本因子 1，无 TLS/SASL；请求 3 分区、回复 1 分区，经专用 Docker 网络通信。保留 10 ms linger、all-ISR 与幂等发布，无业务数据库或重计算，诊断 hooks 关闭。分三个阶段重建 Broker，阶段内顺序测试、未随机化；小幅差异可能受预热和共享宿主机波动影响。

### 启动时自动选择配置

`kafka.AutoCapacityProfile()` 读取当前 `runtime.GOMAXPROCS(0)`：1 选择 1 核档，2 及以上选择 2 核档，超过已测范围不线性外推；该入口显式启用，原有零值默认配置保持不变。它返回普通配置结构，调用方可在初始化前覆盖任意字段。

```go
profile := kafka.AutoCapacityProfile()
profile.Transport.Brokers = []string{"localhost:9092"}
profile.Transport.Prefix = "courier" // 与预先创建的 topic 保持一致
// 在此设置实际部署的 RequestTopics、ReplyTopic、TLSConfig、SASL 等。
transport, err := kafka.New(ctx, profile.Transport)
if err != nil {
    return err
}
// 服务端：demo.ListenOrders(ctx, transport, handler, profile.Server)
// 调用端：demo.DialOrders(ctx, transport, profile.Client)
// 按接入指南处理返回错误，并在退出时关闭 client/server 和 transport。
```

以上为配置接入片段，`demo`、`handler`、`ctx` 使用业务自己的生成代码、实现和生命周期上下文；完整创建、错误处理和关闭顺序见 [接入指南](GETTING-STARTED.md)。`transport`、`server`、`client` 仍由调用方负责关闭。客户端和服务端分别使用各自进程的配置，不会自动协调集群总并发。

该函数只在调用时采样，不监测负载、不修改已有对象，也不检查内存配额；调整容器 CPU 后，需在重新初始化时选择配置。`GOMAXPROCS` 表示 Go 的并行执行预算，不等同于物理核数或精确的 cgroup 配额：例如不足 1 核的容器不能视为已验证的 1 核环境；显式设置 `GOMAXPROCS` 时应与部署预算一致。启动时记录选择后的配置，便于排查。

这些档位尚未发布到 `v0.1.0`，使用该函数需引用包含它的后续提交或版本。

### 使用边界

- 三个客户端合计的发送名额为 3 × PublishWorkers；只有一个客户端或客户端/服务端分开配额时，不可直接照搬上述吞吐。
- 多服务会增加请求 topic worker 与缓冲总量；需要按整个进程核算，不能对每个服务重复承诺整机容量。
- 大消息会放大 MaxInFlight、Window 与 PublishBuffers 的内存占用；小消息峰值不证明 1 MiB 消息也可使用相同档位，内存不足时应降低这些值。
- 数据库连接池、下游限流和计算型 handler 可能要求更低的 MaxConcurrent；按业务容量覆盖档位，并复测 p99、错误率和资源峰值。
- 一分钟级闭环验证不代表长期稳定性或云 Kafka 的容量保证；调用并发不足时 CPU 不会用满，过量并发通常增加排队。

测试入口、参数及资源采样方法见 [测试与性能](TESTING.md)。

## 共享回复的读取成本

每个 `courier.Client` 创建一个独立 session 和直接分区 reader；它读取所选回复 partition 的全部消息，再按 session 过滤。若 partition p 每秒收到 Bp 字节、被 Cp 个 session 读取，所有 reader 的总载荷读取量约为 `Σ(Bp × Cp)`。

定义有效回复载荷速率为 `ΣBp`，则读取放大为 `A = Σ(Bp × Cp) / ΣBp`，其中 `Bp` 的单位为字节/秒，`Cp` 为该分区的 reader 数，假设所有 reader 均跟上读取。流量与客户端分布近似均匀，且每个活跃分区均有 reader 时，可近似为 `客户端数 / 活跃分区数`；不能直接除以配置分区总数，得到小于 1 的结果。

例如，3 个 Client 共享一个回复分区，该分区每秒产生 1 MiB 有效回复，则总 Value 读取量约 3 MiB/s，放大为 3 倍；这是计算示例，实测曲线见 [测试与性能](TESTING.md)。同一进程的多个服务客户端共享一个 `courier.Client` 可以减少 reader 数量。增加 partition 可分摊读取，但仍需考虑 Broker 带宽、连接数和客户端内存。

## 配置与监控

`MaxSubscriptions` 限制每个 transport 的订阅数量，它是资源上限，不是吞吐保证。请求 topic 的 `Workers`、`Window` 和处理超时决定并发及重平衡排空预算，详见 [Kafka 资源配置](KAFKA.md)。

使用 `Stats()` 中的 `ReplyBytes` 和 `ReplyMatchedBytes` 观察读取放大，同时监控消费延迟、进程 RSS、连接数和 Broker 带宽。字节计数只包含消息 Value，不包含 Kafka batch、压缩、协议、TCP 或 TLS 开销。不同权限域应使用不同回复 topic 和 ACL；session 过滤不提供内容保密。

## 负载测试

在专用 Kafka 环境运行共享回复读取测试：

```sh
KAFKA_BROKERS=127.0.0.1:9092 \
  go test -tags=integration -run '^TestKafkaSharedReplyReadAmplification$' -count=1 -v -timeout=180s ./examples/...
```

测试日志中的 `KAFKA_CAPACITY` 行包含 session 数、RPC 数、读取与匹配记录、载荷字节及完成时延；具体统计字段和场景见 `examples/kafka_capacity_test.go`。该测试的计时从发起首个调用前开始，不包含 Broker、transport 和 client 的初始化；64 次调用的短批次速率仍包含调度及缓冲冷启动影响，不能直接当作稳态吞吐上限。

部署容量应使用目标消息大小、并发数、Broker 拓扑和网络条件测量，关注持续负载及故障恢复期间的延迟和资源占用。

## 调整顺序

1. 确定目标成功调用速率、p95/p99 延迟和允许错误率，并固定典型请求/回复大小。
2. 先复用同进程的核心 Client，减少独立回复 reader；再评估增加回复分区，注意已有 session 不会自动迁移。
3. 按 handler 耗时配置 `Workers` 和 `Window`，同时核算重平衡排空预算；扩副本前确认请求分区数足以分配给副本。
4. 使用目标环境重复测量，观察是否出现吞吐趋平、尾延迟增长或错误率上升，再决定容量余量。

当前仓库提供闭环并发扫描，适合比较不同并发下的表现；闭环客户端在响应变慢时会减少发起速率，不能替代固定到达速率的过载测试或长时间稳定性测试。
