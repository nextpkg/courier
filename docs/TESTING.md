# 测试与性能

本文区分功能正确性、短时闭环压测、持续负载验证和容量模型；曲线展示指定环境中的观测，不是吞吐上限或生产服务等级承诺。

## 选择验证入口

- 检查基本正确性：运行 [普通测试](#复现)；检查服务隔离：运行 `TestKafkaCrossProcess`。
- 评估当前资源配置：先看 [容量规划](KAFKA-CAPACITY.md)，再使用 [可配置负载参数](#资源限额与联合并发扫描)。
- 检查持续负载：运行 [多服务持续负载](#多服务持续负载)；故障注入使用 [高可用回归](#扩缩容背压与进程故障)。
- 统一查看 [性能数据口径](KAFKA-CAPACITY.md#性能数据口径)：约 900 RPC/s 是历史低 worker 基线，94,596 RPC/s 是单方法调优短测，63,944 RPC/s 是十分钟混合服务负载，三者不能混用。

## 测试分层

| 层次 | 检查对象 | 入口与环境 |
| --- | --- | --- |
| 普通测试 | 请求关联、重复回复、取消、超限、生成器、内存调用与资源关闭 | 各模块 `*_test.go`，无需 Broker |
| Broker 集成测试 | NATS/Kafka 的路由、恢复、提交与真实传输行为 | `examples`，`integration` 标签及对应 Broker |
| 跨进程拓扑 | 2 个服务、每服务 2 个进程实例、3 个 Client 的返回值归属 | `TestKafkaCrossProcess`，真实 Kafka |
| 读取放大 | 1/4/8 个 session 读取一个共享回复分区的字节开销 | `TestKafkaSharedReplyReadAmplification` |
| 并发扫描／持续负载 | 不同并发或时长下的速率、延迟、失败及资源趋势 | `TestKafkaLoadCurve`，混合模式需显式开启 |

### 跨进程拓扑

```mermaid
flowchart TB
    subgraph Driver[测试驱动进程]
        C1[Client 1]
        C2[Client 2]
        C3[Client 3]
    end
    C1 & C2 & C3 --> K[Kafka：2 个请求 topic + 1 个回复 topic]
    K --> O1[Orders 进程 1]
    K --> O2[Orders 进程 2]
    K --> I1[Inventory 进程 1]
    K --> I2[Inventory 进程 2]
    O1 & O2 & I1 & I2 --> K
    K --> C1 & C2 & C3
```

测试共发起 240 次调用，每个 Client 对 4 个方法各调用 20 次，校验业务结果、响应所属服务、每个服务的两个实例均被使用，以及结束后 pending 为 0；Get/Reserve 返回的 PID 用于统计实例分布，List/Count 不返回 PID。3 个 Client 是同一测试进程中的独立对象，4 个服务实例为独立进程；测试不承诺每个 Client 每次都能均匀命中所有实例。

## CPU 配额下的联合配置扫描

1C2G 与 2C4G 的 worker、窗口及缓冲联合扫描、实测曲线和启动配置见 [Kafka 容量规划](KAFKA-CAPACITY.md#cpu-配额与并发配置)；它与下文默认 worker 的基线使用不同配置，不能直接视为同一性能上限。

## 持续负载参考曲线

本地部署先参考十分钟混合服务验证：应用 4 CPU / 1 GiB、Broker 2 CPU / 2 GiB，1536 并发，成功吞吐中位数 **63,944 RPC/s**、每轮 p99 中位数 **36.852 ms**、0 失败或错配；配置与完整指标见 [容量规划](KAFKA-CAPACITY.md#多服务持续负载与资源配置)，复现见 [多服务持续负载](#多服务持续负载)。

![十分钟混合服务负载：1536 并发，应用 4 CPU / 1 GiB](images/kafka-soak.png)

历史低 worker 基线移至 [文末](#历史基线曲线)；单方法短测和 CPU 配额对照统一在容量规划中报告。

## 复现

从仓库根目录运行普通测试：

```sh
CGO_ENABLED=0 go test ./... ./transport/memory/... ./transport/nats/... ./transport/kafka/... ./examples/...
CGO_ENABLED=0 go vet ./... ./transport/memory/... ./transport/nats/... ./transport/kafka/... ./examples/...
go test -race ./... ./transport/memory/... ./transport/nats/... ./transport/kafka/... ./examples/...
```

真实 Kafka 测试需要可访问的专用 Broker，以及创建/删除测试 topic 的权限；下面地址适用于 Broker 本机监听 9092 的环境，若部署不同，设置 `KAFKA_BROKERS` 为实际地址，测试会创建带唯一前缀的 topic 并在正常结束时删除：

```sh
KAFKA_BROKERS=127.0.0.1:9092 CGO_ENABLED=0 \
  go test -tags=integration \
  -run '^TestKafka(LoadCurve|SharedReplyReadAmplification|CrossProcess)$' \
  -count=1 -v -timeout=240s ./examples/... > kafka-results.log 2>&1
```

确认命令成功且日志末尾为 `PASS` 后生成图表；Python 只用于绘图，不是框架运行依赖：

```sh
python3 -m venv .chart-venv
.chart-venv/bin/pip install matplotlib==3.9.4
.chart-venv/bin/python scripts/plot-kafka.py kafka-results.log
rm -rf .chart-venv
```

绘图脚本输出两个 SVG 和两个 CSV，重新测量后应同时更新环境元数据；原始日志保留作测量依据，审查完成后可删除。进程被强制终止时测试清理可能未执行，应按日志和本次测试前缀核对遗留 topic，不删除其他任务资源。

## 诊断与结果解读

吞吐趋平同时尾延迟上升通常表示某处排队增加，需要结合客户端、服务端和 Broker 指标定位，不能仅凭曲线认定瓶颈；错误率不为零时必须同时报告错误类别，不能只展示成功吞吐。

`TestKafkaSharedReplyReadAmplification` 使用确切的读取/匹配记录数断言校验共享回复行为；`TestKafkaLoadCurve` 校验返回值和 pending 清理，但不设置固定性能阈值，以免把单机调度波动当作功能回归。容量估算见 [Kafka 容量规划](KAFKA-CAPACITY.md)，传输故障排查见 [Kafka 路由与资源配置](KAFKA.md)。

## 发布并发对照

`TestKafkaLoadCurve` 支持通过 `-kafka-publish-workers` 设置每个客户端的发送工作协程数，通过 `-kafka-load-concurrency` 指定调用并发档位；默认分别为 4 和 `1,8,32,64,128`，每档默认运行三轮，可通过 `-kafka-load-trials` 调整，每轮默认持续发起请求 5 秒。每轮日志包含 `publish_workers`，便于区分配置。

例如，在专用 Kafka 环境中测量每客户端 16 个发送 worker：

```sh
KAFKA_BROKERS=127.0.0.1:9092 CGO_ENABLED=0 \
  go test -tags=integration ./examples -run '^TestKafkaLoadCurve$' \
  -count=1 -v -timeout=180s -args \
  -kafka-publish-workers=16 -kafka-load-concurrency=32,128,256
```

该命令本身不设置 CPU 或内存限额；资源受限测试应在容器或目标机器中执行，记录应用和 Broker 各自的配额。比较发送并发时保持 producer linger、确认级别、消息大小及服务端参数不变，同时报告吞吐、尾延迟、失败数和资源占用；仅增加业务调用并发可能只增加框架队列中的等待。

### 资源限额与联合并发扫描

联合调整发送与处理并发时，可使用以下测试参数；这些参数仅影响压测，不改变运行库默认值：

| 参数 | 默认值 | 含义 |
| --- | ---: | --- |
| `-kafka-publish-workers` | 4 | 每个 Client 的发送 worker 数；测试有 3 个 Client |
| `-kafka-server-workers` | 16 | 每个本地请求 topic 订阅的处理 worker 数 |
| `-kafka-load-window` | 64 | 请求消费窗口，必须不小于服务端 worker 数 |
| `-kafka-core-concurrency` | 128 | 核心 Server 的 MaxConcurrent |
| `-kafka-publish-buffers` | 256 | 每个 transport 的 SDK producer 记录缓冲上限 |
| `-kafka-max-in-flight` | 512 | 每个 Client 的 MaxInFlight |
| `-kafka-load-duration` | 5s | 每轮持续发起请求的时间，随后等待在途请求排空 |
| `-kafka-load-trials` | 3 | 每个并发档位的测量轮数 |
| `-kafka-load-mixed` | false | 启动 Orders、Inventory 各两个实例，混合调用四个方法 |
| `-kafka-load-resources` | false | 读取 Linux cgroup v2 的 CPU 与内存计数 |

`-kafka-load-resources` 要求进程处于设置了 CPU quota 的 cgroup v2 容器中；环境不满足时测试失败，不将缺失指标报告为零。每轮 `KAFKA_LOAD` 增加 `cpu_cores`（平均消耗核数）、`cpu_quota_cores`、`cpu_quota_utilization`（消耗核数/配额）、`throttled_periods`（本轮 CPU 配额节流周期数）和 `memory_peak_bytes`（容器自启动以来的峰值，不是本轮独立峰值）。

CPU 计数围绕每轮负载取差值，不包含连接初始化；应用容器同时运行压测调用方、客户端和服务端，因此指标是合计资源。比较 1C2G 与 2C4G 时，应同时调整容器 CPU/内存配额与 GOMAXPROCS，保持 Broker、消息大小和确认策略相同，并区分 CPU 利用率相对一核还是相对全部配额。

### 多服务持续负载

在专用 Kafka 环境、已设置 4 CPU / 1 GiB 配额的 Linux cgroup v2 应用容器内，从仓库根目录运行：

```sh
KAFKA_BROKERS=kafka:9092 GOMAXPROCS=4 CGO_ENABLED=0 \
  go test -tags=integration ./examples -run '^TestKafkaLoadCurve$' \
  -count=1 -v -timeout=15m -args \
  -kafka-load-mixed -kafka-load-trials=30 -kafka-load-duration=20s \
  -kafka-load-concurrency=1536 -kafka-publish-workers=512 \
  -kafka-max-in-flight=4096 -kafka-server-workers=512 \
  -kafka-load-window=1024 -kafka-core-concurrency=512 \
  -kafka-publish-buffers=2048 -kafka-load-resources
```

`kafka:9092` 假定测试网络将 Broker 注册为该主机名，部署不同时需替换为可达地址；该命令本身不创建 Broker 或设置配额，Broker 应独立运行。实测使用单节点 Kafka 4.4.0、2 CPU / 2 GiB、JVM `-Xms256m -Xmx512m`；生产数据不得用于压测。

命令中的 `-kafka-load-resources` 只读取容器资源指标，**不会自动执行宿主机内存压力或 swap 停止保护**；容量报告中的压力保护由外部执行脚本提供，复现时应另设监控与停止策略，不将该测试标志视为保护开关。

混合模式创建三个核心 Client、两个 Orders 实例和两个 Inventory 实例，均在应用进程内运行；每服务三个请求分区，所有客户端共享一个回复分区。四个方法为 Orders.Get、Orders.List、Inventory.Reserve、Inventory.Count，每个并发 worker 固定调用一个方法，完成量不强制等比例。`completed_methods` 记录各方法成功次数。

所有业务请求使用全程递增的唯一数字标记；Get、List、Reserve 逐条校验业务 ID 与服务来源，压测专用 Count handler 回显数字标记，使等长输入也可区分，正常示例的 Count 语义不变；错误、空响应、标记或来源不符都计入失败。每轮结束检查客户端 Pending 为零。30 轮各持续发起 20 秒，轮间仅排空与统计，同一组 Client、服务和 Broker 全程复用，不在每轮重建；它是约 10 分钟持续负载验证，不是 24 小时可靠性承诺或固定到达速率过载试验。

实测数据、曲线及参考环境资源策略见 [Kafka 容量规划](KAFKA-CAPACITY.md#多服务持续负载与资源配置)。

仓库保留逐轮 CSV、资源时间序列及环境摘要，安装 Python 3 与 matplotlib 后可从仓库根目录重绘本次图表：

```sh
python3 scripts/plot-kafka-soak.py \
  docs/benchmarks/kafka-soak.csv docs/benchmarks/kafka-soak-memory.csv \
  --split-summary docs/benchmarks/kafka-split.csv
```

吞吐、延迟和 CPU 横轴为累计请求发起时间；内存横轴为首次资源采样后的实际时间，包含启动阶段，两种横轴不用于精确事件对齐。每轮 p99 不是全程请求合并后的 p99。

### 分段耗时诊断

使用 `-kafka-server-workers` 和 `-kafka-load-window` 对照服务端适配器的 Workers/Window，默认分别为 16/64；`-kafka-load-diagnostics` 开启测试专用的 SDK hooks 与 handler 包装，生产运行时不启用该采样。

```sh
KAFKA_BROKERS=127.0.0.1:9092 CGO_ENABLED=0 \
  go test -tags=integration ./examples -run '^TestKafkaLoadCurve$' \
  -count=1 -v -timeout=180s -args \
  -kafka-publish-workers=16 -kafka-load-concurrency=128 \
  -kafka-server-workers=32 -kafka-load-window=64 -kafka-load-diagnostics
```

`KAFKA_DIAGNOSTICS` 按客户端和服务端分别记录计数、总耗时、最大耗时与批次大小，平均耗时为 `total_ms / count`：

| 字段 | 测量边界 |
| --- | --- |
| `produce_success` | SDK 接收待发记录至成功发布回调，包含攒批、排队、重试与确认 |
| `broker_api_0_wire` | Produce 网络写入开始至响应读完，包含网络和 Broker 处理，不含攒批 |
| `broker_api_8_wire` | OffsetCommit 请求的相同网络往返边界 |
| `broker_api_*_write_wait` | 请求在 SDK 内生成后，等待写入连接的时间 |
| `fetch_buffer_to_handler` | 请求进入 SDK fetch 缓冲至进入核心 handler，包含消费轮询及适配器队列等待 |
| `core_handler` | 核心请求校验、Thrift 编解码与业务方法处理，不含适配器回复发布 |
| `batch_records / batches` | 每个成功写入分区批次的平均记录数 |

诊断数据覆盖诊断对象创建至扫描结束，包括连接初始化、预热与轮次间隙；发布记录数和网络请求数不同，不能将两种平均耗时相减当作精确攒批耗时。采样包含锁和额外 envelope 解码，需与未采样基线对照，避免将观测开销误判为业务耗时。

服务端 worker 在核心 handler 返回后仍等待回复发布确认；业务处理很快时，吞吐可能主要受 worker 数与发布等待的组合限制。定位时应固定 Broker、客户端和消息大小，仅改变服务端 Workers，再对照分段耗时与吞吐；CPU 利用率低本身不能证明 Broker 饱和。

## 完整 Broker 回归

从仓库根目录运行，使用专用测试 Broker；Kafka 测试需要创建及删除 topic 的权限，`NATS_SERVER` 用于启动、停止和重启故障测试专用进程。

```sh
NATS_URL=nats://127.0.0.1:4222 NATS_SERVER="$(command -v nats-server)" \
  go test -tags=integration -run TestNATS -timeout=300s ./examples/...

KAFKA_BROKERS=127.0.0.1:9092 \
  go test -tags=integration -run TestKafka -timeout=600s ./examples/...
```

先运行 `command -v nats-server` 确认可执行文件已加入 PATH；也可将 `NATS_SERVER` 设为已安装的绝对路径；普通测试无需 Broker，race 检查需要启用 CGO。上述 `-timeout` 限制整个测试包的执行时间，不改变单次调用和故障恢复的断言期限。

## 扩缩容、背压与进程故障

`examples/high_availability_test.go` 使用真实 Broker 和独立服务／客户端进程；子进程通过握手报告就绪，故障注入使用 `Process.Kill`，不执行正常关闭流程。

| 场景 | 验证内容 |
| --- | --- |
| Kafka / NATS 动态扩缩容 | 4 个持续调用者下执行两轮 1 → 2 → 1；确认新实例参与、退出后仍可调用、结果匹配，正常扩缩容不重复执行 |
| Kafka / NATS 有界背压 | 3 轮各 256 次突发调用，客户端在途上限为 8；检查拒绝、已接收调用完成、pending 排空与后续调用恢复 |
| Kafka 发送端背压 | 切断真实 TCP 链路、限制原生发送缓冲；恢复网络后在 10 秒内重新调用成功，记录缓冲仍满时的拒绝次数 |
| 服务端强杀 | 在业务执行前及业务执行后、回复前分别终止进程；Kafka 验证重投及重复副作用窗口，NATS 验证旧调用超时和新调用恢复 |
| 客户端强杀 | handler 执行中终止客户端，创建新客户端后释放旧回复；校验新调用不串消息，Kafka 另核对读取数与匹配数 |
| 既有故障回归 | Kafka 回复发布失败、提交顺序、回复 reader 重连、过期重投；NATS Broker 强杀重启 |

从仓库根目录运行，先配置专用 Kafka、NATS Broker 及 `NATS_SERVER` 可执行文件路径：

```sh
KAFKA_BROKERS=127.0.0.1:9092 NATS_URL=nats://127.0.0.1:4222 \
NATS_SERVER="$(command -v nats-server)" \
  go test -race -tags=integration \
  -run '^Test((Kafka|NATS)HA.*|Kafka(LeaderMetadataRecovery|CommitOrderAndRebalance|ProducerBackpressure|ResponseFailureRedeliveryAndCommit|ReplyReconnectRetainsOffset|ExpiredRedeliveryDoesNotRepeatHandler)|NATSDisconnectReconnect|NATSDrainClose)$' \
  -count=1 -v -timeout=300s ./examples/...
```

日志记录调用结果、恢复耗时、重复副作用和 pending；背压测试的 heap 与 goroutine 数是阶段采样，不是峰值，也不构成长时间无泄漏证明。Kafka 的回复发布失败或服务端崩溃可能重执行业务，因此重试有副作用的方法需要业务幂等；NATS 不持久化重投已交付请求。

`TestNATSDrainClose` 另外覆盖超过 worker 数量的待处理请求排空和关闭期限到达后的中止行为；正常缩容应先调用 `Server.Close` 并等其成功，再关闭适配器和 SDK 连接，提前取消启动 context 不属于正常排空路径。

这些测试覆盖应用进程的可用性及单 Broker 故障行为，不验证多 Broker 集群选主、跨可用区故障或长时间稳定性；恢复时间取决于 Broker、客户端超时配置及部署环境，测试耗时不是服务等级承诺。

`TestKafkaLeaderMetadataRecovery` 通过 TCP 转发真实 Kafka 流量，并将一次成功的 Produce 确认替换为 `NOT_LEADER_FOR_PARTITION`：5 秒 metadata 节流配置应无法在 2 秒内完成发送确认，默认配置应恢复，并验证重试没有产生第二条业务执行；Broker 已保存首条记录，因此该测试也覆盖确认丢失后的幂等发布。

### 发布失败诊断

`TestKafkaLoadCurve` 的 `failure_reasons` 按具体错误类型与消息统计失败（最多 16 种，其余归入 other），返回值不匹配使用独立原因分类；成功调用 p99 不包含失败调用的等待时间。

测试默认采样每个 transport 前 5 个 SDK 发布错误（`KAFKA_PUBLISH_ERROR`），以及前 32 个超过 500 ms 或返回错误的 Produce/OffsetCommit 请求（`KAFKA_SLOW_WIRE`），包含 UTC 时间、请求排队、写入及等待/读取响应耗时；该采样只用于测试，不改变生产确认语义。适配器消费重入组日志保留首个处理/回复发布错误，便于区分超时、过载和其他失败；采样不足以代替完整错误计数。

## 历史基线测量方法

闭环扫描使用 1 个 Orders 服务实例、3 个 Client session、3 个请求分区和 1 个回复分区，测量 `Orders.Get(id, 0)`，返回 ID、来源和三个整数；它与上述四服务进程的正确性测试是不同场景。

- 并发档位为 1、8、32、64、128，每档顺序运行 3 次，每次持续发起请求 5 秒，然后等待在途调用结束。
- 每个并发 worker 在上次调用完成后才发起下一次调用，按 worker 编号分配至三个 Client；并发为 1 时仅一个 Client 发起流量，三个回复 reader 均存在。
- 初始化 Broker/适配器和每个 Client 的一次预热调用不计时；成功 RPC/s 为成功数除以实测时间，时间包含末尾排空。
- 延迟从调用同步便利方法前开始，到结果返回结束，包含本地排队、发布、服务执行和回复读取；p50/p95/p99 按成功样本排序后的 `floor((N-1) × p)` 索引计算。
- 返回错误、空结果或 ID/来源不匹配均计为失败；每轮完成后校验无 pending 遗留。
- 服务端 MaxConcurrent=128；Kafka Workers=16、Window=64；每 Client MaxInFlight=512、超时 8 秒，其余为适配器默认值。

闭环负载会在响应变慢时自动降低发起速率，因此曲线不描述固定到达速率下的过载积压；本场景也未覆盖大消息、业务数据库耗时、多 Broker 容灾或长时间稳定性。

## 历史基线曲线

以下曲线是 2026-11-01 的历史基线，代码基线为 `f889f3fa0952ff04a0c268daa2cccbafc0953302`，测试源码摘要见[测量元数据](benchmarks/environment.json)。测量使用 franz-go v1.22.2，metadata 刷新节流为 5 s，producer linger（攒批等待）为 10 ms。

v0.1.0 的 metadata 刷新节流默认为 250 ms，其测试源码与历史基线不同；这些曲线仅表示历史测量结果，当前配置和故障恢复验证见 [Kafka 手册](KAFKA.md)及下文。

测量环境：Apple M4、10 核、16 GiB 内存；Go 1.27.3 darwin/arm64；Kafka 4.4.0 单节点 KRaft，运行于 OrbStack ARM64 虚拟机（8 vCPU、约 8 GiB），容器内存上限 1 GiB、JVM heap 256–512 MiB，副本因子 1，客户端通过本机映射端口访问，无 TLS/SASL；机器同时运行其他常驻容器，未隔离 CPU。

![历史低 worker 基线：PublishWorkers=4、Workers=16，非调优后容量](images/kafka-load.svg)

本次 15 轮扫描共完成 **46,877 次成功调用，失败或结果不匹配为 0**；下表为每档三轮的中位数：

| 并发 worker | 成功 RPC/s | p95（ms） | p99（ms） |
| --- | ---: | ---: | ---: |
| 1 | 38.14 | 28.60 | 29.52 |
| 8 | 305.32 | 28.52 | 29.09 |
| 32 | 915.57 | 42.92 | 49.34 |
| 64 | 913.74 | 82.40 | 86.56 |
| 128 | 902.67 | 150.96 | 156.01 |

在该环境与小载荷配置下，并发达到 32 后吞吐进入平台区间，而尾延迟继续增长；这说明继续增加调用并发没有带来相应吞吐收益，该历史曲线本身不能定位瓶颈，后续 worker 与 CPU 配额对照见 [容量规划](KAFKA-CAPACITY.md)，不能据此推导其他部署的容量。

横轴为同时发起调用的 worker 数；曲线为各档 3 次测量的中位数，吞吐、p95 和失败数的误差线为最小至最大值，p50/p99 仅画中位数；相邻点连线只帮助阅读，不表示未测量档位的预测。

![共享回复分区读取放大](images/kafka-read-amplification.svg)

读取放大图来自每档 64 次调用的独立短批次实验，1/4/8 个 reader 读取同一回复分区；该图的横轴是 session 数，不是并发 worker 数，字节数仅包含 Kafka Value。

原始逐轮数据见 [并发扫描 CSV](benchmarks/kafka-load.csv) 和 [读取放大 CSV](benchmarks/kafka-capacity.csv)，环境与测试源码标识见 [测量元数据](benchmarks/environment.json)。
