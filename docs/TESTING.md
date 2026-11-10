# 测试与性能

## 测试入口

| 层次 | 验证内容 | 环境 |
|---|---|---|
| 普通测试 | 协议校验、所有权、取消、迟到回调、窗口提交、生成器及内存契约 | 无 Broker |
| Broker 集成 | Kafka/NATS 的路由、确认、恢复与背压 | `integration` 标签和专用 Broker |
| 多进程故障 | 扩缩容、进程强杀、业务副作用窗口、重连 | 真实 Broker 与测试子进程 |
| 性能测量 | 返回值归属、吞吐、延迟、资源与 pending 排空 | 固定负载、资源和版本 |

普通断言与 Broker 回归不是相互替代的重复测试：前者稳定覆盖边界，后者验证 SDK、Broker 和故障时序。性能测试不设置机器相关的固定吞吐阈值。

## 复现

从仓库根目录运行全部模块检查：

```sh
go build ./... ./transport/memory/... ./transport/nats/... ./transport/kafka/... ./examples/...
go vet ./... ./transport/memory/... ./transport/nats/... ./transport/kafka/... ./examples/...
go test -race ./... ./transport/memory/... ./transport/nats/... ./transport/kafka/... ./examples/...
```

无需 race 时可用 CGO_ENABLED=0；race 需要 CGO。生成器测试验证确定性、官方生成夹具以及参数重名编译；修改 IDL 或模板时按 [依赖与工具](DEPENDENCIES.md) 重新生成样例。

## 完整 Broker 回归

使用专用 Broker；Kafka 测试需要创建/删除独立前缀 topic 的权限，NATS 故障用例需要可执行的 nats-server：

```sh
KAFKA_BROKERS=127.0.0.1:9092   go test -tags=integration -run '^TestKafka' -count=1 -timeout=600s ./examples/...
NATS_URL=nats://127.0.0.1:4222 NATS_SERVER="$(command -v nats-server)"   go test -tags=integration -run '^TestNATS' -count=1 -timeout=300s ./examples/...
```

取消合约在新 Kafka Broker 首轮可能遇到 producer ID 协调器尚未就绪。已有基线/候选都记录过该失败，第二轮通过不抹去首次失败；不要扩大超时、删断言或跳过用例来宣称全量通过。测试结果须标明 Broker 状态及实际版本。

## 网关路由回归

生产协议选择由 `ReplyRouterOptions.ForwardProtocol` 显式设置，不读取压测环境变量。集成测试自身使用 REPLY_MODE 选择该字段，可分别验证 HTTP 与 Thrift 的 batch 0/32、扩容和实例重启：

```sh
KAFKA_BROKERS=127.0.0.1:9092 REPLY_MODE=http   go test -tags=integration -run '^TestKafkaRouterRoutingExpansionAndRestart$' -count=1 ./examples
KAFKA_BROKERS=127.0.0.1:9092 REPLY_MODE=thrift   go test -tags=integration -run '^TestKafkaRouterRoutingExpansionAndRestart$' -count=1 ./examples
```

## 资源限额与联合并发扫描

当前已测曲线和全部参数在 [并发资料](benchmarks/kafka-gateway-2026-11-09/README.md)，容量解释在 [容量规划](KAFKA-CAPACITY.md)。下面命令本身不会创建 Broker 或设置 CPU/内存配额，应在相应容器限额下运行：

```sh
KAFKA_BROKERS=kafka:9092 GOMAXPROCS=4 REPLY_MODE=thrift THRIFT_BATCH_SIZE=32   go test -tags=integration ./examples -run '^TestKafkaLoadCurve$' -count=1 -timeout=10m -args   -kafka-load-concurrency=256,512,768,1024,1280,1536,2048,3072   -kafka-load-duration=30s -kafka-load-trials=2 -kafka-load-mixed   -kafka-publish-workers=128 -kafka-server-workers=128 -kafka-reply-workers=128   -kafka-load-window=1024 -kafka-core-concurrency=128 -kafka-publish-buffers=1024   -kafka-max-in-flight=2048 -kafka-forward-batches=2 -kafka-load-resources
```

此命令连续复用进程；归档曲线的各点使用独立进程与新 Broker，不能宣称两种运行顺序完全相同。固定默认 GC，不额外设置 GOGC/GOMEMLIMIT。REPLY_MODE=direct 选择独立直读对照；其他值走 HTTP 网关，thrift 走 Thrift 网关。

每个并发 worker 循环发起调用，总在途调用由三个客户端分担。混合负载逐条验证业务 ID、服务来源及 Count 回显标记，统计 `completed_methods`、失败原因和 p50/p95/p99；结束后检查 Pending 为零。`-kafka-load-resources` 只采集 cgroup 指标，不是宿主机压力保护。

## 多服务持续负载

在相同拓扑、资源和固定并发下增大 `-kafka-load-duration`，或设置多轮并保留每轮结果。例如 30×20 秒复用同一组客户端和服务，可测约十分钟持续负载。不得把 30 秒短测改名为长稳，或把旧版本的持续负载数字当作新版本实测。

早期十分钟负载及其绘图脚本见 [历史归档](history/README.md)。当前曲线的重绘入口随数据放在基准目录中。

## 分段耗时诊断

`-kafka-load-diagnostics` 启用测试专用的 SDK hooks 和 handler 包装，记录 `KAFKA_DIAGNOSTICS`。`produce_success` 覆盖入 SDK 至发布回调；`broker_api_*_wire` 是网络往返，`*_write_wait` 是 SDK 写入排队；`fetch_buffer_to_handler` 是 fetch 缓冲至核心 handler；`core_handler` 不含适配器回复发布。采样有锁及解码成本，应使用无采样对照，不能简单相减不同分母的平均值。

`COURIER_PROFILE=1` 在负载测量窗口输出 CPU/分配 profile，容器内 `/bench` 需为可写目录；此开关仅在测试中使用。原始诊断数据及 v1/v2 采样边界见 [并发资料](benchmarks/kafka-gateway-2026-11-09/README.md#分段耗时)。

## 扩缩容、背压与进程故障

`examples/high_availability_test.go` 覆盖 Kafka/NATS 服务动态扩缩容、应用进程强杀和有界背压恢复；子进程通过握手报告就绪。另有 Kafka 回复发布失败重投、offset 顺序/重连、metadata 恢复，以及 NATS 断线重连、Drain 和关闭期限测试。

Kafka 失败重投可能重复业务副作用，去重关联不是事务或恰好执行一次。正常缩容先完成 Server.Close，再关闭 transport/SDK。故障测试不等于多 Broker 跨可用区或长期稳定性验证。

## 发布失败诊断

`failure_reasons` 保留失败类型/消息类别，成功调用 p99 不包含失败等待。负载测试采样发布错误和慢 Produce/OffsetCommit 请求，输出 UTC 时间及 SDK 等待/收发耗时；它不改变生产确认，也不能替代完整失败计数。遗留测试资源只按本次唯一名称和归属清理。
