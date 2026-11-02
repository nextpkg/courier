# Kafka 容量规划

## 共享回复的读取成本

每个 `courier.Client` 创建一个独立 session 和直接分区 reader；它读取所选回复 partition 的全部消息，再按 session 过滤。若 partition p 每秒收到 Bp 字节、被 Cp 个 session 读取，所有 reader 的总载荷读取量约为 `Σ(Bp × Cp)`。

定义有效回复载荷速率为 `ΣBp`，则读取放大为 `A = Σ(Bp × Cp) / ΣBp`，其中 `Bp` 的单位为字节/秒，`Cp` 为该分区的 reader 数，假设所有 reader 均跟上读取。只有在流量及客户端近似均匀、每个活跃分区都有读者时，才可近似为 `客户端数 / 活跃分区数`；不能直接除以配置分区总数，得到小于 1 的结果。

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
