# Kafka 网关并发与分段测量

测量日期：2026-11-09。测量来自整理前的源码快照；整理后的代码尚未重新测量吞吐。测量版本由 `source-sha256.json` 和 `binary-sha256.json` 标识；只有文件指纹匹配的源码提交才能复用这组结果。

## 条件与负载

Linux ARM64、Go 1.27.3、默认 GC；应用 4 CPU / 1 GiB、GOMAXPROCS=4；Kafka 4.4.0，2 CPU / 3 GiB，JVM Xms256m/Xmx512m。三个客户端、两个服务各两个副本、四种混合调用；发布/服务/回复 worker 128，completion window 1024，转发批大小 32，每目标最多两批在飞。客户端、服务和网关共用应用容器，转发使用 loopback Thrift framed TCP 和 v1 二进制格式。

并发指三个客户端的**总在途调用数**，不是每客户端的 MaxInFlight。每档有两次有效的 30 秒测量，按正序和逆序执行；每轮新建 Broker，独立进程预热 5 秒，测试自身也包含初始化和预热。p99 是轮次百分位均值，不是合并请求后的百分位。

## 并发曲线

![吞吐、延迟、CPU 效率与边际收益](concurrency.png)

拐点区间约为 1024–1280。1280 下为 132763 RPC/s、p99 21.2ms，达到实测峰值的 92.93%；1024 下为 125043 RPC/s、p99 18.1ms。1280→1536，吞吐增加 2.08%、p99 增加 16.98%；1280→3072，吞吐增加 7.61%、p99 增加 108.73%。

图中展示成功 RPC/s、p50/p99、每应用 CPU 核的 RPC/s，以及相邻档边际收益 `(X2-X1)/(C2-C1)`。归一化拐点评分为 `(X-Xmin)/(Xmax-Xmin) - (C-Cmin)/(Cmax-Cmin)`；1024/1280 的评分接近，约 0.496/0.506，因此报告区间。最小达到 90% 峰值的并发仅作为另一项选择标尺，不是业务延迟 SLO，也不能直接推广到其他部署。

`rounds.json` 保留全部数值结果及 `included` 标记。两次外部 CPU 竞争明显的测量被排除，并各补一轮；768 两轮波动较大，但未仅因跑分偏低而剔除，图中阴影展示范围。所有有效曲线轮次零失败。

## 分段耗时

![1280 与 1536 的分段等待和执行时间](stages.png)

1280 的细分采样中，请求处理均值 2.411ms，其中回复发布至 SDK 完成约 2.334ms，业务 handler 约 0.0057ms。请求/回复 worker 排队约 1.130/1.040ms，远程转发排队约 0.237ms、批发送往返约 0.471ms，目标交付约 0.0022ms、连接获取约 0.00019ms。

这些值是墙钟时间，不是 CPU 时间；阶段存在嵌套、分母也不同，不能求和作为端到端延迟。队列计时从 PollRecords 返回后、写入 worker channel 前开始，不包含 SDK fetch 缓冲等待。发布计时包含 SDK 缓冲、确认和调度，未进一步拆开。commit 按每次操作统计，目标批交付按每批统计，转发发送按被采样记录加权。

诊断每 256 个事件采样一次，直方图桶宽 50µs，末桶为 ≥12.8ms；分位数是桶上界。v1 对照 1280/1536；v2 增加业务调用与回复发布计时，两者均是诊断版本，不是优化版本。v1 的一轮高外部负载数据保留但不参与阶段汇总；低干扰 v2 开关采样对照为 132451/133591 RPC/s，差 −0.85%，一对结果不代表统计显著性。

## 数据文件与复现

- `concurrency.csv/json`：吞吐、延迟、CPU 效率与边际收益汇总。
- `rounds.json`：原始数值及有效性标记。
- `stages-*.csv`：结论所用的分段汇总；v1/v2 原始观测、完整直方图及排除记录在 [diagnostic-evidence.tar.gz](diagnostic-evidence.tar.gz)，包内 MANIFEST.json 提供逐文件校验。
- `environment.json`、`source-sha256.json`、`binary-sha256.json`：环境与版本指纹。
- `summarize.py`：使用 Python 3 从原始数据重新生成汇总。
- `plot.py`：使用 Python 3 和 matplotlib 生成 PNG/SVG/PDF。

在本目录重新生成数据展示：

```sh
python3 summarize.py
python3 plot.py
```

重跑负载需使用与指纹匹配的源码，以 `go test -c -tags=integration` 将 `./examples` 构建为 Linux ARM64 测试二进制，按上述容器资源运行。环境变量为 `REPLY_MODE=thrift`、`THRIFT_BATCH_SIZE=32`、`COURIER_BENCH_LINGER=0ms`、`COURIER_PROFILE=0`；KAFKA_BROKERS 指向 Broker，GOMAXPROCS=4，不覆盖 GOGC/GOMEMLIMIT。测试参数：

```text
-test.run=^TestKafkaLoadCurve$ -test.timeout=600s -test.v
-kafka-load-concurrency=<level> -kafka-load-duration=30s -kafka-load-trials=1
-kafka-load-mixed=true -kafka-publish-workers=128 -kafka-server-workers=128
-kafka-load-window=1024 -kafka-core-concurrency=128 -kafka-publish-buffers=1024
-kafka-max-in-flight=2048 -kafka-load-resources=true
-kafka-reply-workers=128 -kafka-forward-batches=2
```

每次重跑记录新的版本和环境，重新评估轮次有效性，不能直接沿用本数据的排除标记。这组结果是短时闭环负载，不代表持续负载验证。

相同基线上的实现对照另见 [历史实验归档](../../history/README.md)。
