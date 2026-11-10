# 历史测量与未采用方案

这些资料用于追溯旧测量和实验结论，不作为当前实现或容量推荐。每个压缩包内都有逐文件 SHA-256 的 `MANIFEST.json`；内容保留原来的仓库相对路径。

| 归档 | 内容与适用边界 |
|---|---|
| [pre-gateway-measurements.tar.gz](pre-gateway-measurements.tar.gz) | 早期低 worker、1/2/4 核扫描、单方法/分进程对照、旧版本十分钟持续负载，以及当时的容量/测试文档和绘图脚本。约 900、94596、63944 RPC/s 分属不同版本和负载，不能代表当前网关性能。 |
| [rejected-decoupling.tar.gz](rejected-decoupling.tar.gz) | 2026-11-10 回复消费与转发等待解耦实验：候选 131186、基线 134860 RPC/s，候选下降 2.72%，未采用。含补丁、全部测量与正确性验证记录；补丁仅针对归档中声明的源码快照。 |

当前有效的并发曲线和阶段结论见 [容量规划](../KAFKA-CAPACITY.md)。用于这些结论的诊断原始记录另存于 [diagnostic-evidence.tar.gz](../benchmarks/kafka-gateway-2026-11-09/diagnostic-evidence.tar.gz)：v1 对照 1280/1536，v2 细分发布等待，不能混作同一实现版本的重复采样。

查阅历史材料时，在独立目录解压相应包，再按 `MANIFEST.json` 校验文件；不得将旧测量参数或旧接口说明直接作为当前使用说明。
