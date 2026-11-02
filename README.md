# Courier MQ

**基于 Apache Thrift 的 Go RPC 框架，支持通过 NATS、Kafka 进行服务调用。**

Courier 根据 Thrift 接口生成强类型客户端，通过消息系统传递请求和回复，并提供超时控制与并发限制；业务代码负责接口定义和方法实现，框架负责消息订阅与请求关联，另提供进程内传输用于本地开发和测试。

## 适用场景

- **适合**：Go 服务之间需要通过 NATS 或 Kafka 完成请求—回复调用，并希望使用生成的强类型客户端；也可用内存传输做本地开发和测试。
- **调用方式**：直接调用 `client.Get(ctx, ...)` 等待结果；需要与其他工作并行时，可使用异步方法。
- **环境要求**：Go 1.26+；接入业务接口需安装 Thrift 0.25.0 编译器；跨进程运行需自行准备和配置 NATS 或 Kafka。
- **关键限制**：只支持一次请求对应一个最终结果，不支持流式 RPC，也不提供持久化任务状态或跨客户端重启的结果恢复；不保证业务恰好执行一次，有副作用的方法需要实现幂等。

如果只需要直接网络 RPC、无需经过消息系统，Courier 会增加 Broker 的部署和运维成本；它也不替代业务事件订阅或持久化任务队列。

## 工作原理

```mermaid
flowchart LR
    C[Go 客户端] -->|方法调用| CC[Courier]
    CC -->|请求| MQ[NATS / Kafka / 内存]
    MQ -->|请求| CS[Courier 服务端]
    CS --> H[业务方法]
    H -->|返回值或异常| CS
    CS -->|回复| MQ
    MQ -->|回复| CC
    CC -->|调用结果| C
```

Thrift 定义接口并编解码参数和结果，Courier 用请求标识匹配回复；业务方法无需感知底层消息路由，传输配置在应用启动时完成，详见 [架构与传输语义](docs/ARCHITECTURE.md)。

## 快速开始

准备 Go **1.26 或以上**、Git 和本仓库读取权限，执行：

```sh
git clone https://github.com/nextpkg/courier.git
cd courier
CGO_ENABLED=0 go run ./examples/cmd/memory-demo
```

预期输出：

```text
result: id=order-42 source=Orders values=[1 2 3]
```

示例在同一进程中启动 Orders 服务并调用 `Get`，完成后自动关闭资源并退出；无需安装 Broker 或 Thrift 编译器，生成代码已包含在仓库中。

其中的核心调用如下（省略错误处理和资源关闭，完整代码见 [memory-demo](examples/cmd/memory-demo/main.go)）：

```go
server, err := demo.ListenOrders(ctx, bus, &handlers.Orders{})
client, conn, err := demo.DialOrders(ctx, bus)
result, err := client.Get(ctx, "order-42", 0)
```

内存传输只用于同一进程；需要独立服务端和客户端时，按 [NATS / Kafka 示例](docs/EXAMPLES.md) 运行。

## 业务接入

**定义 `.thrift` → 生成 Go 代码 → 实现 handler → `Listen` 启动服务 → `Dial` 后调用业务方法。**

服务端与客户端共享生成的接口代码，客户端不需要引入 handler；完整依赖安装、两条生成命令和可运行 `main.go` 见 [从零创建 Hello 服务](docs/GETTING-STARTED.md)。

## 技术手册

手册对应当前检出版本；接入教程固定使用已发布的 `v0.1.0`，仅主分支提供的功能在对应手册中单独标明。

| 主题 | 文档 |
| --- | --- |
| 业务接口接入 | [接入教程](docs/GETTING-STARTED.md) |
| 跨进程示例部署 | [NATS / Kafka 示例](docs/EXAMPLES.md) |
| 原理、调用语义、超时及传输差异 | [架构与传输语义](docs/ARCHITECTURE.md) |
| 依赖版本、模块结构与代码生成 | [依赖与生成工具](docs/DEPENDENCIES.md) |
| Kafka topic、消费组与故障处理 | [Kafka 路由与资源配置](docs/KAFKA.md) |
| Kafka 的容量与资源开销 | [容量规划](docs/KAFKA-CAPACITY.md) |
| 测试方法、实测性能曲线与复现 | [测试与性能](docs/TESTING.md) |
