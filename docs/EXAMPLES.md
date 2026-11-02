# 运行跨进程示例

需要 Go 1.26 或以上和 Git；先克隆仓库，以下命令均从仓库根目录执行：

```sh
git clone https://github.com/nextpkg/courier.git
cd courier
```

以下示例复用相同的 IDL 和 handler；先准备对应 Broker，再分别启动服务端和客户端。客户端调用 `Orders.Get` 和 `Inventory.Count`，所以两个服务都必须启动；所有命令应使用同一检出版本。

两个后端的客户端预期输出相同：

```text
inventory count: 7
order: id=order-42 source=Orders
```

## NATS

需要可访问的 NATS Broker。已安装 `nats-server` 时，在一个终端启动：

```sh
nats-server -a 127.0.0.1 -p 4222
```

另开三个终端，均进入仓库根目录，按顺序各运行一条命令：

```sh
CGO_ENABLED=0 go run ./examples/cmd/nats-demo -mode server -service orders -url nats://127.0.0.1:4222
CGO_ENABLED=0 go run ./examples/cmd/nats-demo -mode server -service inventory -url nats://127.0.0.1:4222
CGO_ENABLED=0 go run ./examples/cmd/nats-demo -mode client -url nats://127.0.0.1:4222
```

NATS 适配器接收应用创建的 `*nats.Conn`，原生 SDK 的地址、认证、TLS、重连选项由应用配置。示例使用 `nats.ReconnectBufSize(-1)` 禁用断线期间的发布缓冲；`0` 在当前 SDK 中表示使用默认缓冲，不能用于禁用。

两个服务端终端出现 `server ready` 后再运行客户端；客户端完成后退出，服务端和本地 NATS Broker 用 `Ctrl+C` 停止。

## Kafka

需要已运行且可在 `127.0.0.1:9092` 连接的 Kafka Broker，以及创建 topic 的权限；以下命令不会安装或启动 Broker。先执行 provision，成功后在三个终端分别启动两个服务端和客户端：

```sh
# 创建示例所需的请求和回复 topic，副本因子为 1。
CGO_ENABLED=0 go run ./examples/cmd/kafka-demo -mode provision -brokers 127.0.0.1:9092
CGO_ENABLED=0 go run ./examples/cmd/kafka-demo -mode server -service orders -brokers 127.0.0.1:9092
CGO_ENABLED=0 go run ./examples/cmd/kafka-demo -mode server -service inventory -brokers 127.0.0.1:9092
CGO_ENABLED=0 go run ./examples/cmd/kafka-demo -mode client -brokers 127.0.0.1:9092
```

两个服务端终端出现 `server ready` 后再运行客户端；客户端完成后退出，服务端用 `Ctrl+C` 停止，Broker 由其运维方式管理。

示例默认前缀为 `courier.demo`，provision 后留下 `courier.demo.request.Orders`、`courier.demo.request.Inventory` 和 `courier.demo.replies` 三个 topic；停止示例不会删除它们。需要隔离另一组演示时，为四条命令传入同一个新的 `-prefix` 值；清理时仅删除本组演示创建且已无使用者的 topic。

同一服务的全部方法和副本共享请求 topic；增加方法、客户端、调用不会创建 topic。业务 IDL、handler 和生成客户端与其他后端相同。需要共同部署的服务可配置显式映射；完整说明见 [Kafka 拓扑、预算与故障语义](KAFKA.md)。内存示例把 server/client 放在同一进程，因为该后端没有跨进程传输能力。

## 常见启动问题

| 现象 | 检查与处理 |
| --- | --- |
| 无法连接 Broker | 确认地址、端口与认证配置；Kafka 的 advertised.listeners 必须能被客户端访问 |
| Kafka 提示 topic 不存在 | 在同一 Broker、同一 `-prefix` 下先执行 provision，并确认具备建 topic 权限 |
| 调用超时或没有响应者 | 确认两个服务均显示 `server ready`，客户端和服务端使用同一 Broker 与命名配置 |

返回 [README](../README.md)，或阅读 [业务接入教程](GETTING-STARTED.md)。
