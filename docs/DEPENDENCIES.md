# 依赖与生成工具

项目要求 Go 1.26 或以上，依赖版本由各模块的 `go.mod` 和 `go.sum` 固定。

| 模块 | 直接外部依赖 | 版本 | 用途 |
| --- | --- | --- | --- |
| 核心 | `github.com/apache/thrift` | v0.25.0 | 二进制协议、生成类型、Processor 和异常 |
| `transport/memory` | `github.com/ThreeDotsLabs/watermill` | v1.5.3 | 进程内消息和 GoChannel |
| `transport/nats` | `github.com/nats-io/nats.go` | v1.55.0 | Core NATS 发布、订阅、Inbox 和重连 |
| `transport/kafka` | `github.com/twmb/franz-go` | v1.22.2 | Kafka producer、消费组、分区与 offset 控制 |
| `transport/kafka` | `github.com/twmb/franz-go/pkg/kmsg` | v1.15.0 | Kafka metadata 和 ListOffsets 请求 |

## 模块边界

核心运行库依赖标准库和 Thrift；代码生成扩展使用 Go 标准库。三个传输适配器分别引入自己的 SDK，应用可按所需后端选择依赖。示例、部署工具和集成测试位于 `examples` 模块。

内存适配器使用 Watermill 的消息类型和 GoChannel，由适配器管理服务竞争消费和并发上限。NATS 适配器使用原生 Core NATS 接口；Kafka 适配器使用 franz-go 的发布确认、手动提交和重平衡接口。Kafka 管理操作位于 `examples/brokeradmin`，运行时不创建 topic。

## 外部工具

- Apache Thrift **0.25.0** 编译器用于生成 Go 类型、服务接口和 Processor；`courier-gen` 校验生成文件中的版本标记，并补充异步客户端。修改编译器版本时需同时检查运行库兼容性和生成结果。
- `nats-server` 用于运行 NATS 示例和集成测试；故障测试通过 `NATS_SERVER` 指定可执行文件，启动独立测试进程。
- Kafka Broker 用于 Kafka 示例和集成测试；连接地址由 `KAFKA_BROKERS` 或示例的 `-brokers` 参数指定。

普通构建可使用 `CGO_ENABLED=0`，Go race 检查需要启用 CGO。

## 生成链路与版本约束

```text
业务 .thrift
  └─ thrift 0.25.0 → 官方 Go 类型、服务接口、Processor
       └─ courier-gen -in ... -out ... → Listen/Dial 入口、注册函数、异步客户端、同步便利方法
```

`courier-gen` 使用 Go 标准库 `go/parser` 和 `go/ast` 读取官方输出，不解析 IDL；它复用生成的参数构造和结果/异常处理逻辑。升级 Thrift 时需要同时验证编译器、运行库及生成器支持的代码结构，不能仅修改版本标记。

生成器将 AST 解析和结构校验与输出模板分离：`cmd/courier-gen/templates/` 中的 `file.go.tmpl`、`service.go.tmpl` 和 `method.go.tmpl` 分别定义文件结构、服务入口和同步／异步方法，使用标准库 `text/template` 渲染具名字段，再由 `go/format` 统一格式；模板通过 `go:embed` 编入生成器，运行时不读取外部模板文件。修改模板后运行 `go test ./cmd/courier-gen`，检查生成结果与已提交样例的一致性、确定性及参数重名场景的编译结果。

生成文件纳入版本控制时，修改 IDL 后应重新生成并审查差异，不手工维护生成文件。业务应用的完整操作见 [接入教程](GETTING-STARTED.md)，仓库示例的生成命令见下文。

## 技术资料

- [Apache Thrift Concepts](https://thrift.apache.org/docs/concepts.html)：Transport、Protocol 和 Processor 的职责。
- [Watermill GoChannel](https://watermill.io/pubsubs/gochannel/)：进程内发布订阅；Courier 在其上实现服务竞争消费。
- [Core NATS](https://docs.nats.io/learn/core-nats/)：非持久化发布订阅及请求—回复。
- [franz-go](https://github.com/twmb/franz-go)：Kafka 原生 Go 客户端。

资料用于说明技术概念，具体依赖版本以上表和各模块 `go.mod` 为准，框架实际保证见 [架构与传输语义](ARCHITECTURE.md)。

## 模块获取与仓库内开发

核心模块为 `github.com/nextpkg/courier`，适配器模块为 `github.com/nextpkg/courier/transport/{memory,nats,kafka}`；业务应用只获取需要的模块，使用 `go get <模块路径>@v0.1.0` 获取已发布版本，完整命令见 [接入教程](GETTING-STARTED.md)。

子模块声明真实的核心版本依赖；仓库内的相对 `replace` 和根目录 `go.work` 用于本地联调，不要求下游应用使用同样的目录布局。修改共享接口时需同步验证全部模块，发布子模块前确保其依赖版本已可从远端获取。私有仓库依赖需要 Git 读取权限及匹配的 `GOPRIVATE`，生产构建应使用已提交的 `go.mod/go.sum`，而不是每次更新至主分支。

## 生成命令

安装 Apache Thrift **0.25.0** 编译器并将 `thrift` 加入 PATH，运行库版本由 `go.mod` 固定；也可通过 `THRIFT` 环境变量指定编译器路径。

`internal/generate` 是仓库示例的批量生成脚本，固定处理 `demo.thrift` 和 `shapes.thrift`；以下命令不会自动发现或生成业务项目中的 IDL，在仓库根目录执行：

```sh
go generate ./...
```

也可单独生成 demo 服务：

```sh
thrift -out examples/gen --gen go:skip_remote,package_prefix=github.com/nextpkg/courier/examples/gen/ examples/idl/demo.thrift
go run ./cmd/courier-gen -in examples/gen/demo/demo.go -out examples/gen/demo/demo_courier.go
```

业务项目应执行[接入教程](GETTING-STARTED.md)中的两条生成命令：`thrift` 的 `package_prefix` 填业务生成包的模块前缀，`courier-gen` 的 `-in/-out` 填业务文件路径；`-runtime` 控制生成代码引入的 Courier 模块路径，默认 `github.com/nextpkg/courier`，它不能替代 Go 模块依赖配置。

生成器支持 unary（单次请求—回复）方法、void 返回、声明异常和业务容器类型；不支持 `oneway`、服务继承或产生 `Foo/FooAsync` 名称冲突的服务。生成后的 Go 服务不需要 Thrift 编译器或 CGO。
