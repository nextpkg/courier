# 从零创建 Hello 服务

这个教程创建一个独立 Go 应用，调用自定义的 `Hello(name)` 方法，并输出 `Hello, Courier`。它使用内存传输，服务端和客户端在同一进程中运行，无需外部消息服务。

需要 Go 1.26 或以上，以及 Apache Thrift **0.25.0** 编译器；运行仓库已有示例无需编译器，生成业务接口时需要。macOS 可通过 Homebrew 安装并核对版本：

```sh
brew install thrift
thrift -version
```

Homebrew 默认版本可能随时间变化；继续前必须确认输出为 `Thrift version 0.25.0`，不匹配时安装该版本编译器后再继续，不能仅修改生成文件的版本标记。其他平台同样安装 0.25.0 并将可执行文件加入 PATH。

本文使用 POSIX shell；Go 和 Git 依赖下载需网络连接，依赖获取方式见第 1 步。

## 代码与进程的分工

本教程按“接口 → 生成 → 实现 → 启动 → 调用”组织；生成代码提供接口与调用入口，业务实现和进程启动由应用负责。

| 文件或对象 | 负责什么 | 谁需要它 |
| --- | --- | --- |
| `idl/hello.thrift` | 定义 `Greeter.Hello` 的参数与返回值 | 接口维护者 |
| `gen/hello/` | 类型、接口、Processor、注册函数、客户端 | 服务端与客户端 |
| `handler.go` | 实现 `Hello` 的业务逻辑 | 服务端 |
| `courier.Server` | 接收请求并通过 Processor 调用 handler | 服务端启动程序 |
| `courier.Client` | 发送请求、接收回复、管理 Future | 客户端程序 |

本教程共用一个 `main.go` 和内存总线；它演示完整调用链，不要求实际业务把调用方与被调用方部署在一起。

## 1. 创建应用并获取依赖

仓库公开可读，无需 GitHub 登录或为本仓库设置 `GOPRIVATE`；无需先克隆 Courier，也无需在业务项目添加本地 `replace`。需要 Go、Git 和访问依赖源的网络；以下命令使用 POSIX shell，在准备存放业务项目的目录执行，`courier-hello` 必须是新目录：

```sh
mkdir courier-hello
cd courier-hello
export GOWORK=off
go mod init example.com/courier-hello
go get github.com/nextpkg/courier@v1.0.0 github.com/nextpkg/courier/transport/memory@v1.0.0
mkdir -p idl gen
```

本教程固定使用 `v1.0.0`，将 `go.mod`、`go.sum` 纳入业务项目版本控制以复现依赖。升级时通过 `go get <模块路径>@<目标版本>` 显式选择版本；需要验证主分支代码时可使用 `@main`，Go 会将解析后的版本记录在 `go.mod` 中。核心和各适配器可独立获取，例如 Kafka 适配器路径为 `github.com/nextpkg/courier/transport/kafka`。

`example.com/courier-hello` 是本教程的应用模块名，可替换为业务模块名，但必须同步修改下文的 `package_prefix` 和 `gen/hello` 导入路径；框架的 import 使用 `github.com/nextpkg/courier`。后续命令都在 `courier-hello` 中运行。

## 2. 定义接口

创建 `idl/hello.thrift`：

```sh
cat > idl/hello.thrift <<'IDL'
namespace go hello

service Greeter {
  string Hello(1: string name)
}
IDL
```

`Greeter` 是服务名，`Hello` 是方法名，参数与返回值都是字符串；IDL 声明接口，方法实现位于 handler 中。

## 3. 生成 Go 代码

在当前 `courier-hello` 目录依次执行两条命令；Courier 仓库的 `go generate ./...` 仅用于仓库自带示例，业务接口使用以下显式路径生成。

```sh
thrift -out gen --gen go:skip_remote,package_prefix=example.com/courier-hello/gen/ idl/hello.thrift
go run github.com/nextpkg/courier/cmd/courier-gen -in gen/hello/hello.go -out gen/hello/hello_courier.go
```

第一条命令生成 `gen/hello/hello.go` 等文件，其中包含 `Greeter` 接口与 Processor；第二条生成 `hello_courier.go`，补充 `ListenGreeter`、`DialGreeter`、注册函数及同步/异步客户端方法。生成文件无需手动编辑；修改 IDL 后重新运行这两条命令。

## 4. 实现业务方法

将以下完整代码保存为 `handler.go`：

```go
package main

import "context"

type greeter struct{}

func (*greeter) Hello(ctx context.Context, name string) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	return "Hello, " + name, nil
}
```

这个类型实现了生成的 `hello.Greeter` 接口；方法签名必须与生成接口一致，业务逻辑只放在 handler 中。

## 5. 注册服务并发起调用

将以下完整代码保存为 `main.go`：

```go
package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"example.com/courier-hello/gen/hello"
	"github.com/nextpkg/courier/transport/memory"
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	bus, err := memory.New(memory.Options{})
	if err != nil {
		return err
	}
	defer closeResource(bus)

	server, err := hello.ListenGreeter(ctx, bus, &greeter{})
	if err != nil {
		return err
	}
	defer closeResource(server)

	client, conn, err := hello.DialGreeter(ctx, bus)
	if err != nil {
		return err
	}
	defer closeResource(conn)

	result, err := client.Hello(ctx, "Courier")
	if err != nil {
		return err
	}
	fmt.Println(result)
	return nil
}

func closeResource(resource interface{ Close(context.Context) error }) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := resource.Close(ctx); err != nil {
		log.Printf("close: %v", err)
	}
}
```

`ListenGreeter` 完成创建 Server、注册 handler 和启动订阅，成功返回即表示可以接收请求；`DialGreeter` 返回类型化客户端及其专属连接，随后直接调用 `client.Hello(ctx, "Courier")` 取得结果。Dial 只准备客户端回复入口，不探测远端服务是否在线；方法调用仍需检查错误与超时。

内存模式下两端共用同一个 `bus`；应用负责关闭连接、服务端和传输，顺序由上面的 `defer` 保证。客户端方法与连接分开，业务接口即使有名为 `Close` 的方法，也不与连接关闭混淆。关闭使用独立超时，避免调用已经超时后无法清理。

普通调用不需要管理 Future；需要异步时，同一个 `client` 仍可调用 `HelloAsync` 后再 `Await`：

```go
future, err := client.HelloAsync(ctx, "Courier")
if err != nil {
    return err
}
// 在等待结果前执行其他工作。
result, err := future.Await(ctx)
if err != nil {
    return err
}
```

`ListenGreeter` 和 `DialGreeter` 可分别传入一个 `courier.ServerOptions`、`courier.ClientOptions` 来配置并发或超时；不传使用核心默认值。多服务共用一个 Server、多个服务客户端共用一个连接或使用自定义服务名时，继续使用 `NewServer/RegisterGreeterAs`、`NewClient/NewGreeterAsyncClientFor` 组合，避免每个服务重复创建连接。

## 6. 运行应用

```sh
go mod tidy
CGO_ENABLED=0 go run .
```

预期输出为：

```text
Hello, Courier
```

程序随即退出，服务端和客户端资源均由 `defer` 关闭。最终文件结构如下：

```text
courier-hello/              业务应用
  go.mod
  go.sum
  idl/hello.thrift          应用维护的接口定义
  gen/hello/               工具生成的 Go 代码
  handler.go               应用维护的业务逻辑
  main.go                  启动与调用代码
```

## 拆分为服务端和客户端

跨进程部署保留相同的 IDL 和生成代码，将启动职责拆开：

| 服务端程序 | 客户端程序 |
| --- | --- |
| 初始化 NATS 或 Kafka 适配器 | 初始化同一种适配器，使用匹配的 Broker 和路由配置 |
| `hello.ListenGreeter(ctx, transport, &greeter{})` | `hello.DialGreeter(ctx, transport)` |
| 成功后保持进程运行，等待退出信号 | 直接调用 `client.Hello(ctx, name)` 取得结果 |
| 收到退出信号后关闭 server 和适配器 | 调用完成后关闭 client 和适配器 |

同一服务的多个实例注册相同服务名和接口；调用方按服务和方法调用，由传输分配处理实例，不在业务方法调用中指定实例地址。

更换为 NATS 或 Kafka 时，保留 IDL、生成代码和 handler，按 [NATS / Kafka 跨进程示例](EXAMPLES.md) 替换传输初始化，并将 server 与 client 放入独立进程；Kafka 还需提前创建请求与回复 topic。
