package examples_test

import (
	"context"
	"github.com/apache/thrift/lib/go/thrift"
	"github.com/nextpkg/courier/examples/gen/demo"
	"testing"
)

type contextOrders struct {
	demo.Orders
	seen context.Context
}

func (h *contextOrders) Get(ctx context.Context, id string, delay int32) (*demo.Item, error) {
	h.seen = ctx
	return &demo.Item{ID: id}, nil
}

func TestCourierProcessorPreservesContextAndThriftSetting(t *testing.T) {
	interval := thrift.ServerConnectivityCheckInterval
	if interval <= 0 {
		t.Fatal("test requires default native connectivity checking")
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	input, output := thrift.NewTMemoryBuffer(), thrift.NewTMemoryBuffer()
	ip, op := thrift.NewTBinaryProtocolTransport(input), thrift.NewTBinaryProtocolTransport(output)
	args := demo.OrdersGetArgs{ID: "owned-context"}
	if err := ip.WriteMessageBegin(ctx, "Get", thrift.CALL, 7); err != nil {
		t.Fatal(err)
	}
	if err := args.Write(ctx, ip); err != nil {
		t.Fatal(err)
	}
	if err := ip.WriteMessageEnd(ctx); err != nil {
		t.Fatal(err)
	}
	h := &contextOrders{}
	if ok, err := demo.NewOrdersCourierProcessor(h).Process(ctx, ip, op); err != nil || !ok {
		t.Fatalf("process: %v %v", ok, err)
	}
	if h.seen != ctx {
		t.Fatal("processor wrapped the request context for connectivity polling")
	}
	name, kind, seq, err := op.ReadMessageBegin(ctx)
	if err != nil || name != "Get" || kind != thrift.REPLY || seq != 7 {
		t.Fatalf("reply header: %s %d %d %v", name, kind, seq, err)
	}
	result := demo.OrdersGetResult{}
	if err := result.Read(ctx, op); err != nil {
		t.Fatal(err)
	}
	if result.Success == nil || result.Success.ID != "owned-context" {
		t.Fatalf("reply: %+v", result)
	}
	if thrift.ServerConnectivityCheckInterval != interval {
		t.Fatal("changed process-wide Thrift setting")
	}
	cancel()
	if h.seen.Err() != context.Canceled {
		t.Fatal("handler lost caller cancellation")
	}
}
