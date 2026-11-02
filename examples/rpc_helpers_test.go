package examples_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/nextpkg/courier"
	"github.com/nextpkg/courier/examples/gen/demo"
	"github.com/nextpkg/courier/examples/handlers"
	"github.com/nextpkg/courier/transport/memory"
)

func TestGeneratedRPCEntryPoints(t *testing.T) {
	bus, err := memory.New(memory.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer closeTestResource(t, bus)
	ctx := context.Background()
	server, err := demo.ListenOrders(ctx, bus, &handlers.Orders{})
	if err != nil {
		t.Fatal(err)
	}
	defer closeTestResource(t, server)
	api, conn, err := demo.DialOrders(ctx, bus, courier.ClientOptions{DefaultTimeout: 100 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	defer closeTestResource(t, conn)
	result, err := api.Get(ctx, "ordinary-rpc", 0)
	if err != nil || result == nil || result.ID != "ordinary-rpc" {
		t.Fatalf("sync result: %v, %v", result, err)
	}
	if err := api.Ping(ctx); err != nil {
		t.Fatal(err)
	}
	var rejected *demo.Rejected
	if _, err := api.Get(ctx, "reject", 0); !errors.As(err, &rejected) {
		t.Fatalf("business exception: %v", err)
	}
	future, err := api.GetAsync(ctx, "async-rpc", 0)
	if err != nil {
		t.Fatal(err)
	}
	result, err = future.Await(ctx)
	if err != nil || result.ID != "async-rpc" {
		t.Fatalf("async result: %v, %v", result, err)
	}
	if _, err = api.Get(ctx, "timeout", 500); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("configured deadline: %v", err)
	}
	if conn.Pending() != 0 {
		t.Fatal("pending calls remain")
	}
	closeTestResource(t, conn)
	if _, err = api.Get(ctx, "closed", 0); !errors.Is(err, courier.ErrClosed) {
		t.Fatalf("closed connection: %v", err)
	}
	// Closing the dedicated connection must not close the caller-owned bus.
	next, nextConn, err := demo.DialOrders(ctx, bus)
	if err != nil {
		t.Fatal(err)
	}
	defer closeTestResource(t, nextConn)
	if _, err = next.Get(ctx, "reconnected", 0); err != nil {
		t.Fatal(err)
	}
	closeTestResource(t, nextConn)
	closeTestResource(t, server)
	if stats := bus.Stats(); stats.Services != 0 || stats.Inboxes != 0 {
		t.Fatalf("resource leak: %+v", stats)
	}
}

func TestGeneratedRPCEntryPointFailures(t *testing.T) {
	bus, err := memory.New(memory.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer closeTestResource(t, bus)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if api, conn, err := demo.DialOrders(ctx, bus); err == nil || api != nil || conn != nil {
		t.Fatal("canceled dial succeeded")
	}
	if server, err := demo.ListenOrders(ctx, bus, &handlers.Orders{}); err == nil || server != nil {
		t.Fatal("canceled listen succeeded")
	}
	if _, _, err := demo.DialOrders(context.Background(), bus, courier.ClientOptions{}, courier.ClientOptions{}); err == nil {
		t.Fatal("multiple client options accepted")
	}
	if _, err := demo.ListenOrders(context.Background(), bus, &handlers.Orders{}, courier.ServerOptions{}, courier.ServerOptions{}); err == nil {
		t.Fatal("multiple server options accepted")
	}
	if _, _, err := demo.DialOrders(context.Background(), bus, courier.ClientOptions{MaxInFlight: -1}); err == nil {
		t.Fatal("invalid client options accepted")
	}
	if _, err := demo.ListenOrders(context.Background(), bus, &handlers.Orders{}, courier.ServerOptions{MaxConcurrent: -1}); err == nil {
		t.Fatal("invalid server options accepted")
	}
	if stats := bus.Stats(); stats.Services != 0 || stats.Inboxes != 0 {
		t.Fatalf("failed startup leaked resources: %+v", stats)
	}
	closeTestResource(t, bus)
	if _, err := demo.ListenOrders(context.Background(), bus, &handlers.Orders{}); err == nil {
		t.Fatal("closed transport listen succeeded")
	}
	if _, _, err := demo.DialOrders(context.Background(), bus); err == nil {
		t.Fatal("closed transport dial succeeded")
	}
}
