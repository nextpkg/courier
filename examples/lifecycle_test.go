package examples_test

import (
	"context"
	"github.com/nextpkg/courier"
	"github.com/nextpkg/courier/examples/gen/demo"
	"github.com/nextpkg/courier/examples/handlers"
	"github.com/nextpkg/courier/transport/memory"
	"runtime"
	"testing"
	"time"
)

func assertGoroutinesReturn(t *testing.T, baseline int) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for {
		n := runtime.NumGoroutine()
		if n <= baseline+4 {
			return
		}
		if time.Now().After(deadline) {
			stack := make([]byte, 256<<10)
			nbytes := runtime.Stack(stack, true)
			t.Fatalf("goroutines did not return: baseline=%d now=%d\n%s", baseline, n, stack[:nbytes])
		}
		time.Sleep(10 * time.Millisecond)
	}
}
func TestMemoryRepeatedLifecycle(t *testing.T) {
	baseline := runtime.NumGoroutine()
	for i := 0; i < 30; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		b, err := memory.New(memory.Options{})
		if err != nil {
			t.Fatal(err)
		}
		s, _ := courier.NewServer(b, courier.ServerOptions{})
		_ = demo.RegisterOrders(s, &handlers.Orders{})
		if err = s.Start(ctx); err != nil {
			t.Fatal(err)
		}
		c, err := courier.NewClient(ctx, b, courier.ClientOptions{})
		if err != nil {
			t.Fatal(err)
		}
		if _, err = demo.NewOrdersAsyncClient(c).Get(ctx, "lifecycle", 0); err != nil {
			t.Fatal(err)
		}
		if err = c.Close(ctx); err != nil {
			t.Fatal(err)
		}
		if err = s.Close(ctx); err != nil {
			t.Fatal(err)
		}
		if err = b.Close(ctx); err != nil {
			t.Fatal(err)
		}
		cancel()
		if stats := b.Stats(); stats.Services != 0 || stats.Inboxes != 0 || stats.Queued != 0 {
			t.Fatalf("bus retained resources: %+v", stats)
		}
	}
	assertGoroutinesReturn(t, baseline)
}

// closeTestResource uses a fresh deadline even when the call context has expired.
func closeTestResource(t *testing.T, resource interface{ Close(context.Context) error }) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := resource.Close(ctx); err != nil {
		t.Errorf("resource cleanup: %v", err)
	}
}
