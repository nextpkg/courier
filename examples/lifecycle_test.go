package examples_test

import (
	"context"
	"github.com/nextpkg/courier"
	"github.com/nextpkg/courier/examples/gen/demo"
	"github.com/nextpkg/courier/examples/handlers"
	"github.com/nextpkg/courier/internal/testcheck"
	"github.com/nextpkg/courier/transport/memory"
	"github.com/stretchr/testify/require"
	"runtime"
	"testing"
	"time"
)

func TestMemoryRepeatedLifecycle(t *testing.T) {
	baseline := runtime.NumGoroutine()
	for i := 0; i < 30; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		b, err := memory.New(memory.Options{})
		require.NoError(t, err)

		s, _ := courier.NewServer(b, courier.ServerOptions{})
		_ = demo.RegisterOrders(s, &handlers.Orders{})
		if err = s.Start(ctx); err != nil {
			t.Fatal(err)
		}
		c, err := courier.NewClient(ctx, b, courier.ClientOptions{})
		require.NoError(t, err)

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
	testcheck.AssertGoroutinesReturn(t, baseline)
}

// testcheck.Close uses a fresh deadline even when the call context has expired.
