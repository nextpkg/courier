package testutil

import (
	"context"
	"github.com/nextpkg/courier"
	"github.com/nextpkg/courier/examples/gen/demo"
	"github.com/nextpkg/courier/examples/handlers"
	"runtime"
	"sync"
	"testing"
	"time"
)

func AssertGoroutinesReturn(t *testing.T, baseline int) {
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

func Close(t *testing.T, resource interface{ Close(context.Context) error }) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := resource.Close(ctx); err != nil {
		t.Errorf("resource cleanup: %v", err)
	}
}
func WaitFor(t *testing.T, d time.Duration, condition func() bool, message string) {
	t.Helper()
	end := time.Now().Add(d)
	for {
		if condition() {
			return
		}
		if time.Now().After(end) {
			t.Fatal(message)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func Client(t *testing.T, tr courier.Transport) *courier.Client {
	t.Helper()
	c, err := courier.NewClient(context.Background(), tr, courier.ClientOptions{MaxInFlight: 512, DefaultTimeout: 8 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := c.Close(ctx); err != nil {
			t.Error(err)
		}
	})
	return c
}

func Server(t *testing.T, tr courier.Transport, h demo.Orders) *courier.Server {
	t.Helper()
	s, err := courier.NewServer(tr, courier.ServerOptions{MaxConcurrent: 128})
	if err != nil {
		t.Fatal(err)
	}
	if err = demo.RegisterOrders(s, h); err != nil {
		t.Fatal(err)
	}
	if err = s.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := s.Close(ctx); err != nil {
			t.Error(err)
		}
	})
	return s
}

type HeldOrders struct {
	*handlers.Orders
	Started chan struct{}
	Release chan struct{}
	once    sync.Once
}

func (h *HeldOrders) Get(ctx context.Context, id string, ms int32) (*demo.Item, error) {
	if id == "first" {
		h.once.Do(func() { close(h.Started) })
		select {
		case <-h.Release:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	return h.Orders.Get(ctx, id, ms)
}
