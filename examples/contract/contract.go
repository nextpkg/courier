// Package contract tests the shared RPC behavior of transport implementations.
package contract

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/nextpkg/courier"
	"github.com/nextpkg/courier/examples/gen/demo"
	"github.com/nextpkg/courier/examples/handlers"
	"github.com/stretchr/testify/require"
)

// Factory supplies independent endpoints sharing one broker namespace. Cleanup
// must release endpoint resources. The suite owns client/server subscriptions.
type Factory func(t *testing.T) courier.Transport

func closeContext() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), 3*time.Second)
}
func server(t *testing.T, f Factory, orders bool, r *handlers.Recorder, max int) *courier.Server {
	t.Helper()
	s, err := courier.NewServer(f(t), courier.ServerOptions{MaxConcurrent: max})
	require.NoError(t, err)

	if orders {
		err = demo.RegisterOrders(s, &handlers.Orders{Recorder: r})
	} else {
		err = demo.RegisterInventory(s, &handlers.Inventory{Recorder: r})
	}
	require.NoError(t, err)

	if err = s.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := closeContext()
		defer cancel()
		if err := s.Close(ctx); err != nil {
			t.Error(err)
		}
	})
	return s
}
func client(t *testing.T, f Factory, max int) *courier.Client {
	t.Helper()
	c, err := courier.NewClient(context.Background(), f(t), courier.ClientOptions{MaxInFlight: max, DefaultTimeout: 5 * time.Second})
	require.NoError(t, err)

	t.Cleanup(func() {
		ctx, cancel := closeContext()
		defer cancel()
		if err := c.Close(ctx); err != nil {
			t.Error(err)
		}
	})
	return c
}

// heldOrders keeps the first call active until the overload assertion finishes.
type heldOrders struct {
	*handlers.Orders
	started, release chan struct{}
}

func (h *heldOrders) Get(ctx context.Context, id string, delay int32) (*demo.Item, error) {
	if id == "hold" {
		close(h.started)
		select {
		case <-h.release:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	return h.Orders.Get(ctx, id, delay)
}

func Run(t *testing.T, f Factory) {
	t.Run("ServicesMethodsReplicasClients", func(t *testing.T) {
		r := &handlers.Recorder{}
		for i := 0; i < 2; i++ {
			server(t, f, true, r, 128)
			server(t, f, false, r, 128)
		}
		clients := []*courier.Client{client(t, f, 512), client(t, f, 512), client(t, f, 512)}
		var wg sync.WaitGroup
		for i := 0; i < 120; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				c := clients[i%len(clients)]
				id := fmt.Sprintf("request-%d", i)
				switch i % 4 {
				case 0:
					x, err := demo.NewOrdersAsyncClient(c).GetAsync(context.Background(), id, int32(i%9))
					if err != nil {
						t.Error(err)
						return
					}
					v, err := x.Await(context.Background())
					if err != nil || v == nil || v.ID != id || v.Source != "Orders" {
						t.Errorf("Get %s: %v %v", id, v, err)
					}
					if r.Count("Orders.Get:"+id) != 1 {
						t.Errorf("execution count %s", id)
					}
				case 1:
					x, err := demo.NewOrdersAsyncClient(c).List(context.Background(), []string{id, "second"})
					if err != nil || len(x) != 2 || x[0].ID != id || x[0].Source != "Orders" || len(x[0].Values) != 3 {
						t.Errorf("List %s: %v %v", id, x, err)
					}
				case 2:
					v, err := demo.NewInventoryAsyncClient(c).Count(context.Background(), id)
					if err != nil || v != int64(len(id)) {
						t.Errorf("Count %s: %v %v", id, v, err)
					}
					if r.Count("Inventory.Count:"+id) != 1 {
						t.Errorf("execution count %s", id)
					}
				case 3:
					v, err := demo.NewInventoryAsyncClient(c).Reserve(context.Background(), id)
					if err != nil || v == nil || v.ID != id || v.Source != "Inventory" {
						t.Errorf("Reserve %s: %v %v", id, v, err)
					}
					if r.Count("Inventory.Reserve:"+id) != 1 {
						t.Errorf("execution count %s", id)
					}
				}
			}(i)
		}
		wg.Wait()
		for _, c := range clients {
			if c.Pending() != 0 {
				t.Errorf("pending=%d", c.Pending())
			}
		}
	})
	t.Run("OutOfOrderAndIndependentWait", func(t *testing.T) {
		server(t, f, true, nil, 32)
		c := client(t, f, 64)
		api := demo.NewOrdersAsyncClient(c)
		slow, err := api.GetAsync(context.Background(), "slow", 250)
		if err != nil {
			t.Fatal(err)
		}
		fast, err := api.GetAsync(context.Background(), "fast", 0)
		if err != nil {
			t.Fatal(err)
		}
		if v, err := fast.Await(context.Background()); err != nil || v.ID != "fast" {
			t.Fatalf("fast=%v %v", v, err)
		}
		wait, cancel := context.WithCancel(context.Background())
		cancel()
		if _, err := slow.Await(wait); !errors.Is(err, context.Canceled) {
			t.Fatalf("wait=%v", err)
		}
		var wg sync.WaitGroup
		for i := 0; i < 8; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				v, err := slow.Await(context.Background())
				if err != nil || v.ID != "slow" {
					t.Errorf("future poisoned: %v %v", v, err)
				}
			}()
		}
		wg.Wait()
	})
	t.Run("BusinessErrorsAndPanic", func(t *testing.T) {
		server(t, f, true, nil, 32)
		c := client(t, f, 64)
		api := demo.NewOrdersAsyncClient(c)
		x, err := api.GetAsync(context.Background(), "reject", 0)
		if err != nil {
			t.Fatal(err)
		}
		_, err = x.Await(context.Background())
		var business *demo.Rejected
		if !errors.As(err, &business) || business.Reason != "business rejection" {
			t.Fatalf("exception=%v", err)
		}
		if err := x.Sent(context.Background()); err != nil {
			t.Fatalf("business error misreported as send failure: %v", err)
		}
		_, err = api.Get(context.Background(), "error", 0)
		if err == nil {
			t.Fatal("lost undeclared exception")
		}
		_, err = api.Get(context.Background(), "panic", 0)
		var remote *courier.RPCError
		if !errors.As(err, &remote) || remote.Code != "internal" {
			t.Fatalf("panic=%v", err)
		}
		_, err = api.Get(context.Background(), "missing", 0)
		if err == nil {
			t.Fatal("missing result accepted")
		}
	})
	t.Run("DeadlineCancellationAndLateReply", func(t *testing.T) {
		server(t, f, true, nil, 32)
		c := client(t, f, 64)
		api := demo.NewOrdersAsyncClient(c)
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
		defer cancel()
		x, err := api.GetAsync(ctx, "deadline", 200)
		if err != nil {
			t.Fatal(err)
		}
		_, err = x.Await(context.Background())
		if !errors.Is(err, context.DeadlineExceeded) {
			var remote *courier.RPCError
			if !errors.As(err, &remote) || remote.Code != "deadline" {
				t.Fatalf("deadline=%v", err)
			}
		}
		ctx2, cancel2 := context.WithCancel(context.Background())
		y, err := api.GetAsync(ctx2, "cancel", 100)
		if err != nil {
			t.Fatal(err)
		}
		if err = y.Sent(context.Background()); err != nil {
			t.Fatal(err)
		}
		cancel2()
		if _, err = y.Await(context.Background()); !errors.Is(err, context.Canceled) {
			t.Fatalf("cancel=%v", err)
		}
		v, err := api.Get(context.Background(), "next", 150)
		if err != nil || v.ID != "next" {
			t.Fatalf("late reply corrupted next: %v %v", v, err)
		}
		if c.Pending() != 0 {
			t.Fatal("pending leak")
		}
	})
	t.Run("ClientOverloadAndCloseIsolation", func(t *testing.T) {
		server(t, f, true, nil, 32)
		c := client(t, f, 1)
		other := client(t, f, 8)
		api := demo.NewOrdersAsyncClient(c)
		first, err := api.GetAsync(context.Background(), "hold", 200)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := api.GetAsync(context.Background(), "overload", 0); !errors.Is(err, courier.ErrOverloaded) {
			t.Fatalf("admission=%v", err)
		}
		ctx, cancel := closeContext()
		defer cancel()
		if err := c.Close(ctx); err != nil {
			t.Fatal(err)
		}
		if _, err := first.Await(ctx); !errors.Is(err, courier.ErrClosed) {
			t.Fatalf("close=%v", err)
		}
		if c.Pending() != 0 {
			t.Fatal("pending leak")
		}
		if _, err := api.GetAsync(ctx, "closed", 0); !errors.Is(err, courier.ErrClosed) {
			t.Fatalf("closed admission=%v", err)
		}
		if _, err := demo.NewOrdersAsyncClient(other).Get(ctx, "other-client", 0); err != nil {
			t.Fatalf("shared transport closed: %v", err)
		}
	})
	t.Run("ServerOverload", func(t *testing.T) {
		h := &heldOrders{Orders: &handlers.Orders{}, started: make(chan struct{}), release: make(chan struct{})}
		s, err := courier.NewServer(f(t), courier.ServerOptions{MaxConcurrent: 1})
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
			ctx, cancel := closeContext()
			defer cancel()
			if err := s.Close(ctx); err != nil {
				t.Error(err)
			}
		})
		var release sync.Once
		defer release.Do(func() { close(h.release) })
		c := client(t, f, 8)
		api := demo.NewOrdersAsyncClient(c)
		first, err := api.GetAsync(context.Background(), "hold", 0)
		if err != nil {
			t.Fatal(err)
		}
		select {
		case <-h.started:
		case <-time.After(3 * time.Second):
			t.Fatal("server not entered")
		}
		_, err = api.Get(context.Background(), "overload", 0)
		var remote *courier.RPCError
		if !errors.As(err, &remote) || remote.Code != "overloaded" {
			t.Fatalf("server admission=%v", err)
		}
		release.Do(func() { close(h.release) })
		if _, err := first.Await(context.Background()); err != nil {
			t.Fatal(err)
		}
	})
}
