//go:build integration

package examples_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/nextpkg/courier"
	"github.com/nextpkg/courier/examples/gen/demo"
	"github.com/nextpkg/courier/examples/handlers"
)

type drainOrders struct {
	*handlers.Orders
	release chan struct{}
	entered chan struct{}
}

func (h *drainOrders) Get(ctx context.Context, id string, delay int32) (*demo.Item, error) {
	select {
	case h.entered <- struct{}{}:
	default:
	}
	select {
	case <-h.release:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	return h.Orders.Get(ctx, id, delay)
}

func TestNATSDrainClose(t *testing.T) {
	for _, expire := range []bool{false, true} {
		t.Run(fmt.Sprintf("deadline=%t", expire), func(t *testing.T) {
			prefix := fmt.Sprintf("courier.drain.%d", time.Now().UnixNano())
			factory := natsFactory(natsURL(t), prefix)
			h := &drainOrders{&handlers.Orders{}, make(chan struct{}), make(chan struct{}, 40)}
			defer close(h.release)
			s := kafkaServer(t, factory(t), h)
			c := kafkaClient(t, factory(t))
			api := demo.NewOrdersAsyncClient(c)
			var calls []*courier.Future[*demo.Item]
			for i := 0; i < 40; i++ {
				f, err := api.GetAsync(context.Background(), fmt.Sprint(i), 0)
				if err != nil {
					t.Fatal(err)
				}
				calls = append(calls, f)
				if err := f.Sent(context.Background()); err != nil {
					t.Fatal(err)
				}
			}
			select {
			case <-h.entered:
			case <-time.After(time.Second):
				t.Fatal("handler not entered")
			}
			budget := 3 * time.Second
			if expire {
				budget = 100 * time.Millisecond
			}
			ctx, cancel := context.WithTimeout(context.Background(), budget)
			defer cancel()
			closed := make(chan error, 1)
			go func() { closed <- s.Close(ctx) }()
			if expire {
				if err := <-closed; !errors.Is(err, context.DeadlineExceeded) {
					t.Fatalf("close deadline=%v", err)
				}
				// Canceled handlers and discarded queued requests need not all receive replies.
				// Closing the client must terminate every remaining waiter.
				closeTestResource(t, c)
				if c.Pending() != 0 {
					t.Fatal("pending after bounded shutdown")
				}
				return
			}
			select {
			case err := <-closed:
				t.Fatalf("close returned before held handlers completed: %v", err)
			case <-time.After(50 * time.Millisecond):
			}
			for i := 0; i < 40; i++ {
				h.release <- struct{}{}
			}
			if err := <-closed; err != nil {
				t.Fatal(err)
			}
			for i, f := range calls {
				v, err := f.Await(context.Background())
				if err != nil || v == nil || v.ID != fmt.Sprint(i) {
					t.Fatalf("drained call %d: %v %v", i, v, err)
				}
			}
			if c.Pending() != 0 {
				t.Fatal("pending after graceful drain")
			}
		})
	}
}
