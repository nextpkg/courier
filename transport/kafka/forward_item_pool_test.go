package kafka

import (
	"context"
	"github.com/nextpkg/courier/transport/kafka/internal/wire"
	"github.com/stretchr/testify/require"
	"sync"
	"testing"
)

func TestForwardItemPoolCancellationIsolation(t *testing.T) {
	pool := newForwardItemPool(1)
	ctx, cancel := context.WithCancel(context.Background())
	first := pool.acquire(ctx, wire.BatchMessage{Instance: "first", Expires: 1, Data: []byte("first data")})
	cancel()
	first.release()
	require.Empty(t, pool.idle, "queued item reused before sender finished")
	second := pool.acquire(context.Background(), wire.BatchMessage{Instance: "second", Expires: 2, Data: []byte("second data")})
	require.NotSame(t, first, second, "pending sender lost ownership")
	if string(first.message.Data) != "first data" || first.ctx.Err() == nil {
		t.Fatal("pending data or cancellation lost")
	}
	first.complete(204)
	select {
	case got := <-second.done:
		t.Fatalf("late result crossed items: %d", got)
	default:
	}
	if first.ctx != nil || first.message.Data != nil || first.message.Instance != "" {
		t.Fatal("released item retained input")
	}
	second.complete(409)
	if got := <-second.done; got != 409 {
		t.Fatal(got)
	}
	second.release()
	reused := pool.acquire(context.Background(), wire.BatchMessage{Instance: "third", Data: []byte("third data")})
	require.Same(t, first, reused, "expected first released item")
	select {
	case got := <-reused.done:
		t.Fatalf("stale completion %d", got)
	default:
	}
	reused.complete(204)
	<-reused.done
	reused.release()
}

func TestForwardItemPoolConcurrentCompletion(t *testing.T) {
	pool := newForwardItemPool(8)
	var workers, senders sync.WaitGroup
	for n := 0; n < 16; n++ {
		workers.Add(1)
		go func(n int) {
			defer workers.Done()
			for i := 0; i < 100; i++ {
				item := pool.acquire(context.Background(), wire.BatchMessage{Data: []byte{byte(n)}})
				senders.Add(1)
				if i%2 == 0 {
					item.release()
					go func(item *forwardItem) { defer senders.Done(); item.complete(410) }(item)
				} else {
					go func(item *forwardItem) { defer senders.Done(); item.complete(204) }(item)
					if got := <-item.done; got != 204 {
						t.Errorf("crossed status %d", got)
					}
					item.release()
				}
			}
		}(n)
	}
	workers.Wait()
	senders.Wait()
	if len(pool.idle) > 8 {
		t.Fatal("idle bound exceeded")
	}
}
