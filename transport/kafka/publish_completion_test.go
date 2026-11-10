package kafka

import (
	"errors"
	"github.com/stretchr/testify/require"
	"sync"
	"testing"
)

func TestPublishCompletionLateCallbackIsolation(t *testing.T) {
	pool := newPublishCompletionPool(1)
	first := pool.acquire()
	first.release() // The waiter timed out, but the callback still owns the slot.
	if len(pool.idle) != 0 {
		t.Fatal("cancelled slot recycled before callback")
	}
	second := pool.acquire()
	if first == second {
		t.Fatal("active slot reused")
	}
	late := errors.New("late first result")
	first.complete(nil, late)
	select {
	case err := <-second.ready:
		t.Fatalf("late result crossed requests: %v", err)
	default:
	}
	second.complete(nil, nil)
	require.NoError(t, <-second.ready)

	second.release()
	reused := pool.acquire()
	if reused != first {
		t.Fatal("expected the released first slot")
	}
	select {
	case err := <-reused.ready:
		t.Fatalf("stale result left in slot: %v", err)
	default:
	}
	want := errors.New("new result")
	reused.complete(nil, want)
	if got := <-reused.ready; got != want {
		t.Fatal(got)
	}
	reused.release()
}

func TestPublishCompletionCallbackBeforeWaiter(t *testing.T) {
	pool := newPublishCompletionPool(1)
	c := pool.acquire()
	c.complete(nil, nil)
	if len(pool.idle) != 0 {
		t.Fatal("callback recycled slot while waiter owns it")
	}
	require.NoError(t, <-c.ready)

	c.release()
	if len(pool.idle) != 1 {
		t.Fatal("completed slot not reusable")
	}
}

func TestPublishCompletionConcurrentCancellation(t *testing.T) {
	pool := newPublishCompletionPool(8)
	var workers, callbacks sync.WaitGroup
	for n := 0; n < 16; n++ {
		workers.Add(1)
		go func(n int) {
			defer workers.Done()
			for i := 0; i < 100; i++ {
				c := pool.acquire()
				callbacks.Add(1)
				if (i+n)%2 == 0 {
					c.release()
					go func(c *publishCompletion) { defer callbacks.Done(); c.complete(nil, errors.New("cancelled")) }(c)
				} else {
					go func(c *publishCompletion) { defer callbacks.Done(); c.complete(nil, nil) }(c)
					if err := <-c.ready; err != nil {
						t.Errorf("crossed result: %v", err)
					}
					c.release()
				}
			}
		}(n)
	}
	workers.Wait()
	callbacks.Wait()
	if len(pool.idle) > 8 {
		t.Fatal("idle cache exceeded bound")
	}
}

func BenchmarkPublishCompletion(b *testing.B) {
	b.Run("allocate", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			done := make(chan error, 1)
			done <- nil
			<-done
		}
	})
	b.Run("reuse", func(b *testing.B) {
		pool := newPublishCompletionPool(128)
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			c := pool.acquire()
			c.complete(nil, nil)
			<-c.ready
			c.release()
		}
	})
}
