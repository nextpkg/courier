package kafka

import (
	"context"
	"github.com/nextpkg/courier/transport/kafka/internal/wire"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

func TestParallelBatchOverlapAndCancellation(t *testing.T) {
	r, _, _ := routerFixture(t)
	r.opts.BatchSize = 2
	r.opts.BatchConcurrency = 2
	r.forwardItems.Store(newForwardItemPool(8))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	r.t.ctx = ctx
	firstStarted := make(chan struct{})
	secondStarted := make(chan int, 1)
	release := make(chan struct{})
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		raw, err := io.ReadAll(req.Body)
		if err != nil {
			return
		}
		messages, err := wire.DecodeBatch(raw, 1024)
		if err != nil {
			t.Error(err)
			return
		}
		if calls.Add(1) == 1 {
			close(firstStarted)
			select {
			case <-release:
			case <-ctx.Done():
				return
			}
		} else {
			secondStarted <- len(messages)
		}
		statuses := make([]int, len(messages))
		for i := range statuses {
			statuses[i] = 204
		}
		_, _ = w.Write(wire.EncodeStatuses(statuses))
	}))
	defer srv.Close()
	defer close(release)
	r.client = srv.Client()
	firstCtx, stopFirst := context.WithCancel(ctx)
	defer stopFirst()
	firstDone := make(chan int, 1)
	go func() { firstDone <- r.enqueueForward(firstCtx, srv.URL, "instance", 1, nil) }()
	select {
	case <-firstStarted:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	later := make(chan int, 2)
	for i := 0; i < 2; i++ {
		go func() { later <- r.enqueueForward(ctx, srv.URL, "instance", 1, nil) }()
	}
	select {
	case n := <-secondStarted:
		if n != 2 {
			t.Fatalf("overlapping batch was split: %d", n)
		}
	case <-ctx.Done():
		t.Fatal("second full batch waited for first")
	}
	for i := 0; i < 2; i++ {
		select {
		case status := <-later:
			if status != 204 {
				t.Fatal(status)
			}
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	}
	stopFirst()
	select {
	case status := <-firstDone:
		if status != 0 {
			t.Fatalf("cancelled waiter status %d", status)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	release <- struct{}{}
	waited := make(chan struct{})
	go func() { r.batchWG.Wait(); close(waited) }()
	select {
	case <-waited:
	case <-ctx.Done():
		t.Fatal("cancelled item prevented drain completion")
	}
	r.batchMu.Lock()
	defer r.batchMu.Unlock()
	if len(r.batches) != 0 || len(r.batchSignals) != 0 {
		t.Fatal("endpoint state leaked")
	}
}
