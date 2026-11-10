package kafka

import (
	"context"
	"github.com/nextpkg/courier"
	"github.com/nextpkg/courier/transport/kafka/internal/wire"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestBatchCompletionDoesNotWaitForLaterRequests(t *testing.T) {
	r, _, _ := routerFixture(t)
	r.opts.BatchSize = 32
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	first, second := make(chan struct{}), make(chan struct{})
	release := make(chan struct{})
	defer close(release)
	calls := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		calls++
		if calls == 1 {
			close(first)
			select {
			case <-release:
			case <-ctx.Done():
				return
			}
		} else {
			close(second)
			<-ctx.Done()
			return
		}
		w.Write(wire.EncodeStatuses([]int{204}))
	}))
	defer srv.Close()
	r.client = srv.Client()
	r.t.ctx = ctx
	done := make(chan int, 1)
	go func() { done <- r.enqueueForward(ctx, srv.URL, "instance", 1, nil) }()
	<-first
	go r.enqueueForward(ctx, srv.URL, "instance", 1, nil)
	// Release the first request after the second has entered the queue.
	for {
		r.batchMu.Lock()
		n := len(r.batches[srv.URL])
		r.batchMu.Unlock()
		if n > 0 {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatal("enqueue timed out")
		case <-time.After(time.Millisecond):
		}
	}
	release <- struct{}{}
	<-second
	select {
	case status := <-done:
		if status != 204 {
			t.Fatalf("status %d", status)
		}
	case <-time.After(100 * time.Millisecond):
		t.Error("completed reply waited for a later request")
	}
	cancel()
	r.batchWG.Wait()
}

func TestBatchPerItemResultsAndReplay(t *testing.T) {
	r, route, body := routerFixture(t)
	r.inbound = make(chan struct{}, 1)
	calls := 0
	in, err := r.openInbox(context.Background(), route.Session, func([]byte) { calls++ })
	if err != nil {
		t.Fatal(err)
	}
	defer in.Close(context.Background())
	srv := httptest.NewServer(http.HandlerFunc(r.serve))
	defer srv.Close()
	r.client = srv.Client()
	messages := []wire.BatchMessage{{Instance: route.Instance, Expires: route.Expires, Data: body}, {Instance: "old", Expires: route.Expires, Data: body}, {Instance: "", Expires: route.Expires, Data: body}}
	for i := 0; i < 2; i++ {
		statuses := r.sendBatch(srv.URL, messages)
		if len(statuses) != 3 || statuses[0] != 204 || statuses[1] != 409 || statuses[2] != 400 {
			t.Fatalf("statuses: %v", statuses)
		}
	}
	if calls != 1 || r.duplicate.Load() != 1 {
		t.Fatalf("calls=%d duplicates=%d", calls, r.duplicate.Load())
	}
	invalid, err := courier.DecodeEnvelope(body, courier.DefaultMaxFrameBytes)
	if err != nil {
		t.Fatal(err)
	}
	invalid.Reply.Topic = "wrong"
	bad, _ := courier.EncodeEnvelope(invalid, courier.DefaultMaxFrameBytes)
	statuses := r.sendBatch(srv.URL, []wire.BatchMessage{{Instance: route.Instance, Expires: route.Expires, Data: bad}})
	if statuses[0] != 400 || calls != 1 {
		t.Fatalf("invalid reply delivered: %v", statuses)
	}
}
