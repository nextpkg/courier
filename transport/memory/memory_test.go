package memory

import (
	"bytes"
	"context"
	"errors"
	"github.com/nextpkg/courier"
	"testing"
	"time"
)

func frame(t *testing.T, route courier.Route) []byte {
	t.Helper()
	b, err := courier.EncodeEnvelope(courier.Envelope{Version: 1, Kind: "request", Service: "service", Method: "method", ID: "request-id", Session: "session-id", Deadline: time.Now().Add(time.Second).UnixNano(), Reply: route, Payload: []byte{0, 255, 1, 2}}, 1024)
	if err != nil {
		t.Fatal(err)
	}
	return b
}
func bounded(t *testing.T) (context.Context, context.CancelFunc) {
	t.Helper()
	return context.WithTimeout(context.Background(), 3*time.Second)
}
func TestPayloadMetadataDeliveryAndUnsubscribe(t *testing.T) {
	ctx, cancel := bounded(t)
	defer cancel()
	b, _ := New(Options{})
	defer closeTestResource(t, b)
	received := make(chan []byte, 1)
	inbox, err := b.OpenInbox(ctx, "session-id", func(data []byte) { received <- data })
	if err != nil {
		t.Fatal(err)
	}
	defer closeTestResource(t, inbox)
	want := frame(t, inbox.Route())
	sub, err := b.Listen(ctx, "service", func(_ context.Context, data []byte) ([]byte, error) { return data, nil })
	if err != nil {
		t.Fatal(err)
	}
	if err = b.Send(ctx, "service", inbox.Route(), want); err != nil {
		t.Fatal(err)
	}
	select {
	case got := <-received:
		if !bytes.Equal(got, want) {
			t.Fatalf("raw payload/metadata changed: %q", got)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	if err = sub.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if err = b.Send(ctx, "service", inbox.Route(), want); !errors.Is(err, courier.ErrNoResponders) {
		t.Fatalf("unsubscribe=%v", err)
	}
}
func TestBoundedBackpressureAndBusShutdown(t *testing.T) {
	ctx, cancel := bounded(t)
	defer cancel()
	b, _ := New(Options{QueueSize: 1, PublishWorkers: 1, HandlerWorkers: 1})
	entered := make(chan struct{})
	sub, err := b.Listen(ctx, "service", func(ctx context.Context, _ []byte) ([]byte, error) {
		select {
		case <-entered:
		default:
			close(entered)
		}
		<-ctx.Done()
		return nil, ctx.Err()
	})
	if err != nil {
		t.Fatal(err)
	}
	data := frame(t, courier.Route{Topic: "unused"})
	if err = b.Send(ctx, "service", courier.Route{}, data); err != nil {
		t.Fatal(err)
	}
	select {
	case <-entered:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	overloaded := false
	for i := 0; i < 100; i++ {
		err = b.Send(ctx, "service", courier.Route{}, data)
		if errors.Is(err, courier.ErrOverloaded) {
			overloaded = true
			break
		}
		if err != nil {
			t.Fatal(err)
		}
	}
	if !overloaded {
		t.Fatal("queue not bounded")
	}
	if err = b.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if err = sub.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if s := b.Stats(); s.Services != 0 || s.Inboxes != 0 || s.Queued != 0 {
		t.Fatalf("leak: %+v", s)
	}
}
func TestBusIsolationAndRapidResubscribe(t *testing.T) {
	ctx, cancel := bounded(t)
	defer cancel()
	a, _ := New(Options{})
	b, _ := New(Options{})
	defer closeTestResource(t, a)
	defer closeTestResource(t, b)
	for i := 0; i < 30; i++ {
		sub, err := a.Listen(ctx, "service", func(context.Context, []byte) ([]byte, error) { return nil, nil })
		if err != nil {
			t.Fatal(err)
		}
		if err = b.Send(ctx, "service", courier.Route{}, frame(t, courier.Route{})); !errors.Is(err, courier.ErrNoResponders) {
			t.Fatalf("separate bus saw service: %v", err)
		}
		if err = sub.Close(ctx); err != nil {
			t.Fatal(err)
		}
	}
	if s := a.Stats(); s.Services != 0 {
		t.Fatalf("leaked service: %+v", s)
	}
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
