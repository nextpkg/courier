//go:build integration

package examples_test

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/nextpkg/courier"
	"github.com/twmb/franz-go/pkg/kgo"
)

func TestKafkaSharedReplyReader(t *testing.T) {
	producer, opts := newKafkaEnv(t, 3)
	tr := newKafkaTransport(t, opts)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	var inboxes []courier.Inbox
	got := make(chan string, 64)
	for i := 0; i < 3; i++ {
		session := fmt.Sprint(i)
		inbox, err := tr.OpenInbox(ctx, session, func(data []byte) {
			e, err := courier.DecodeEnvelope(data, courier.DefaultMaxFrameBytes)
			if err != nil || e.Session != session {
				t.Errorf("reply crossed sessions: %+v %v", e, err)
			}
			got <- e.ID
		})
		if err != nil {
			t.Fatal(err)
		}
		inboxes = append(inboxes, inbox)
		t.Cleanup(func() { closeTestResource(t, inbox) })
	}
	if dup, err := tr.OpenInbox(ctx, "0", func([]byte) {}); err == nil {
		closeTestResource(t, dup)
		t.Fatal("duplicate session accepted")
	}
	publish := func(session, id string) {
		e := courier.Envelope{Version: 1, Kind: "response", Service: "Orders", Method: "Get", ID: id, Session: session, Reply: inboxes[0].Route()}
		data, err := courier.EncodeEnvelope(e, courier.DefaultMaxFrameBytes)
		if err != nil {
			t.Fatal(err)
		}
		if err = producer.ProduceSync(ctx, &kgo.Record{Topic: opts.ReplyTopic, Value: data}).FirstErr(); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < 30; i++ {
		publish(fmt.Sprint(i%3), fmt.Sprint(i))
	}
	seen := map[string]bool{}
	for i := 0; i < 30; i++ {
		select {
		case id := <-got:
			if seen[id] {
				t.Fatalf("duplicate %s", id)
			}
			seen[id] = true
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	}
	if s := tr.Stats(); s.ReplyRecords != 30 || s.ReplyMatched != 30 {
		t.Fatalf("expected one read per reply: %+v", s)
	}
	if err := inboxes[0].Close(ctx); err != nil {
		t.Fatal(err)
	}
	publish("0", "closed")
	publish("1", "survivor")
	select {
	case id := <-got:
		if id != "survivor" {
			t.Fatal(id)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	if err := tr.Close(ctx); err != nil {
		t.Fatal(err)
	}
	for _, inbox := range inboxes {
		select {
		case <-inbox.Done():
		default:
			t.Fatal("inbox survived transport close")
		}
	}
}

func TestKafkaSharedReaderSkipsNewSessionHistory(t *testing.T) {
	producer, opts := newKafkaEnv(t, 1)
	tr := newKafkaTransport(t, opts)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	entered, release := make(chan struct{}), make(chan struct{})
	// Release callbacks before cleanup even when an assertion fails.
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	defer unblock()
	a, err := tr.OpenInbox(ctx, "a", func([]byte) {
		close(entered)
		select {
		case <-release:
		case <-ctx.Done():
		}
	})
	if err != nil {
		t.Fatal(err)
	}
	publish := func(session, id string) {
		e := courier.Envelope{Version: 1, Kind: "response", Service: "Orders", Method: "Get", ID: id, Session: session, Reply: a.Route()}
		data, err := courier.EncodeEnvelope(e, courier.DefaultMaxFrameBytes)
		if err != nil {
			t.Fatal(err)
		}
		if err = producer.ProduceSync(ctx, &kgo.Record{Topic: opts.ReplyTopic, Value: data}).FirstErr(); err != nil {
			t.Fatal(err)
		}
	}
	publish("a", "block")
	select {
	case <-entered:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	publish("b", "historical")
	got := make(chan string, 2)
	b, err := tr.OpenInbox(ctx, "b", func(data []byte) { e, _ := courier.DecodeEnvelope(data, courier.DefaultMaxFrameBytes); got <- e.ID })
	if err != nil {
		t.Fatal(err)
	}
	publish("b", "live")
	unblock()
	select {
	case id := <-got:
		if id != "live" {
			t.Fatalf("historical reply delivered: %s", id)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	if err := b.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if err := a.Close(ctx); err != nil {
		t.Fatal(err)
	}
}
