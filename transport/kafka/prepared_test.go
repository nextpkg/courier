package kafka

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/nextpkg/courier"
	"github.com/stretchr/testify/require"
)

func TestPreparedRequestOwnsFrameAndEnforcesLimits(t *testing.T) {
	r, _, _ := routerFixture(t)
	tr := r.t
	tr.router = r
	tr.opts.DynamicReplyPartitions = true
	tr.opts.RequestTopics = map[string]string{"S": "requests"}
	tr.topics = map[string]metadata{
		"requests":         {partitions: []int32{0}, expires: time.Now().Add(time.Minute)},
		tr.opts.ReplyTopic: {partitions: []int32{0, 1, 2}, expires: time.Now().Add(time.Minute)},
	}
	e := courier.Envelope{Version: 1, Kind: "request", Service: "S", Method: "M", ID: "id", Session: "s", Reply: courier.Route{Topic: tr.opts.ReplyTopic}, Payload: []byte("owned"), Deadline: time.Now().Add(time.Second).UnixNano()}
	ready, err := tr.PrepareRequest(context.Background(), e, 512)
	require.NoError(t, err)

	e.Payload[0] = 'X'
	got, err := courier.DecodeEnvelope(ready.(*preparedRequest).value, 512)
	if err != nil || string(got.Payload) != "owned" || got.Reply.Partition != int32(hash(e.ID)%3) {
		t.Fatalf("invalid frame: %+v %v", got, err)
	}
	if _, err := tr.PrepareRequest(context.Background(), e, 1); !errors.Is(err, courier.ErrTooLarge) {
		t.Fatal("caller limit bypassed")
	}
	tr.opts.MaxFrameBytes = 1
	if _, err := tr.PrepareRequest(context.Background(), e, 512); !errors.Is(err, courier.ErrTooLarge) {
		t.Fatal("adapter limit bypassed")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := tr.PrepareRequest(ctx, e, 512); !errors.Is(err, context.Canceled) {
		t.Fatal("cancel ignored")
	}
}

func TestRouterParsedInboxStillValidatesWire(t *testing.T) {
	r, f, b := routerFixture(t)
	calls := 0
	inbox, err := r.openInboxParsed(context.Background(), f.Session, nil, func(e courier.Envelope, n int) {
		calls++
		if e.ID != f.ID || n != len(b) {
			t.Error("wrong decoded response")
		}
	})
	require.NoError(t, err)

	defer inbox.Close(context.Background())
	if r.deliver(f, []byte("invalid")) != 400 || calls != 0 {
		t.Fatal("malformed wire accepted")
	}
	if r.deliver(f, b) != 204 || calls != 1 {
		t.Fatal("delivery failed")
	}
	if r.deliver(f, b) != 204 || calls != 1 {
		t.Fatal("duplicate delivered")
	}
}
