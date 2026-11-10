package kafka

import (
	"bytes"
	"context"
	"github.com/nextpkg/courier"
	"github.com/twmb/franz-go/pkg/kgo"
	"strings"
	"testing"
)

func TestCompactReplyValidationAndDedupOwnership(t *testing.T) {
	r, f, b := routerFixture(t)
	calls := 0
	inbox, err := r.openInbox(context.Background(), f.Session, func(data []byte) { calls++; clear(data) })
	if err != nil {
		t.Fatal(err)
	}
	defer inbox.Close(context.Background())
	for i := 0; i < 2; i++ {
		m := receivedBatchMessage{Instance: []byte(f.Instance), Expires: f.Expires, Data: bytes.Clone(b)}
		if status := r.deliverCompact(m); status != 204 {
			t.Fatal(status)
		}
	}
	if calls != 1 {
		t.Fatal("mutating raw callback corrupted dedup identity")
	}
	for _, tt := range []struct {
		instance string
		expiry   int64
		want     int
	}{{"", f.Expires, 400}, {f.Instance, 0, 400}, {"old", f.Expires, 409}, {f.Instance, 1, 410}} {
		if got := r.deliverCompact(receivedBatchMessage{[]byte(tt.instance), tt.expiry, bytes.Clone(b)}); got != tt.want {
			t.Fatalf("status=%d want=%d", got, tt.want)
		}
	}
	invalid, _ := courier.DecodeEnvelope(b, courier.DefaultMaxFrameBytes)
	invalid.ID = strings.Repeat("x", maxRouteBytes)
	oversized, err := courier.EncodeEnvelope(invalid, courier.DefaultMaxFrameBytes)
	if err != nil {
		t.Fatal(err)
	}
	if r.deliverCompact(receivedBatchMessage{[]byte(f.Instance), f.Expires, oversized}) != 400 {
		t.Fatal("oversized routing metadata accepted")
	}
	invalid.ID = f.ID
	invalid.Reply.Partition = -1
	raw, err := courier.EncodeEnvelope(invalid, courier.DefaultMaxFrameBytes)
	if err != nil {
		t.Fatal(err)
	}
	if r.deliverCompact(receivedBatchMessage{[]byte(f.Instance), f.Expires, raw}) != 400 {
		t.Fatal("invalid partition accepted")
	}
}

func TestCompactForwardRejectsMismatchedKafkaRoute(t *testing.T) {
	r, f, b := routerFixture(t)
	f.ID = "different-request"
	header, err := marshalForwardRoute(f)
	if err != nil {
		t.Fatal(err)
	}
	err = r.forward(context.Background(), &kgo.Record{Topic: f.Topic, Partition: f.Partition, Headers: []kgo.RecordHeader{{Key: routeHeader, Value: header}}, Value: b})
	if err != nil || r.t.discarded.Load() != 1 {
		t.Fatalf("mismatch not discarded: %v", err)
	}
}

func FuzzCompactBatchRoundTrip(f *testing.F) {
	f.Add(encodeBinaryBatch([]batchMessage{{"instance", 123, []byte("body")}}))
	f.Add([]byte("invalid"))
	f.Fuzz(func(t *testing.T, b []byte) {
		if len(b) > 4096 {
			return
		}
		got, err := decodeBinaryBatch(b, 1024)
		if err != nil {
			return
		}
		outbound := make([]batchMessage, len(got))
		for i, m := range got {
			outbound[i] = batchMessage{string(m.Instance), m.Expires, m.Data}
		}
		if !bytes.Equal(encodeBinaryBatch(outbound), b) {
			t.Fatal("compact frame changed on roundtrip")
		}
	})
}
