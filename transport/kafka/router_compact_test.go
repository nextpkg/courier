package kafka

import (
	"bytes"
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/nextpkg/courier"
	"github.com/nextpkg/courier/transport/kafka/internal/wire"
)

func TestCompactReplyValidationAndDedupOwnership(t *testing.T) {
	r, f, b := routerFixture(t)
	calls := 0
	inbox, err := r.openInbox(context.Background(), f.Session, func(data []byte) { calls++; clear(data) })
	require.NoError(t, err)

	defer func() { _ = inbox.Close(context.Background()) }()
	for i := 0; i < 2; i++ {
		m := wire.ReceivedBatchMessage{Instance: []byte(f.Instance), Expires: f.Expires, Data: bytes.Clone(b)}
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
		if got := r.deliverCompact(wire.ReceivedBatchMessage{Instance: []byte(tt.instance), Expires: tt.expiry, Data: bytes.Clone(b)}); got != tt.want {
			t.Fatalf("status=%d want=%d", got, tt.want)
		}
	}
	invalid, _ := courier.DecodeEnvelope(b, courier.DefaultMaxFrameBytes)
	invalid.ID = strings.Repeat("x", wire.MaxRouteBytes)
	oversized, err := courier.EncodeEnvelope(invalid, courier.DefaultMaxFrameBytes)
	require.NoError(t, err)

	if r.deliverCompact(wire.ReceivedBatchMessage{Instance: []byte(f.Instance), Expires: f.Expires, Data: oversized}) != 400 {
		t.Fatal("oversized routing metadata accepted")
	}
	invalid.ID = f.ID
	invalid.Reply.Partition = -1
	raw, err := courier.EncodeEnvelope(invalid, courier.DefaultMaxFrameBytes)
	require.NoError(t, err)

	if r.deliverCompact(wire.ReceivedBatchMessage{Instance: []byte(f.Instance), Expires: f.Expires, Data: raw}) != 400 {
		t.Fatal("invalid partition accepted")
	}
}

func TestCompactForwardRejectsMismatchedKafkaRoute(t *testing.T) {
	r, f, b := routerFixture(t)
	f.ID = "different-request"
	header, err := wire.EncodeRoute(f)
	require.NoError(t, err)

	err = r.forward(context.Background(), &kgo.Record{Topic: f.Topic, Partition: f.Partition, Headers: []kgo.RecordHeader{{Key: routeHeader, Value: header}}, Value: b})
	if err != nil || r.t.discarded.Load() != 1 {
		t.Fatalf("mismatch not discarded: %v", err)
	}
}

func FuzzCompactBatchRoundTrip(f *testing.F) {
	f.Add(wire.EncodeBatch([]wire.BatchMessage{{Instance: "instance", Expires: 123, Data: []byte("body")}}))
	f.Add([]byte("invalid"))
	f.Fuzz(func(t *testing.T, b []byte) {
		if len(b) > 4096 {
			return
		}
		got, err := wire.DecodeBatch(b, 1024)
		if err != nil {
			return
		}
		outbound := make([]wire.BatchMessage, len(got))
		for i, m := range got {
			outbound[i] = wire.BatchMessage{Instance: string(m.Instance), Expires: m.Expires, Data: m.Data}
		}
		if !bytes.Equal(wire.EncodeBatch(outbound), b) {
			t.Fatal("compact frame changed on roundtrip")
		}
	})
}
