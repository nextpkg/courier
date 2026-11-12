package courier

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestEnvelopeHandlerPreservesValidation(t *testing.T) {
	ctx := context.Background()
	payload, err := encodeCall(ctx, "Ping", &unit{}, 1024)
	require.NoError(t, err)

	base := Envelope{Version: ProtocolVersion, Kind: "request", Service: "S", Method: "Ping", ID: "id", Session: "s", Deadline: time.Now().Add(time.Minute).UnixNano(), Payload: payload, Reply: Route{Topic: "reply"}}
	s, err := NewServer(&fakeTransport{}, ServerOptions{MaxFrameBytes: 1024})
	require.NoError(t, err)

	for _, change := range []func(*Envelope){func(e *Envelope) {}, func(e *Envelope) { e.Deadline = 1 }, func(e *Envelope) { e.Service = "wrong" }, func(e *Envelope) { e.Payload = []byte("bad") }, func(e *Envelope) { e.Payload = append(bytes.Clone(e.Payload), 1) }, func(e *Envelope) { e.Kind = "response" }, func(e *Envelope) { e.Version = 99 }} {
		e := base
		change(&e)
		wire, err := EncodeEnvelope(e, 1024)
		require.NoError(t, err)

		a, ae := s.handler("S", verboseErrorProcessor{})(ctx, wire)
		b, be := s.envelopeHandler("S", verboseErrorProcessor{})(ctx, e, len(wire))
		if (ae == nil) != (be == nil) || !bytes.Equal(a, b) {
			t.Fatalf("paths differ: %v %v", ae, be)
		}
	}
	if _, err := s.envelopeHandler("S", verboseErrorProcessor{})(ctx, base, 1025); !errors.Is(err, ErrTooLarge) {
		t.Fatal("core frame limit bypassed")
	}
}
