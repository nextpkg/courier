package courier

import (
	"context"
	"errors"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

type preparedFunc func(context.Context) error

func (f preparedFunc) Send(ctx context.Context) error { return f(ctx) }

type fastTestTransport struct {
	fakeTransport
	parsed   func(Envelope, int)
	prepared chan Envelope
}

func (f *fastTestTransport) OpenEnvelopeInbox(_ context.Context, _ string, recv func(Envelope, int)) (Inbox, error) {
	f.parsed = recv
	f.sub.done = make(chan struct{})
	return &f.sub, nil
}
func (f *fastTestTransport) PrepareRequest(_ context.Context, e Envelope, limit int) (PreparedRequest, error) {
	if _, err := EncodeEnvelope(e, limit); err != nil {
		return nil, err
	}
	return preparedFunc(func(ctx context.Context) error {
		select {
		case f.prepared <- e:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}), nil
}
func (f *fastTestTransport) Send(context.Context, string, Route, []byte) error {
	panic("unexpected legacy send")
}
func TestClientPreparedAndParsedPaths(t *testing.T) {
	f := &fastTestTransport{prepared: make(chan Envelope, 1)}
	c, err := NewClient(context.Background(), f, ClientOptions{MaxFrameBytes: 512})
	require.NoError(t, err)

	defer func() { _ = c.Close(context.Background()) }()
	call, err := c.Submit(context.Background(), "S", "Ping", &unit{})
	require.NoError(t, err)

	var e Envelope
	select {
	case e = <-f.prepared:
	case <-time.After(time.Second):
		t.Fatal("not published")
	}
	e.Kind = "response"
	e.Error = &RPCError{Code: "test", Message: "expected"}
	f.parsed(e, 513)
	select {
	case <-call.done:
		t.Fatal("core size limit bypassed")
	default:
	}
	wrong := e
	wrong.Method = "wrong"
	f.parsed(wrong, 100)
	select {
	case <-call.done:
		t.Fatal("method mismatch accepted")
	default:
	}
	f.parsed(e, 100)
	select {
	case <-call.done:
	case <-time.After(time.Second):
		t.Fatal("reply not delivered")
	}
	var rpc *RPCError
	if !errors.As(call.err, &rpc) || rpc.Code != "test" {
		t.Fatalf("bad result: %v", call.err)
	}
	f.parsed(e, 100)
}
