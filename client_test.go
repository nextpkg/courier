package courier

import (
	"context"
	"errors"
	"fmt"
	"github.com/apache/thrift/lib/go/thrift"
	"github.com/stretchr/testify/require"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type unit struct{}

func (*unit) Read(ctx context.Context, p thrift.TProtocol) error { return p.Skip(ctx, thrift.STRUCT) }
func (*unit) Write(ctx context.Context, p thrift.TProtocol) error {
	if err := p.WriteStructBegin(ctx, "unit"); err != nil {
		return err
	}
	if err := p.WriteFieldStop(ctx); err != nil {
		return err
	}
	return p.WriteStructEnd(ctx)
}

type fakeSub struct {
	closed atomic.Bool
	done   chan struct{}
}

func (s *fakeSub) Close(context.Context) error {
	if s.closed.CompareAndSwap(false, true) {
		close(s.done)
	}
	return nil
}
func (s *fakeSub) Route() Route          { return Route{Topic: "responses", Partition: 2} }
func (s *fakeSub) Done() <-chan struct{} { return s.done }

type fakeTransport struct {
	recv func([]byte)
	sub  fakeSub
	sent chan Envelope
	send func(context.Context, Envelope) error
}

func (f *fakeTransport) OpenInbox(_ context.Context, _ string, r func([]byte)) (Inbox, error) {
	f.recv = r
	f.sub.done = make(chan struct{})
	return &f.sub, nil
}
func (f *fakeTransport) Listen(context.Context, string, Handler) (Subscription, error) {
	return nil, fmt.Errorf("unused")
}
func (f *fakeTransport) Send(ctx context.Context, _ string, _ Route, data []byte) error {
	e, err := DecodeEnvelope(data, DefaultMaxFrameBytes)
	if err != nil {
		return err
	}
	if f.send != nil {
		return f.send(ctx, e)
	}
	select {
	case f.sent <- e:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
func (f *fakeTransport) respond(e Envelope) {
	e.Kind = "response"
	data, err := EncodeEnvelope(e, DefaultMaxFrameBytes)
	if err != nil {
		panic(err)
	}
	f.recv(data)
}
func newTestClient(t *testing.T, f *fakeTransport, opts ClientOptions) *Client {
	t.Helper()
	if f.sent == nil {
		f.sent = make(chan Envelope, 32)
	}
	c, err := NewClient(context.Background(), f, opts)
	require.NoError(t, err)

	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := c.Close(ctx); err != nil {
			t.Error(err)
		}
	})
	return c
}
func next(t *testing.T, ch <-chan Envelope) Envelope {
	t.Helper()
	select {
	case e := <-ch:
		return e
	case <-time.After(time.Second):
		t.Fatal("request not published")
		return Envelope{}
	}
}
func TestCorrelationAndDuplicates(t *testing.T) {
	f := &fakeTransport{}
	c := newTestClient(t, f, ClientOptions{})
	a, _ := c.Submit(context.Background(), "A", "Get", &unit{})
	b, _ := c.Submit(context.Background(), "B", "Count", &unit{})
	ea, eb := next(t, f.sent), next(t, f.sent)
	if ea.ID != a.ID() {
		ea, eb = eb, ea
	}
	bad := ea
	bad.Session = "old-session"
	f.respond(bad)
	bad = ea
	bad.Method = "wrong"
	f.respond(bad)
	bad = ea
	bad.Service = "wrong"
	f.respond(bad)
	bad = ea
	bad.ID = "unknown"
	f.respond(bad)
	f.recv([]byte("garbage"))
	if c.Pending() != 2 {
		t.Fatal("invalid reply completed call")
	}
	eb.Payload = []byte("B")
	f.respond(eb)
	ea.Payload = []byte("A")
	f.respond(ea)
	f.respond(eb)
	for _, x := range []struct {
		c    *Call
		want string
	}{{a, "A"}, {b, "B"}} {
		got, err := x.c.Await(context.Background())
		if err != nil || string(got) != x.want {
			t.Fatalf("got=%q %v", got, err)
		}
	}
	if c.Pending() != 0 {
		t.Fatal("pending leaked")
	}
}
func TestResponseBeforeSendReturns(t *testing.T) {
	f := &fakeTransport{}
	f.send = func(_ context.Context, e Envelope) error {
		e.Error = &RPCError{Code: "overloaded", Message: "full"}
		f.respond(e)
		return nil
	}
	c := newTestClient(t, f, ClientOptions{})
	x, _ := c.Submit(context.Background(), "A", "Get", &unit{})
	if _, err := x.Await(context.Background()); err == nil {
		t.Fatal("lost remote error")
	}
	if err := x.Sent(context.Background()); err != nil {
		t.Fatalf("response proves publication: %v", err)
	}
}
func TestPublicationFailureAndCancellation(t *testing.T) {
	t.Run("failure", func(t *testing.T) {
		f := &fakeTransport{send: func(context.Context, Envelope) error { return ErrNoResponders }}
		c := newTestClient(t, f, ClientOptions{})
		x, _ := c.Submit(context.Background(), "A", "Get", &unit{})
		_, err := x.Await(context.Background())
		var transport *TransportError
		if !errors.As(err, &transport) || !errors.Is(err, ErrNoResponders) {
			t.Fatal(err)
		}
	})
	t.Run("cancel during send", func(t *testing.T) {
		entered := make(chan struct{})
		f := &fakeTransport{send: func(ctx context.Context, e Envelope) error { close(entered); <-ctx.Done(); return ctx.Err() }}
		c := newTestClient(t, f, ClientOptions{})
		ctx, cancel := context.WithCancel(context.Background())
		x, _ := c.Submit(ctx, "A", "Get", &unit{})
		<-entered
		cancel()
		_, err := x.Await(context.Background())
		if !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
		if c.Pending() != 0 {
			t.Fatal("pending leak")
		}
	})
}
func TestParentCancellation(t *testing.T) {
	f := &fakeTransport{sent: make(chan Envelope, 1)}
	parent, cancel := context.WithCancel(context.Background())
	c, err := NewClient(parent, f, ClientOptions{})
	require.NoError(t, err)

	x, _ := c.Submit(context.Background(), "A", "Get", &unit{})
	next(t, f.sent)
	cancel()
	wait, stop := context.WithTimeout(context.Background(), time.Second)
	defer stop()
	if _, err = x.Await(wait); !errors.Is(err, ErrClosed) {
		t.Fatalf("pending outlived parent: %v", err)
	}
	if err = c.Close(wait); err != nil {
		t.Fatal(err)
	}
	if !f.sub.closed.Load() {
		t.Fatal("inbox not closed")
	}
}
func TestConcurrentCloseAndCompletion(t *testing.T) {
	f := &fakeTransport{}
	c := newTestClient(t, f, ClientOptions{})
	var calls []*Call
	for i := 0; i < 24; i++ {
		x, err := c.Submit(context.Background(), "A", "Get", &unit{})
		require.NoError(t, err)

		calls = append(calls, x)
	}
	var wg sync.WaitGroup
	wg.Add(2)
	go func() { defer wg.Done(); _ = c.Close(context.Background()) }()
	go func() {
		defer wg.Done()
		for {
			select {
			case e := <-f.sent:
				f.respond(e)
				f.respond(e)
			default:
				return
			}
		}
	}()
	wg.Wait()
	for _, x := range calls {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		_, err := x.Await(ctx)
		cancel()
		if err != nil && !errors.Is(err, ErrClosed) {
			t.Error(err)
		}
	}
	if c.Pending() != 0 {
		t.Fatal("pending leaked")
	}
}
func TestInvalidArgumentsHaveNoSideEffects(t *testing.T) {
	f := &fakeTransport{}
	c := newTestClient(t, f, ClientOptions{})
	var nilArgs *unit
	if _, err := c.Submit(context.Background(), "A", "Get", nilArgs); err == nil {
		t.Fatal("typed nil accepted")
	}
	if _, err := (ThriftClient{Client: c, Service: "A"}).Call(context.Background(), "Oneway", &unit{}, nil); err == nil {
		t.Fatal("oneway accepted")
	}
	if len(f.sent) != 0 || c.Pending() != 0 {
		t.Fatal("invalid call published")
	}
}
func TestProtocolBounds(t *testing.T) {
	b, p := binaryProtocol(nil, 512)
	if err := p.WriteString(context.Background(), strings.Repeat("x", 1024)); !errors.Is(err, ErrTooLarge) {
		t.Fatalf("size=%v", err)
	}
	if b.Len() > 512 {
		t.Fatal("oversized serialization buffer")
	}
	if _, err := DecodeEnvelope([]byte(strings.Repeat("x", 513)), 512); !errors.Is(err, ErrTooLarge) {
		t.Fatal(err)
	}
	if _, err := DecodeEnvelope([]byte(`{"version":99}`), 512); err == nil {
		t.Fatal("unsupported version accepted")
	}
	ctx := context.Background()
	out, op := binaryProtocol(nil, 1024)
	_ = op.WriteMessageBegin(ctx, "Get", thrift.REPLY, 0)
	_ = (&unit{}).Write(ctx, op)
	_ = op.WriteMessageEnd(ctx)
	require.NoError(t, decodeResult(ctx, "Get", out.Bytes(), &unit{}, 1024))
	require.Error(t, decodeResult(ctx, "Other", out.Bytes(), &unit{}, 1024),
		"wrong method accepted")

	bad := append(append([]byte(nil), out.Bytes()...), 0)
	require.Error(t, decodeResult(ctx, "Get", bad, &unit{}, 1024),
		"trailing bytes accepted")

}

func TestClientDrainWaitsAndRejectsNewCalls(t *testing.T) {
	f := &fakeTransport{sent: make(chan Envelope, 1)}
	c := newTestClient(t, f, ClientOptions{})
	call, err := c.Submit(context.Background(), "A", "Get", &unit{})
	require.NoError(t, err)

	e := <-f.sent
	done := make(chan error, 1)
	go func() { done <- c.Drain(context.Background()) }()
	deadline := time.Now().Add(time.Second)
	for {
		c.mu.Lock()
		closed := c.closed
		c.mu.Unlock()
		if closed {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("drain did not start")
		}
		time.Sleep(time.Millisecond)
	}
	if _, err := c.Submit(context.Background(), "A", "Get", &unit{}); !errors.Is(err, ErrClosed) {
		t.Fatal(err)
	}
	select {
	case <-done:
		t.Fatal("drain returned before pending reply")
	default:
	}
	f.respond(e)
	if _, err := call.Await(context.Background()); err != nil {
		t.Fatal(err)
	}
	require.NoError(t, <-done)

}
