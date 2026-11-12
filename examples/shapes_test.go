package examples_test

import (
	"bytes"
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/nextpkg/courier"
	"github.com/nextpkg/courier/examples/gen/demo"
	"github.com/nextpkg/courier/examples/gen/shapes"
	"github.com/nextpkg/courier/examples/handlers"
	"github.com/nextpkg/courier/internal/testcheck"
	"github.com/nextpkg/courier/transport/memory"
)

type shapeHandler struct{}

func (shapeHandler) Ping(context.Context) error { return nil }
func (shapeHandler) Check(_ context.Context, fail bool) error {
	if fail {
		return &shapes.ShapeError{Reason: "expected"}
	}
	return nil
}
func (shapeHandler) Echo(_ context.Context, data []byte) ([]byte, error) { return data, nil }
func (shapeHandler) Nest(_ context.Context, data map[string][]int32) (map[string][]int32, error) {
	return data, nil
}
func TestGeneratedVoidBinaryAndContainers(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	b, _ := memory.New(memory.Options{})
	defer testcheck.Close(t, b)
	s, _ := courier.NewServer(b, courier.ServerOptions{})
	defer testcheck.Close(t, s)
	require.NoError(t, shapes.RegisterShapes(s, shapeHandler{}))
	require.NoError(t, s.Start(ctx))

	c, err := courier.NewClient(ctx, b, courier.ClientOptions{})
	require.NoError(t, err)

	defer testcheck.Close(t, c)
	api := shapes.NewShapesAsyncClient(c)
	require.NoError(t, api.Ping(ctx))

	var business *shapes.ShapeError
	if err := api.Check(ctx, true); !errors.As(err, &business) {
		t.Fatalf("void exception=%v", err)
	}
	data := []byte{0, 1, 255, 128}
	got, err := api.Echo(ctx, data)
	if err != nil || !bytes.Equal(data, got) {
		t.Fatalf("binary=%v %v", got, err)
	}
	values := map[string][]int32{"one": {1, -1, 42}, "empty": {}}
	v, err := api.Nest(ctx, values)
	if err != nil || !reflect.DeepEqual(values, v) {
		t.Fatalf("container=%v %v", v, err)
	}
}

type corruptTransport struct {
	courier.Transport
	mutate func(*courier.Envelope)
}

func (t corruptTransport) Send(ctx context.Context, service string, reply courier.Route, data []byte) error {
	e, err := courier.DecodeEnvelope(data, courier.DefaultMaxFrameBytes)
	if err != nil {
		return err
	}
	t.mutate(&e)
	out, err := courier.EncodeEnvelope(e, courier.DefaultMaxFrameBytes)
	if err != nil {
		return err
	}
	return t.Transport.Send(ctx, service, reply, out)
}
func TestMalformedRequestsNeverExecuteHandler(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(*courier.Envelope)
	}{
		{"trailing", func(e *courier.Envelope) { e.Payload = append(e.Payload, 0) }},
		{"broken arguments", func(e *courier.Envelope) { e.Payload = e.Payload[:len(e.Payload)-2] }},
		{"header mismatch", func(e *courier.Envelope) { e.Method = "List" }},
		{"service mismatch", func(e *courier.Envelope) { e.Service = "Inventory" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			b, _ := memory.New(memory.Options{})
			defer testcheck.Close(t, b)
			r := &handlers.Recorder{}
			s, _ := courier.NewServer(b, courier.ServerOptions{})
			defer testcheck.Close(t, s)
			_ = demo.RegisterOrders(s, &handlers.Orders{Recorder: r})
			if err := s.Start(ctx); err != nil {
				t.Fatal(err)
			}
			// Raw replies bypass client service/method matching only for corruption tests
			// that intentionally alter those fields. The server must produce a protocol error.
			replies := make(chan []byte, 1)
			inbox, err := b.OpenInbox(ctx, "corrupt-session", func(data []byte) { replies <- data })
			if err != nil {
				t.Fatal(err)
			}
			defer testcheck.Close(t, inbox)
			interceptor := corruptTransport{Transport: b, mutate: func(e *courier.Envelope) { e.Reply = inbox.Route(); e.Session = "corrupt-session"; tc.mutate(e) }}
			c, err := courier.NewClient(ctx, interceptor, courier.ClientOptions{})
			if err != nil {
				t.Fatal(err)
			}
			defer testcheck.Close(t, c)
			if _, err = demo.NewOrdersAsyncClient(c).GetAsync(ctx, "bad", 0); err != nil {
				t.Fatal(err)
			}
			select {
			case data := <-replies:
				e, err := courier.DecodeEnvelope(data, courier.DefaultMaxFrameBytes)
				if err != nil || e.Error == nil || e.Error.Code != "protocol" {
					t.Fatalf("response=%+v %v", e, err)
				}
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			if r.Count("Orders.Get:bad") != 0 {
				t.Fatal("malformed request executed handler")
			}
		})
	}
}
func TestLargeRequestAndResponse(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	b, _ := memory.New(memory.Options{})
	defer testcheck.Close(t, b)
	s, _ := courier.NewServer(b, courier.ServerOptions{MaxFrameBytes: 600})
	defer testcheck.Close(t, s)
	_ = demo.RegisterOrders(s, &handlers.Orders{})
	require.NoError(t, s.Start(ctx))

	c, err := courier.NewClient(ctx, b, courier.ClientOptions{MaxFrameBytes: 600})
	require.NoError(t, err)

	defer testcheck.Close(t, c)
	if _, err = demo.NewOrdersAsyncClient(c).GetAsync(ctx, strings.Repeat("x", 1000), 0); !errors.Is(err, courier.ErrTooLarge) {
		t.Fatalf("request limit=%v", err)
	}
	// Repeated IDs fit in the request more cheaply than full returned Item structs.
	_, err = demo.NewOrdersAsyncClient(c).List(ctx, []string{"one", "two", "three", "four", "five", "six", "seven", "eight", "nine", "ten", "eleven", "twelve"})
	var remote *courier.RPCError
	if !errors.As(err, &remote) || remote.Code != "too_large" {
		t.Fatalf("response limit=%v", err)
	}
}
