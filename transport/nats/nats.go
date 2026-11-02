// Package nats transports unary RPC over Core NATS (not JetStream).
package nats

import (
	"context"
	"encoding/base64"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	sdk "github.com/nats-io/nats.go"
	"github.com/nextpkg/courier"
)

type Options struct {
	Prefix                                              string
	Workers, PendingMessages, MaxFrameBytes, MaxPending int
	FlushTimeout                                        time.Duration
	Logger                                              *slog.Logger
}

// Transport borrows a caller-owned native SDK connection. Close only closes
// this adapter's subscriptions; the application owns connection configuration.
type Transport struct {
	conn    *sdk.Conn
	opts    Options
	mu      sync.Mutex
	closed  bool
	subs    map[*subscription]bool
	pending map[string]*request
}
type request struct {
	e    courier.Envelope
	stop func() bool
}
type subscription struct {
	t         *Transport
	sub       *sdk.Subscription
	cancel    context.CancelFunc
	done      chan struct{}
	once      sync.Once
	route     courier.Route
	session   string
	service   bool
	drainOnce sync.Once
	drainErr  error
	finishErr error
}

func (s *subscription) Route() courier.Route  { return s.route }
func (s *subscription) Done() <-chan struct{} { return s.done }
func (s *subscription) stop()                 { s.once.Do(func() { s.cancel(); _ = s.sub.Unsubscribe() }) }
func (s *subscription) Close(ctx context.Context) error {
	select {
	case <-s.done:
		return s.finishErr
	default:
	}
	if s.service {
		s.drainOnce.Do(func() { s.drainErr = s.sub.Drain() })
		if s.drainErr != nil {
			s.stop()
			return s.drainErr
		}
	} else {
		s.stop()
	}
	select {
	case <-s.done:
		return s.finishErr
	case <-ctx.Done():
		s.stop()
		return ctx.Err()
	}
}
func (s *subscription) finished() {
	s.stop()
	s.t.mu.Lock()
	delete(s.t.subs, s)
	s.t.mu.Unlock()
	close(s.done)
}
func New(conn *sdk.Conn, opts Options) (*Transport, error) {
	if conn == nil || conn.IsClosed() {
		return nil, fmt.Errorf("nats: open connection required")
	}
	if opts.Prefix == "" {
		opts.Prefix = "courier"
	}
	if opts.Workers == 0 {
		opts.Workers = 16
	}
	if opts.PendingMessages == 0 {
		opts.PendingMessages = 256
	}
	if opts.MaxFrameBytes == 0 {
		opts.MaxFrameBytes = courier.DefaultMaxFrameBytes
	}
	if opts.MaxPending == 0 {
		opts.MaxPending = 4096
	}
	if opts.FlushTimeout == 0 {
		opts.FlushTimeout = 3 * time.Second
	}
	if opts.Logger == nil {
		opts.Logger = slog.Default()
	}
	if !validSubject(opts.Prefix) || opts.Workers < 1 || opts.PendingMessages < 1 || opts.MaxFrameBytes < 512 || opts.MaxFrameBytes > 1<<30 || opts.MaxPending < 1 || opts.FlushTimeout < 0 {
		return nil, fmt.Errorf("nats: invalid options")
	}
	return &Transport{conn: conn, opts: opts, subs: map[*subscription]bool{}, pending: map[string]*request{}}, nil
}
func validSubject(s string) bool {
	if s == "" || strings.ContainsAny(s, " *>\t\r\n") {
		return false
	}
	for _, p := range strings.Split(s, ".") {
		if p == "" {
			return false
		}
	}
	return true
}
func (t *Transport) subject(service string) string {
	return t.opts.Prefix + ".request." + base64.RawURLEncoding.EncodeToString([]byte(service))
}
func (t *Transport) flush(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, t.opts.FlushTimeout)
	defer cancel()
	return t.conn.FlushWithContext(ctx)
}
func (t *Transport) add(s *subscription) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.closed {
		return courier.ErrClosed
	}
	t.subs[s] = true
	return nil
}
func (t *Transport) forget(key string) {
	t.mu.Lock()
	r := t.pending[key]
	delete(t.pending, key)
	t.mu.Unlock()
	if r != nil && r.stop != nil {
		r.stop()
	}
}
func (t *Transport) OpenInbox(parent context.Context, session string, receive func([]byte)) (courier.Inbox, error) {
	if err := parent.Err(); err != nil {
		return nil, err
	}
	if session == "" || receive == nil {
		return nil, fmt.Errorf("nats: invalid inbox")
	}
	ctx, cancel := context.WithCancel(parent)
	s := &subscription{t: t, cancel: cancel, done: make(chan struct{}), session: session, route: courier.Route{Topic: sdk.NewInbox(), Partition: -1}}
	// ChanSubscribe preserves the subject on a 503 response. NextMsg converts
	// it into ErrNoResponders and discards the message, losing multiplexed IDs.
	messages := make(chan *sdk.Msg, t.opts.PendingMessages)
	sub, err := t.conn.ChanSubscribe(s.route.Topic+".*", messages)
	if err != nil {
		cancel()
		return nil, err
	}
	s.sub = sub
	// ChanSubscribe is bounded by channel capacity; the SDK does not support
	// SetPendingLimits for this subscription type. Broker MaxPayload bounds bytes.
	if err = t.add(s); err != nil {
		s.stop()
		return nil, err
	}
	if err = t.flush(ctx); err != nil {
		s.finished()
		return nil, err
	}
	go func() {
		defer s.finished()
		closed := t.conn.StatusChanged(sdk.CLOSED)
		defer t.conn.RemoveStatusListener(closed)
		if t.conn.IsClosed() {
			return
		}
		defer func() {
			t.mu.Lock()
			var keys []string
			for key, r := range t.pending {
				if r.e.Session == session {
					keys = append(keys, key)
				}
			}
			t.mu.Unlock()
			for _, key := range keys {
				t.forget(key)
			}
		}()
		for {
			var m *sdk.Msg
			select {
			case <-ctx.Done():
				return
			case <-closed:
				return
			case m = <-messages:
			}
			if m.Header.Get("Status") == "503" && len(m.Data) == 0 {
				t.mu.Lock()
				r := t.pending[m.Subject]
				var e courier.Envelope
				if r != nil {
					e = r.e
				}
				t.mu.Unlock()
				if r == nil {
					continue
				}
				e.Kind = "response"
				e.Deadline = 0
				e.Error = &courier.RPCError{Code: "no_responders", Message: "NATS has no matching queue subscriber"}
				data, err := courier.EncodeEnvelope(e, t.opts.MaxFrameBytes)
				if err == nil {
					receive(data)
				}
			} else if len(m.Data) <= t.opts.MaxFrameBytes {
				receive(m.Data)
			}
			// Call context cancellation cleans up correlation after valid completion.
		}
	}()
	return s, nil
}

// Send publishes and completes a PING/PONG round trip. That confirms the broker
// processed preceding protocol traffic; Core NATS has no durable publish ack.
func (t *Transport) Send(ctx context.Context, service string, reply courier.Route, data []byte) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if len(data) > t.opts.MaxFrameBytes {
		return courier.ErrTooLarge
	}
	if !t.conn.IsConnected() {
		return sdk.ErrDisconnected
	}
	e, err := courier.DecodeEnvelope(data, t.opts.MaxFrameBytes)
	if err != nil {
		return err
	}
	if e.Kind != "request" || e.Service != service || e.Reply != reply || !validSubject(reply.Topic) || reply.Partition != -1 {
		return fmt.Errorf("nats: invalid request route")
	}
	key := reply.Topic + "." + e.ID
	e.Payload = nil
	t.mu.Lock()
	if t.closed {
		t.mu.Unlock()
		return courier.ErrClosed
	}
	if len(t.pending) >= t.opts.MaxPending {
		t.mu.Unlock()
		return courier.ErrOverloaded
	}
	if _, ok := t.pending[key]; ok {
		t.mu.Unlock()
		return fmt.Errorf("nats: duplicate request")
	}
	r := &request{e: e}
	t.pending[key] = r
	r.stop = context.AfterFunc(ctx, func() { t.forget(key) })
	t.mu.Unlock()
	if err = t.conn.PublishRequest(t.subject(service), key, data); err != nil {
		t.forget(key)
		return err
	}
	if err = t.flush(ctx); err != nil {
		t.forget(key)
		return err
	}
	return nil
}
func (t *Transport) Listen(parent context.Context, service string, h courier.Handler) (courier.Subscription, error) {
	if err := parent.Err(); err != nil {
		return nil, err
	}
	if service == "" || h == nil {
		return nil, fmt.Errorf("nats: invalid service")
	}
	ctx, cancel := context.WithCancel(parent)
	s := &subscription{t: t, cancel: cancel, done: make(chan struct{}), service: true}
	subject := t.subject(service)
	sub, err := t.conn.QueueSubscribeSync(subject, subject+".workers")
	if err != nil {
		cancel()
		return nil, err
	}
	s.sub = sub
	if err = sub.SetPendingLimits(t.opts.PendingMessages, t.opts.PendingMessages*t.opts.MaxFrameBytes); err != nil {
		s.stop()
		return nil, err
	}
	if err = t.add(s); err != nil {
		s.stop()
		return nil, err
	}
	if err = t.flush(ctx); err != nil {
		s.finished()
		return nil, err
	}
	var wg sync.WaitGroup
	for i := 0; i < t.opts.Workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				m, err := sub.NextMsgWithContext(ctx)
				if err != nil {
					if err == sdk.ErrSlowConsumer {
						t.opts.Logger.Warn("NATS service pending limit exceeded", "service", service)
						continue
					}
					return
				}
				e, err := courier.DecodeEnvelope(m.Data, t.opts.MaxFrameBytes)
				if err != nil || e.Kind != "request" || e.Service != service || !validSubject(e.Reply.Topic) || m.Reply != e.Reply.Topic+"."+e.ID {
					continue
				}
				out, err := h(ctx, m.Data)
				if err != nil {
					t.opts.Logger.Warn("NATS request rejected", "service", service, "error", err)
					continue
				}
				if len(out) > t.opts.MaxFrameBytes {
					continue
				}
				if err = m.Respond(out); err != nil {
					t.opts.Logger.Warn("NATS response publish failed", "service", service, "error", err)
				}
			}
		}()
	}
	go func() {
		wg.Wait()
		// Subscription drain does not flush replies published by handlers.
		// Finish that round trip before callers can close their SDK connection.
		flush, done := context.WithTimeout(context.Background(), t.opts.FlushTimeout)
		s.finishErr = t.flush(flush)
		done()
		s.finished()
	}()
	return s, nil
}
func (t *Transport) Close(ctx context.Context) error {
	t.mu.Lock()
	t.closed = true
	subs := make([]*subscription, 0, len(t.subs))
	for s := range t.subs {
		subs = append(subs, s)
	}
	keys := make([]string, 0, len(t.pending))
	for k := range t.pending {
		keys = append(keys, k)
	}
	t.mu.Unlock()
	for _, s := range subs {
		s.stop()
	}
	for _, key := range keys {
		t.forget(key)
	}
	for _, s := range subs {
		if err := s.Close(ctx); err != nil {
			return err
		}
	}
	return nil
}

type Stats struct{ Subscriptions, Pending int }

func (t *Transport) Stats() Stats {
	t.mu.Lock()
	defer t.mu.Unlock()
	return Stats{len(t.subs), len(t.pending)}
}
