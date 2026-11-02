package courier

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/apache/thrift/lib/go/thrift"
)

type ServerOptions struct {
	MaxConcurrent, MaxFrameBytes int
	Logger                       *slog.Logger
}
type Server struct {
	t        Transport
	opts     ServerOptions
	mu       sync.Mutex
	started  bool
	closed   bool
	services map[string]thrift.TProcessor
	subs     []Subscription
	slots    chan struct{}
	ctx      context.Context
	cancel   context.CancelFunc
}

func NewServer(t Transport, opts ServerOptions) (*Server, error) {
	if t == nil {
		return nil, fmt.Errorf("courier: nil transport")
	}
	if opts.MaxConcurrent == 0 {
		opts.MaxConcurrent = 32
	}
	if opts.MaxFrameBytes == 0 {
		opts.MaxFrameBytes = DefaultMaxFrameBytes
	}
	if opts.MaxConcurrent < 1 || opts.MaxFrameBytes < 512 || opts.MaxFrameBytes > 1<<30 {
		return nil, fmt.Errorf("courier: invalid server options")
	}
	if opts.Logger == nil {
		opts.Logger = slog.Default()
	}
	return &Server{t: t, opts: opts, services: make(map[string]thrift.TProcessor), slots: make(chan struct{}, opts.MaxConcurrent)}, nil
}
func (s *Server) Register(service string, p thrift.TProcessor) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.started || s.closed {
		return fmt.Errorf("courier: register before Start")
	}
	if service == "" || p == nil {
		return fmt.Errorf("courier: invalid registration")
	}
	if _, ok := s.services[service]; ok {
		return fmt.Errorf("courier: duplicate service %s", service)
	}
	s.services[service] = p
	return nil
}

// Start installs all subscriptions before returning. On failure already-opened
// subscriptions are closed, so readiness is never partially advertised.
func (s *Server) Start(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.started || s.closed {
		return fmt.Errorf("courier: already started")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if len(s.services) == 0 {
		return fmt.Errorf("courier: no services")
	}
	s.started = true
	s.ctx, s.cancel = context.WithCancel(ctx)
	if batch, ok := s.t.(ServiceListener); ok {
		handlers := make(map[string]Handler, len(s.services))
		for name, p := range s.services {
			handlers[name] = s.handler(name, p)
		}
		sub, err := batch.ListenServices(s.ctx, handlers)
		if err != nil {
			s.cancel()
			return err
		}
		s.subs = append(s.subs, sub)
		return nil
	}
	for name, p := range s.services {
		sub, err := s.t.Listen(s.ctx, name, s.handler(name, p))
		if err != nil {
			s.cancel()
			cleanup, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			for _, opened := range s.subs {
				_ = opened.Close(cleanup)
			}
			return err
		}
		s.subs = append(s.subs, sub)
	}
	return nil
}
func (s *Server) Serve(ctx context.Context) error {
	if err := s.Start(ctx); err != nil {
		return err
	}
	<-ctx.Done()
	stop, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return s.Close(stop)
}

// Close lets subscriptions drain before canceling their shared context.
// Canceling the Start context remains an immediate shutdown signal.
func (s *Server) Close(ctx context.Context) error {
	s.mu.Lock()
	s.closed = true
	cancel := s.cancel
	subs := append([]Subscription(nil), s.subs...)
	s.mu.Unlock()
	if cancel != nil {
		defer cancel()
	}
	var wg sync.WaitGroup
	results := make(chan error, len(subs))
	for _, sub := range subs {
		wg.Add(1)
		go func() { defer wg.Done(); results <- sub.Close(ctx) }()
	}
	wg.Wait()
	close(results)
	var err error
	for e := range results {
		err = errors.Join(err, e)
	}
	return err
}
func (s *Server) handler(service string, p thrift.TProcessor) Handler {
	return func(parent context.Context, data []byte) (out []byte, err error) {
		e, err := DecodeEnvelope(data, s.opts.MaxFrameBytes)
		if err != nil {
			return nil, err
		}
		if e.Kind != "request" {
			return nil, &RPCError{Code: "protocol", Message: "expected request"}
		}
		r := e
		r.Kind = "response"
		r.Deadline = 0
		r.Payload = nil
		r.Error = nil
		fail := func(code, message string) ([]byte, error) {
			r.Payload = nil
			r.Error = &RPCError{Code: code, Message: message}
			data, err := EncodeEnvelope(r, s.opts.MaxFrameBytes)
			if errors.Is(err, ErrTooLarge) {
				// Processor error text may be arbitrarily large or JSON-escaped.
				// Admission reserved room for this bounded fallback.
				r.Error.Message = "request failed"
				return EncodeEnvelope(r, s.opts.MaxFrameBytes)
			}
			return data, err
		}
		defer func() {
			if v := recover(); v != nil {
				s.opts.Logger.Error("handler panic", "service", service, "method", e.Method)
				out, err = fail("internal", "handler panicked")
			}
		}()
		if e.Service != service {
			return fail("protocol", "service route mismatch")
		}
		if e.Deadline <= 0 {
			return fail("protocol", "request deadline required")
		}
		ctx, cancel := context.WithDeadline(parent, time.Unix(0, e.Deadline))
		defer cancel()
		if ctx.Err() != nil {
			return fail("deadline", ctx.Err().Error())
		}
		select {
		case s.slots <- struct{}{}:
			defer func() { <-s.slots }()
		default:
			return fail("overloaded", "server capacity exceeded")
		}
		// Verify envelope/header consistency before invoking generated dispatch.
		checkBuf, check := binaryProtocol(e.Payload, s.opts.MaxFrameBytes)
		method, kind, seq, checkErr := check.ReadMessageBegin(ctx)
		if checkErr != nil || method != e.Method || kind != thrift.CALL || seq != 0 {
			return fail("protocol", "invalid Thrift call header")
		}
		if _, ok := p.ProcessorMap()[method]; !ok {
			return fail("method_not_found", "unknown method")
		}
		// Validate framing before executing any user code. Thrift owns parsing.
		if err := check.Skip(ctx, thrift.STRUCT); err != nil {
			return fail("protocol", "invalid Thrift arguments")
		}
		if err := check.ReadMessageEnd(ctx); err != nil || checkBuf.Len() != 0 {
			return fail("protocol", "trailing request bytes")
		}
		in, ip := binaryProtocol(e.Payload, s.opts.MaxFrameBytes)
		obuf, op := binaryProtocol(nil, s.opts.MaxFrameBytes)
		_, processErr := p.Process(ctx, ip, op)
		if errors.Is(processErr, ErrTooLarge) {
			return fail("too_large", "response exceeds frame limit")
		}
		if in.Len() != 0 {
			return fail("protocol", "trailing request bytes")
		}
		if ctx.Err() != nil {
			return fail("deadline", ctx.Err().Error())
		}
		if processErr != nil && obuf.Len() == 0 {
			return fail("protocol", processErr.Error())
		}
		r.Payload = append([]byte(nil), obuf.Bytes()...)
		out, err = EncodeEnvelope(r, s.opts.MaxFrameBytes)
		if errors.Is(err, ErrTooLarge) {
			r.Payload = nil
			return fail("too_large", "response exceeds frame limit")
		}
		return out, err
	}
}
