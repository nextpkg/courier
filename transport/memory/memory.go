// Package memory provides a bounded process-local Watermill transport.
package memory

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/ThreeDotsLabs/watermill/pubsub/gochannel"

	"github.com/nextpkg/courier"
)

type Options struct{ QueueSize, PublishWorkers, HandlerWorkers, MaxFrameBytes int }
type publication struct {
	pipe *pipe
	data []byte
	ctx  context.Context
}
type pipe struct {
	g      *gochannel.GoChannel
	ch     <-chan *message.Message
	cancel context.CancelFunc
	refs   int
}
type Bus struct {
	opts      Options
	ctx       context.Context
	cancel    context.CancelFunc
	mu        sync.Mutex
	closed    bool
	services  map[string]*pipe
	inboxes   map[string]*subscription
	queue     chan publication
	wg        sync.WaitGroup
	done      chan struct{}
	closeOnce sync.Once
	seq       atomic.Uint64
}
type subscription struct {
	cancel  context.CancelFunc
	done    chan struct{}
	once    sync.Once
	cleanup func()
	route   courier.Route
	g       *gochannel.GoChannel
}

func (s *subscription) Route() courier.Route  { return s.route }
func (s *subscription) Done() <-chan struct{} { return s.done }
func (s *subscription) stop() {
	s.once.Do(func() {
		s.cancel()
		if s.cleanup != nil {
			s.cleanup()
		}
	})
}
func (s *subscription) Close(ctx context.Context) error {
	s.stop()
	select {
	case <-s.done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
func channel() *gochannel.GoChannel {
	// Fixed adapter workers are the only callers of blocking Publish. This bounds
	// Watermill's per-publication goroutines. Each logical channel has a separate
	// GoChannel: its blocking publication lock cannot obstruct reply delivery.
	return gochannel.NewGoChannel(gochannel.Config{BlockPublishUntilSubscriberAck: true}, watermill.NopLogger{})
}
func New(opts Options) (*Bus, error) {
	if opts.QueueSize == 0 {
		opts.QueueSize = 256
	}
	if opts.PublishWorkers == 0 {
		opts.PublishWorkers = 4
	}
	if opts.HandlerWorkers == 0 {
		opts.HandlerWorkers = 4
	}
	if opts.MaxFrameBytes == 0 {
		opts.MaxFrameBytes = courier.DefaultMaxFrameBytes
	}
	if opts.QueueSize < 1 || opts.PublishWorkers < 1 || opts.HandlerWorkers < 1 || opts.MaxFrameBytes < 512 || opts.MaxFrameBytes > 1<<30 {
		return nil, fmt.Errorf("memory: invalid options")
	}
	ctx, cancel := context.WithCancel(context.Background())
	b := &Bus{opts: opts, ctx: ctx, cancel: cancel, services: make(map[string]*pipe), inboxes: make(map[string]*subscription), queue: make(chan publication, opts.QueueSize), done: make(chan struct{})}
	for i := 0; i < opts.PublishWorkers; i++ {
		b.wg.Add(1)
		go b.publishLoop()
	}
	return b, nil
}
func (b *Bus) publishLoop() {
	defer b.wg.Done()
	for {
		select {
		case <-b.ctx.Done():
			return
		case p := <-b.queue:
			if p.ctx.Err() != nil {
				continue
			}
			// Send has already confirmed local admission; shutdown may discard this
			// non-durable message. Its caller still has a bounded deadline.
			_ = p.pipe.g.Publish("request", message.NewMessage(fmt.Sprint(b.seq.Add(1)), p.data))
		}
	}
}

// Send confirms bounded local admission, not receipt or business execution.
func (b *Bus) Send(ctx context.Context, service string, _ courier.Route, data []byte) error {
	if len(data) > b.opts.MaxFrameBytes {
		return courier.ErrTooLarge
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return courier.ErrClosed
	}
	p := b.services[service]
	if p == nil || p.refs == 0 {
		return courier.ErrNoResponders
	}
	select {
	case b.queue <- publication{p, append([]byte(nil), data...), ctx}:
		return nil
	default:
		return courier.ErrOverloaded
	}
}
func (b *Bus) OpenInbox(parent context.Context, session string, receive func([]byte)) (courier.Inbox, error) {
	if err := parent.Err(); err != nil {
		return nil, err
	}
	if session == "" || receive == nil {
		return nil, fmt.Errorf("memory: invalid inbox")
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return nil, courier.ErrClosed
	}
	topic := "reply." + session
	if _, ok := b.inboxes[topic]; ok {
		return nil, fmt.Errorf("memory: duplicate inbox")
	}
	ctx, cancel := context.WithCancel(parent)
	stopBus := context.AfterFunc(b.ctx, cancel)
	g := channel()
	ch, err := g.Subscribe(ctx, "reply")
	if err != nil {
		cancel()
		stopBus()
		_ = g.Close()
		return nil, err
	}
	s := &subscription{cancel: cancel, done: make(chan struct{}), route: courier.Route{Topic: topic, Partition: -1}, g: g}
	s.cleanup = func() { b.mu.Lock(); delete(b.inboxes, topic); b.mu.Unlock(); stopBus(); _ = g.Close() }
	b.inboxes[topic] = s
	b.wg.Add(1)
	go func() {
		defer b.wg.Done()
		defer close(s.done)
		defer s.stop()
		for {
			select {
			case <-ctx.Done():
				return
			case msg, ok := <-ch:
				if !ok {
					return
				}
				receive(msg.Payload)
				msg.Ack()
			}
		}
	}()
	return s, nil
}
func (b *Bus) Listen(parent context.Context, service string, h courier.Handler) (courier.Subscription, error) {
	if err := parent.Err(); err != nil {
		return nil, err
	}
	if service == "" || h == nil {
		return nil, fmt.Errorf("memory: invalid listener")
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return nil, courier.ErrClosed
	}
	p := b.services[service]
	if p == nil {
		pc, pcancel := context.WithCancel(b.ctx)
		g := channel()
		ch, err := g.Subscribe(pc, "request")
		if err != nil {
			pcancel()
			_ = g.Close()
			return nil, err
		}
		p = &pipe{g: g, ch: ch, cancel: pcancel}
		b.services[service] = p
	}
	p.refs++
	ctx, cancel := context.WithCancel(parent)
	stopBus := context.AfterFunc(b.ctx, cancel)
	s := &subscription{cancel: cancel, done: make(chan struct{})}
	s.cleanup = func() {
		b.mu.Lock()
		p.refs--
		last := p.refs == 0
		if last {
			delete(b.services, service)
			p.cancel()
		}
		b.mu.Unlock()
		stopBus()
		if last {
			_ = p.g.Close()
		}
	}
	var workers sync.WaitGroup
	for i := 0; i < b.opts.HandlerWorkers; i++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for {
				select {
				case <-ctx.Done():
					return
				case msg, ok := <-p.ch:
					if !ok {
						return
					}
					msg.Ack()
					if ctx.Err() != nil {
						continue
					}
					e, err := courier.DecodeEnvelope(msg.Payload, b.opts.MaxFrameBytes)
					if err != nil {
						continue
					}
					out, err := h(ctx, msg.Payload)
					if err != nil || len(out) > b.opts.MaxFrameBytes {
						continue
					}
					b.mu.Lock()
					inbox := b.inboxes[e.Reply.Topic]
					b.mu.Unlock()
					if inbox != nil {
						_ = inbox.g.Publish("reply", message.NewMessage(fmt.Sprint(b.seq.Add(1)), out))
					}
				}
			}
		}()
	}
	b.wg.Add(1)
	go func() { defer b.wg.Done(); workers.Wait(); s.stop(); close(s.done) }()
	return s, nil
}

// Close cancels all owned subscriptions and waits for workers. A handler that
// ignores cancellation can make this return the supplied context's error.
func (b *Bus) Close(ctx context.Context) error {
	b.closeOnce.Do(func() {
		b.mu.Lock()
		b.closed = true
		b.cancel()
		b.mu.Unlock()
		go func() {
			b.wg.Wait()
			for {
				select {
				case <-b.queue:
				default:
					close(b.done)
					return
				}
			}
		}()
	})
	select {
	case <-b.done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Stats reports adapter-owned resources (useful for lifecycle monitoring).
type Stats struct{ Services, Inboxes, Queued int }

func (b *Bus) Stats() Stats {
	b.mu.Lock()
	defer b.mu.Unlock()
	return Stats{len(b.services), len(b.inboxes), len(b.queue)}
}
