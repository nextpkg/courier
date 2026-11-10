package courier

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"sync"
	"time"

	"github.com/apache/thrift/lib/go/thrift"
)

// ClientOptions configures per-client admission, publishing, and deadlines.
// Zero fields select defaults: 256 in-flight calls, 4 publishers, 1 MiB frames,
// and a 30-second timeout for calls without an explicit context deadline.
type ClientOptions struct {
	MaxInFlight    int
	PublishWorkers int
	MaxFrameBytes  int
	DefaultTimeout time.Duration
}

// Client owns one reply inbox and can be shared by generated service clients.
// It does not own the Transport passed to NewClient.
type Client struct {
	t       Transport
	inbox   Inbox
	opts    ClientOptions
	session string
	ctx     context.Context
	cancel  context.CancelFunc
	// Submit and reply completion share the pending map and synchronize here.
	mu      sync.Mutex
	closed  bool
	pending map[string]*Call
	jobs    chan *Call
	workers sync.WaitGroup
	done    chan struct{}
}

// Call is the raw asynchronous result. Generated clients normally return Future.
type Call struct {
	id, service, method string
	ctx                 context.Context
	cancel              context.CancelFunc
	stop                func() bool
	data                []byte
	prepared            PreparedRequest
	done                chan struct{}
	sent                chan struct{}
	sentOnce            sync.Once
	sendErr             error
	payload             []byte
	err                 error
}

func randomID() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(b[:]), nil
}

// NewClient opens the reply inbox before accepting calls.
// ctx controls the client lifetime; canceling it terminates pending calls.
// Construction does not verify that a remote service is available.
func NewClient(ctx context.Context, t Transport, opts ClientOptions) (*Client, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if t == nil {
		return nil, fmt.Errorf("courier: nil transport")
	}
	if opts.MaxInFlight == 0 {
		opts.MaxInFlight = 256
	}
	if opts.PublishWorkers == 0 {
		opts.PublishWorkers = 4
	}
	if opts.MaxFrameBytes == 0 {
		opts.MaxFrameBytes = DefaultMaxFrameBytes
	}
	if opts.DefaultTimeout == 0 {
		opts.DefaultTimeout = 30 * time.Second
	}
	if opts.MaxInFlight < 1 || opts.PublishWorkers < 1 || opts.MaxFrameBytes < 512 || opts.MaxFrameBytes > 1<<30 || opts.DefaultTimeout < 0 {
		return nil, fmt.Errorf("courier: invalid client options")
	}
	session, err := randomID()
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(ctx)
	c := &Client{t: t, opts: opts, session: session, ctx: ctx, cancel: cancel, pending: make(map[string]*Call), jobs: make(chan *Call, opts.MaxInFlight), done: make(chan struct{})}
	if parsed, ok := t.(EnvelopeInboxTransport); ok {
		c.inbox, err = parsed.OpenEnvelopeInbox(ctx, session, c.receiveEnvelope)
	} else {
		c.inbox, err = t.OpenInbox(ctx, session, c.receive)
	}
	if err != nil {
		cancel()
		return nil, err
	}
	for i := 0; i < opts.PublishWorkers; i++ {
		c.workers.Add(1)
		go c.publishLoop()
	}
	go func() {
		c.workers.Wait()
		for {
			select {
			case x := <-c.jobs:
				x.data = nil
				x.prepared = nil
			default:
				close(c.done)
				return
			}
		}
	}()
	go func() {
		select {
		case <-ctx.Done():
		case <-c.inbox.Done():
		}
		c.abort()
	}()
	return c, nil
}

// Submit serializes a request and admits it to the bounded publishing queue.
// ctx controls the call lifetime; successful admission does not confirm delivery.
func (c *Client) Submit(ctx context.Context, service, method string, args thrift.TStruct) (*Call, error) {
	if service == "" || method == "" {
		return nil, fmt.Errorf("courier: empty service or method")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if _, ok := ctx.Deadline(); !ok {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, c.opts.DefaultTimeout)
		return c.submit(ctx, cancel, service, method, args)
	}
	ctx, cancel := context.WithCancel(ctx)
	return c.submit(ctx, cancel, service, method, args)
}
func (c *Client) submit(ctx context.Context, cancel context.CancelFunc, service, method string, args thrift.TStruct) (*Call, error) {
	payload, err := encodeCall(ctx, method, args, c.opts.MaxFrameBytes)
	if err != nil {
		cancel()
		return nil, err
	}
	id, err := randomID()
	if err != nil {
		cancel()
		return nil, err
	}
	deadline, _ := ctx.Deadline()
	e := Envelope{Version: ProtocolVersion, Kind: "request", Service: service, Method: method, ID: id, Session: c.session, Deadline: deadline.UnixNano(), Reply: c.inbox.Route(), Payload: payload}
	var data []byte
	var prepared PreparedRequest
	if transport, ok := c.t.(RequestPreparer); ok {
		prepared, err = transport.PrepareRequest(ctx, e, c.opts.MaxFrameBytes)
		if err == nil && prepared == nil {
			err = fmt.Errorf("courier: nil prepared request")
		}
	} else {
		data, err = EncodeEnvelope(e, c.opts.MaxFrameBytes)
	}
	if err != nil {
		cancel()
		return nil, err
	}
	x := &Call{id: id, service: service, method: method, ctx: ctx, cancel: cancel, data: data, prepared: prepared, done: make(chan struct{}), sent: make(chan struct{})}
	c.mu.Lock()
	if c.closed || c.ctx.Err() != nil {
		c.mu.Unlock()
		cancel()
		return nil, ErrClosed
	}
	if len(c.pending) >= c.opts.MaxInFlight {
		c.mu.Unlock()
		cancel()
		return nil, ErrOverloaded
	}
	c.pending[id] = x
	x.stop = context.AfterFunc(ctx, func() { c.finish(id, nil, ctx.Err()) })
	select {
	case c.jobs <- x:
		c.mu.Unlock()
		return x, nil
	default:
		delete(c.pending, id)
		c.mu.Unlock()
		x.stop()
		cancel()
		return nil, ErrOverloaded
	}
}
func (c *Client) publishLoop() {
	defer c.workers.Done()
	for {
		select {
		case <-c.ctx.Done():
			return
		case x := <-c.jobs:
			if x.ctx.Err() != nil {
				x.data = nil
				x.prepared = nil
				continue
			}
			var err error
			if x.prepared != nil {
				err = x.prepared.Send(x.ctx)
			} else {
				err = c.t.Send(x.ctx, x.service, c.inbox.Route(), x.data)
			}
			x.data = nil
			x.prepared = nil
			if err != nil {
				err = &TransportError{Cause: err}
			}
			x.markSent(err)
			if err != nil {
				c.finish(x.id, nil, err)
			}
		}
	}
}
func (x *Call) markSent(err error) { x.sentOnce.Do(func() { x.sendErr = err; close(x.sent) }) }
func (c *Client) receive(data []byte) {
	e, err := DecodeEnvelope(data, c.opts.MaxFrameBytes)
	if err != nil {
		return
	}
	c.receiveEnvelope(e, len(data))
}
// Parsed delivery avoids another envelope decode. Correlation and size checks
// still apply before completing a pending call.
func (c *Client) receiveEnvelope(e Envelope, frameBytes int) {
	if frameBytes < 0 || frameBytes > c.opts.MaxFrameBytes || e.Version != ProtocolVersion || e.Kind != "response" || e.Session != c.session || e.ID == "" || e.Service == "" || e.Method == "" {
		return
	}

	c.mu.Lock()
	x := c.pending[e.ID]
	valid := x != nil && x.service == e.Service && x.method == e.Method
	c.mu.Unlock()
	if !valid {
		return
	}
	// A valid reply (including broker errors) proves successful publication.
	x.markSent(nil)
	if e.Error != nil {
		c.finish(e.ID, nil, e.Error)
	} else {
		c.finish(e.ID, e.Payload, nil)
	}
}
func (c *Client) finish(id string, payload []byte, err error) {
	c.mu.Lock()
	x := c.pending[id]
	if x != nil {
		delete(c.pending, id)
	}
	c.mu.Unlock()
	if x == nil {
		return
	}
	x.stop()
	x.payload = payload
	x.err = err
	// If cancellation wins before Send returns, delivery is indeterminate.
	x.markSent(err)
	close(x.done)
	x.cancel()
}
func (c *Client) Pending() int { c.mu.Lock(); defer c.mu.Unlock(); return len(c.pending) }
func (c *Client) abort() {
	c.mu.Lock()
	c.closed = true
	ids := make([]string, 0, len(c.pending))
	for id := range c.pending {
		ids = append(ids, id)
	}
	c.mu.Unlock()
	for _, id := range ids {
		c.finish(id, nil, ErrClosed)
	}
	c.cancel()
}

// Drain rejects new calls, waits for admitted calls, then closes the inbox.
// A deadline aborts remaining calls. Close the caller-owned Transport afterwards.
func (c *Client) Drain(ctx context.Context) error {
	c.mu.Lock()
	c.closed = true
	calls := make([]*Call, 0, len(c.pending))
	for _, call := range c.pending {
		calls = append(calls, call)
	}
	c.mu.Unlock()
	for _, call := range calls {
		select {
		case <-call.done:
		case <-ctx.Done():
			_ = c.Close(ctx)
			return ctx.Err()
		}
	}
	return c.Close(ctx)
}

// Close rejects new calls, terminates pending calls, and closes the reply inbox.
// ctx bounds cleanup; Close does not close the caller-owned transport.
func (c *Client) Close(ctx context.Context) error {
	c.abort()
	err := c.inbox.Close(ctx)
	select {
	case <-c.done:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}
func (x *Call) ID() string            { return x.id }
func (x *Call) Done() <-chan struct{} { return x.done }

// Sent waits for the transport publication result, not business completion.
// Canceling ctx ends this wait without canceling the submitted call.
func (x *Call) Sent(ctx context.Context) error {
	select {
	case <-x.sent:
		return x.sendErr
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Await waits for the response payload or call error.
// Canceling ctx ends only this wait. The returned payload must not be mutated.
func (x *Call) Await(ctx context.Context) ([]byte, error) {
	select {
	case <-x.done:
		return x.payload, x.err
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// ThriftClient adapts the official synchronous generated client to this core.
type ThriftClient struct {
	Client  *Client
	Service string
}

func (t ThriftClient) Call(ctx context.Context, method string, args, result thrift.TStruct) (thrift.ResponseMeta, error) {
	if nilStruct(result) {
		return thrift.ResponseMeta{}, fmt.Errorf("courier: oneway calls are not supported")
	}
	x, err := t.Client.Submit(ctx, t.Service, method, args)
	if err != nil {
		return thrift.ResponseMeta{}, err
	}
	payload, err := x.Await(ctx)
	if err != nil {
		return thrift.ResponseMeta{}, err
	}
	return thrift.ResponseMeta{}, decodeResult(ctx, method, payload, result, t.Client.opts.MaxFrameBytes)
}
