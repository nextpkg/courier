//go:build integration

package kafka_test

import (
	"context"
	"errors"
	"fmt"
	"github.com/nextpkg/courier"
	"github.com/nextpkg/courier/examples/brokeradmin"
	"github.com/nextpkg/courier/examples/gen/demo"
	"github.com/nextpkg/courier/examples/handlers"
	"github.com/nextpkg/courier/examples/internal/kafkatest"
	"github.com/nextpkg/courier/examples/internal/testutil"
	"net"
	"runtime"
	"sync"
	"testing"
	"time"
)

// cutDialer cuts real TCP connections; it never fabricates Kafka responses.
// Native metadata-discovered broker connections also use this test dialer.
type cutDialer struct {
	mu      sync.Mutex
	offline bool
	conns   map[*cutConn]bool
}
type cutConn struct {
	net.Conn
	owner *cutDialer
}

func (c *cutConn) Close() error {
	err := c.Conn.Close()
	c.owner.mu.Lock()
	delete(c.owner.conns, c)
	c.owner.mu.Unlock()
	return err
}
func (d *cutDialer) Dial(ctx context.Context, network, address string) (net.Conn, error) {
	d.mu.Lock()
	off := d.offline
	d.mu.Unlock()
	if off {
		return nil, &net.OpError{Op: "dial", Net: network, Err: errors.New("test network cut")}
	}
	conn, err := (&net.Dialer{}).DialContext(ctx, network, address)
	if err != nil {
		return nil, err
	}
	c := &cutConn{Conn: conn, owner: d}
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.offline {
		_ = conn.Close()
		return nil, &net.OpError{Op: "dial", Net: network, Err: errors.New("test network cut")}
	}
	if d.conns == nil {
		d.conns = map[*cutConn]bool{}
	}
	d.conns[c] = true
	return c, nil
}
func (d *cutDialer) Cut() {
	d.mu.Lock()
	d.offline = true
	var cs []*cutConn
	for c := range d.conns {
		cs = append(cs, c)
	}
	d.mu.Unlock()
	for _, c := range cs {
		_ = c.Close()
	}
}
func (d *cutDialer) Resume() { d.mu.Lock(); d.offline = false; d.mu.Unlock() }

type effectOrders struct {
	*handlers.Orders
	first func()
	once  sync.Once
}

func (h *effectOrders) Get(ctx context.Context, id string, ms int32) (*demo.Item, error) {
	v, err := h.Orders.Get(ctx, id, ms)
	h.once.Do(h.first)
	return v, err
}
func TestKafkaResponseFailureRedeliveryAndCommit(t *testing.T) {
	admin, opts := kafkatest.NewEnv(t, 1)
	network := &cutDialer{}
	serverOpts := opts
	serverOpts.Dialer = network.Dial
	serverOpts.OperationTimeout = time.Second
	tr := kafkatest.NewTransport(t, serverOpts)
	r := &handlers.Recorder{}
	failed := make(chan struct{})
	release := make(chan struct{})
	h := &effectOrders{Orders: &handlers.Orders{Recorder: r}, first: func() { network.Cut(); close(failed); <-release }}
	testutil.Server(t, tr, h)
	c := testutil.Client(t, kafkatest.NewTransport(t, opts))
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()
	f, err := demo.NewOrdersAsyncClient(c).GetAsync(ctx, "side-effect", 0)
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-failed:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	close(release)
	testutil.WaitFor(t, 5*time.Second, func() bool { return tr.Stats().ConsumerRestarts > 0 }, "response publication failure did not restart uncommitted consumption")
	topic, _ := tr.RequestTopic("Orders")
	offset, err := brokeradmin.Committed(ctx, admin, tr.Group(topic), topic, 0)
	if err != nil {
		t.Fatal(err)
	}
	if offset > 0 {
		t.Fatalf("failed response advanced offset to %d", offset)
	}
	if r.Count("Orders.Get:side-effect") != 1 {
		t.Fatal("unexpected execution before reconnection")
	}
	network.Resume()
	v, err := f.Await(ctx)
	if err != nil || v == nil || v.ID != "side-effect" {
		t.Fatalf("redelivery result=%v %v", v, err)
	}
	if r.Count("Orders.Get:side-effect") < 2 {
		t.Fatal("test did not exercise side-effect replay after failed response")
	}
	testutil.WaitFor(t, 3*time.Second, func() bool {
		n, e := brokeradmin.Committed(context.Background(), admin, tr.Group(topic), topic, 0)
		return e == nil && n >= 1
	}, "response-confirmed record was not committed")
	t.Logf("side effect executions=%d; one client invocation; failed response caused redelivery", r.Count("Orders.Get:side-effect"))
}
func TestKafkaReplyReconnectRetainsOffset(t *testing.T) {
	_, opts := kafkatest.NewEnv(t, 1)
	h := &testutil.HeldOrders{Orders: &handlers.Orders{}, Started: make(chan struct{}), Release: make(chan struct{})}
	testutil.Server(t, kafkatest.NewTransport(t, opts), h)
	network := &cutDialer{}
	clientOpts := opts
	clientOpts.Dialer = network.Dial
	tr := kafkatest.NewTransport(t, clientOpts)
	c := testutil.Client(t, tr)
	api := demo.NewOrdersAsyncClient(c)
	if _, err := api.Get(context.Background(), "before", 0); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	f, err := api.GetAsync(ctx, "first", 0)
	if err != nil {
		t.Fatal(err)
	}
	if err = f.Sent(ctx); err != nil {
		t.Fatal(err)
	}
	select {
	case <-h.Started:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	network.Cut()
	close(h.Release)
	time.Sleep(120 * time.Millisecond)
	network.Resume()
	v, err := f.Await(ctx)
	if err != nil || v == nil || v.ID != "first" {
		t.Fatalf("reconnected reply=%v %v", v, err)
	}
	if _, err = api.Get(ctx, "after", 0); err != nil {
		t.Fatal(err)
	}
	if c.Pending() != 0 || tr.Stats().ReplyMatched < 3 {
		t.Fatalf("reconnect state=%+v pending=%d", tr.Stats(), c.Pending())
	}
}

func TestKafkaExpiredRedeliveryDoesNotRepeatHandler(t *testing.T) {
	admin, opts := kafkatest.NewEnv(t, 1)
	network := &cutDialer{}
	serverOpts := opts
	serverOpts.Dialer = network.Dial
	serverOpts.OperationTimeout = time.Second
	tr := kafkatest.NewTransport(t, serverOpts)
	r := &handlers.Recorder{}
	cut := make(chan struct{})
	h := &effectOrders{Orders: &handlers.Orders{Recorder: r}, first: func() { network.Cut(); close(cut) }}
	testutil.Server(t, tr, h)
	c := testutil.Client(t, kafkatest.NewTransport(t, opts))
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	f, err := demo.NewOrdersAsyncClient(c).GetAsync(ctx, "expired-effect", 0)
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-cut:
	case <-ctx.Done():
		t.Fatal("fault did not occur before deadline")
	}
	if _, err = f.Await(context.Background()); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal(err)
	}
	testutil.WaitFor(t, 5*time.Second, func() bool { return tr.Stats().ConsumerRestarts > 0 }, "no failed-response redelivery path")
	network.Resume()
	topic, _ := tr.RequestTopic("Orders")
	testutil.WaitFor(t, 12*time.Second, func() bool {
		n, e := brokeradmin.Committed(context.Background(), admin, tr.Group(topic), topic, 0)
		return e == nil && n >= 1
	}, "expired redelivery was not answered/committed")
	if r.Count("Orders.Get:expired-effect") != 1 {
		t.Fatal("expired redelivery executed side effect again")
	}
}
func TestKafkaProducerBackpressure(t *testing.T) {
	_, opts := kafkatest.NewEnv(t, 1)
	testutil.Server(t, kafkatest.NewTransport(t, opts), &handlers.Orders{})
	network := &cutDialer{}
	opts.Dialer = network.Dial
	opts.PublishBuffers = 1
	opts.OperationTimeout = time.Second
	tr := kafkatest.NewTransport(t, opts)
	c := testutil.Client(t, tr)
	api := demo.NewOrdersAsyncClient(c)
	if _, err := api.Get(context.Background(), "prime-metadata", 0); err != nil {
		t.Fatal(err)
	}
	var before runtime.MemStats
	runtime.ReadMemStats(&before)
	startGoroutines := runtime.NumGoroutine()
	network.Cut()
	var calls []*courier.Future[*demo.Item]
	for i := 0; i < 8; i++ {
		f, err := api.GetAsync(context.Background(), fmt.Sprint(i), 0)
		if err != nil {
			t.Fatal(err)
		}
		calls = append(calls, f)
	}
	overloaded := 0
	for _, f := range calls {
		_, err := f.Await(context.Background())
		if errors.Is(err, courier.ErrOverloaded) {
			overloaded++
		}
		if err == nil {
			t.Fatal("send succeeded through disconnected broker")
		}
	}
	network.Resume()
	if overloaded == 0 {
		t.Fatal("bounded native producer did not reject overload")
	}
	if c.Pending() != 0 {
		t.Fatal("pending calls leaked after publication failures")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	start := time.Now()
	attempts := 0
	for {
		attempts++
		v, err := api.Get(ctx, "after-backpressure", 0)
		if err == nil {
			if v == nil || v.ID != "after-backpressure" {
				t.Fatalf("recovery mismatch: %v", v)
			}
			break
		}
		if !errors.Is(err, courier.ErrOverloaded) {
			t.Fatalf("pressure recovery: %v", err)
		}
		select {
		case <-ctx.Done():
			t.Fatal("producer did not recover within 10s")
		case <-time.After(20 * time.Millisecond):
		}
	}
	t.Logf("recovery probes=%d (transient full-buffer rejections included)", attempts)
	var after runtime.MemStats
	runtime.ReadMemStats(&after)
	t.Logf("backpressure rejected=%d/%d recovery=%s pending=%d heap_before=%d heap_after=%d goroutines_before=%d goroutines_after=%d", overloaded, len(calls), time.Since(start), c.Pending(), before.HeapAlloc, after.HeapAlloc, startGoroutines, runtime.NumGoroutine())
}
