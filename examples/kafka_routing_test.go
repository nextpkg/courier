//go:build integration

package examples_test

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/nextpkg/courier"
	"github.com/nextpkg/courier/examples/brokeradmin"
	"github.com/nextpkg/courier/examples/gen/demo"
	"github.com/nextpkg/courier/examples/handlers"
	transport "github.com/nextpkg/courier/transport/kafka"
	"github.com/twmb/franz-go/pkg/kgo"
)

func waitFor(t *testing.T, d time.Duration, condition func() bool, message string) {
	t.Helper()
	end := time.Now().Add(d)
	for {
		if condition() {
			return
		}
		if time.Now().After(end) {
			t.Fatal(message)
		}
		time.Sleep(10 * time.Millisecond)
	}
}
func kafkaClient(t *testing.T, tr courier.Transport) *courier.Client {
	t.Helper()
	c, err := courier.NewClient(context.Background(), tr, courier.ClientOptions{MaxInFlight: 512, DefaultTimeout: 8 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := c.Close(ctx); err != nil {
			t.Error(err)
		}
	})
	return c
}
func kafkaServer(t *testing.T, tr courier.Transport, h demo.Orders) *courier.Server {
	t.Helper()
	s, err := courier.NewServer(tr, courier.ServerOptions{MaxConcurrent: 128})
	if err != nil {
		t.Fatal(err)
	}
	if err = demo.RegisterOrders(s, h); err != nil {
		t.Fatal(err)
	}
	if err = s.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := s.Close(ctx); err != nil {
			t.Error(err)
		}
	})
	return s
}
func newKafkaEnv(t *testing.T, partitions int32) (*kgo.Client, transport.Options) {
	t.Helper()
	admin := kafkaAdmin(t)
	return admin, provisionKafka(t, admin, fmt.Sprintf("courier.test.%d", time.Now().UnixNano()), partitions)
}
func TestKafkaMethodEvolutionAndFixedTopicCount(t *testing.T) {
	admin, opts := newKafkaEnv(t, 2)
	tr := newKafkaTransport(t, opts)
	old, _ := courier.NewServer(tr, courier.ServerOptions{})
	p := demo.NewOrdersProcessor(&handlers.Orders{})
	delete(p.ProcessorMap(), "Ping")
	_ = old.Register("Orders", p)
	if err := old.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = old.Close(context.Background()) })
	c := kafkaClient(t, newKafkaTransport(t, opts))
	api := demo.NewOrdersAsyncClient(c)
	if _, err := api.Get(context.Background(), "old", 0); err != nil {
		t.Fatal(err)
	}
	if _, err := api.List(context.Background(), []string{"old"}); err != nil {
		t.Fatal(err)
	}
	before, err := brokeradmin.Names(context.Background(), admin)
	if err != nil {
		t.Fatal(err)
	}
	if err = old.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	// Replace the two-method processor with its regenerated three-method version.
	kafkaServer(t, tr, &handlers.Orders{})
	kafkaServer(t, newKafkaTransport(t, opts), &handlers.Orders{})
	for i := 0; i < 4; i++ {
		next := kafkaClient(t, newKafkaTransport(t, opts))
		a := demo.NewOrdersAsyncClient(next)
		if err := a.Ping(context.Background()); err != nil {
			t.Fatal(err)
		}
		for j := 0; j < 5; j++ {
			if _, err := a.Get(context.Background(), fmt.Sprint(i, j), 0); err != nil {
				t.Fatal(err)
			}
		}
	}
	timeout, cancel := context.WithTimeout(context.Background(), 80*time.Millisecond)
	defer cancel()
	_, err = demo.NewOrdersAsyncClientFor(c, "Unprovisioned").Get(timeout, "missing", 0)
	if err == nil {
		t.Fatal("missing topic call succeeded")
	}
	after, err := brokeradmin.Names(context.Background(), admin)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(before, after) {
		t.Fatalf("topics changed with methods/replicas/clients/calls: before=%v after=%v", before, after)
	}
}

type heldOrders struct {
	*handlers.Orders
	started chan struct{}
	release chan struct{}
	once    sync.Once
}

func (h *heldOrders) Get(ctx context.Context, id string, ms int32) (*demo.Item, error) {
	if id == "first" {
		h.once.Do(func() { close(h.started) })
		select {
		case <-h.release:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	return h.Orders.Get(ctx, id, ms)
}
func TestKafkaCommitOrderAndRebalance(t *testing.T) {
	admin, opts := newKafkaEnv(t, 1)
	a := newKafkaTransport(t, opts)
	r := &handlers.Recorder{}
	h := &heldOrders{Orders: &handlers.Orders{Recorder: r}, started: make(chan struct{}), release: make(chan struct{})}
	s1 := kafkaServer(t, a, h)
	c := kafkaClient(t, newKafkaTransport(t, opts))
	api := demo.NewOrdersAsyncClient(c)
	first, err := api.GetAsync(context.Background(), "first", 0)
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-h.started:
	case <-time.After(5 * time.Second):
		t.Fatal("slow handler not entered")
	}
	second, err := api.GetAsync(context.Background(), "second", 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = second.Await(context.Background()); err != nil {
		t.Fatal(err)
	}
	topic, _ := a.RequestTopic("Orders")
	offset, err := brokeradmin.Committed(context.Background(), admin, a.Group(topic), topic, 0)
	if err != nil {
		t.Fatal(err)
	}
	if offset > 0 {
		t.Fatalf("committed %d past unfinished first record", offset)
	}
	b := newKafkaTransport(t, opts)
	s2, _ := courier.NewServer(b, courier.ServerOptions{})
	_ = demo.RegisterOrders(s2, &handlers.Orders{Recorder: r})
	started := make(chan error, 1)
	go func() { started <- s2.Start(context.Background()) }()
	t.Cleanup(func() { _ = s2.Close(context.Background()) })
	close(h.release)
	if _, err = first.Await(context.Background()); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-started:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("rebalance did not complete")
	}
	waitFor(t, 3*time.Second, func() bool {
		n, e := brokeradmin.Committed(context.Background(), admin, a.Group(topic), topic, 0)
		return e == nil && n >= 2
	}, "contiguous completed offsets were not committed")
	if a.Stats().GroupRevocations+b.Stats().GroupRevocations == 0 {
		t.Fatal("test did not observe actual partition revocation")
	}
	for i := 0; i < 20; i++ {
		id := fmt.Sprintf("post-rebalance-%d", i)
		if _, err = api.Get(context.Background(), id, 0); err != nil {
			t.Fatal(err)
		}
		if r.Count("Orders.Get:"+id) != 1 {
			t.Fatal("duplicate/missing execution without failure")
		}
	}
	if err = s1.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err = api.Get(context.Background(), "after-leave", 0); err != nil {
		t.Fatal(err)
	}
}
func TestKafkaSharedTopicRequiresCompleteHandlers(t *testing.T) {
	_, opts := newKafkaEnv(t, 2)
	shared := opts.Prefix + ".request.Orders"
	opts.RequestTopics = map[string]string{"Orders": shared, "Inventory": shared}
	tr := newKafkaTransport(t, opts)
	incomplete, _ := courier.NewServer(tr, courier.ServerOptions{})
	_ = demo.RegisterOrders(incomplete, &handlers.Orders{})
	if err := incomplete.Start(context.Background()); err == nil {
		t.Fatal("partial shared-topic consumer accepted")
	}
	_ = incomplete.Close(context.Background())
	for i := 0; i < 2; i++ {
		s, _ := courier.NewServer(tr, courier.ServerOptions{})
		_ = demo.RegisterOrders(s, &handlers.Orders{})
		_ = demo.RegisterInventory(s, &handlers.Inventory{})
		if err := s.Start(context.Background()); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = s.Close(context.Background()) })
	}
	c := kafkaClient(t, newKafkaTransport(t, opts))
	for i := 0; i < 20; i++ {
		v, err := demo.NewOrdersAsyncClient(c).Get(context.Background(), "A", 0)
		if err != nil || v.Source != "Orders" {
			t.Fatal(v, err)
		}
		v, err = demo.NewInventoryAsyncClient(c).Reserve(context.Background(), "B")
		if err != nil || v.Source != "Inventory" {
			t.Fatal(v, err)
		}
	}
}
func TestKafkaReplyExpansionKeepsInflightRoute(t *testing.T) {
	admin, opts := newKafkaEnv(t, 1)
	h := &heldOrders{Orders: &handlers.Orders{}, started: make(chan struct{}), release: make(chan struct{})}
	s := kafkaServer(t, newKafkaTransport(t, opts), h)
	_ = s
	original := newKafkaTransport(t, opts)
	c := kafkaClient(t, original)
	future, err := demo.NewOrdersAsyncClient(c).GetAsync(context.Background(), "first", 0)
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-h.started:
	case <-time.After(3 * time.Second):
		t.Fatal("handler not entered")
	}
	if err = brokeradmin.Expand(context.Background(), admin, opts.Prefix+".replies", 2); err != nil {
		t.Fatal(err)
	}
	close(h.release)
	v, err := future.Await(context.Background())
	if err != nil || v.ID != "first" {
		t.Fatal(v, err)
	}
	// Old client remains on partition 0. A new server config accepts both partitions;
	// a new client explicitly uses partition 1 without changing old calls.
	_ = s.Close(context.Background())
	serverOpts := opts
	serverOpts.ReplyPartitions = []int32{0, 1}
	kafkaServer(t, newKafkaTransport(t, serverOpts), &handlers.Orders{})
	newOpts := opts
	newOpts.ReplyPartitions = []int32{1}
	next := kafkaClient(t, newKafkaTransport(t, newOpts))
	if _, err = demo.NewOrdersAsyncClient(next).Get(context.Background(), "new-partition", 0); err != nil {
		t.Fatal(err)
	}
	if _, err = demo.NewOrdersAsyncClient(c).Get(context.Background(), "old-partition", 0); err != nil {
		t.Fatal(err)
	}
	if original.Stats().ReplyMatched < 2 || c.Pending() != 0 {
		t.Fatalf("old session route changed: %+v", original.Stats())
	}
}
func TestKafkaReplyReaderReadyAndSessionIsolation(t *testing.T) {
	_, opts := newKafkaEnv(t, 1)
	tr := newKafkaTransport(t, opts)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	p, err := kgo.NewClient(kgo.SeedBrokers(opts.Brokers...), kgo.RecordPartitioner(kgo.ManualPartitioner()))
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	// A concrete end offset must skip even a historical frame with the same
	// session value. Session filtering alone would not prove start positioning.
	history := courier.Envelope{Version: 1, Kind: "response", Service: "Orders", Method: "Get", ID: "historical", Session: "new-session", Reply: courier.Route{Topic: opts.ReplyTopic, Partition: 0}}
	data, err := courier.EncodeEnvelope(history, courier.DefaultMaxFrameBytes)
	if err != nil {
		t.Fatal(err)
	}
	if err = p.ProduceSync(ctx, &kgo.Record{Topic: opts.ReplyTopic, Partition: 0, Value: data}).FirstErr(); err != nil {
		t.Fatal(err)
	}
	got := make(chan string, 4)
	inbox, err := tr.OpenInbox(ctx, "new-session", func(data []byte) { e, _ := courier.DecodeEnvelope(data, courier.DefaultMaxFrameBytes); got <- e.ID })
	if err != nil {
		t.Fatal(err)
	}
	defer closeTestResource(t, inbox)
	for _, session := range []string{"old-session", "new-session"} {
		e := courier.Envelope{Version: 1, Kind: "response", Service: "Orders", Method: "Get", ID: session, Session: session, Reply: inbox.Route()}
		data, _ := courier.EncodeEnvelope(e, courier.DefaultMaxFrameBytes)
		if err = p.ProduceSync(ctx, &kgo.Record{Topic: inbox.Route().Topic, Partition: inbox.Route().Partition, Value: data}).FirstErr(); err != nil {
			t.Fatal(err)
		}
	}
	select {
	case id := <-got:
		if id != "new-session" {
			t.Fatalf("old session delivered: %s", id)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	if tr.Stats().ReplyRecords != 2 || tr.Stats().ReplyMatched != 1 {
		t.Fatalf("readiness/session counters: %+v", tr.Stats())
	}
	if err = tr.Close(ctx); err != nil {
		t.Fatal(err)
	}
	_, err = tr.OpenInbox(ctx, "closed", func([]byte) {})
	if !errors.Is(err, courier.ErrClosed) {
		t.Fatal(err)
	}
}
