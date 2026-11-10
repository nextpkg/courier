// Package kafka implements unary RPC using preprovisioned request/reply topics.
package kafka

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"hash/fnv"
	"io"
	"log/slog"
	"net"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nextpkg/courier"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
	"github.com/twmb/franz-go/pkg/sasl"
)

// Options configures Kafka routing, per-transport limits, and SDK connections.
// Topics must exist before use; New applies defaults to zero-valued optional fields.
type Options struct {

	// ReplyRouter enables embedded, consumer-group reply forwarding.
	ReplyRouter *ReplyRouterOptions
	// DynamicReplyPartitions validates replies against live topic metadata (server side).
	DynamicReplyPartitions bool

	Brokers                                                               []string
	Prefix                                                                string
	RequestTopics                                                         map[string]string
	ReplyTopic                                                            string
	ReplyPartitions                                                       []int32
	Workers, Window, PublishBuffers, MaxFrameBytes                        int
	MaxTopics, MaxTopicPartitions, MaxSubscriptions                       int
	OperationTimeout, ProcessingTimeout, RebalanceTimeout, StartupTimeout time.Duration
	// MetadataMinAge is the SDK metadata refresh throttle for producers and readers.
	// Zero selects 250 ms; values below 10 ms are rejected. It is not a recovery deadline.
	MetadataMinAge time.Duration
	// ClientID identifies SDK connections created by this transport.
	ClientID  string
	TLSConfig *tls.Config
	SASL      []sasl.Mechanism
	Dialer    func(context.Context, string, string) (net.Conn, error)
	Hooks     []kgo.Hook
	Logger    *slog.Logger
}
type metadata struct {
	partitions []int32
	expires    time.Time
}

// Transport owns its Kafka producer, consumers, and reply readers.
// Close client and server subscriptions before closing the transport.
type Transport struct {
	router *replyRouter

	inboxGate          chan struct{}
	readers            map[courier.Route]*replyReader
	opts               Options
	producer           *kgo.Client
	publishCompletions *publishCompletionPool
	ctx                context.Context
	cancel             context.CancelFunc
	mu                 sync.Mutex
	closed             bool
	subs               map[*subscription]bool
	topics             map[string]metadata
	closeOnce          sync.Once
	done               chan struct{}
	// Consumption and delivery update these counters concurrently. They are
	// atomic but not cache-line isolated; atomicity does not remove contention.
	readRecords, readBytes, matched, handled, responded, committed, restarts, discarded atomic.Uint64

	assignments, revocations, matchedBytes atomic.Uint64
}
type subscription struct {
	t      *Transport
	cancel context.CancelFunc
	done   chan struct{}
	route  courier.Route
}

func (s *subscription) Route() courier.Route  { return s.route }
func (s *subscription) Done() <-chan struct{} { return s.done }
func (s *subscription) Close(ctx context.Context) error {
	s.cancel()
	select {
	case <-s.done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
func (s *subscription) finish() {
	s.cancel()
	s.t.mu.Lock()
	delete(s.t.subs, s)
	s.t.mu.Unlock()
	close(s.done)
}
func hash(s string) uint32 { h := fnv.New32a(); _, _ = h.Write([]byte(s)); return h.Sum32() }
func validTopic(s string) bool {
	if len(s) == 0 || len(s) > 249 || s == "." || s == ".." {
		return false
	}
	for _, c := range s {
		if (c < 'a' || c > 'z') && (c < 'A' || c > 'Z') && (c < '0' || c > '9') && c != '.' && c != '_' && c != '-' {
			return false
		}
	}
	return true
}

// Requests hash their ID; replies use their original explicit partition.
type partitioner struct{}

func (partitioner) ForTopic(string) kgo.TopicPartitioner { return partitioner{} }
func (partitioner) RequiresConsistency(*kgo.Record) bool { return true }
func (partitioner) Partition(r *kgo.Record, n int) int {
	if r.Partition >= 0 {
		return int(r.Partition)
	}
	return int(hash(string(r.Key)) % uint32(n))
}
func normalize(o Options) (Options, error) {
	if len(o.Brokers) == 0 {
		return o, fmt.Errorf("kafka: Brokers required")
	}
	o.Brokers = append([]string(nil), o.Brokers...)
	if o.Prefix == "" {
		o.Prefix = "courier"
	}
	if o.ReplyTopic == "" {
		o.ReplyTopic = o.Prefix + ".replies"
	}
	if len(o.ReplyPartitions) == 0 {
		o.ReplyPartitions = []int32{0}
	} else {
		o.ReplyPartitions = append([]int32(nil), o.ReplyPartitions...)
	}
	if o.Workers == 0 {
		o.Workers = 16
	}
	if o.Window == 0 {
		o.Window = 64
	}
	if o.PublishBuffers == 0 {
		o.PublishBuffers = 256
	}
	if o.MaxFrameBytes == 0 {
		o.MaxFrameBytes = courier.DefaultMaxFrameBytes
	}
	if o.MaxTopics == 0 {
		o.MaxTopics = 128
	}
	if o.MaxTopicPartitions == 0 {
		o.MaxTopicPartitions = 64
	}
	if o.MaxSubscriptions == 0 {
		o.MaxSubscriptions = 128
	}
	if o.MetadataMinAge == 0 {
		o.MetadataMinAge = 250 * time.Millisecond
	}
	if o.OperationTimeout == 0 {
		o.OperationTimeout = 5 * time.Second
	}
	if o.ProcessingTimeout == 0 {
		o.ProcessingTimeout = 30 * time.Second
	}
	if o.StartupTimeout == 0 {
		o.StartupTimeout = 15 * time.Second
	}
	if o.Logger == nil {
		o.Logger = slog.Default()
	}
	if !validTopic(o.Prefix) || !validTopic(o.ReplyTopic) || o.Workers < 1 || o.Window < o.Workers || o.PublishBuffers < 1 || o.MaxFrameBytes < 512 || o.MaxFrameBytes > 16<<20 || o.MaxTopics < 1 || o.MaxTopicPartitions < 1 || o.MaxSubscriptions < 1 || o.MetadataMinAge < 10*time.Millisecond || o.OperationTimeout < time.Second || o.ProcessingTimeout <= 0 || o.StartupTimeout <= 0 {
		return o, fmt.Errorf("kafka: invalid limits, topic, or rebalance deadline budget")
	}
	// Queued records also need time to drain before a rebalance. Budget every
	// worker wave plus one commit, not just one handler. Broker max-poll policy
	// may require smaller windows or processing limits for a given deployment.
	waves := 1 + (o.Window-1)/o.Workers
	drainBudget := time.Duration(waves)*(o.ProcessingTimeout+o.OperationTimeout) + o.OperationTimeout
	if drainBudget <= 0 || drainBudget > 24*time.Hour {
		return o, fmt.Errorf("kafka: rebalance drain budget exceeds 24 hours")
	}
	if o.RebalanceTimeout == 0 {
		o.RebalanceTimeout = drainBudget + 5*time.Second
	}
	if o.RebalanceTimeout <= drainBudget {
		return o, fmt.Errorf("kafka: RebalanceTimeout must exceed the complete window drain budget %s", drainBudget)
	}
	seen := map[int32]bool{}
	for _, p := range o.ReplyPartitions {
		if p < 0 || seen[p] {
			return o, fmt.Errorf("kafka: invalid reply partition list")
		}
		seen[p] = true
	}
	mappings := map[string]string{}
	for service, topic := range o.RequestTopics {
		if service == "" || !validTopic(topic) || topic == o.ReplyTopic {
			return o, fmt.Errorf("kafka: invalid request topic mapping")
		}
		mappings[service] = topic
	}
	o.RequestTopics = mappings
	if o.Dialer != nil && o.TLSConfig != nil {
		return o, fmt.Errorf("kafka: use TLSConfig or a TLS-capable Dialer, not both")
	}
	if o.ClientID == "" {
		o.ClientID = "courier"
	}
	if o.TLSConfig != nil {
		o.TLSConfig = o.TLSConfig.Clone()
	}
	o.SASL = append([]sasl.Mechanism(nil), o.SASL...)
	o.Hooks = append([]kgo.Hook(nil), o.Hooks...)
	return o, nil
}
func (o Options) base() []kgo.Opt {
	var out []kgo.Opt
	if o.DynamicReplyPartitions || o.ReplyRouter != nil {
		out = append(out, kgo.MetadataMaxAge(time.Second))
	}
	if o.Dialer != nil {
		out = append(out, kgo.Dialer(o.Dialer))
	}
	if o.TLSConfig != nil {
		out = append(out, kgo.DialTLSConfig(o.TLSConfig))
	}
	if len(o.SASL) > 0 {
		out = append(out, kgo.SASL(o.SASL...))
	}
	if len(o.Hooks) > 0 {
		out = append(out, kgo.WithHooks(o.Hooks...))
	}
	fetchBytes := min(4*o.MaxFrameBytes, 24<<20)
	return append(out, kgo.SeedBrokers(o.Brokers...), kgo.ClientID(o.ClientID), kgo.MetadataMinAge(o.MetadataMinAge), kgo.RequestTimeoutOverhead(o.OperationTimeout), kgo.BrokerMaxReadBytes(int32(32<<20)), kgo.FetchMaxBytes(int32(fetchBytes)), kgo.FetchMaxPartitionBytes(int32(o.MaxFrameBytes+1024)), kgo.FetchMaxWait(25*time.Millisecond), kgo.MaxConcurrentFetches(1), kgo.MaxDecompressBatchBytes(max(4*o.MaxFrameBytes, 4<<20)))
}

// New validates the shared reply topic. ctx bounds startup, not transport life.
func New(ctx context.Context, opts Options) (*Transport, error) {
	o, err := normalize(opts)
	if err != nil {
		return nil, err
	}
	pc := o.base()
	pc = append(pc, kgo.ProducerLinger(0))
	pc = append(pc, kgo.RecordPartitioner(partitioner{}), kgo.RequiredAcks(kgo.AllISRAcks()), kgo.MaxBufferedRecords(o.PublishBuffers), kgo.MaxBufferedBytes(o.PublishBuffers*(o.MaxFrameBytes+512)), kgo.ProducerBatchMaxBytes(int32(o.MaxFrameBytes+1024)), kgo.RecordDeliveryTimeout(o.OperationTimeout))
	p, err := kgo.NewClient(pc...)
	if err != nil {
		return nil, err
	}
	life, cancel := context.WithCancel(context.Background())
	t := &Transport{inboxGate: make(chan struct{}, 1), opts: o, producer: p, publishCompletions: newPublishCompletionPool(o.PublishBuffers), ctx: life, cancel: cancel, subs: map[*subscription]bool{}, topics: map[string]metadata{}, done: make(chan struct{})}
	start, stop := context.WithTimeout(ctx, o.StartupTimeout)
	defer stop()
	parts, err := t.partitions(start, o.ReplyTopic)
	if err == nil {
		for _, want := range o.ReplyPartitions {
			found := false
			for _, p := range parts {
				if p == want {
					found = true
				}
			}
			if !found {
				err = fmt.Errorf("kafka: reply partition %d not provisioned", want)
				break
			}
		}
	}
	if err != nil {
		cancel()
		p.Close()
		return nil, err
	}
	if o.ReplyRouter != nil {
		t.router, err = t.startReplyRouter(start)
		if err != nil {
			cancel()
			p.Close()
			return nil, err
		}
	}
	return t, nil
}
func (t *Transport) RequestTopic(service string) (string, error) {
	if service == "" {
		return "", fmt.Errorf("kafka: service name required")
	}
	topic := t.opts.RequestTopics[service]
	if topic == "" {
		topic = t.opts.Prefix + ".request." + service
	}
	if !validTopic(topic) || topic == t.opts.ReplyTopic {
		return "", fmt.Errorf("kafka: invalid physical request topic; configure RequestTopics")
	}
	return topic, nil
}
func (t *Transport) Group(topic string) string { return t.opts.Prefix + ".servers." + topic }
func (t *Transport) partitions(ctx context.Context, topic string) ([]int32, error) {
	t.mu.Lock()
	if t.closed {
		t.mu.Unlock()
		return nil, courier.ErrClosed
	}
	m, exists := t.topics[topic]
	if exists && time.Now().Before(m.expires) {
		t.mu.Unlock()
		return m.partitions, nil
	}
	if !exists && len(t.topics) >= t.opts.MaxTopics {
		t.mu.Unlock()
		return nil, courier.ErrOverloaded
	}
	t.mu.Unlock()
	req := kmsg.NewPtrMetadataRequest()
	req.AllowAutoTopicCreation = false
	rt := kmsg.NewMetadataRequestTopic()
	rt.Topic = &topic
	req.Topics = []kmsg.MetadataRequestTopic{rt}
	ctx, cancel := context.WithTimeout(ctx, t.opts.OperationTimeout)
	defer cancel()
	var resp *kmsg.MetadataResponse
	for {
		var err error
		resp, err = req.RequestWith(ctx, t.producer)
		if err == nil {
			for _, mt := range resp.Topics {
				if mt.Topic != nil && *mt.Topic == topic {
					err = kerr.ErrorForCode(mt.ErrorCode)
					if err == nil {
						for _, p := range mt.Partitions {
							if e := kerr.ErrorForCode(p.ErrorCode); e != nil {
								err = e
								break
							}
						}
					}
					break
				}
			}
		}
		if err == nil {
			break
		}
		if !kerr.IsRetriable(err) {
			return nil, err
		}
		timer := time.NewTimer(25 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, fmt.Errorf("kafka: topic %s not ready (%v): %w", topic, err, ctx.Err())
		case <-timer.C:
		}
	}
	var parts []int32
	for _, r := range resp.Topics {
		if r.Topic == nil || *r.Topic != topic {
			continue
		}
		if err := kerr.ErrorForCode(r.ErrorCode); err != nil {
			return nil, fmt.Errorf("kafka: topic %s must be provisioned: %w", topic, err)
		}
		for _, p := range r.Partitions {
			if err := kerr.ErrorForCode(p.ErrorCode); err != nil {
				return nil, err
			}
			parts = append(parts, p.Partition)
		}
	}
	if len(parts) == 0 || len(parts) > t.opts.MaxTopicPartitions {
		return nil, fmt.Errorf("kafka: topic %s violates partition budget", topic)
	}
	sort.Slice(parts, func(i, j int) bool { return parts[i] < parts[j] })
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.closed {
		return nil, courier.ErrClosed
	}
	if _, ok := t.topics[topic]; !ok && len(t.topics) >= t.opts.MaxTopics {
		return nil, courier.ErrOverloaded
	}
	t.topics[topic] = metadata{parts, time.Now().Add(t.metadataTTL())}
	return parts, nil
}

func (t *Transport) publish(ctx context.Context, r *kgo.Record) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	t.mu.Lock()
	closed := t.closed
	pool := t.publishCompletions
	t.mu.Unlock()
	if closed {
		return courier.ErrClosed
	}
	ctx, cancel := context.WithTimeout(ctx, t.opts.OperationTimeout)
	defer cancel()
	completion := pool.acquire()
	defer completion.release()
	t.producer.TryProduce(ctx, r, completion.complete)
	select {
	case err := <-completion.ready:
		if errors.Is(err, kgo.ErrMaxBuffered) {
			return courier.ErrOverloaded
		}
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}
func (t *Transport) Send(ctx context.Context, service string, reply courier.Route, data []byte) error {
	if len(data) > t.opts.MaxFrameBytes {
		return courier.ErrTooLarge
	}
	e, err := courier.DecodeEnvelope(data, t.opts.MaxFrameBytes)
	if err != nil {
		return err
	}
	if e.Kind != "request" || e.Service != service || e.Reply != reply || !t.validReply(reply) {
		return fmt.Errorf("kafka: invalid request route")
	}
	topic, err := t.RequestTopic(service)
	if err != nil {
		return err
	}
	if _, err = t.partitions(ctx, topic); err != nil {
		return err
	}
	var headers []kgo.RecordHeader
	if t.router != nil {
		parts, err := t.partitions(ctx, t.opts.ReplyTopic)
		if err != nil {
			return err
		}
		partition := parts[int(hash(e.ID))%len(parts)]
		if partition != e.Reply.Partition {
			e.Reply.Partition = partition
			data, err = courier.EncodeEnvelope(e, t.opts.MaxFrameBytes)
			if err != nil {
				return err
			}
		}
		h, err := t.router.routeHeader(e)
		if err != nil {
			return err
		}
		headers = []kgo.RecordHeader{h}
	}
	return t.publish(ctx, &kgo.Record{Topic: topic, Partition: -1, Key: []byte(e.ID), Value: append([]byte(nil), data...), Headers: headers})
}

// Preparation owns immutable encoded bytes. Send hands them to a fresh producer
// record without decoding/re-encoding; preparedRequest never recycles that storage.
type preparedRequest struct {
	transport  *Transport
	topic      string
	key, value []byte
	headers    []kgo.RecordHeader
}

func (p *preparedRequest) Send(ctx context.Context) error {
	return p.transport.publish(ctx, &kgo.Record{Topic: p.topic, Partition: -1, Key: p.key, Value: p.value, Headers: p.headers})
}
func (t *Transport) PrepareRequest(ctx context.Context, e courier.Envelope, limit int) (courier.PreparedRequest, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if limit > t.opts.MaxFrameBytes {
		limit = t.opts.MaxFrameBytes
	}
	if e.Version != courier.ProtocolVersion || e.Kind != "request" || e.ID == "" || e.Session == "" || e.Service == "" || e.Method == "" || !t.validReply(e.Reply) {
		return nil, fmt.Errorf("kafka: invalid request envelope")
	}
	topic, err := t.RequestTopic(e.Service)
	if err != nil {
		return nil, err
	}
	if _, err = t.partitions(ctx, topic); err != nil {
		return nil, err
	}
	if t.router != nil {
		parts, err := t.partitions(ctx, t.opts.ReplyTopic)
		if err != nil {
			return nil, err
		}
		e.Reply.Partition = parts[int(hash(e.ID))%len(parts)]
	}
	data, err := courier.EncodeEnvelope(e, limit)
	if err != nil {
		return nil, err
	}
	var headers []kgo.RecordHeader
	if t.router != nil {
		h, err := t.router.routeHeader(e)
		if err != nil {
			return nil, err
		}
		headers = []kgo.RecordHeader{h}
	}
	return &preparedRequest{transport: t, topic: topic, key: []byte(e.ID), value: data, headers: headers}, nil
}

func (t *Transport) validReply(r courier.Route) bool {
	if r.Topic != t.opts.ReplyTopic {
		return false
	}
	if t.opts.DynamicReplyPartitions || t.opts.ReplyRouter != nil {
		return r.Partition >= 0 && int(r.Partition) < t.opts.MaxTopicPartitions
	}
	for _, p := range t.opts.ReplyPartitions {
		if r.Partition == p {
			return true
		}
	}
	return false
}
func (t *Transport) add(parent context.Context, route courier.Route) (*subscription, context.Context, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.closed {
		return nil, nil, courier.ErrClosed
	}
	if len(t.subs) >= t.opts.MaxSubscriptions {
		return nil, nil, courier.ErrOverloaded
	}
	ctx, cancel := context.WithCancel(parent)
	stop := context.AfterFunc(t.ctx, cancel)
	s := &subscription{t: t, cancel: func() { cancel(); stop() }, done: make(chan struct{}), route: route}
	t.subs[s] = true
	return s, ctx, nil
}
func (t *Transport) Close(ctx context.Context) error {
	t.closeOnce.Do(func() {
		t.mu.Lock()
		t.closed = true
		// In-flight slots retain their pool until both owners finish. Detaching
		// drops the closed transport's cache without closing a callback channel.
		t.publishCompletions = nil
		t.cancel()
		subs := make([]*subscription, 0, len(t.subs))
		for s := range t.subs {
			subs = append(subs, s)
		}
		t.mu.Unlock()
		go func() {
			for _, s := range subs {
				_ = s.Close(context.Background())
			}
			t.inboxGate <- struct{}{}
			readers := make([]*replyReader, 0, len(t.readers))
			for _, r := range t.readers {
				readers = append(readers, r)
			}
			<-t.inboxGate
			for _, r := range readers {
				<-r.done
			}
			if t.router != nil {
				t.router.close()
			}
			t.producer.Close()
			close(t.done)
		}()
	})
	select {
	case <-t.done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

type Stats struct {
	Subscriptions                                                                                                                int
	ReplyRecords, ReplyBytes, ReplyMatched, RequestsHandled, ResponsesSent, CommittedRecords, ConsumerRestarts, DiscardedRecords uint64

	GroupAssignments, GroupRevocations, ReplyMatchedBytes uint64
}

func (t *Transport) Stats() Stats {
	t.mu.Lock()
	n := len(t.subs)
	t.mu.Unlock()
	return Stats{Subscriptions: n, ReplyRecords: t.readRecords.Load(), ReplyBytes: t.readBytes.Load(), ReplyMatched: t.matched.Load(), RequestsHandled: t.handled.Load(), ResponsesSent: t.responded.Load(), CommittedRecords: t.committed.Load(), ConsumerRestarts: t.restarts.Load(), DiscardedRecords: t.discarded.Load(), GroupAssignments: t.assignments.Load(), GroupRevocations: t.revocations.Load(), ReplyMatchedBytes: t.matchedBytes.Load()}
}

func fatalFetch(err error) bool {
	var broker *kerr.Error
	if errors.As(err, &broker) {
		return !broker.Retriable
	}
	var network net.Error
	return !errors.As(err, &network) && !errors.Is(err, io.EOF) && !errors.Is(err, io.ErrUnexpectedEOF)
}
