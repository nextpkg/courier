package kafka

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nextpkg/courier"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
)

type groupSubscriptions struct{ subs []courier.Subscription }

func (g *groupSubscriptions) Close(ctx context.Context) error {
	var err error
	for _, s := range g.subs {
		err = errors.Join(err, s.Close(ctx))
	}
	return err
}
func (t *Transport) Listen(ctx context.Context, service string, h courier.Handler) (courier.Subscription, error) {
	return t.ListenServices(ctx, map[string]courier.Handler{service: h})
}

type decodedRequestKey struct{}
type decodedRequest struct {
	envelope courier.Envelope
	size     int
}

// Reuse the envelope already decoded and validated by the transport. Preserve
// its original byte length so the core still enforces its own frame limit.
func (t *Transport) ListenEnvelopeServices(ctx context.Context, handlers map[string]courier.EnvelopeHandler) (courier.Subscription, error) {
	raw := make(map[string]courier.Handler, len(handlers))
	for service, handler := range handlers {
		if handler == nil {
			return nil, fmt.Errorf("kafka: nil handler")
		}
		raw[service] = func(ctx context.Context, data []byte) ([]byte, error) {
			request, ok := ctx.Value(decodedRequestKey{}).(*decodedRequest)
			if !ok {
				return nil, fmt.Errorf("kafka: missing validated envelope")
			}
			return handler(ctx, request.envelope, request.size)
		}
	}
	return t.ListenServices(context.WithValue(ctx, decodedRequestKey{}, true), raw)
}

// ListenServices validates shared-topic aliases before starting any consumer.
// Every replica consuming a topic must register its entire configured bundle.
func (t *Transport) ListenServices(parent context.Context, handlers map[string]courier.Handler) (courier.Subscription, error) {
	if len(handlers) == 0 {
		return nil, fmt.Errorf("kafka: no services")
	}
	groups := map[string]map[string]courier.Handler{}
	for service, h := range handlers {
		if h == nil {
			return nil, fmt.Errorf("kafka: nil handler")
		}
		topic, err := t.RequestTopic(service)
		if err != nil {
			return nil, err
		}
		if groups[topic] == nil {
			groups[topic] = map[string]courier.Handler{}
		}
		groups[topic][service] = h
	}
	for service, topic := range t.opts.RequestTopics {
		if bundle := groups[topic]; bundle != nil && bundle[service] == nil {
			return nil, fmt.Errorf("kafka: shared topic %s requires handler %s on every consumer", topic, service)
		}
	}
	names := make([]string, 0, len(groups))
	for n := range groups {
		names = append(names, n)
	}
	sort.Strings(names)
	all := &groupSubscriptions{}
	for _, topic := range names {
		sub, err := t.listenTopic(parent, topic, groups[topic])
		if err != nil {
			ctx, cancel := context.WithTimeout(context.Background(), t.opts.OperationTimeout)
			_ = all.Close(ctx)
			cancel()
			return nil, err
		}
		all.subs = append(all.subs, sub)
	}
	return all, nil
}
func (t *Transport) listenTopic(parent context.Context, topic string, handlers map[string]courier.Handler) (courier.Subscription, error) {
	start, cancel := context.WithTimeout(parent, t.opts.StartupTimeout)
	defer cancel()
	if _, err := t.partitions(start, topic); err != nil {
		return nil, err
	}
	s, ctx, err := t.add(parent, courier.Route{})
	if err != nil {
		return nil, err
	}
	ready := make(chan error, 1)
	var once sync.Once
	signal := func(err error) { once.Do(func() { ready <- err }) }
	go func() {
		defer s.finish()
		backoff := 100 * time.Millisecond
		for ctx.Err() == nil {
			err := t.consume(ctx, topic, handlers, func() { signal(nil) })
			if ctx.Err() != nil {
				return
			}
			t.restarts.Add(1)
			t.opts.Logger.Warn("Kafka consumer rejoining from committed offsets", "topic", topic, "error", err)
			timer := time.NewTimer(backoff)
			select {
			case <-ctx.Done():
				timer.Stop()
				return
			case <-timer.C:
			}
			if backoff < 2*time.Second {
				backoff *= 2
			}
		}
	}()
	select {
	case err := <-ready:
		return s, err
	case <-start.Done():
		s.cancel()
		stop, done := context.WithTimeout(context.Background(), t.opts.OperationTimeout)
		_ = s.Close(stop)
		done()
		return nil, start.Err()
	}
}

type entry struct {
	record *kgo.Record
	done   bool
	err    error
}
type completion struct {
	entry *entry
	err   error
}

// window is owned by the poll loop; workers only send completion events.
// A consumed offset may skip control records, so continuity is tracked by
// fetched record order, not by assuming consecutive integer offsets.
type window struct {
	partitions   map[int32][]*entry
	size, active int
	failed       bool
	firstError   error
}

func newWindow() *window { return &window{partitions: map[int32][]*entry{}} }
func (w *window) add(r *kgo.Record) *entry {
	e := &entry{record: r}
	w.partitions[r.Partition] = append(w.partitions[r.Partition], e)
	w.size++
	w.active++
	return e
}
func (w *window) complete(c completion) {
	c.entry.done = true
	c.entry.err = c.err
	w.active--
	if c.err != nil {
		w.failed = true
		if w.firstError == nil {
			w.firstError = c.err
		}
	}
}
func (w *window) prefixes() ([]*kgo.Record, map[int32]int) {
	var records []*kgo.Record
	counts := map[int32]int{}
	for p, es := range w.partitions {
		n := 0
		for _, e := range es {
			if !e.done || e.err != nil {
				break
			}
			n++
		}
		if n > 0 {
			records = append(records, es[n-1].record)
			counts[p] = n
		}
	}
	return records, counts
}
func (w *window) committed(counts map[int32]int) int {
	total := 0
	for p, n := range counts {
		es := w.partitions[p]
		for i := 0; i < n; i++ {
			es[i] = nil
		}
		es = es[n:]
		if len(es) == 0 {
			delete(w.partitions, p)
		} else {
			w.partitions[p] = es
		}
		w.size -= n
		total += n
	}
	return total
}
func (t *Transport) consume(parent context.Context, topic string, handlers map[string]courier.Handler, ready func()) error {
	return t.consumeGroup(parent, topic, t.Group(topic), func(ctx context.Context, r *kgo.Record) error { return t.handle(ctx, handlers, r) }, ready)
}

func (t *Transport) consumeGroup(parent context.Context, topic, group string, handle func(context.Context, *kgo.Record) error, ready func()) error {
	ctx, cancel := context.WithCancel(parent)
	defer cancel()
	var wantsRebalance atomic.Bool
	opts := t.opts.base()
	opts = append(opts, kgo.ConsumeTopics(topic), kgo.ConsumerGroup(group), kgo.MetadataMaxAge(t.metadataTTL()), kgo.DisableAutoCommit(), kgo.BlockRebalanceOnPoll(), kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()), kgo.SessionTimeout(6*time.Second), kgo.HeartbeatInterval(time.Second), kgo.RebalanceTimeout(t.opts.RebalanceTimeout), kgo.OnPartitionsAssigned(func(context.Context, *kgo.Client, map[string][]int32) {
		wantsRebalance.Store(false)
		t.assignments.Add(1)
		ready()
	}), kgo.OnPartitionsRevoked(func(context.Context, *kgo.Client, map[string][]int32) { t.revocations.Add(1) }), kgo.OnPartitionsLost(func(context.Context, *kgo.Client, map[string][]int32) {}), kgo.OnPartitionsCallbackBlocked(func(context.Context, *kgo.Client) { wantsRebalance.Store(true) }))
	cl, err := kgo.NewClient(opts...)
	if err != nil {
		return err
	}
	defer cl.CloseAllowingRebalance()
	jobs := make(chan *entry, t.opts.Window)
	results := make(chan completion, t.opts.Window)
	var workers sync.WaitGroup
	for i := 0; i < t.opts.Workers; i++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for {
				select {
				case <-ctx.Done():
					return
				case e := <-jobs:
					if ctx.Err() != nil {
						return
					}
					err := handle(ctx, e.record)
					results <- completion{e, err}
				}
			}
		}()
	}
	w := newWindow()
	defer func() {
		cancel()
		workers.Wait()
		for {
			select {
			case c := <-results:
				w.complete(c)
			default:
				goto drained
			}
		}
	drained:
		// Preserve already-published responses during orderly shutdown. The
		// rebalance gate stays held until this final contiguous commit finishes.
		records, counts := w.prefixes()
		if len(records) > 0 {
			cleanup, done := context.WithTimeout(context.Background(), t.opts.OperationTimeout)
			defer done()
			if err := cl.CommitRecords(cleanup, records...); err == nil {
				t.committed.Add(uint64(w.committed(counts)))
			}
		}
	}()
	commit := func() error {
		records, counts := w.prefixes()
		if len(records) == 0 {
			return nil
		}
		commitCtx, done := context.WithTimeout(ctx, t.opts.OperationTimeout)
		defer done()
		if err := cl.CommitRecords(commitCtx, records...); err != nil {
			return err
		}
		t.committed.Add(uint64(w.committed(counts)))
		return nil
	}
	for ctx.Err() == nil {
		for draining := true; draining; {
			select {
			case c := <-results:
				w.complete(c)
			default:
				draining = false
			}
		}
		if err := commit(); err != nil {
			return err
		}
		if w.failed && w.active == 0 {
			return fmt.Errorf("reply publication/processing failed; unfinished offsets will be redelivered: %w", w.firstError)
		}
		if w.size == 0 {
			wantsRebalance.Store(false)
			cl.AllowRebalance()
		}
		if w.failed || wantsRebalance.Load() && w.size > 0 || w.size >= t.opts.Window {
			select {
			case c := <-results:
				w.complete(c)
			case <-ctx.Done():
				return ctx.Err()
			}
			continue
		}
		poll, cancelPoll := context.WithTimeout(ctx, 10*time.Millisecond)
		fetches := cl.PollRecords(poll, t.opts.Window-w.size)
		cancelPoll()
		for _, fe := range fetches.Errors() {
			if errors.Is(fe.Err, context.DeadlineExceeded) || errors.Is(fe.Err, context.Canceled) {
				continue
			}
			if !kerr.IsRetriable(fe.Err) {
				return fe.Err
			}
			t.opts.Logger.Warn("Kafka request fetch will retry", "topic", topic, "error", fe.Err)
		}
		fetches.EachRecord(func(r *kgo.Record) { e := w.add(r); jobs <- e })
	}
	return ctx.Err()
}
func (t *Transport) handle(parent context.Context, handlers map[string]courier.Handler, r *kgo.Record) error {
	e, err := courier.DecodeEnvelope(r.Value, t.opts.MaxFrameBytes)
	if err != nil || e.Kind != "request" || !t.validReply(e.Reply) || handlers[e.Service] == nil {
		// Without a valid route we cannot safely reply. An explicitly logged discard
		// prevents poison records from blocking a partition indefinitely.
		t.discarded.Add(1)
		t.opts.Logger.Warn("Kafka discarded unroutable record", "topic", r.Topic, "partition", r.Partition, "offset", r.Offset)
		return nil
	}
	if t.opts.DynamicReplyPartitions || t.opts.ReplyRouter != nil {
		if err := t.validateLiveReply(parent, e.Reply); err != nil {
			if errors.Is(err, errReplyPartition) {
				t.discarded.Add(1)
				return nil
			}
			return err
		}
	}
	ctx, cancel := context.WithTimeout(parent, t.opts.ProcessingTimeout)
	defer cancel()
	t.handled.Add(1)
	if parent.Value(decodedRequestKey{}) == true {
		ctx = context.WithValue(ctx, decodedRequestKey{}, &decodedRequest{envelope: e, size: len(r.Value)})
	}
	out, err := handlers[e.Service](ctx, r.Value)
	if err == nil && len(out) > t.opts.MaxFrameBytes {
		err = courier.ErrTooLarge
	}
	if errors.Is(err, courier.ErrTooLarge) {
		// A smaller core frame limit or oversized raw handler response cannot be
		// repaired by redelivery. Keep it from permanently blocking the partition.
		t.discarded.Add(1)
		t.opts.Logger.Warn("Kafka discarded unreplyable record", "topic", r.Topic, "partition", r.Partition, "offset", r.Offset, "error", err)
		return nil
	}
	if err != nil {
		return err
	}
	// Let an already-produced response finish its bounded acknowledgement
	// during listener shutdown, before the final contiguous commit.
	err = t.publish(t.ctx, &kgo.Record{Topic: e.Reply.Topic, Partition: e.Reply.Partition, Key: []byte(e.Session), Value: out, Headers: replyHeaders(r.Headers)})
	if err == nil {
		t.responded.Add(1)
	}
	return err
}
