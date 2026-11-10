package kafka

import (
	"bytes"
	"context"
	"fmt"
	"sync"

	"github.com/nextpkg/courier"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// replyReader is shared by all sessions on one transport and reply partition.
// It lives until transport shutdown, even between inbox registrations.
type replyReader struct {
	t        *Transport
	route    courier.Route
	client   *kgo.Client
	ctx      context.Context
	cancel   context.CancelFunc
	done     chan struct{}
	mu       sync.Mutex
	sessions map[string]*replyInbox
}

type replyInbox struct {
	s               *subscription
	ctx             context.Context
	offset          int64
	receive         func([]byte)
	receiveEnvelope func(courier.Envelope, int)
	mu              sync.Mutex // Close waits for an already executing callback.
}

func (t *Transport) replyEnd(ctx context.Context, route courier.Route) (int64, error) {
	req := kmsg.NewPtrListOffsetsRequest()
	p := kmsg.NewListOffsetsRequestTopicPartition()
	p.Partition, p.Timestamp = route.Partition, -1
	req.Topics = []kmsg.ListOffsetsRequestTopic{{Topic: route.Topic, Partitions: []kmsg.ListOffsetsRequestTopicPartition{p}}}
	resp, err := req.RequestWith(ctx, t.producer)
	if err != nil {
		return 0, err
	}
	for _, topic := range resp.Topics {
		for _, part := range topic.Partitions {
			if topic.Topic == route.Topic && part.Partition == route.Partition {
				if err := kerr.ErrorForCode(part.ErrorCode); err != nil {
					return 0, err
				}
				if part.Offset >= 0 {
					return part.Offset, nil
				}
			}
		}
	}
	return 0, fmt.Errorf("kafka: missing reply end offset")
}

// OpenInbox registers a session with a shared partition reader. Clients in one
// process should reuse a Transport; independent Transports remain isolated.
func (t *Transport) OpenInbox(parent context.Context, session string, receive func([]byte)) (courier.Inbox, error) {
	return t.openInbox(parent, session, receive, nil)
}
func (t *Transport) OpenEnvelopeInbox(parent context.Context, session string, receive func(courier.Envelope, int)) (courier.Inbox, error) {
	return t.openInbox(parent, session, nil, receive)
}
func (t *Transport) openInbox(parent context.Context, session string, receive func([]byte), parsed func(courier.Envelope, int)) (courier.Inbox, error) {
	if t.router != nil {
		return t.router.openInboxParsed(parent, session, receive, parsed)
	}
	if session == "" || (receive == nil && parsed == nil) {
		return nil, fmt.Errorf("kafka: invalid inbox")
	}
	route := courier.Route{Topic: t.opts.ReplyTopic, Partition: t.opts.ReplyPartitions[int(hash(session))%len(t.opts.ReplyPartitions)]}
	s, ctx, err := t.add(parent, route)
	if err != nil {
		return nil, err
	}
	start, stop := context.WithTimeout(ctx, t.opts.StartupTimeout)
	defer stop()
	// Serialize creation/registration, not message dispatch or inbox close.
	select {
	case t.inboxGate <- struct{}{}:
	case <-start.Done():
		s.finish()
		return nil, start.Err()
	}
	defer func() { <-t.inboxGate }()
	offset, err := t.replyEnd(start, route)
	if err != nil {
		s.finish()
		return nil, err
	}
	if t.readers == nil {
		t.readers = make(map[courier.Route]*replyReader)
	}
	r := t.readers[route]
	fresh := r == nil || r.ctx.Err() != nil
	if fresh && r != nil {
		select {
		case <-r.done:
		case <-start.Done():
			s.finish()
			return nil, start.Err()
		}
	}
	if fresh {
		opts := t.opts.base()
		opts = append(opts, kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{route.Topic: {route.Partition: kgo.NewOffset().At(offset)}}), kgo.ConsumeResetOffset(kgo.NoResetOffset()))
		client, e := kgo.NewClient(opts...)
		if e == nil {
			e = client.Ping(start)
		}
		if e != nil {
			if client != nil {
				client.Close()
			}
			s.finish()
			return nil, e
		}
		life, cancel := context.WithCancel(t.ctx)
		r = &replyReader{t: t, route: route, client: client, ctx: life, cancel: cancel, done: make(chan struct{}), sessions: make(map[string]*replyInbox)}
		t.readers[route] = r
	}
	r.mu.Lock()
	if _, exists := r.sessions[session]; exists {
		r.mu.Unlock()
		s.finish()
		return nil, fmt.Errorf("kafka: reply session already open")
	}
	inbox := &replyInbox{s: s, ctx: ctx, offset: offset, receive: receive, receiveEnvelope: parsed}
	r.sessions[session] = inbox
	r.mu.Unlock()
	if fresh {
		go r.run()
	}
	go func() {
		select {
		case <-ctx.Done():
		case <-r.ctx.Done():
			s.cancel()
		}
		r.mu.Lock()
		if r.sessions[session] == inbox {
			delete(r.sessions, session)
		}
		r.mu.Unlock()
		inbox.mu.Lock()
		inbox.mu.Unlock()
		s.finish()
	}()
	return s, nil
}

func (r *replyReader) run() {
	defer close(r.done)
	defer r.client.Close()
	defer r.cancel()
	for r.ctx.Err() == nil {
		fetches := r.client.PollRecords(r.ctx, 64)
		if r.ctx.Err() != nil {
			return
		}
		for _, fe := range fetches.Errors() {
			r.t.opts.Logger.Warn("Kafka reply fetch failed", "error", fe.Err)
			if fatalFetch(fe.Err) {
				return
			}
		}
		fetches.EachRecord(func(record *kgo.Record) {
			r.t.readRecords.Add(1)
			r.t.readBytes.Add(uint64(len(record.Value)))
			view, err := courier.DecodeReplyEnvelopeView(record.Value, r.t.opts.MaxFrameBytes)
			if err != nil || view.Partition != r.route.Partition || !bytes.Equal(view.Topic, []byte(r.route.Topic)) {
				return
			}
			r.mu.Lock()
			// Lookup borrows the session bytes only for this map access.
			inbox := r.sessions[string(view.Session)]
			r.mu.Unlock()
			if inbox == nil {
				return
			}
			inbox.mu.Lock()
			defer inbox.mu.Unlock()
			if inbox.ctx.Err() != nil || record.Offset < inbox.offset {
				return
			}
			r.t.matched.Add(1)
			r.t.matchedBytes.Add(uint64(len(record.Value)))
			if inbox.receiveEnvelope != nil {
				// Only a matching live inbox needs owned strings and payload.
				inbox.receiveEnvelope(view.OwnedEnvelope(), len(record.Value))
			} else {
				inbox.receive(record.Value)
			}
		})
	}
}
