package kafka

import (
	"bytes"
	"context"
	"fmt"
	"github.com/nextpkg/courier"
	"github.com/nextpkg/courier/transport/kafka/internal/wire"
	"github.com/twmb/franz-go/pkg/kgo"
	"net/http"
	"time"
)

func (r *replyRouter) openInbox(parent context.Context, session string, receive func([]byte)) (courier.Inbox, error) {
	return r.openInboxParsed(parent, session, receive, nil)
}
func (r *replyRouter) openInboxParsed(parent context.Context, session string, receive func([]byte), parsed func(courier.Envelope, int)) (courier.Inbox, error) {
	if session == "" || (receive == nil && parsed == nil) {
		return nil, fmt.Errorf("kafka: invalid inbox")
	}
	// Send chooses a live partition per request; the core route is only a template.
	route := courier.Route{Topic: r.t.opts.ReplyTopic, Partition: 0}
	s, ctx, err := r.t.add(parent, route)
	if err != nil {
		return nil, err
	}
	in := &routedInbox{replyInbox: &replyInbox{s: s, ctx: ctx, receive: receive, receiveEnvelope: parsed}, seen: map[string]bool{}}
	r.mu.Lock()
	if r.sessions[session] != nil {
		r.mu.Unlock()
		s.finish()
		return nil, fmt.Errorf("kafka: duplicate reply session")
	}
	r.sessions[session] = in
	r.mu.Unlock()
	go func() {
		<-ctx.Done()
		r.mu.Lock()
		if r.sessions[session] == in {
			delete(r.sessions, session)
		}
		r.mu.Unlock()
		in.mu.Lock()
		in.mu.Unlock() //nolint:staticcheck // SA2001: wait for the active reply callback before finishing the session.
		s.finish()
	}()
	return s, nil
}
func (r *replyRouter) routeHeader(e courier.Envelope) (kgo.RecordHeader, error) {
	expiry := time.Now().Add(r.opts.MaxReplyAge).UnixNano()
	if e.Deadline > 0 && e.Deadline < expiry {
		expiry = e.Deadline
	}
	f := wire.Route{Endpoint: r.endpoint, Instance: r.instance, Session: e.Session, ID: e.ID, Topic: e.Reply.Topic, Partition: e.Reply.Partition, Expires: expiry}
	b, err := wire.EncodeRoute(f)
	if len(b) > wire.MaxRouteBytes {
		return kgo.RecordHeader{}, courier.ErrTooLarge
	}
	return kgo.RecordHeader{Key: routeHeader, Value: b}, err
}

// Decode binary routing metadata by value to avoid a heap-allocated route
// container; its strings own storage independently of the codec scratch buffer.
func (r *replyRouter) decodeRoute(b []byte) (wire.Route, error) {
	if len(b) > wire.MaxRouteBytes {
		return wire.Route{}, fmt.Errorf("routing header too large")
	}
	if !bytes.HasPrefix(b, wire.RoutePrefix) {
		return wire.Route{}, fmt.Errorf("invalid binary route prefix")
	}
	f, err, canonical := r.decodeCanonicalRoute(b)
	if !canonical {
		f, err = wire.DecodeRoute(b)
	}
	if err != nil {
		return f, err
	}
	if f.Instance == "" || f.Session == "" || f.ID == "" || f.Topic != r.t.opts.ReplyTopic || f.Partition < 0 || f.Expires <= 0 {
		return f, fmt.Errorf("invalid routing fields")
	}
	if !canonical {
		if err := r.validateRouteEndpoint(f.Endpoint); err != nil {
			return f, err
		}
	}
	return f, nil
}
func (r *replyRouter) deliver(f wire.Route, data []byte) int {
	view, err := courier.DecodeReplyEnvelopeView(data, r.t.opts.MaxFrameBytes)
	if err != nil {
		return http.StatusBadRequest
	}
	return r.deliverView(f, data, view)
}

// Validate before ownership allocation; duplicates, expired replies and closed
// inboxes do not need copied strings or payloads.
func (r *replyRouter) deliverView(f wire.Route, data []byte, view courier.ReplyEnvelopeView) int {
	if time.Now().UnixNano() >= f.Expires {
		return http.StatusGone
	}
	if f.Instance != r.instance {
		return http.StatusConflict
	}
	if !bytes.Equal(view.Session, []byte(f.Session)) || !bytes.Equal(view.ID, []byte(f.ID)) || !bytes.Equal(view.Topic, []byte(f.Topic)) || view.Partition != f.Partition {
		return http.StatusBadRequest
	}

	return r.deliverReply(data, view, f.ID)
}

// Compact batches omit metadata already present in the validated reply frame.
// Instance and expiry remain explicit to reject restarted or expired targets.
func (r *replyRouter) deliverCompact(m wire.ReceivedBatchMessage) int {
	view, err := courier.DecodeReplyEnvelopeView(m.Data, r.t.opts.MaxFrameBytes)
	if err != nil || len(m.Instance) == 0 || m.Expires <= 0 {
		return http.StatusBadRequest
	}
	// Keep the original routing-metadata budget even though the full header is
	// omitted on this hop. This bounds identifiers retained by the dedup ring.
	const routeFieldOverhead = 5*(3+4) + (3 + 4) + (3 + 8) + 1
	routingBytes := len(wire.RoutePrefix) + routeFieldOverhead + len(r.endpoint) + len(m.Instance) + len(view.Session) + len(view.ID) + len(view.Topic)
	if routingBytes > wire.MaxRouteBytes {
		return http.StatusBadRequest
	}
	if time.Now().UnixNano() >= m.Expires {
		return http.StatusGone
	}
	if !bytes.Equal(m.Instance, []byte(r.instance)) {
		return http.StatusConflict
	}
	return r.deliverReply(m.Data, view, "")
}

// ownedID, when supplied by full-route delivery, already owns matching ID bytes.
func (r *replyRouter) deliverReply(data []byte, view courier.ReplyEnvelopeView, ownedID string) int {
	if !bytes.Equal(view.Topic, []byte(r.t.opts.ReplyTopic)) || !r.t.validReply(courier.Route{Topic: r.t.opts.ReplyTopic, Partition: view.Partition}) {
		return http.StatusBadRequest
	}

	r.mu.Lock()
	in := r.sessions[string(view.Session)]
	r.mu.Unlock()
	if in == nil {
		return http.StatusGone
	}
	in.mu.Lock()
	defer in.mu.Unlock()
	if in.ctx.Err() != nil {
		return http.StatusGone
	}
	if in.seen[string(view.ID)] {
		r.duplicate.Add(1)
		return http.StatusNoContent
	}
	// Own the dedup key before invoking a raw callback that may retain/mutate data.
	id := ownedID
	if id == "" {
		id = string(view.ID)
	}
	if in.receiveEnvelope != nil {
		in.receiveEnvelope(view.OwnedEnvelope(), len(data))
	} else {
		in.receive(data)
	}
	in.seen[id] = true
	if len(in.order) < r.opts.DedupEntries {
		in.order = append(in.order, id)
	} else {
		delete(in.seen, in.order[in.next])
		in.order[in.next] = id
		in.next = (in.next + 1) % len(in.order)
	}
	r.t.matched.Add(1)
	r.t.matchedBytes.Add(uint64(len(data)))
	return http.StatusNoContent
}
