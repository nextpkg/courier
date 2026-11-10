package kafka

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/tls"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"github.com/nextpkg/courier/transport/kafka/internal/wire"
	"io"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nextpkg/courier"
	"github.com/twmb/franz-go/pkg/kgo"
)

// ReplyRouterOptions enables a peer reply router. All peers sharing a reply
// topic must share Group, and advertise directly reachable endpoints.
// Use a separate reply topic from direct-reader clients. Servers only
// need DynamicReplyPartitions and the routing-header-capable adapter.
type ReplyRouterOptions struct {
	ForwardProtocol   string // "http" (default, with optional TLS) or "thrift" (trusted plaintext peers).
	BatchConcurrency  int    // In-flight batches per endpoint: 0/1 selects serial, 2 enables bounded overlap.
	ListenAddress     string
	AdvertiseURL      string
	Group             string
	ServerTLS         *tls.Config
	ClientTLS         *tls.Config
	AllowInsecureHTTP bool          // Explicit opt-in for trusted private networks/tests.
	MaxReplyAge       time.Duration // Bounds retries and routing metadata lifetime; default 30s.
	BatchSize         int           // Optional ready-only batching, 0 disables; maximum 32.
	DedupEntries      int           // Last completed IDs retained per inbox; default 4096.
}

// Route metadata carries correlation and expiry for trusted internal peers;
// it is not an authentication token.

var errReplyPartition = errors.New("kafka: reply partition not provisioned")

const routeHeader = "courier-reply-route-v1"

type routedInbox struct {
	*replyInbox
	seen  map[string]bool
	order []string
	next  int
}

type replyRouter struct {
	t                                                 *Transport
	opts                                              ReplyRouterOptions
	instance                                          string
	endpoint                                          string
	server                                            *http.Server
	rpc                                               *rpcPeer
	client                                            *http.Client
	httpTransport                                     *http.Transport
	done                                              chan struct{}
	served                                            chan struct{}
	mu                                                sync.Mutex
	sessions                                          map[string]*routedInbox
	stale                                             map[string]int64
	inbound                                           chan struct{}
	batchMu                                           sync.Mutex
	batchWG                                           sync.WaitGroup
	batches                                           map[string][]*forwardItem
	httpRequests                                      atomic.Uint64
	httpMessages                                      atomic.Uint64
	forwarded, expired, duplicate, staleHits, retries atomic.Uint64
	identityMu                                        sync.Mutex
	identities                                        atomic.Pointer[routeIdentityTable]
	batchSignals                                      map[string]chan struct{}
	forwardItems                                      atomic.Pointer[forwardItemPool]
}

// ReplyRouterStats separates forwarded deliveries from Kafka fetch records.
// ReplyRecords is counted at the consumer only; ReplyMatched at the target.
type ReplyRouterStats struct{ Forwarded, Expired, Duplicates, StaleInstances, Retries, HTTPRequests, HTTPMessages uint64 }

func (t *Transport) RouterStats() ReplyRouterStats {
	if t.router == nil {
		return ReplyRouterStats{}
	}
	r := t.router
	return ReplyRouterStats{r.forwarded.Load(), r.expired.Load(), r.duplicate.Load(), r.staleHits.Load(), r.retries.Load(), r.httpRequests.Load(), r.httpMessages.Load()}
}

// ReplyEndpoint is the actual advertised URL, including the assigned port when
// ListenAddress and AdvertiseURL use port zero.
func (t *Transport) ReplyEndpoint() string {
	if t.router == nil {
		return ""
	}
	return t.router.endpoint
}
func (t *Transport) metadataTTL() time.Duration {
	if t.opts.DynamicReplyPartitions || t.opts.ReplyRouter != nil {
		return time.Second
	}
	return 30 * time.Second
}
func (t *Transport) validateLiveReply(ctx context.Context, route courier.Route) error {
	for attempt := 0; attempt < 2; attempt++ {
		ps, err := t.partitions(ctx, route.Topic)
		if err != nil {
			return err
		}
		for _, p := range ps {
			if p == route.Partition {
				return nil
			}
		}
		t.mu.Lock()
		delete(t.topics, route.Topic)
		t.mu.Unlock()
	}
	return errReplyPartition
}
func replyHeaders(headers []kgo.RecordHeader) []kgo.RecordHeader {
	for _, h := range headers {
		if h.Key == routeHeader {
			return []kgo.RecordHeader{{Key: h.Key, Value: append([]byte(nil), h.Value...)}}
		}
	}
	return nil
}
func (t *Transport) startReplyRouter(ctx context.Context) (*replyRouter, error) {
	o, u, err := normalizeReplyRouter(*t.opts.ReplyRouter, t.opts)
	if err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	ln, err := net.Listen("tcp", o.ListenAddress)
	if err != nil {
		return nil, err
	}
	if u.Port() == "0" {
		u.Host = net.JoinHostPort(u.Hostname(), strconv.Itoa(ln.Addr().(*net.TCPAddr).Port))
	}
	u.Path = ""
	id := make([]byte, 24)
	if _, err = rand.Read(id); err != nil {
		_ = ln.Close()
		return nil, err
	}
	r := &replyRouter{t: t, opts: o, instance: hex.EncodeToString(id), endpoint: u.String(), done: make(chan struct{}), served: make(chan struct{}), sessions: map[string]*routedInbox{}, stale: map[string]int64{}, inbound: make(chan struct{}, t.opts.Workers)}
	r.forwardItems.Store(newForwardItemPool(t.opts.Window))
	ht := &http.Transport{MaxIdleConns: 128, MaxIdleConnsPerHost: t.opts.Workers, MaxConnsPerHost: t.opts.Workers, IdleConnTimeout: 30 * time.Second, ResponseHeaderTimeout: t.opts.OperationTimeout, DialContext: (&net.Dialer{Timeout: t.opts.OperationTimeout}).DialContext}
	if o.ClientTLS != nil {
		ht.TLSClientConfig = o.ClientTLS.Clone()
	}
	r.httpTransport = ht
	r.client = &http.Client{Transport: ht, Timeout: t.opts.OperationTimeout, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	r.server = &http.Server{Handler: http.HandlerFunc(r.serve), ReadHeaderTimeout: t.opts.OperationTimeout, ReadTimeout: t.opts.OperationTimeout, WriteTimeout: t.opts.OperationTimeout, IdleTimeout: 30 * time.Second, MaxHeaderBytes: 8192, BaseContext: func(net.Listener) context.Context { return t.ctx }}
	if o.ServerTLS != nil {
		r.server.TLSConfig = o.ServerTLS.Clone()
	}
	if o.ForwardProtocol == "thrift" {
		r.rpc = newRPCPeer(r, ln)
	}
	go func() {
		defer close(r.served)
		var err error
		if r.rpc != nil {
			err = r.rpc.serve(ln)
		} else if u.Scheme == "https" {
			err = r.server.ServeTLS(ln, "", "")
		} else {
			err = r.server.Serve(ln)
		}
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			t.opts.Logger.Error("reply router listener failed", "error", err)
			t.cancel()
		}
	}()
	go func() {
		defer close(r.done)
		for t.ctx.Err() == nil {
			err := t.consumeGroup(t.ctx, t.opts.ReplyTopic, o.Group, r.forward, func() {})
			if t.ctx.Err() != nil {
				return
			}
			t.restarts.Add(1)
			t.opts.Logger.Warn("reply router rejoining", "error", err)
			select {
			case <-t.ctx.Done():
				return
			case <-time.After(100 * time.Millisecond):
			}
		}
	}()
	return r, nil
}
func (r *replyRouter) close() {
	ctx, cancel := context.WithTimeout(context.Background(), r.t.opts.OperationTimeout)
	defer cancel()
	if r.rpc != nil {
		r.rpc.close()
	}
	if err := r.server.Shutdown(ctx); err != nil {
		_ = r.server.Close()
	}
	<-r.served
	<-r.done
	r.batchWG.Wait()
	// Detach idle storage after all batch senders finish; retained waiters keep
	// their pool alive until their own final release.
	r.forwardItems.Store(nil)
	r.httpTransport.CloseIdleConnections()
}
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
func (r *replyRouter) serve(w http.ResponseWriter, req *http.Request) {
	if req.Method == "POST" && req.URL.Path == "/courier/replies" {
		r.serveBatch(w, req)
		return
	}
	if req.Method != "POST" || req.URL.Path != "/courier/reply" {
		w.WriteHeader(http.StatusNotFound)
		return
	}
	select {
	case r.inbound <- struct{}{}:
		defer func() { <-r.inbound }()
	default:
		w.WriteHeader(http.StatusTooManyRequests)
		return
	}
	data, err := io.ReadAll(http.MaxBytesReader(w, req.Body, int64(r.t.opts.MaxFrameBytes)))
	if err != nil {
		w.WriteHeader(http.StatusRequestEntityTooLarge)
		return
	}
	hdr := req.Header.Get("X-Courier-Route")
	if strings.HasPrefix(hdr, "base64:") {
		raw, err := base64.StdEncoding.DecodeString(strings.TrimPrefix(hdr, "base64:"))
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		hdr = string(raw)
	}
	f, err := r.decodeRoute([]byte(hdr))
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	w.WriteHeader(r.deliver(f, data))
}

// borrowedRouteHeader returns read-only fetch storage. Fetch pooling/Recycle must
// not be enabled without extending ownership through every queued send.
func borrowedRouteHeader(headers []kgo.RecordHeader) ([]byte, bool) {
	for _, h := range headers {
		if h.Key == routeHeader {
			return h.Value[:len(h.Value):len(h.Value)], true
		}
	}
	return nil, false
}

func (r *replyRouter) forward(ctx context.Context, record *kgo.Record) error {
	r.t.readRecords.Add(1)
	r.t.readBytes.Add(uint64(len(record.Value)))
	header, found := borrowedRouteHeader(record.Headers)
	if !found {
		r.t.discarded.Add(1)
		return nil
	}
	f, err := r.decodeRoute(header)
	if err != nil || f.Topic != record.Topic || f.Partition != record.Partition {
		r.t.discarded.Add(1)
		return nil
	}
	view, err := courier.DecodeReplyEnvelopeView(record.Value, r.t.opts.MaxFrameBytes)
	if err != nil || !bytes.Equal(view.ID, []byte(f.ID)) || !bytes.Equal(view.Session, []byte(f.Session)) || !bytes.Equal(view.Topic, []byte(f.Topic)) || view.Partition != f.Partition {
		r.t.discarded.Add(1)
		return nil
	}
	until := time.Unix(0, f.Expires)
	if bound := time.Now().Add(r.opts.MaxReplyAge); until.After(bound) {
		until = bound
	}
	// Local delivery is synchronous and checks route expiry itself. Remote
	// forwarding needs a cancellable deadline while queued or awaiting I/O.
	life := ctx
	if f.Endpoint != r.endpoint {
		if parentDeadline, ok := ctx.Deadline(); !ok || until.Before(parentDeadline) {
			var cancel context.CancelFunc
			life, cancel = context.WithDeadline(ctx, until)
			defer cancel()
		}
	}
	for life.Err() == nil && time.Now().Before(until) {
		r.mu.Lock()
		expired := r.stale[f.Instance]
		r.mu.Unlock()
		if expired > time.Now().UnixNano() {
			r.staleHits.Add(1)
			return nil
		}
		status := 0
		if f.Endpoint == r.endpoint {
			// Local delivery materializes only after inbox/duplicate checks.
			status = r.deliverView(f, record.Value, view)
		} else {
			status, err = r.forwardRemote(life, f, header, record.Value)
			if err != nil {
				return err
			}
		}

		switch status {
		case http.StatusNoContent:
			r.forwarded.Add(1)
			return nil
		case http.StatusConflict:
			r.mu.Lock()
			now := time.Now().UnixNano()
			for k, v := range r.stale {
				if v <= now {
					delete(r.stale, k)
				}
			}
			if len(r.stale) < 1024 {
				r.stale[f.Instance] = now + int64(r.opts.MaxReplyAge)
			}
			r.mu.Unlock()
			r.staleHits.Add(1)
			return nil
		case http.StatusGone:
			r.expired.Add(1)
			return nil
		case http.StatusBadRequest:
			r.t.discarded.Add(1)
			return nil
		}
		r.retries.Add(1)
		select {
		case <-life.Done():
		case <-time.After(25 * time.Millisecond):
		}
	}
	if ctx.Err() != nil {
		return ctx.Err()
	}
	r.expired.Add(1)
	return nil // Terminal expiry permits contiguous offset advancement.
}
