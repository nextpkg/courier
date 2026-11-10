package kafka

import (
	"context"
	"crypto/tls"
	"errors"
	"github.com/nextpkg/courier"
	"github.com/nextpkg/courier/transport/kafka/internal/wire"
	"github.com/twmb/franz-go/pkg/kgo"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func routerFixture(t *testing.T) (*replyRouter, wire.Route, []byte) {
	t.Helper()
	opts, err := normalize(Options{Brokers: []string{"localhost:9092"}})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	tr := &Transport{opts: opts, ctx: ctx, subs: map[*subscription]bool{}}
	r := &replyRouter{t: tr, opts: ReplyRouterOptions{AllowInsecureHTTP: true, MaxReplyAge: time.Second, DedupEntries: 16}, instance: "new", endpoint: "http://127.0.0.1:1", sessions: map[string]*routedInbox{}, stale: map[string]int64{}}
	f := wire.Route{Endpoint: r.endpoint, Instance: "new", Session: "s", ID: "id", Topic: opts.ReplyTopic, Expires: time.Now().Add(time.Second).UnixNano()}
	b, err := courier.EncodeEnvelope(courier.Envelope{Version: 1, Kind: "response", Service: "S", Method: "M", ID: "id", Session: "s", Reply: courier.Route{Topic: opts.ReplyTopic}}, courier.DefaultMaxFrameBytes)
	if err != nil {
		t.Fatal(err)
	}
	return r, f, b
}
func TestRouterIdentityDedupAndClose(t *testing.T) {
	r, f, b := routerFixture(t)
	count := 0
	in, err := r.openInbox(context.Background(), "s", func([]byte) { count++ })
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if r.deliver(f, b) != 204 {
				t.Error("delivery failed")
			}
		}()
	}
	wg.Wait()
	if count != 1 || r.duplicate.Load() != 19 {
		t.Fatalf("callbacks=%d duplicates=%d", count, r.duplicate.Load())
	}
	old := f
	old.Instance = "old"
	if r.deliver(old, b) != 409 {
		t.Fatal("stale process accepted")
	}
	if err := in.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if r.deliver(f, b) != 410 {
		t.Fatal("closed inbox accepted")
	}
}
func TestRouterValidationAndExpiry(t *testing.T) {
	r, f, b := routerFixture(t)
	e, _ := courier.DecodeEnvelope(b, courier.DefaultMaxFrameBytes)
	h, _ := r.routeHeader(e)
	parsed, err := r.decodeRoute(h.Value)
	if err != nil {
		t.Fatal(err)
	}
	parsed.Endpoint = "http://127.0.0.1:9999/path"
	invalid, _ := wire.EncodeRoute(parsed)
	if _, err := r.decodeRoute(invalid); err == nil {
		t.Fatal("invalid endpoint accepted")
	}
	var attempts atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) { attempts.Add(1); w.WriteHeader(503) }))
	defer srv.Close()
	f.Endpoint = srv.URL
	f.Expires = time.Now().Add(80 * time.Millisecond).UnixNano()
	r.client = srv.Client()
	raw, _ := wire.EncodeRoute(f)
	record := &kgo.Record{Topic: f.Topic, Value: b, Headers: []kgo.RecordHeader{{Key: routeHeader, Value: raw}}}
	if err := r.forward(context.Background(), record); err != nil {
		t.Fatal(err)
	}
	if attempts.Load() == 0 || r.expired.Load() != 1 {
		t.Fatal("expiry did not terminate unreachable target")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	f.Expires = time.Now().Add(time.Second).UnixNano()
	raw, _ = wire.EncodeRoute(f)
	record.Headers[0].Value = raw
	if err := r.forward(ctx, record); err == nil {
		t.Fatal("shutdown must not acknowledge unfinished reply")
	}
}

func TestRouterRejectsMissingRouteHTTP(t *testing.T) {
	r, _, b := routerFixture(t)
	r.inbound = make(chan struct{}, 1)
	req := httptest.NewRequest("POST", "http://router/courier/reply", strings.NewReader(string(b)))
	w := httptest.NewRecorder()
	r.serve(w, req)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("status=%d", w.Code)
	}
}

func TestRouterRejectsOversizedRoutingHeader(t *testing.T) {
	r, _, b := routerFixture(t)
	e, _ := courier.DecodeEnvelope(b, courier.DefaultMaxFrameBytes)
	e.Session = strings.Repeat("s", wire.MaxRouteBytes)
	if _, err := r.routeHeader(e); err != courier.ErrTooLarge {
		t.Fatalf("oversized header accepted: %v", err)
	}
}

func TestRouterForwardProtocolConfiguration(t *testing.T) {
	// Benchmark environment must not choose a production wire protocol.
	t.Setenv("REPLY_MODE", "thrift")
	for _, tc := range []struct {
		name, protocol string
		tls            bool
		valid          bool
	}{
		{"default", "", false, true}, {"http", "http", false, true}, {"thrift", "thrift", false, true},
		{"unknown", "other", false, false}, {"thrift TLS", "thrift", true, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r, _, _ := routerFixture(t)
			o := &ReplyRouterOptions{ListenAddress: "127.0.0.1:0", AdvertiseURL: "http://127.0.0.1:0", AllowInsecureHTTP: true, ForwardProtocol: tc.protocol}
			if tc.tls {
				o.ClientTLS = &tls.Config{}
			}
			r.t.opts.ReplyRouter = o
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			_, err := r.t.startReplyRouter(ctx)
			if tc.valid && !errors.Is(err, context.Canceled) {
				t.Fatalf("valid configuration rejected before lifecycle check: %v", err)
			}
			if !tc.valid && (err == nil || errors.Is(err, context.Canceled)) {
				t.Fatalf("invalid protocol/TLS accepted: %v", err)
			}
		})
	}
}
