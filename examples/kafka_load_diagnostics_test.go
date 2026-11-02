//go:build integration

package examples_test

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nextpkg/courier"
	"github.com/twmb/franz-go/pkg/kgo"
)

// loadDiagnostics observes SDK publication and handler timing without changing acknowledgements.
// Publication time includes buffering and retries; broker E2E covers wire request/response time.
type loadDiagnostics struct {
	mu                    sync.Mutex
	started               time.Time
	produced              map[*kgo.Record]time.Time
	fetched               map[string]time.Time
	stages                map[string]*loadTiming
	batches, batchRecords int64
}
type loadTiming struct {
	Count   int64   `json:"count"`
	TotalMS float64 `json:"total_ms"`
	MaxMS   float64 `json:"max_ms"`
}

func newLoadDiagnostics() *loadDiagnostics {
	return &loadDiagnostics{started: time.Now(), produced: map[*kgo.Record]time.Time{}, fetched: map[string]time.Time{}, stages: map[string]*loadTiming{}}
}
func (d *loadDiagnostics) addLocked(name string, elapsed time.Duration) {
	s := d.stages[name]
	if s == nil {
		s = &loadTiming{}
		d.stages[name] = s
	}
	ms := float64(elapsed) / float64(time.Millisecond)
	s.Count++
	s.TotalMS += ms
	s.MaxMS = max(s.MaxMS, ms)
}
func (d *loadDiagnostics) OnProduceRecordBuffered(r *kgo.Record) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.produced[r] = time.Now()
}
func (d *loadDiagnostics) OnProduceRecordUnbuffered(r *kgo.Record, err error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if began, ok := d.produced[r]; ok {
		name := "produce_success"
		if err != nil {
			name = "produce_error"
		}
		d.addLocked(name, time.Since(began))
		delete(d.produced, r)
	}
}
func (d *loadDiagnostics) OnProduceBatchWritten(_ kgo.BrokerMetadata, _ string, _ int32, m kgo.ProduceBatchMetrics) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.batches++
	d.batchRecords += int64(m.NumRecords)
}
func (d *loadDiagnostics) OnBrokerE2E(_ kgo.BrokerMetadata, key int16, e kgo.BrokerE2E) {
	if key != 0 && key != 8 {
		return
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	name := fmt.Sprintf("broker_api_%d", key)
	if e.Err() != nil {
		name += "_error"
	}
	d.addLocked(name+"_wire", e.DurationE2E())
	d.addLocked(name+"_write_wait", e.WriteWait)
}
func (d *loadDiagnostics) OnFetchRecordBuffered(r *kgo.Record) {
	var e courier.Envelope
	if json.Unmarshal(r.Value, &e) != nil || e.Kind != "request" {
		return
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	d.fetched[e.ID] = time.Now()
}
func (d *loadDiagnostics) wrap(h courier.Handler) courier.Handler {
	return func(ctx context.Context, b []byte) ([]byte, error) {
		var e courier.Envelope
		_ = json.Unmarshal(b, &e)
		d.mu.Lock()
		if start, ok := d.fetched[e.ID]; ok {
			d.addLocked("fetch_buffer_to_handler", time.Since(start))
			delete(d.fetched, e.ID)
		}
		d.mu.Unlock()
		began := time.Now()
		out, err := h(ctx, b)
		d.mu.Lock()
		d.addLocked("core_handler", time.Since(began))
		d.mu.Unlock()
		return out, err
	}
}
func (d *loadDiagnostics) report(t *testing.T, name string) {
	t.Helper()
	d.mu.Lock()
	defer d.mu.Unlock()
	b, err := json.Marshal(map[string]any{"role": name, "elapsed_seconds": time.Since(d.started).Seconds(), "stages": d.stages, "batches": d.batches, "batch_records": d.batchRecords, "pending_produce": len(d.produced), "pending_fetch": len(d.fetched)})
	if err != nil {
		t.Fatal(err)
	}
	t.Log("KAFKA_DIAGNOSTICS " + string(b))
}

type loadObservedTransport struct {
	courier.Transport
	d *loadDiagnostics
}

func (o loadObservedTransport) Listen(ctx context.Context, service string, h courier.Handler) (courier.Subscription, error) {
	return o.Transport.Listen(ctx, service, o.d.wrap(h))
}
func (o loadObservedTransport) ListenServices(ctx context.Context, handlers map[string]courier.Handler) (courier.Subscription, error) {
	wrapped := make(map[string]courier.Handler, len(handlers))
	for name, h := range handlers {
		wrapped[name] = o.d.wrap(h)
	}
	return o.Transport.(courier.ServiceListener).ListenServices(ctx, wrapped)
}

// loadPublishErrors samples failed SDK completions without tracing successful records.
type loadPublishErrors struct {
	t         *testing.T
	role      string
	count     atomic.Int64
	slowCount atomic.Int64
}

func (h *loadPublishErrors) OnProduceRecordUnbuffered(_ *kgo.Record, err error) {
	if err != nil && h.count.Add(1) <= 5 {
		h.t.Logf("KAFKA_PUBLISH_ERROR time=%s role=%s type=%T error=%v", time.Now().UTC().Format(time.RFC3339Nano), h.role, err, err)
	}
}

func (h *loadPublishErrors) OnBrokerE2E(_ kgo.BrokerMetadata, key int16, e kgo.BrokerE2E) {
	if (key == 0 || key == 8) && (e.DurationE2E()+e.WriteWait > 500*time.Millisecond || e.Err() != nil) && h.slowCount.Add(1) <= 32 {
		h.t.Logf("KAFKA_SLOW_WIRE time=%s role=%s key=%d wait=%s write=%s read_wait=%s read=%s error=%v", time.Now().UTC().Format(time.RFC3339Nano), h.role, key, e.WriteWait, e.TimeToWrite, e.ReadWait, e.TimeToRead, e.Err())
	}
}
