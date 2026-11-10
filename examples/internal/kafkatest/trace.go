//go:build integration

package kafkatest

import (
	"fmt"
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// Trace keeps a bounded timeline for diagnosing failed integration calls.
type Trace struct {
	mu     sync.Mutex
	start  time.Time
	events []string
}

func (h *Trace) add(format string, args ...any) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if len(h.events) < 512 {
		h.events = append(h.events, fmt.Sprintf("%s %s", time.Since(h.start), fmt.Sprintf(format, args...)))
	}
}
func (h *Trace) OnProduceRecordBuffered(r *kgo.Record) {
	h.add("produce buffered topic=%s key=%s", r.Topic, r.Key)
}
func (h *Trace) OnProduceRecordUnbuffered(r *kgo.Record, err error) {
	h.add("produce done topic=%s partition=%d offset=%d key=%s err=%v", r.Topic, r.Partition, r.Offset, r.Key, err)
}
func (h *Trace) OnFetchRecordBuffered(r *kgo.Record) {
	h.add("fetch buffered topic=%s partition=%d offset=%d", r.Topic, r.Partition, r.Offset)
}
func (h *Trace) OnBrokerE2E(_ kgo.BrokerMetadata, key int16, e kgo.BrokerE2E) {
	if e.DurationE2E()+e.WriteWait > 250*time.Millisecond || e.Err() != nil {
		h.add("broker api=%d queue=%s wire=%s err=%v", key, e.WriteWait, e.DurationE2E(), e.Err())
	}
}
func (h *Trace) Snapshot() []string {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]string(nil), h.events...)
}

func NewTrace() *Trace { return &Trace{start: time.Now()} }
