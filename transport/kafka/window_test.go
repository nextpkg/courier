package kafka

import (
	"context"
	"errors"
	"github.com/nextpkg/courier"
	"strings"

	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"
	"testing"
	"time"
)

func TestUnreplyableRecordCanBeCommitted(t *testing.T) {
	opts, err := normalize(Options{Brokers: []string{"localhost:9092"}, MaxFrameBytes: 512})
	require.NoError(t, err)

	tr := &Transport{opts: opts}
	e := courier.Envelope{Version: 1, Kind: "request", Service: "S", Method: "Ping", ID: "id", Session: "s", Reply: structRoute(opts.ReplyTopic, 0)}
	base, err := courier.EncodeEnvelope(e, courier.DefaultMaxFrameBytes)
	require.NoError(t, err)

	e.Session = strings.Repeat("s", 1+opts.MaxFrameBytes-len(base))
	raw, err := courier.EncodeEnvelope(e, courier.DefaultMaxFrameBytes)
	if err != nil || len(raw) != opts.MaxFrameBytes {
		t.Fatalf("invalid fixture: %v, bytes=%d", err, len(raw))
	}
	w := newWindow()
	entry := w.add(&kgo.Record{Topic: "requests", Partition: 0, Offset: 10, Value: raw})
	err = tr.handle(context.Background(), map[string]courier.Handler{"S": func(context.Context, []byte) ([]byte, error) {
		t.Fatal("unreplyable record reached handler")
		return nil, nil
	}}, entry.record)
	w.complete(completion{entry, err})
	records, counts := w.prefixes()
	if err != nil || tr.discarded.Load() != 1 || w.failed || len(records) != 1 || records[0].Offset != 10 || counts[0] != 1 {
		t.Fatalf("poison record blocked commit: err=%v, records=%v, counts=%v", err, records, counts)
	}
}

func TestPermanentSizeFailureDoesNotRetry(t *testing.T) {
	opts, err := normalize(Options{Brokers: []string{"localhost:9092"}, MaxFrameBytes: 512})
	require.NoError(t, err)

	e := courier.Envelope{Version: 1, Kind: "request", Service: "S", Method: "Ping", ID: "id", Session: "s", Reply: structRoute(opts.ReplyTopic, 0)}
	data, err := courier.EncodeEnvelope(e, opts.MaxFrameBytes)
	require.NoError(t, err)

	for _, tc := range []struct {
		name      string
		out       []byte
		err       error
		permanent bool
	}{
		{"smaller core limit", nil, courier.ErrTooLarge, true},
		{"oversized raw response", make([]byte, 513), nil, true},
		{"recoverable failure", nil, errors.New("temporarily unavailable"), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tr := &Transport{opts: opts}
			err := tr.handle(context.Background(), map[string]courier.Handler{"S": func(context.Context, []byte) ([]byte, error) { return tc.out, tc.err }}, &kgo.Record{Value: data})
			if tc.permanent {
				if err != nil || tr.discarded.Load() != 1 {
					t.Fatalf("size failure will retry: %v", err)
				}
			} else if !errors.Is(err, tc.err) || tr.discarded.Load() != 0 {
				t.Fatalf("recoverable failure was discarded: %v", err)
			}
		})
	}
}

func TestCommitPrefixNeverSkipsUnfinishedOrFailed(t *testing.T) {
	w := newWindow()
	a := w.add(&kgo.Record{Topic: "requests", Partition: 0, Offset: 10})
	b := w.add(&kgo.Record{Topic: "requests", Partition: 0, Offset: 14})
	c := w.add(&kgo.Record{Topic: "requests", Partition: 1, Offset: 20})
	w.complete(completion{b, nil})
	w.complete(completion{c, nil})
	records, counts := w.prefixes()
	if len(records) != 1 || records[0] != c.record || counts[0] != 0 {
		t.Fatalf("committed across unfinished offset: %+v", records)
	}
	if n := w.committed(counts); n != 1 || w.size != 2 {
		t.Fatal("wrong committed removal")
	}
	w.complete(completion{a, nil})
	records, counts = w.prefixes()
	if len(records) != 1 || records[0].Offset != 14 || counts[0] != 2 {
		t.Fatal("control-record offset gaps broke ordered prefix")
	}
	w.committed(counts)
	if w.size != 0 || w.active != 0 || len(w.partitions) != 0 {
		t.Fatal("window retained records")
	}
	a = w.add(&kgo.Record{Partition: 0, Offset: 30})
	b = w.add(&kgo.Record{Partition: 0, Offset: 31})
	cause := errors.New("reply not published")
	w.complete(completion{a, cause})
	w.complete(completion{b, nil})
	records, _ = w.prefixes()
	if len(records) != 0 || !w.failed || !errors.Is(w.firstError, cause) {
		t.Fatal("advanced past response publication failure")
	}
}
func TestRoutingAndConfiguration(t *testing.T) {
	opts, err := normalize(Options{Brokers: []string{"localhost:9092"}})
	require.NoError(t, err)

	tr := &Transport{opts: opts}
	topic, err := tr.RequestTopic("Orders")
	if err != nil || topic != "courier.request.Orders" {
		t.Fatal(topic, err)
	}
	if tr.validReply(structRoute("other", 0)) {
		t.Fatal("arbitrary reply topic accepted")
	}
	// Four waves of 16 handlers must fit, including reply acknowledgements and
	// a final commit. A deadline that fits only the first wave is unsafe.
	if opts.RebalanceTimeout <= 4*(opts.ProcessingTimeout+opts.OperationTimeout)+opts.OperationTimeout {
		t.Fatal("rebalance budget omits queued handler waves")
	}
	if _, err := normalize(Options{Brokers: []string{"a"}, RebalanceTimeout: 45 * time.Second}); err == nil {
		t.Fatal("accepted rebalance budget shorter than bounded drain")
	}
	for _, bad := range []Options{{}, {Brokers: []string{"a"}, Prefix: "bad*"}, {Brokers: []string{"a"}, ReplyPartitions: []int32{-1}}, {Brokers: []string{"a"}, ReplyPartitions: []int32{0, 0}}, {Brokers: []string{"a"}, Window: 1}, {Brokers: []string{"a"}, RequestTopics: map[string]string{"A": "courier.replies"}}} {
		if _, err := normalize(bad); err == nil {
			t.Fatalf("invalid options accepted: %+v", bad)
		}
	}
}
func structRoute(topic string, p int32) courier.Route {
	return courier.Route{Topic: topic, Partition: p}
}
