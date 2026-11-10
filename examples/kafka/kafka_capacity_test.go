//go:build integration

package kafka_test

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/nextpkg/courier"
	"github.com/nextpkg/courier/examples/gen/demo"
	"github.com/nextpkg/courier/examples/handlers"
	"github.com/nextpkg/courier/examples/internal/kafkatest"
	"github.com/nextpkg/courier/examples/internal/testutil"
	transport "github.com/nextpkg/courier/transport/kafka"
	"runtime"
	"sort"
	"sync"
	"testing"
	"time"
)

func TestKafkaSharedReplyReadAmplification(t *testing.T) {
	for _, sessions := range []int{1, 4, 8} {
		t.Run(fmt.Sprint(sessions), func(t *testing.T) {
			_, opts := kafkatest.NewEnv(t, 3)
			testutil.Server(t, kafkatest.NewTransport(t, opts), &handlers.Orders{})
			var ts []*transport.Transport
			var cs []*courier.Client
			for i := 0; i < sessions; i++ {
				tr := kafkatest.NewTransport(t, opts)
				ts = append(ts, tr)
				cs = append(cs, testutil.Client(t, tr))
			}
			const count = 64
			var fs []*courier.Future[*demo.Item]
			var starts []time.Time
			latencies := make([]float64, count)
			var completed sync.WaitGroup
			begin := time.Now()
			for i := 0; i < count; i++ {
				starts = append(starts, time.Now())
				f, err := demo.NewOrdersAsyncClient(cs[i%sessions]).GetAsync(context.Background(), fmt.Sprint(i), 0)
				if err != nil {
					t.Fatal(err)
				}
				fs = append(fs, f)
				completed.Add(1)
				go func(index int, future *courier.Future[*demo.Item], started time.Time) {
					defer completed.Done()
					<-future.Done()
					latencies[index] = float64(time.Since(started).Microseconds()) / 1000
				}(i, f, starts[i])
			}
			for _, f := range fs {
				if _, err := f.Await(context.Background()); err != nil {
					t.Fatal(err)
				}
			}
			completed.Wait()
			elapsed := time.Since(begin)
			testutil.WaitFor(t, 3*time.Second, func() bool {
				for _, tr := range ts {
					if tr.Stats().ReplyRecords < count {
						return false
					}
				}
				return true
			}, "reply readers did not catch up")
			var records, matched, readBytes, matchedBytes uint64
			for _, tr := range ts {
				s := tr.Stats()
				records += s.ReplyRecords
				matched += s.ReplyMatched
				readBytes += s.ReplyBytes
				matchedBytes += s.ReplyMatchedBytes
			}
			if records != uint64(count*sessions) || matched != count {
				t.Fatalf("read amplification counts: reads=%d matches=%d", records, matched)
			}
			sort.Float64s(latencies)
			result := map[string]any{"sessions": sessions, "reply_partitions": 1, "calls": count, "read_records": records, "matched_records": matched, "read_payload_bytes": readBytes, "matched_payload_bytes": matchedBytes, "read_amplification": float64(readBytes) / float64(matchedBytes), "elapsed_ms": elapsed.Milliseconds(), "calls_per_second": float64(count) / elapsed.Seconds(), "completion_p95_ms": latencies[len(latencies)*95/100]}
			data, _ := json.Marshal(result)
			t.Log("KAFKA_CAPACITY " + string(data))
		})
	}
}
func TestKafkaRepeatedLifecycle(t *testing.T) {
	_, opts := kafkatest.NewEnv(t, 1)
	testutil.Server(t, kafkatest.NewTransport(t, opts), &handlers.Orders{})
	baseline := runtime.NumGoroutine()
	for i := 0; i < 12; i++ {
		tr, err := transport.New(context.Background(), opts)
		if err != nil {
			t.Fatal(err)
		}
		c, err := courier.NewClient(context.Background(), tr, courier.ClientOptions{})
		if err != nil {
			t.Fatal(err)
		}
		if _, err = demo.NewOrdersAsyncClient(c).Get(context.Background(), "lifecycle", 0); err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		if err = c.Close(ctx); err != nil {
			t.Fatal(err)
		}
		if err = tr.Close(ctx); err != nil {
			t.Fatal(err)
		}
		cancel()
		if tr.Stats().Subscriptions != 0 || c.Pending() != 0 {
			t.Fatal("retained Kafka resources")
		}
	}
	testutil.AssertGoroutinesReturn(t, baseline)
}
