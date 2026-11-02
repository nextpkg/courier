//go:build integration

package examples_test

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/nextpkg/courier"
	"github.com/nextpkg/courier/examples/gen/demo"
	"github.com/nextpkg/courier/examples/handlers"
)

// TestKafkaLoadCurve measures closed-loop load after transport startup and warmup.
func TestKafkaLoadCurve(t *testing.T) {
	_, opts := newKafkaEnv(t, 3)
	kafkaServer(t, newKafkaTransport(t, opts), &handlers.Orders{})
	var clients []*courier.Client
	for i := 0; i < 3; i++ {
		c := kafkaClient(t, newKafkaTransport(t, opts))
		clients = append(clients, c)
		if _, err := demo.NewOrdersAsyncClient(c).Get(context.Background(), "warmup", 0); err != nil {
			t.Fatal(err)
		}
	}
	for _, concurrency := range []int{1, 8, 32, 64, 128} {
		for trial := 1; trial <= 3; trial++ {
			var mu sync.Mutex
			var wg sync.WaitGroup
			var latencies []float64
			failures := 0
			start := time.Now()
			until := start.Add(5 * time.Second)
			for worker := 0; worker < concurrency; worker++ {
				wg.Add(1)
				go func(worker int) {
					defer wg.Done()
					api := demo.NewOrdersAsyncClient(clients[worker%len(clients)])
					for sequence := 0; time.Now().Before(until); sequence++ {
						id := fmt.Sprintf("%d/%d/%d/%d", concurrency, trial, worker, sequence)
						began := time.Now()
						result, err := api.Get(context.Background(), id, 0)
						elapsed := float64(time.Since(began).Microseconds()) / 1000
						mu.Lock()
						if err != nil || result == nil || result.ID != id || result.Source != "Orders" {
							failures++
						} else {
							latencies = append(latencies, elapsed)
						}
						mu.Unlock()
					}
				}(worker)
			}
			wg.Wait()
			elapsed := time.Since(start).Seconds()
			if len(latencies) == 0 {
				t.Fatal("no successful calls")
			}
			sort.Float64s(latencies)
			percentile := func(p float64) float64 { return latencies[int(float64(len(latencies)-1)*p)] }
			data, err := json.Marshal(map[string]any{
				"concurrency": concurrency, "trial": trial, "sessions": 3,
				"successes": len(latencies), "failures": failures, "elapsed_seconds": elapsed,
				"successful_calls_per_second": float64(len(latencies)) / elapsed,
				"p50_ms":                      percentile(.50), "p95_ms": percentile(.95), "p99_ms": percentile(.99),
			})
			if err != nil {
				t.Fatal(err)
			}
			t.Log("KAFKA_LOAD " + string(data))
			if failures != 0 {
				t.Errorf("%d calls failed or returned mismatched results", failures)
			}
			for _, c := range clients {
				if c.Pending() != 0 {
					t.Error("pending calls after load drained")
				}
			}
		}
	}
}
