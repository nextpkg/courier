//go:build integration

package examples_test

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nextpkg/courier"
	"github.com/nextpkg/courier/examples/gen/demo"
	"github.com/nextpkg/courier/examples/handlers"
)

var loadPublishWorkers = flag.Int("kafka-publish-workers", 4, "publishing workers per client in TestKafkaLoadCurve")
var loadConcurrency = flag.String("kafka-load-concurrency", "1,8,32,64,128", "comma-separated call concurrency levels in TestKafkaLoadCurve")

var loadServerWorkers = flag.Int("kafka-server-workers", 16, "Kafka service workers in TestKafkaLoadCurve")
var loadWindow = flag.Int("kafka-load-window", 64, "Kafka completion window in TestKafkaLoadCurve")
var loadDiagnosticsEnabled = flag.Bool("kafka-load-diagnostics", false, "log SDK and handler timing in TestKafkaLoadCurve")

var loadDuration = flag.Duration("kafka-load-duration", 5*time.Second, "request admission duration per load trial")
var loadMaxInFlight = flag.Int("kafka-max-in-flight", 512, "per-client in-flight limit in TestKafkaLoadCurve")
var loadCoreConcurrency = flag.Int("kafka-core-concurrency", 128, "core Server MaxConcurrent in TestKafkaLoadCurve")
var loadPublishBuffers = flag.Int("kafka-publish-buffers", 256, "per-transport Kafka producer record buffer")
var loadResourceMetrics = flag.Bool("kafka-load-resources", false, "include Linux cgroup v2 measurements per round")

// TestKafkaLoadCurve measures closed-loop load after transport startup and warmup.
func TestKafkaLoadCurve(t *testing.T) {
	if *loadPublishWorkers < 1 || *loadDuration <= 0 {
		t.Fatal("kafka-publish-workers and kafka-load-duration must be positive")
	}
	if *loadServerWorkers < 1 || *loadWindow < *loadServerWorkers {
		t.Fatal("kafka-load-window must be at least the positive kafka-server-workers value")
	}
	if *loadCoreConcurrency < 1 || *loadPublishBuffers < 1 || *loadMaxInFlight < 1 {
		t.Fatal("core concurrency, publish buffers and in-flight limit must be positive")
	}
	var levels []int
	for _, value := range strings.Split(*loadConcurrency, ",") {
		n, err := strconv.Atoi(strings.TrimSpace(value))
		if err != nil || n < 1 {
			t.Fatalf("invalid concurrency %q", value)
		}
		levels = append(levels, n)
	}
	_, opts := newKafkaEnv(t, 3)
	opts.PublishBuffers = *loadPublishBuffers
	serverOpts := opts
	serverOpts.Hooks = append(serverOpts.Hooks, &loadPublishErrors{t: t, role: "server"})
	opts.Hooks = append(opts.Hooks, &loadPublishErrors{t: t, role: "client"})
	serverOpts.Workers, serverOpts.Window = *loadServerWorkers, *loadWindow
	serverTiming, clientTiming := newLoadDiagnostics(), newLoadDiagnostics()
	if *loadDiagnosticsEnabled {
		serverOpts.Hooks = append(serverOpts.Hooks, serverTiming)
		opts.Hooks = append(opts.Hooks, clientTiming)
	}
	var serverTransport courier.Transport = newKafkaTransport(t, serverOpts)
	if *loadDiagnosticsEnabled {
		serverTransport = loadObservedTransport{serverTransport, serverTiming}
		defer serverTiming.report(t, "server")
		defer clientTiming.report(t, "client")
	}
	server, err := demo.ListenOrders(context.Background(), serverTransport, &handlers.Orders{}, courier.ServerOptions{MaxConcurrent: *loadCoreConcurrency})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { closeTestResource(t, server) })
	var clients []*courier.Client
	for i := 0; i < 3; i++ {
		c, err := courier.NewClient(context.Background(), newKafkaTransport(t, opts), courier.ClientOptions{MaxInFlight: *loadMaxInFlight, DefaultTimeout: 8 * time.Second, PublishWorkers: *loadPublishWorkers})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { closeTestResource(t, c) })
		clients = append(clients, c)
		if _, err := demo.NewOrdersAsyncClient(c).Get(context.Background(), "warmup", 0); err != nil {
			t.Fatal(err)
		}
	}
	for _, concurrency := range levels {
		for trial := 1; trial <= 3; trial++ {
			var mu sync.Mutex
			var wg sync.WaitGroup
			var latencies []float64
			failures := 0
			failureReasons := make(map[string]int)
			var resourcesBefore map[string]uint64
			if *loadResourceMetrics {
				var err error
				resourcesBefore, _, err = loadResources()
				if err != nil {
					t.Fatal(err)
				}
			}
			start := time.Now()
			until := start.Add(*loadDuration)
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
							reason := "nil or mismatched response"
							if err != nil {
								reason = fmt.Sprintf("%T: %v", err, err)
							}
							if _, known := failureReasons[reason]; !known && len(failureReasons) >= 16 {
								reason = "other errors (sample limit)"
							}
							failureReasons[reason]++
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
			metrics := map[string]any{
				"admission_seconds": loadDuration.Seconds(), "concurrency": concurrency, "trial": trial, "sessions": 3, "publish_workers": *loadPublishWorkers, "server_workers": *loadServerWorkers, "window": *loadWindow, "core_concurrency": *loadCoreConcurrency, "publish_buffers": *loadPublishBuffers, "max_in_flight": *loadMaxInFlight,
				"successes": len(latencies), "failures": failures, "failure_reasons": failureReasons, "elapsed_seconds": elapsed,
				"successful_calls_per_second": float64(len(latencies)) / elapsed,
				"p50_ms":                      percentile(.50), "p95_ms": percentile(.95), "p99_ms": percentile(.99),
			}
			if *loadResourceMetrics {
				after, quota, err := loadResources()
				if err != nil {
					t.Fatal(err)
				}
				metrics["cpu_cores"] = float64(after["usage_usec"]-resourcesBefore["usage_usec"]) / 1e6 / elapsed
				metrics["cpu_quota_cores"] = quota
				metrics["cpu_quota_utilization"] = metrics["cpu_cores"].(float64) / quota
				metrics["throttled_periods"] = after["nr_throttled"] - resourcesBefore["nr_throttled"]
				metrics["memory_peak_bytes"] = after["memory_peak_bytes"]
			}
			data, err := json.Marshal(metrics)
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
