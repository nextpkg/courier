//go:build integration

package examples_test

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	kafka "github.com/nextpkg/courier/transport/kafka"
	"os"
	"runtime"
	runtimeMetrics "runtime/metrics"
	"runtime/pprof"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nextpkg/courier"
	"github.com/nextpkg/courier/examples/gen/demo"
	"github.com/nextpkg/courier/examples/handlers"
)

var loadSharedClient = flag.Bool("kafka-load-shared-client", false, "share one transport across load clients")

var loadPublishWorkers = flag.Int("kafka-publish-workers", 4, "publishing workers per client in TestKafkaLoadCurve")
var loadConcurrency = flag.String("kafka-load-concurrency", "1,8,32,64,128", "comma-separated call concurrency levels in TestKafkaLoadCurve")

var loadServerWorkers = flag.Int("kafka-server-workers", 16, "Kafka service workers in TestKafkaLoadCurve")
var loadWindow = flag.Int("kafka-load-window", 64, "Kafka completion window in TestKafkaLoadCurve")
var loadDiagnosticsEnabled = flag.Bool("kafka-load-diagnostics", false, "log SDK and handler timing in TestKafkaLoadCurve")

var loadDuration = flag.Duration("kafka-load-duration", 5*time.Second, "request admission duration per load trial")
var loadMaxInFlight = flag.Int("kafka-max-in-flight", 512, "per-client in-flight limit in TestKafkaLoadCurve")
var loadCoreConcurrency = flag.Int("kafka-core-concurrency", 128, "core Server MaxConcurrent in TestKafkaLoadCurve")
var loadPublishBuffers = flag.Int("kafka-publish-buffers", 256, "per-transport Kafka producer record buffer")
var loadMixed = flag.Bool("kafka-load-mixed", false, "exercise two replicas each of Orders and Inventory with four methods")
var loadTrials = flag.Int("kafka-load-trials", 3, "number of measured rounds per concurrency level")

// Keep reply consumption independently tunable from request handling.
var loadForwardBatches = flag.Int("kafka-forward-batches", 1, "maximum in-flight reply batches per endpoint")

var loadReplyWorkers = flag.Int("kafka-reply-workers", 128, "client reply consumer workers; request workers stay independent")

var loadResourceMetrics = flag.Bool("kafka-load-resources", false, "include Linux cgroup v2 measurements per round")

// TestKafkaLoadCurve measures closed-loop load after transport startup and warmup.
func TestKafkaLoadCurve(t *testing.T) { runLoadCurve(t) }

func runLoadCurve(t *testing.T) {
	gcSettings := []runtimeMetrics.Sample{{Name: "/gc/gogc:percent"}, {Name: "/gc/gomemlimit:bytes"}}
	runtimeMetrics.Read(gcSettings)
	t.Logf("GC_RUNTIME gogc=%d gomemlimit=%d", gcSettings[0].Value.Uint64(), gcSettings[1].Value.Uint64())

	if *loadPublishWorkers < 1 || *loadDuration <= 0 || *loadTrials < 1 {
		t.Fatal("publish workers, duration and trials must be positive")
	}
	if *loadReplyWorkers < 1 || *loadWindow < *loadReplyWorkers {
		t.Fatal("invalid reply worker/window configuration")
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
	var transports []*kafka.Transport
	var newTransport func(bool) courier.Transport
	{
		_, opts := newKafkaEnv(t, 3)
		opts.PublishBuffers = *loadPublishBuffers
		serverOpts := opts
		if os.Getenv("REPLY_MODE") != "direct" {
			serverOpts.DynamicReplyPartitions = true
			opts.Workers, opts.Window = *loadReplyWorkers, *loadWindow
			batchSize, _ := strconv.Atoi(os.Getenv("THRIFT_BATCH_SIZE"))
			opts.ReplyRouter = &kafka.ReplyRouterOptions{BatchConcurrency: *loadForwardBatches, BatchSize: batchSize, ListenAddress: "127.0.0.1:0", AdvertiseURL: "http://127.0.0.1:0", AllowInsecureHTTP: true}
		}
		serverOpts.Hooks = append(serverOpts.Hooks, &loadPublishErrors{t: t, role: "server"})
		opts.Hooks = append(opts.Hooks, &loadPublishErrors{t: t, role: "client"})
		serverOpts.Workers, serverOpts.Window = *loadServerWorkers, *loadWindow
		serverTiming, clientTiming := newLoadDiagnostics(), newLoadDiagnostics()
		if *loadDiagnosticsEnabled {
			serverOpts.Hooks = append(serverOpts.Hooks, serverTiming)
			opts.Hooks = append(opts.Hooks, clientTiming)
			defer serverTiming.report(t, "server")
			defer clientTiming.report(t, "client")
		}
		newTransport = func(server bool) courier.Transport {
			if server {
				var tr courier.Transport = newKafkaTransport(t, serverOpts)
				if *loadDiagnosticsEnabled {
					tr = loadObservedTransport{tr, serverTiming}
				}
				return tr
			}
			tr := newKafkaTransport(t, opts)
			transports = append(transports, tr)
			return tr
		}
	}
	serverTransport := newTransport(true)
	server, err := demo.ListenOrders(context.Background(), serverTransport, &handlers.Orders{}, courier.ServerOptions{MaxConcurrent: *loadCoreConcurrency})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { closeTestResource(t, server) })
	if *loadMixed {
		for _, service := range []string{"Orders", "Inventory", "Inventory"} {
			tr := newTransport(true)
			var replica *courier.Server
			if service == "Orders" {
				replica, err = demo.ListenOrders(context.Background(), tr, &handlers.Orders{}, courier.ServerOptions{MaxConcurrent: *loadCoreConcurrency})
			} else {
				replica, err = demo.ListenInventory(context.Background(), tr, &loadInventory{}, courier.ServerOptions{MaxConcurrent: *loadCoreConcurrency})
			}
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { closeTestResource(t, replica) })
		}
	}
	var requestSequence atomic.Uint64
	var clients []*courier.Client
	var shared courier.Transport
	if *loadSharedClient {
		shared = newTransport(false)
	}

	for i := 0; i < 3; i++ {
		tr := shared
		if tr == nil {
			tr = newTransport(false)
		}
		c, err := courier.NewClient(context.Background(), tr, courier.ClientOptions{MaxInFlight: *loadMaxInFlight, DefaultTimeout: 8 * time.Second, PublishWorkers: *loadPublishWorkers})
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
		for trial := 1; trial <= *loadTrials; trial++ {
			var mu sync.Mutex
			var wg sync.WaitGroup
			latencies := newLoadHistogram()
			failures := 0
			failureReasons := make(map[string]int)
			completedMethods := make(map[string]int)
			var readsBefore, matchedBefore, httpBefore, messagesBefore uint64
			for _, tr := range transports {
				readsBefore += tr.Stats().ReplyRecords
				matchedBefore += tr.Stats().ReplyMatched
				httpBefore += tr.RouterStats().HTTPRequests
				messagesBefore += tr.RouterStats().HTTPMessages
			}
			var resourcesBefore map[string]uint64
			if *loadResourceMetrics {
				var err error
				resourcesBefore, _, err = loadResources()
				if err != nil {
					t.Fatal(err)
				}
			}
			profiling := os.Getenv("COURIER_PROFILE") == "1"
			var pf *os.File
			var err error
			if profiling {
				pf, err = os.Create("/bench/" + os.Getenv("REPLY_MODE") + ".cpu")
				if err != nil {
					t.Fatal(err)
				}
				runtime.SetBlockProfileRate(0)
				runtime.SetMutexProfileFraction(0)
				if err := pprof.StartCPUProfile(pf); err != nil {
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
					inventory := demo.NewInventoryAsyncClient(clients[worker%len(clients)])
					for sequence := 0; time.Now().Before(until); sequence++ {
						id := fmt.Sprintf("%d/%d/%d/%d", concurrency, trial, worker, sequence)
						if *loadMixed {
							id = strconv.FormatUint(requestSequence.Add(1), 10)
						}
						began := time.Now()
						method := "Orders.Get"
						valid := false
						var err error
						if !*loadMixed || worker%4 == 0 {
							var result *demo.Item
							result, err = api.Get(context.Background(), id, 0)
							valid = result != nil && result.ID == id && result.Source == "Orders"
						} else {
							switch worker % 4 {
							case 1:
								method = "Orders.List"
								var result []*demo.Item
								result, err = api.List(context.Background(), []string{id})
								valid = len(result) == 1 && result[0] != nil && result[0].ID == id && result[0].Source == "Orders"
							case 2:
								method = "Inventory.Reserve"
								var result *demo.Item
								result, err = inventory.Reserve(context.Background(), id)
								valid = result != nil && result.ID == id && result.Source == "Inventory"
							case 3:
								method = "Inventory.Count"
								var result int64
								result, err = inventory.Count(context.Background(), id)
								expected, _ := strconv.ParseInt(id, 10, 64)
								valid = result == expected
							}
						}
						elapsed := float64(time.Since(began).Microseconds()) / 1000
						mu.Lock()
						if err != nil || !valid {
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
							latencies.add(elapsed)
							completedMethods[method]++
						}
						mu.Unlock()
					}
				}(worker)
			}
			wg.Wait()
			elapsed := time.Since(start).Seconds()
			if profiling {
				pprof.StopCPUProfile()
				pf.Close()
				for _, name := range []string{"allocs", "block", "mutex"} {
					f, err := os.Create("/bench/" + os.Getenv("REPLY_MODE") + "." + name)
					if err != nil {
						t.Fatal(err)
					}
					pprof.Lookup(name).WriteTo(f, 0)
					f.Close()
				}
				runtime.SetBlockProfileRate(0)
				runtime.SetMutexProfileFraction(0)
			}
			if latencies.count == 0 {
				t.Fatal("no successful calls")
			}
			percentile := latencies.percentile
			metrics := map[string]any{
				"admission_seconds": loadDuration.Seconds(), "concurrency": concurrency, "trial": trial, "sessions": 3, "publish_workers": *loadPublishWorkers, "server_workers": *loadServerWorkers, "window": *loadWindow, "core_concurrency": *loadCoreConcurrency, "publish_buffers": *loadPublishBuffers, "max_in_flight": *loadMaxInFlight,
				"latency_bin_ms": 0.1, "latency_overflow": latencies.overflow, "transport": "kafka", "mixed_services": *loadMixed, "completed_methods": completedMethods,
				"successes": latencies.count, "failures": failures, "failure_reasons": failureReasons, "elapsed_seconds": elapsed,
				"successful_calls_per_second": float64(latencies.count) / elapsed,
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
			var reads, matched, httpCalls, messages uint64
			for _, tr := range transports {
				reads += tr.Stats().ReplyRecords
				matched += tr.Stats().ReplyMatched
				httpCalls += tr.RouterStats().HTTPRequests
				messages += tr.RouterStats().HTTPMessages
			}
			metrics["reply_reads"] = reads - readsBefore
			metrics["reply_matched"] = matched - matchedBefore
			metrics["http_requests"] = httpCalls - httpBefore
			metrics["forward_messages"] = messages - messagesBefore
			metrics["batch_limit"] = os.Getenv("THRIFT_BATCH_SIZE")
			metrics["reply_mode"] = os.Getenv("REPLY_MODE")
			metrics["reply_workers"] = *loadReplyWorkers
			metrics["forward_batches"] = *loadForwardBatches
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

// loadInventory echoes the numeric request token so Count responses can be
// checked for cross-call mixups even when input strings have equal lengths.
type loadInventory struct{ handlers.Inventory }

func (loadInventory) Count(_ context.Context, id string) (int64, error) {
	return strconv.ParseInt(id, 10, 64)
}
