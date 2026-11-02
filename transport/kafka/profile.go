package kafka

import (
	"runtime"
	"time"

	"github.com/nextpkg/courier"
)

// CapacityProfile groups startup settings for small-message Kafka RPC workloads.
// Transport still requires Brokers and any deployment-specific routing or security.
// Callers may override fields before constructing transports, clients, and servers.
type CapacityProfile struct {
	Client    courier.ClientOptions
	Server    courier.ServerOptions
	Transport Options
}

// AutoCapacityProfile selects an opt-in throughput profile from the current
// GOMAXPROCS, capped at the measured two-CPU profile. It does not change defaults,
// inspect memory limits, or resize existing workers when CPU quotas change.
// Measurements used three clients and one server sharing a CPU/memory quota;
// validate memory, handler concurrency, and latency for the target workload.
func AutoCapacityProfile() CapacityProfile {
	cores := min(runtime.GOMAXPROCS(0), 2)
	return CapacityProfile{
		Client: courier.ClientOptions{
			PublishWorkers: 128 * cores,
			MaxInFlight:    512 * cores,
			DefaultTimeout: 8 * time.Second,
		},
		Server: courier.ServerOptions{MaxConcurrent: 512 * cores},
		Transport: Options{
			Workers:        512 * cores,
			Window:         1024 * cores,
			PublishBuffers: 512 * cores,
		},
	}
}
