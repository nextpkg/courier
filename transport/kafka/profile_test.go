package kafka

import (
	"runtime"
	"testing"
	"time"
)

func TestAutoCapacityProfile(t *testing.T) {
	previous := runtime.GOMAXPROCS(0)
	t.Cleanup(func() { runtime.GOMAXPROCS(previous) })
	for _, tc := range []struct{ cpus, publishers, workers, inFlight int }{
		{1, 128, 512, 512}, {2, 256, 1024, 1024}, {8, 256, 1024, 1024},
	} {
		runtime.GOMAXPROCS(tc.cpus)
		p := AutoCapacityProfile()
		if p.Client.PublishWorkers != tc.publishers || p.Client.MaxInFlight != tc.inFlight ||
			p.Client.DefaultTimeout != 8*time.Second || p.Server.MaxConcurrent != tc.workers ||
			p.Transport.Workers != tc.workers || p.Transport.Window != 2*tc.workers ||
			p.Transport.PublishBuffers != tc.workers {
			t.Fatalf("CPU %d: unexpected profile: %+v", tc.cpus, p)
		}
		p.Transport.Brokers = []string{"localhost:9092"}
		normalized, err := normalize(p.Transport)
		if err != nil {
			t.Fatalf("CPU %d: invalid profile: %v", tc.cpus, err)
		}
		if normalized.RebalanceTimeout != 80*time.Second {
			t.Fatalf("CPU %d: unexpected drain budget %s", tc.cpus, normalized.RebalanceTimeout)
		}
	}
}
