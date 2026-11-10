//go:build integration

package kafka_test

import (
	"fmt"
	"github.com/nextpkg/courier"
	"github.com/nextpkg/courier/examples/contract"
	"github.com/nextpkg/courier/examples/internal/kafkatest"
	transport "github.com/nextpkg/courier/transport/kafka"
	"sync"
	"testing"
	"time"
)

func TestKafkaContract(t *testing.T) {
	admin := kafkatest.Admin(t)
	var mu sync.Mutex
	configs := map[string]transport.Options{}
	contract.Run(t, func(t *testing.T) courier.Transport {
		mu.Lock()
		defer mu.Unlock()
		opts, ok := configs[t.Name()]
		if !ok {
			opts = kafkatest.Provision(t, admin, fmt.Sprintf("courier.contract.%d", time.Now().UnixNano()), 3)
			configs[t.Name()] = opts
		}
		return kafkatest.NewTransport(t, opts)
	})
}
