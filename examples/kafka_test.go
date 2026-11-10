//go:build integration

package examples_test

import (
	"context"
	"fmt"
	"github.com/nextpkg/courier"
	"github.com/nextpkg/courier/examples/brokeradmin"
	"github.com/nextpkg/courier/examples/contract"
	transport "github.com/nextpkg/courier/transport/kafka"
	"github.com/twmb/franz-go/pkg/kgo"
	"os"
	"strings"
	"sync"
	"testing"
	"time"
)

func kafkaBrokers(t *testing.T) []string {
	t.Helper()
	u := os.Getenv("KAFKA_BROKERS")
	if u == "" {
		t.Fatal("integration requires KAFKA_BROKERS pointing to real Kafka")
	}
	return strings.Split(u, ",")
}
func kafkaAdmin(t *testing.T) *kgo.Client {
	t.Helper()
	c, err := kgo.NewClient(kgo.SeedBrokers(kafkaBrokers(t)...))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(c.Close)
	return c
}
func provisionKafka(t *testing.T, c *kgo.Client, prefix string, n int32) transport.Options {
	t.Helper()
	topics := map[string]int32{prefix + ".request.Orders": n, prefix + ".request.Inventory": n, prefix + ".replies": 1}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := brokeradmin.Create(ctx, c, topics); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		var names []string
		for n := range topics {
			names = append(names, n)
		}
		if err := brokeradmin.Delete(ctx, c, names); err != nil {
			t.Error(err)
		}
	})
	return transport.Options{Brokers: kafkaBrokers(t), Prefix: prefix, ReplyTopic: prefix + ".replies"}
}
func newKafkaTransport(t *testing.T, o transport.Options) *transport.Transport {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	tr, err := transport.New(ctx, o)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := tr.Close(ctx); err != nil {
			t.Error(err)
		}
		if tr.Stats().Subscriptions != 0 {
			t.Fatal("Kafka subscription leak")
		}
	})
	return tr
}
func TestKafkaContract(t *testing.T) {
	admin := kafkaAdmin(t)
	var mu sync.Mutex
	configs := map[string]transport.Options{}
	contract.Run(t, func(t *testing.T) courier.Transport {
		mu.Lock()
		defer mu.Unlock()
		opts, ok := configs[t.Name()]
		if !ok {
			opts = provisionKafka(t, admin, fmt.Sprintf("courier.contract.%d", time.Now().UnixNano()), 3)
			configs[t.Name()] = opts
		}
		return newKafkaTransport(t, opts)
	})
}
