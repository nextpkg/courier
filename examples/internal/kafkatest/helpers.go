//go:build integration

package kafkatest

import (
	"context"
	"fmt"
	"github.com/nextpkg/courier/examples/brokeradmin"
	transport "github.com/nextpkg/courier/transport/kafka"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"
	"os"
	"strings"
	"testing"
	"time"
)

func Brokers(t *testing.T) []string {
	t.Helper()
	u := os.Getenv("KAFKA_BROKERS")
	if u == "" {
		t.Fatal("integration requires KAFKA_BROKERS pointing to real Kafka")
	}
	return strings.Split(u, ",")
}

func Admin(t *testing.T) *kgo.Client {
	t.Helper()
	c, err := kgo.NewClient(kgo.SeedBrokers(Brokers(t)...))
	require.NoError(t, err)

	t.Cleanup(c.Close)
	return c
}

func Provision(t *testing.T, c *kgo.Client, prefix string, n int32) transport.Options {
	t.Helper()
	topics := map[string]int32{prefix + ".request.Orders": n, prefix + ".request.Inventory": n, prefix + ".replies": 1}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	require.NoError(t, brokeradmin.Create(ctx, c, topics))

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
	return transport.Options{Brokers: Brokers(t), Prefix: prefix, ReplyTopic: prefix + ".replies"}
}

func NewTransport(t *testing.T, o transport.Options) *transport.Transport {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	tr, err := transport.New(ctx, o)
	require.NoError(t, err)

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
func NewEnv(t *testing.T, partitions int32) (*kgo.Client, transport.Options) {
	t.Helper()
	admin := Admin(t)
	return admin, Provision(t, admin, fmt.Sprintf("courier.test.%d", time.Now().UnixNano()), partitions)
}
