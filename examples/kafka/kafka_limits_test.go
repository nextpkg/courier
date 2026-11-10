//go:build integration

package kafka_test

import (
	"context"
	"errors"
	"github.com/nextpkg/courier/examples/internal/kafkatest"
	"github.com/nextpkg/courier/examples/internal/testutil"
	"github.com/nextpkg/courier/internal/testcheck"
	"strings"
	"testing"
	"time"

	"github.com/nextpkg/courier"
	"github.com/nextpkg/courier/examples/brokeradmin"
	"github.com/nextpkg/courier/examples/gen/demo"
	"github.com/nextpkg/courier/examples/handlers"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"
)

func TestKafkaUnroutableRecordsAndBudgets(t *testing.T) {
	admin, opts := kafkatest.NewEnv(t, 3)
	tr := kafkatest.NewTransport(t, opts)
	testutil.Server(t, tr, &handlers.Orders{})
	topic, _ := tr.RequestTopic("Orders")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	p, err := kgo.NewClient(kgo.SeedBrokers(opts.Brokers...), kgo.RecordPartitioner(kgo.ManualPartitioner()))
	require.NoError(t, err)

	defer p.Close()
	frames := [][]byte{[]byte("malformed")}
	for _, e := range []courier.Envelope{
		{Version: 1, Kind: "request", Service: "Inventory", Method: "Count", ID: "wrong-service", Session: "test", Reply: courier.Route{Topic: opts.ReplyTopic, Partition: 0}},
		{Version: 1, Kind: "request", Service: "Orders", Method: "Get", ID: "wrong-route", Session: "test", Reply: courier.Route{Topic: "foreign-replies", Partition: 0}},
	} {
		data, err := courier.EncodeEnvelope(e, courier.DefaultMaxFrameBytes)
		require.NoError(t, err)

		frames = append(frames, data)
	}
	for _, frame := range frames {
		require.NoError(t, p.ProduceSync(ctx, &kgo.Record{Topic: topic, Partition: 0, Value: frame}).FirstErr())

	}
	testcheck.WaitFor(t, 5*time.Second, func() bool {
		n, err := brokeradmin.Committed(ctx, admin, tr.Group(topic), topic, 0)
		return err == nil && n == 3
	}, "poison records blocked committed progress")
	if tr.Stats().DiscardedRecords != 3 || tr.Stats().RequestsHandled != 0 {
		t.Fatalf("invalid route reached a handler: %+v", tr.Stats())
	}
	if _, err := demo.NewOrdersAsyncClient(testutil.Client(t, kafkatest.NewTransport(t, opts))).Get(ctx, "after-poison", 0); err != nil {
		t.Fatal(err)
	}

	limited := opts
	limited.MaxSubscriptions = 1
	lt := kafkatest.NewTransport(t, limited)
	testutil.Client(t, lt)
	if _, err := lt.OpenInbox(ctx, "extra", func([]byte) {}); !errors.Is(err, courier.ErrOverloaded) {
		t.Fatalf("subscription budget: %v", err)
	}
	limited = opts
	limited.MaxTopics = 1 // The one allowed metadata entry is the reply topic.
	api := demo.NewOrdersAsyncClient(testutil.Client(t, kafkatest.NewTransport(t, limited)))
	if _, err := api.Get(ctx, "topic-budget", 0); !errors.Is(err, courier.ErrOverloaded) {
		t.Fatalf("topic budget: %v", err)
	}
	limited = opts
	limited.MaxTopicPartitions = 1
	api = demo.NewOrdersAsyncClient(testutil.Client(t, kafkatest.NewTransport(t, limited)))
	if _, err := api.Get(ctx, "partition-budget", 0); err == nil || !strings.Contains(err.Error(), "partition budget") {
		t.Fatalf("partition budget: %v", err)
	}
}
