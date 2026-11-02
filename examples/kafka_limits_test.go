//go:build integration

package examples_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/nextpkg/courier"
	"github.com/nextpkg/courier/examples/brokeradmin"
	"github.com/nextpkg/courier/examples/gen/demo"
	"github.com/nextpkg/courier/examples/handlers"
	"github.com/twmb/franz-go/pkg/kgo"
)

func TestKafkaUnroutableRecordsAndBudgets(t *testing.T) {
	admin, opts := newKafkaEnv(t, 3)
	tr := newKafkaTransport(t, opts)
	kafkaServer(t, tr, &handlers.Orders{})
	topic, _ := tr.RequestTopic("Orders")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	p, err := kgo.NewClient(kgo.SeedBrokers(opts.Brokers...), kgo.RecordPartitioner(kgo.ManualPartitioner()))
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	frames := [][]byte{[]byte("malformed")}
	for _, e := range []courier.Envelope{
		{Version: 1, Kind: "request", Service: "Inventory", Method: "Count", ID: "wrong-service", Session: "test", Reply: courier.Route{Topic: opts.ReplyTopic, Partition: 0}},
		{Version: 1, Kind: "request", Service: "Orders", Method: "Get", ID: "wrong-route", Session: "test", Reply: courier.Route{Topic: "foreign-replies", Partition: 0}},
	} {
		data, err := courier.EncodeEnvelope(e, courier.DefaultMaxFrameBytes)
		if err != nil {
			t.Fatal(err)
		}
		frames = append(frames, data)
	}
	for _, frame := range frames {
		if err := p.ProduceSync(ctx, &kgo.Record{Topic: topic, Partition: 0, Value: frame}).FirstErr(); err != nil {
			t.Fatal(err)
		}
	}
	waitFor(t, 5*time.Second, func() bool {
		n, err := brokeradmin.Committed(ctx, admin, tr.Group(topic), topic, 0)
		return err == nil && n == 3
	}, "poison records blocked committed progress")
	if tr.Stats().DiscardedRecords != 3 || tr.Stats().RequestsHandled != 0 {
		t.Fatalf("invalid route reached a handler: %+v", tr.Stats())
	}
	if _, err := demo.NewOrdersAsyncClient(kafkaClient(t, newKafkaTransport(t, opts))).Get(ctx, "after-poison", 0); err != nil {
		t.Fatal(err)
	}

	limited := opts
	limited.MaxSubscriptions = 1
	lt := newKafkaTransport(t, limited)
	kafkaClient(t, lt)
	if _, err := lt.OpenInbox(ctx, "extra", func([]byte) {}); !errors.Is(err, courier.ErrOverloaded) {
		t.Fatalf("subscription budget: %v", err)
	}
	limited = opts
	limited.MaxTopics = 1 // The one allowed metadata entry is the reply topic.
	api := demo.NewOrdersAsyncClient(kafkaClient(t, newKafkaTransport(t, limited)))
	if _, err := api.Get(ctx, "topic-budget", 0); !errors.Is(err, courier.ErrOverloaded) {
		t.Fatalf("topic budget: %v", err)
	}
	limited = opts
	limited.MaxTopicPartitions = 1
	api = demo.NewOrdersAsyncClient(kafkaClient(t, newKafkaTransport(t, limited)))
	if _, err := api.Get(ctx, "partition-budget", 0); err == nil || !strings.Contains(err.Error(), "partition budget") {
		t.Fatalf("partition budget: %v", err)
	}
}
