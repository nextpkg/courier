//go:build integration

package kafka_test

import (
	"context"
	"fmt"
	"github.com/nextpkg/courier"
	"github.com/nextpkg/courier/examples/internal/kafkatest"
	"github.com/nextpkg/courier/internal/testcheck"
	kafka "github.com/nextpkg/courier/transport/kafka"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kmsg"
	"net/url"
	"os"
	"sync"
	"testing"
	"time"
)

func routerOptions(o kafka.Options) kafka.Options {
	o.ReplyRouter = &kafka.ReplyRouterOptions{ForwardProtocol: func() string {
		if os.Getenv("REPLY_MODE") == "thrift" {
			return "thrift"
		}
		return "http"
	}(), BatchConcurrency: *loadForwardBatches, ListenAddress: "127.0.0.1:0", AdvertiseURL: "http://127.0.0.1:0", AllowInsecureHTTP: true, MaxReplyAge: 15 * time.Second}
	return o
}
func eventuallyRouter(t *testing.T, f func() bool) {
	t.Helper()
	testcheck.Poll(t, 20*time.Second, 20*time.Millisecond, f, "router condition timed out")
}
func TestKafkaRouterRoutingExpansionAndRestart(t *testing.T) {
	for _, size := range []int{0, 32} {
		t.Run(fmt.Sprintf("batch-%d", size), func(t *testing.T) { testKafkaRouterLifecycle(t, size) })
	}
}
func testKafkaRouterLifecycle(t *testing.T, batchSize int) {
	routerOptions := func(o kafka.Options) kafka.Options {
		o = routerOptions(o)
		o.ReplyRouter.BatchSize = batchSize
		return o
	}
	admin, opts := kafkatest.NewEnv(t, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	serverOpts := opts
	serverOpts.DynamicReplyPartitions = true
	server := kafkatest.NewTransport(t, serverOpts)
	var mu sync.Mutex
	partitions := map[int32]bool{}
	block, release := make(chan struct{}), make(chan struct{})
	var releaseOnce sync.Once
	defer releaseOnce.Do(func() { close(release) })
	sub, err := server.Listen(ctx, "Orders", func(ctx context.Context, b []byte) ([]byte, error) {
		e, err := courier.DecodeEnvelope(b, courier.DefaultMaxFrameBytes)
		if err != nil {
			return nil, err
		}
		if e.ID == "old" {
			close(block)
			select {
			case <-release:
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
		mu.Lock()
		partitions[e.Reply.Partition] = true
		mu.Unlock()
		e.Kind = "response"
		return courier.EncodeEnvelope(e, courier.DefaultMaxFrameBytes)
	})
	require.NoError(t, err)

	defer sub.Close(context.Background())
	peers := []*kafka.Transport{}
	ins := []courier.Inbox{}
	got := make(chan string, 200)
	for i := 0; i < 3; i++ {
		tr := kafkatest.NewTransport(t, routerOptions(opts))
		peers = append(peers, tr)
		session := fmt.Sprint("s", i)
		in, err := tr.OpenInbox(ctx, session, func(b []byte) {
			e, err := courier.DecodeEnvelope(b, courier.DefaultMaxFrameBytes)
			if err != nil || e.Session != session {
				t.Error("cross-session reply")
			}
			got <- e.ID
		})
		require.NoError(t, err)

		ins = append(ins, in)
	}
	send := func(i int, id string) {
		e := courier.Envelope{Version: 1, Kind: "request", Service: "Orders", Method: "Get", ID: id, Session: fmt.Sprint("s", i), Deadline: time.Now().Add(15 * time.Second).UnixNano(), Reply: ins[i].Route()}
		prepared, err := peers[i].PrepareRequest(ctx, e, courier.DefaultMaxFrameBytes)
		if err != nil {
			t.Fatal(err)
		}
		if err := prepared.Send(ctx); err != nil {
			t.Fatal(err)
		}
	}
	receive := func(n int) {
		t.Logf("await %d replies", n)
		seen := map[string]bool{}
		for j := 0; j < n; j++ {
			select {
			case id := <-got:
				if seen[id] {
					t.Fatal("duplicate", id)
				}
				seen[id] = true
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
		}
	}
	// Join all peers before measuring normal-mode fetch amplification.
	eventuallyRouter(t, func() bool {
		for _, p := range peers {
			if p.Stats().GroupAssignments == 0 {
				return false
			}
		}
		return true
	})
	for i := 0; i < 30; i++ {
		send(i%3, fmt.Sprint(i))
	}
	receive(30)
	eventuallyRouter(t, func() bool {
		var n uint64
		for _, p := range peers {
			n += p.Stats().CommittedRecords
		}
		return n == 30
	})
	var reads uint64
	for _, p := range peers {
		reads += p.Stats().ReplyRecords
	}
	if reads != 30 {
		t.Fatalf("read amplification: %d/30", reads)
	}
	req := kmsg.NewPtrCreatePartitionsRequest()
	topic := kmsg.NewCreatePartitionsRequestTopic()
	topic.Topic = opts.ReplyTopic
	topic.Count = 3
	req.Topics = []kmsg.CreatePartitionsRequestTopic{topic}
	res, err := req.RequestWith(ctx, admin)
	require.NoError(t, err)

	for _, x := range res.Topics {
		if x.ErrorCode != 0 {
			t.Fatal(x)
		}
	}
	time.Sleep(1500 * time.Millisecond)
	for i := 0; i < 60; i++ {
		send(i%3, fmt.Sprint("expanded-", i))
	}
	receive(60)
	mu.Lock()
	count := len(partitions)
	mu.Unlock()
	if count != 3 {
		t.Fatalf("only %d partitions used after expansion", count)
	}
	// Lose a forwarding peer while replies for another origin are in flight.
	sent := make(chan struct{})
	go func() {
		defer close(sent)
		for i := 0; i < 90; i++ {
			send(0, fmt.Sprint("rebalance-", i))
		}
	}()
	require.NoError(t, peers[2].Close(ctx))

	<-sent
	receive(90)
	peers[2] = kafkatest.NewTransport(t, routerOptions(opts))
	// Hold one reply across origin restart with the exact same endpoint/session.
	send(0, "old")
	select {
	case <-block:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	oldEndpoint := peers[0].ReplyEndpoint()
	require.NoError(t, peers[0].Close(ctx))

	o := routerOptions(opts)
	u, _ := url.Parse(oldEndpoint)
	o.ReplyRouter.ListenAddress = u.Host
	o.ReplyRouter.AdvertiseURL = oldEndpoint
	peers[0] = kafkatest.NewTransport(t, o)
	ins[0], err = peers[0].OpenInbox(ctx, "s0", func(b []byte) { e, _ := courier.DecodeEnvelope(b, courier.DefaultMaxFrameBytes); got <- e.ID })
	require.NoError(t, err)

	releaseOnce.Do(func() { close(release) })
	eventuallyRouter(t, func() bool {
		var n uint64
		for _, p := range peers {
			n += p.RouterStats().StaleInstances
		}
		return n > 0
	})
	send(0, "new")
	select {
	case id := <-got:
		if id != "new" {
			t.Fatal("old instance misdelivery", id)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
}
