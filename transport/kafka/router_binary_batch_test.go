package kafka

import (
	"context"
	"github.com/nextpkg/courier/transport/kafka/internal/wire"
	"github.com/stretchr/testify/require"
	"reflect"
	"testing"
)

func TestBinaryBatchWireUnsignedPartialReplay(t *testing.T) {
	r, f, b := routerFixture(t)
	r.inbound = make(chan struct{}, 2)
	calls := 0
	in, e := r.openInbox(context.Background(), f.Session, func([]byte) { calls++ })
	require.NoError(t, e)
	defer func() { _ = in.Close(context.Background()) }()
	peer, ln, _ := startTestRPCPeer(t, r)
	r.rpc = peer
	msgs := []wire.BatchMessage{{Instance: f.Instance, Expires: f.Expires, Data: b}, {Instance: "old", Expires: f.Expires, Data: b}, {Instance: "", Expires: f.Expires, Data: b}, {Instance: f.Instance, Expires: f.Expires, Data: b}}
	endpoint := "http://" + ln.Addr().String()
	if got := r.sendBatch(endpoint, msgs); !reflect.DeepEqual(got, []int{204, 409, 400, 204}) {
		t.Fatal(got)
	}
	if calls != 1 {
		t.Fatal(calls)
	}
	for _, tc := range []struct {
		body []byte
		want int
	}{{wire.EncodeBatch(msgs), 200}, {append(wire.EncodeBatch(msgs), 1), 400}} {
		result, err := peer.forward(context.Background(), ln.Addr().String(), "ForwardBatch", nil, tc.body, r.batchLimit())
		require.NoError(t, err)

		if result.status != tc.want {
			t.Fatal(result.status)
		}
	}
	if calls != 1 {
		t.Fatal("invalid batch delivered")
	}
	require.NoError(t, in.Close(context.Background()))

	if got := r.sendBatch(endpoint, msgs[:1]); !reflect.DeepEqual(got, []int{410}) {
		t.Fatal(got)
	}
}
