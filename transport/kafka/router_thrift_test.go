package kafka

import (
	"context"
	"github.com/nextpkg/courier/transport/kafka/internal/wire"
	"net"
	"testing"
)

func TestThriftForwardUnsignedIdentityReplayAndClose(t *testing.T) {
	r, f, b := routerFixture(t)
	r.inbound = make(chan struct{}, 2)
	calls := 0
	in, e := r.openInbox(context.Background(), f.Session, func([]byte) { calls++ })
	if e != nil {
		t.Fatal(e)
	}
	defer func() { _ = in.Close(context.Background()) }()
	ln, e := net.Listen("tcp", "127.0.0.1:0")
	if e != nil {
		t.Fatal(e)
	}
	peer := newRPCPeer(r, ln)
	done := make(chan struct{})
	go func() { _ = peer.serve(ln); close(done) }()
	defer func() { peer.close(); <-done }()
	send := func() int {
		hdr, _ := wire.EncodeRoute(f)
		result, err := peer.forward(context.Background(), ln.Addr().String(), "Forward", hdr, b, int64(r.t.opts.MaxFrameBytes))
		if err != nil {
			t.Fatal(err)
		}
		return result.status
	}
	if s := send(); s != 204 {
		t.Fatalf("delivery status %d", s)
	}
	if s := send(); s != 204 {
		t.Fatalf("replay status %d", s)
	}
	if calls != 1 {
		t.Fatalf("duplicate calls %d", calls)
	}
	f.Instance = "old"
	if s := send(); s != 409 {
		t.Fatalf("instance status %d", s)
	}
	peer.close()
	<-done
	if _, err := peer.forward(context.Background(), ln.Addr().String(), "Forward", nil, b, int64(r.t.opts.MaxFrameBytes)); err == nil {
		t.Fatal("closed peer accepted")
	}
}
