package kafka

import (
	"context"
	"io"
	"net"
	"net/http"
	"strings"
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
	defer in.Close(context.Background())
	ln, e := net.Listen("tcp", "127.0.0.1:0")
	if e != nil {
		t.Fatal(e)
	}
	peer := newRPCPeer(r, ln)
	done := make(chan struct{})
	go func() { peer.serve(ln); close(done) }()
	defer func() { peer.close(); <-done }()
	client := &http.Client{Transport: peer}
	send := func() int {
		hdr, _ := marshalForwardRoute(f)
		req, _ := http.NewRequest("POST", "http://"+ln.Addr().String()+"/courier/reply", strings.NewReader(string(b)))
		req.Header.Set("X-Courier-Route", string(hdr))
		resp, err := client.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
		return resp.StatusCode
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
	req, _ := http.NewRequest("POST", "http://"+ln.Addr().String()+"/courier/reply", nil)
	if _, err := client.Do(req); err == nil {
		t.Fatal("closed peer accepted")
	}
}
