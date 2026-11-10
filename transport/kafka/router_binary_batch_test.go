package kafka

import (
	"bytes"
	"context"
	"io"
	"net"
	"net/http"
	"reflect"
	"testing"
)

func TestBinaryBatchWireUnsignedPartialReplay(t *testing.T) {
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
	r.rpc = peer
	r.client = &http.Client{Transport: peer}
	done := make(chan struct{})
	go func() { peer.serve(ln); close(done) }()
	defer func() { peer.close(); <-done }()
	msgs := []batchMessage{{f.Instance, f.Expires, b}, {"old", f.Expires, b}, {"", f.Expires, b}, {f.Instance, f.Expires, b}}
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
	}{{encodeBinaryBatch(msgs), 200}, {append(encodeBinaryBatch(msgs), 1), 400}} {
		req, _ := http.NewRequest("POST", endpoint+"/courier/replies", bytes.NewReader(tc.body))
		resp, err := r.client.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
		if resp.StatusCode != tc.want {
			t.Fatal(resp.StatusCode)
		}
	}
	if calls != 1 {
		t.Fatal("invalid batch delivered")
	}
	if err := in.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := r.sendBatch(endpoint, msgs[:1]); !reflect.DeepEqual(got, []int{410}) {
		t.Fatal(got)
	}
}
func TestBinaryBatchRejectsMalformedFrames(t *testing.T) {
	good := encodeBinaryBatch([]batchMessage{{"instance", 123, []byte("body")}})
	for i := 0; i < len(good); i++ {
		if _, err := decodeBinaryBatch(good[:i], 1024); err == nil {
			t.Fatalf("accepted truncated byte %d", i)
		}
	}
	if _, err := decodeBinaryBatch(good, 1); err == nil {
		t.Fatal("accepted oversized item")
	}
	if _, err := decodeBinaryBatch(append(good, 0), 1024); err == nil {
		t.Fatal("accepted trailing data")
	}
	if _, err := decodeBinaryStatuses([]byte{0}, 1); err == nil {
		t.Fatal("accepted partial status")
	}
}
