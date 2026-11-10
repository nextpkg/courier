package kafka

import (
	"bytes"
	"github.com/twmb/franz-go/pkg/kgo"
	"runtime"
	"testing"
)

func TestBorrowedHeaderSelectionAndCapacity(t *testing.T) {
	backing := []byte("route-sentinel")
	headers := []kgo.RecordHeader{{Key: routeHeader, Value: backing[:5]}, {Key: routeHeader, Value: []byte("second")}}
	h, ok := borrowedRouteHeader(headers)
	if !ok || string(h) != "route" || cap(h) != len(h) {
		t.Fatal("selection or capacity")
	}
	appended := append(h, []byte("payload")...)
	if string(backing) != "route-sentinel" || string(appended) != "routepayload" {
		t.Fatal("append corrupted fetch storage")
	}
	if _, found := borrowedRouteHeader(nil); found {
		t.Fatal("missing header")
	}
	if v, found := borrowedRouteHeader([]kgo.RecordHeader{{Key: routeHeader}}); !found || len(v) != 0 {
		t.Fatal("empty header")
	}
}

func BenchmarkHeaderAccess(b *testing.B) {
	headers := []kgo.RecordHeader{{Key: routeHeader, Value: bytes.Repeat([]byte("x"), 300)}}
	b.Run("clone", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			h := replyHeaders(headers)
			runtime.KeepAlive(h)
		}
	})
	b.Run("borrow", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			h, _ := borrowedRouteHeader(headers)
			runtime.KeepAlive(h)
		}
	})
}
