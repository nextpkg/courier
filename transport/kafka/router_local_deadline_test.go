package kafka

import (
	"context"
	"errors"
	"github.com/nextpkg/courier/transport/kafka/internal/wire"
	"github.com/twmb/franz-go/pkg/kgo"
	"testing"
	"time"
)

func TestLocalForwardDeadlineAndCancellation(t *testing.T) {
	for _, name := range []string{"live", "expired", "cancelled", "parent-expired"} {
		t.Run(name, func(t *testing.T) {
			r, f, b := routerFixture(t)
			calls := 0
			in, err := r.openInbox(context.Background(), f.Session, func([]byte) { calls++ })
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = in.Close(context.Background()) }()
			ctx := context.Background()
			switch name {
			case "expired":
				f.Expires = time.Now().Add(-time.Second).UnixNano()
			case "cancelled":
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				cancel()
			case "parent-expired":
				var cancel context.CancelFunc
				ctx, cancel = context.WithDeadline(ctx, time.Now().Add(-time.Second))
				defer cancel()
			}
			raw, _ := wire.EncodeRoute(f)
			err = r.forward(ctx, &kgo.Record{Topic: f.Topic, Value: b, Headers: []kgo.RecordHeader{{Key: routeHeader, Value: raw}}})
			if name == "cancelled" || name == "parent-expired" {
				if !errors.Is(err, ctx.Err()) {
					t.Fatalf("unfinished message acknowledged: %v", err)
				}
			} else if err != nil {
				t.Fatal(err)
			}
			want := 0
			if name == "live" {
				want = 1
			}
			if calls != want {
				t.Fatalf("callbacks=%d want=%d", calls, want)
			}
			if name == "expired" && r.expired.Load() != 1 {
				t.Fatal("expiry not recorded")
			}
		})
	}
}
