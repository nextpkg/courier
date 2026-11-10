package kafka

import (
	"context"
	"github.com/nextpkg/courier/transport/kafka/internal/wire"
	"testing"
)

// BenchmarkPoolLifecycle measures object acquisition, completion and both owners'
// release, with and without reuse; no broker or socket I/O is involved.
func BenchmarkPoolLifecycle(b *testing.B) {
	for _, size := range []int{0, 1} {
		name := "hot"
		if size == 0 {
			name = "miss"
		}
		b.Run(name, func(b *testing.B) {
			b.Run("publish", func(b *testing.B) {
				pool := newPublishCompletionPool(size)
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					c := pool.acquire()
					c.complete(nil, nil)
					c.release()
				}
			})
			b.Run("forward", func(b *testing.B) {
				pool := newForwardItemPool(size)
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					c := pool.acquire(context.Background(), wire.BatchMessage{})
					c.complete(204)
					c.release()
				}
			})
		})
	}
}
