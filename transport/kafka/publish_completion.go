package kafka

import (
	"sync/atomic"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/nextpkg/courier/internal/reuse"
)

const maxCachedPublishCompletions = 1024

type publishCompletionPool struct {
	idle reuse.Cache[*publishCompletion]
}
type publishCompletion struct {
	ready chan error
	refs  atomic.Int32
	pool  *publishCompletionPool
}

// The idle cache is bounded by the producer buffer setting and a small fixed
// ceiling. Entries are allocated on demand; this is not unused heap ballast.
func newPublishCompletionPool(buffers int) *publishCompletionPool {
	return &publishCompletionPool{idle: make(reuse.Cache[*publishCompletion], min(buffers, maxCachedPublishCompletions))}
}
func (p *publishCompletionPool) acquire() *publishCompletion {
	var c *publishCompletion
	if p != nil {
		c = p.idle.Take()
	}
	if c == nil {
		c = &publishCompletion{ready: make(chan error, 1), pool: p}
	}
	// The waiter and SDK callback each own one reference. Cancellation only
	// releases the waiter; a late callback must finish before this slot is reused.
	c.refs.Store(2)
	return c
}
func (c *publishCompletion) complete(_ *kgo.Record, err error) {
	c.ready <- err
	c.release()
}
func (c *publishCompletion) release() {
	if c.refs.Add(-1) != 0 {
		return
	}
	// A cancelled waiter may leave a buffered result. Drain it before publication
	// to another caller, including the references held by an error value.
	select {
	case err := <-c.ready:
		_ = err // Drain a cancelled waiter's result before reuse.
	default:
	}
	if c.pool != nil {
		c.pool.idle.Put(c)
	}
}
