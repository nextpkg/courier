package kafka

import (
	"context"

	"github.com/nextpkg/courier/internal/reuse"
	"github.com/nextpkg/courier/transport/kafka/internal/wire"
)

const maxCachedForwardItems = 1024

type forwardItemPool struct{ idle reuse.Cache[*forwardItem] }

func newForwardItemPool(window int) *forwardItemPool {
	return &forwardItemPool{idle: make(reuse.Cache[*forwardItem], min(window, maxCachedForwardItems))}
}
func (p *forwardItemPool) acquire(ctx context.Context, message wire.BatchMessage) *forwardItem {
	var item *forwardItem
	if p != nil {
		item = p.idle.Take()
	}
	if item == nil {
		item = &forwardItem{done: make(chan int, 1), pool: p}
	}
	item.ctx = ctx
	item.message = message
	item.refs.Store(2)
	return item
}
func (item *forwardItem) complete(status int) {
	item.done <- status
	item.release()
}
func (item *forwardItem) release() {
	if item.refs.Add(-1) != 0 {
		return
	}
	// Both the waiter and batch sender have finished. Remove stale completion and
	// input/context references before reuse; cancelled waiters can leave a result.
	select {
	case <-item.done:
	default:
	}
	item.message = wire.BatchMessage{}
	item.ctx = nil
	if item.pool != nil {
		item.pool.idle.Put(item)
	}
}
