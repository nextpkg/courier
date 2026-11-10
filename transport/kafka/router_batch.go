package kafka

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/url"
	"sync/atomic"
)

type batchMessage struct {
	Instance string
	Expires  int64
	Data     []byte
}

// The drain may complete after the waiter cancels. Each item owns its buffered
// completion channel; reuse requires both sides to have released the item.
type forwardItem struct {
	message batchMessage
	ctx     context.Context
	done    chan int
	refs    atomic.Int32
	pool    *forwardItemPool
}

// One coordinator owns each endpoint queue. Bounded consumer workers enqueue
// while sends are in flight; a lone reply starts immediately when no send is active.
func (r *replyRouter) enqueueForward(ctx context.Context, endpoint string, instance string, expires int64, data []byte) int {
	item := r.forwardItems.Load().acquire(ctx, batchMessage{instance, expires, data})
	defer item.release()
	r.batchMu.Lock()
	if r.batches == nil {
		r.batches = make(map[string][]*forwardItem)
	}
	_, running := r.batches[endpoint]
	r.batches[endpoint] = append(r.batches[endpoint], item)
	if r.opts.BatchConcurrency > 1 {
		if r.batchSignals == nil {
			r.batchSignals = make(map[string]chan struct{})
		}
		if !running {
			r.batchSignals[endpoint] = make(chan struct{}, 1)
		}
		if len(r.batches[endpoint]) == r.opts.BatchSize {
			select {
			case r.batchSignals[endpoint] <- struct{}{}:
			default:
			}
		}
	}
	if !running {
		r.batchWG.Add(1)
		go func() { defer r.batchWG.Done(); r.drainForward(endpoint) }()
	}
	r.batchMu.Unlock()
	select {
	case status := <-item.done:
		return status
	case <-ctx.Done():
		return 0
	}
}

func (r *replyRouter) drainForward(endpoint string) {
	if r.opts.BatchConcurrency > 1 {
		r.drainConcurrentForward(endpoint)
		return
	}
	for {
		r.batchMu.Lock()
		queue := r.batches[endpoint]
		if len(queue) == 0 {
			delete(r.batches, endpoint)
			r.batchMu.Unlock()
			return
		}
		n, size := 0, 0
		for n < len(queue) && n < r.opts.BatchSize {
			next := len(queue[n].message.Instance) + 8 + len(queue[n].message.Data)
			if n > 0 && size+next > r.t.opts.MaxFrameBytes+maxRouteBytes {
				break
			}
			size += next
			n++
		}
		// Snapshot pointers before unlocking: new enqueues can change the queue.
		items := append([]*forwardItem(nil), queue[:n]...)
		clear(queue[:n]) // Release consumed references while later batches remain queued.
		r.batches[endpoint] = queue[n:]
		r.batchMu.Unlock()
		r.completeForwardBatch(endpoint, items)
	}
}

func (r *replyRouter) batchLimit() int64 { return 2*int64(r.t.opts.MaxFrameBytes+maxRouteBytes) + 4096 }

func (r *replyRouter) sendBatch(endpoint string, messages []batchMessage) []int {
	statuses := make([]int, len(messages))
	var body []byte
	if r.rpc != nil {
		buffer := borrowBinaryBatch(messages)
		defer releaseBinaryBatch(buffer)
		body = buffer.data
	} else {
		// net/http can close a request body asynchronously after Do returns.
		// Keep this body independently owned instead of returning it to the pool.
		body = encodeBinaryBatch(messages)
	}
	if int64(len(body)) > r.batchLimit() {
		return statuses
	}
	ctx, cancel := context.WithTimeout(r.t.ctx, r.t.opts.OperationTimeout)
	defer cancel()
	if r.rpc != nil {
		u, err := url.Parse(endpoint)
		if err != nil {
			return statuses
		}
		r.httpRequests.Add(1)
		r.httpMessages.Add(uint64(len(messages)))
		result, err := r.rpc.forward(ctx, u.Host, "ForwardBatch", nil, body, r.batchLimit())
		if err != nil || result.status != http.StatusOK {
			return statuses
		}
		decoded, err := decodeBinaryStatuses(result.body, len(messages))
		if err != nil {
			return statuses
		}
		return decoded
	}
	req, err := http.NewRequestWithContext(ctx, "POST", endpoint+"/courier/replies", bytes.NewReader(body))
	if err != nil {
		return statuses
	}
	r.httpRequests.Add(1)
	r.httpMessages.Add(uint64(len(messages)))
	resp, err := r.client.Do(req)
	if err != nil {
		return statuses
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(io.LimitReader(resp.Body, 4096))
	if err != nil || resp.StatusCode != http.StatusOK {
		return statuses
	}
	result, err := decodeBinaryStatuses(data, len(messages))
	if err != nil {
		return statuses
	}
	return result
}

func (r *replyRouter) serveBatch(w http.ResponseWriter, req *http.Request) {
	select {
	case r.inbound <- struct{}{}:
		defer func() { <-r.inbound }()
	default:
		w.WriteHeader(429)
		return
	}
	body, err := io.ReadAll(http.MaxBytesReader(w, req.Body, r.batchLimit()))
	if err != nil {
		w.WriteHeader(413)
		return
	}
	messages, err := decodeBinaryBatch(body, r.t.opts.MaxFrameBytes)
	if err != nil {
		w.WriteHeader(400)
		return
	}
	// Validate the entire shape before delivering any item.
	for _, m := range messages {
		if len(m.Instance) > maxRouteBytes || len(m.Data) > r.t.opts.MaxFrameBytes {
			w.WriteHeader(413)
			return
		}
	}
	statuses := make([]int, len(messages))
	for i, m := range messages {
		statuses[i] = r.deliverCompact(m)
	}
	w.Header().Set("Content-Type", "application/octet-stream")
	_, _ = w.Write(encodeBinaryStatuses(statuses))
}

func (r *replyRouter) completeForwardBatch(endpoint string, items []*forwardItem) {
	active := items[:0]
	messages := make([]batchMessage, 0, len(items))
	for _, item := range items {
		if item.ctx.Err() != nil {
			item.complete(0)
			continue
		}
		active = append(active, item)
		messages = append(messages, item.message)
	}
	if len(active) == 0 {
		return
	}
	statuses := r.sendBatch(endpoint, messages)
	for i, item := range active {
		item.complete(statuses[i])
	}
}

// One coordinator owns queue removal and the endpoint's in-flight count. Only
// a queue with BatchSize items overlaps an existing send; byte limits and
// cancellation can still reduce the encoded batch. Completion goes to each
// item's own channel. Partial queues wait for the active send to finish.
func (r *replyRouter) drainConcurrentForward(endpoint string) {
	r.batchMu.Lock()
	wake := r.batchSignals[endpoint]
	r.batchMu.Unlock()
	finished := make(chan struct{}, 2)
	inFlight := 0
	for {
		r.batchMu.Lock()
		queue := r.batches[endpoint]
		if len(queue) == 0 && inFlight == 0 {
			delete(r.batches, endpoint)
			delete(r.batchSignals, endpoint)
			r.batchMu.Unlock()
			return
		}
		if inFlight == 2 {
			r.batchMu.Unlock()
			<-finished
			inFlight--
			continue
		}
		if len(queue) == 0 || (inFlight > 0 && len(queue) < r.opts.BatchSize) {
			r.batchMu.Unlock()
			select {
			case <-finished:
				inFlight--
			case <-wake:
			}
			continue
		}
		n, size := 0, 0
		for n < len(queue) && n < r.opts.BatchSize {
			next := len(queue[n].message.Instance) + 8 + len(queue[n].message.Data)
			if n > 0 && size+next > r.t.opts.MaxFrameBytes+maxRouteBytes {
				break
			}
			size += next
			n++
		}
		items := append([]*forwardItem(nil), queue[:n]...)
		clear(queue[:n])
		r.batches[endpoint] = queue[n:]
		r.batchMu.Unlock()
		inFlight++
		go func(items []*forwardItem) {
			r.completeForwardBatch(endpoint, items)
			finished <- struct{}{}
		}(items)
	}
}
