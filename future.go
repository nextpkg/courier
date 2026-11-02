package courier

import (
	"context"
	"fmt"
	"sync"

	"github.com/apache/thrift/lib/go/thrift"
)

// Future holds a typed result decoded once from a completed call.
// Concurrent waiters share the decoded value and must coordinate mutations.
type Future[T any] struct {
	call   *Call
	once   sync.Once
	decode func([]byte) (T, error)
	value  T
	err    error
}

// Invoke is used by generated asynchronous methods. Result and unwrap belong
// exclusively to this Future; callers must not mutate them.
func Invoke[T any](ctx context.Context, c *Client, service, method string, args, result thrift.TStruct, unwrap func() (T, error)) (*Future[T], error) {
	if c == nil || nilStruct(result) || unwrap == nil {
		return nil, fmt.Errorf("courier: invalid invocation")
	}
	x, err := c.Submit(ctx, service, method, args)
	if err != nil {
		return nil, err
	}
	return &Future[T]{call: x, decode: func(data []byte) (T, error) {
		var zero T
		// Decode independently of the waiter context so cancellation cannot cache
		// a decode failure for subsequent waiters.
		if err := decodeResult(context.Background(), method, data, result, c.opts.MaxFrameBytes); err != nil {
			return zero, err
		}
		return unwrap()
	}}, nil
}
func (f *Future[T]) ID() string            { return f.call.ID() }
func (f *Future[T]) Done() <-chan struct{} { return f.call.Done() }

// Sent waits for the transport publication result, not business completion.
// Canceling ctx ends only this wait.
func (f *Future[T]) Sent(ctx context.Context) error { return f.call.Sent(ctx) }

// Await returns the decoded result or call error; repeated waits reuse the result.
// Canceling ctx ends only this wait and does not cancel the submitted call.
func (f *Future[T]) Await(ctx context.Context) (T, error) {
	data, err := f.call.Await(ctx)
	if err != nil {
		var zero T
		return zero, err
	}
	f.once.Do(func() { f.value, f.err = f.decode(data) })
	return f.value, f.err
}
