package courier

import (
	"context"
	"fmt"
	"sync"

	"github.com/apache/thrift/lib/go/thrift"
)

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
		// Await cancellation cannot poison a completed Future's cached decode.
		if err := decodeResult(context.Background(), method, data, result, c.opts.MaxFrameBytes); err != nil {
			return zero, err
		}
		return unwrap()
	}}, nil
}
func (f *Future[T]) ID() string                     { return f.call.ID() }
func (f *Future[T]) Done() <-chan struct{}          { return f.call.Done() }
func (f *Future[T]) Sent(ctx context.Context) error { return f.call.Sent(ctx) }
func (f *Future[T]) Await(ctx context.Context) (T, error) {
	data, err := f.call.Await(ctx)
	if err != nil {
		var zero T
		return zero, err
	}
	f.once.Do(func() { f.value, f.err = f.decode(data) })
	return f.value, f.err
}
