package testutil

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/nextpkg/courier"
	"github.com/nextpkg/courier/examples/gen/demo"
	"github.com/nextpkg/courier/examples/handlers"
	"github.com/nextpkg/courier/internal/testcheck"
)

func Client(t *testing.T, tr courier.Transport) *courier.Client {
	t.Helper()
	c, err := courier.NewClient(context.Background(), tr, courier.ClientOptions{MaxInFlight: 512, DefaultTimeout: 8 * time.Second})
	require.NoError(t, err)

	t.Cleanup(func() { testcheck.Close(t, c) })
	return c
}

func Server(t *testing.T, tr courier.Transport, h demo.Orders) *courier.Server {
	t.Helper()
	s, err := courier.NewServer(tr, courier.ServerOptions{MaxConcurrent: 128})
	require.NoError(t, err)

	require.NoError(t, demo.RegisterOrders(s, h))
	require.NoError(t, s.Start(context.Background()))
	t.Cleanup(func() { testcheck.Close(t, s) })
	return s
}

type HeldOrders struct {
	*handlers.Orders
	Started chan struct{}
	Release chan struct{}
	once    sync.Once
}

func (h *HeldOrders) Get(ctx context.Context, id string, ms int32) (*demo.Item, error) {
	if id == "first" {
		h.once.Do(func() { close(h.Started) })
		select {
		case <-h.Release:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	return h.Orders.Get(ctx, id, ms)
}
