package examples_test

import (
	"context"
	"github.com/nextpkg/courier"
	"github.com/nextpkg/courier/examples/contract"
	"github.com/nextpkg/courier/transport/memory"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func TestMemoryContract(t *testing.T) {
	b, err := memory.New(memory.Options{QueueSize: 1024, PublishWorkers: 8, HandlerWorkers: 16})
	require.NoError(t, err)

	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if err := b.Close(ctx); err != nil {
			t.Error(err)
		}
		s := b.Stats()
		if s.Services != 0 || s.Inboxes != 0 {
			t.Errorf("leaked resources: %+v", s)
		}
	})
	contract.Run(t, func(t *testing.T) courier.Transport { return b })
}
