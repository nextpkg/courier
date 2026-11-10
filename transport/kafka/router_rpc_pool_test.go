package kafka

import (
	"context"
	"fmt"
	"io"
	"net"
	"testing"
	"time"

	"github.com/nextpkg/courier"
	"github.com/stretchr/testify/require"
)

func TestRPCPoolsBoundFailedDestinations(t *testing.T) {
	r, _, _ := routerFixture(t)
	peer, _, _ := startTestRPCPeer(t, r)
	for i := 0; i < maxRPCPools+100; i++ {
		_, err := peer.forward(context.Background(), fmt.Sprintf("127.0.0.1:%d", 65536+i), "Forward", nil, nil, 1024)
		require.Error(t, err)
	}
	require.Len(t, peer.pools, maxRPCPools)
	require.Empty(t, peer.conns)
}

func TestRPCPoolsKeepBorrowersAndCloseEvictedConnections(t *testing.T) {
	r, _, _ := routerFixture(t)
	peer, _, _ := startTestRPCPeer(t, r)
	ctx := context.Background()
	pinned := make([]*rpcPool, maxRPCPools)
	for i := range pinned {
		pool, err := peer.acquirePool(ctx, fmt.Sprint(i))
		require.NoError(t, err)
		pinned[i] = pool
	}
	_, err := peer.acquirePool(ctx, "overflow")
	require.ErrorIs(t, err, courier.ErrOverloaded)
	// Existing destinations share the pinned pool even while the table is full.
	same, err := peer.acquirePool(ctx, "0")
	require.NoError(t, err)
	require.Same(t, pinned[0], same)
	peer.releasePool(same)
	local, remote := net.Pipe()
	t.Cleanup(func() { _ = remote.Close() })
	require.True(t, peer.track(local))
	require.True(t, pinned[0].idle.Put(&rpcConn{Conn: local}))
	peer.idleCount.Add(1)
	require.NoError(t, remote.SetReadDeadline(time.Now().Add(time.Second)))
	peer.releasePool(pinned[0])
	replacement, err := peer.acquirePool(ctx, "replacement")
	require.NoError(t, err)
	peer.releasePool(replacement)
	require.NotContains(t, peer.pools, "0")
	require.Len(t, peer.pools, maxRPCPools)
	require.Zero(t, peer.idleCount.Load())
	_, err = remote.Read(make([]byte, 1))
	require.ErrorIs(t, err, io.EOF)
	for _, pool := range pinned[1:] {
		peer.releasePool(pool)
	}
}
