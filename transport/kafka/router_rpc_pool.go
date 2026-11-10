package kafka

import (
	"context"
	"net"

	"github.com/nextpkg/courier"
	"github.com/nextpkg/courier/internal/reuse"
)

const (
	maxRPCPools           = 512
	maxRPCIdleConnections = 512
)

// A pool remains pinned through slot waiting and connection return. Evicting
// only unpinned pools preserves one concurrency limit per destination.
func (s *rpcPeer) acquirePool(ctx context.Context, host string) (*rpcPool, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	var retired []*rpcConn
	s.mu.Lock()
	defer func() {
		s.mu.Unlock()
		for _, c := range retired {
			s.drop(c.Conn)
		}
	}()
	if s.closed {
		return nil, net.ErrClosed
	}
	if pool := s.pools[host]; pool != nil {
		pool.users++
		return pool, nil
	}
	if len(s.pools) >= maxRPCPools {
		for key, pool := range s.pools {
			if pool.users != 0 {
				continue
			}
			delete(s.pools, key)
			for c := pool.idle.Take(); c != nil; c = pool.idle.Take() {
				s.idleCount.Add(-1)
				retired = append(retired, c)
			}
			break
		}
		if len(s.pools) >= maxRPCPools {
			return nil, courier.ErrOverloaded
		}
	}
	pool := &rpcPool{idle: make(reuse.Cache[*rpcConn], s.r.t.opts.Workers), slots: make(chan struct{}, s.r.t.opts.Workers), users: 1}
	s.pools[host] = pool
	return pool, nil
}

func (s *rpcPeer) releasePool(pool *rpcPool) {
	s.mu.Lock()
	pool.users--
	s.mu.Unlock()
}
