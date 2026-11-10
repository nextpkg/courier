package kafka

// Thrift Binary over framed TCP, preserving the
// existing envelope, delivery statuses and retry/commit path.
import (
	"context"
	"fmt"
	"github.com/apache/thrift/lib/go/thrift"
	"github.com/nextpkg/courier/internal/reuse"
	"github.com/nextpkg/courier/transport/kafka/internal/wire"
	"net"
	"net/http"
	"sync"
	"sync/atomic"
	"time"
)

// Argument IDs are defined by the Forward/ForwardBatch methods. Thrift uses
// field zero for a successful method result; these are not protocol type tags.
const (
	forwardRouteFieldID   int16 = 1
	forwardDataFieldID    int16 = 2
	forwardSuccessFieldID int16 = 0
)

type rpcConn struct {
	net.Conn
	p *thrift.TBinaryProtocol
}

// Each borrowed connection carries one in-flight call; this pool does not
// multiplex replies. slots bounds occupancy and idle keeps reusable connections.
type rpcPool struct {
	idle  reuse.Cache[*rpcConn]
	slots chan struct{}
	users int // Borrowers and slot waiters; guarded by rpcPeer.mu.
}
type rpcPeer struct {
	r         *replyRouter
	ln        net.Listener
	mu        sync.Mutex
	closed    bool
	conns     map[net.Conn]bool
	pools     map[string]*rpcPool
	idleCount atomic.Int64
	wg        sync.WaitGroup
}

func newRPCPeer(r *replyRouter, ln net.Listener) *rpcPeer {
	return &rpcPeer{r: r, ln: ln, conns: map[net.Conn]bool{}, pools: map[string]*rpcPool{}}
}
func (s *rpcPeer) wrap(c net.Conn) *rpcConn {
	cfg := &thrift.TConfiguration{MaxFrameSize: int32(s.r.batchLimit() + 4096), MaxMessageSize: int32(s.r.batchLimit() + 4096)}
	t := thrift.NewTFramedTransportConf(thrift.NewTSocketFromConnConf(c, cfg), cfg)
	return &rpcConn{c, thrift.NewTBinaryProtocolConf(t, cfg)}
}
func (s *rpcPeer) track(c net.Conn) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		_ = c.Close()
		return false
	}
	s.conns[c] = true
	return true
}
func (s *rpcPeer) drop(c net.Conn) { _ = c.Close(); s.mu.Lock(); delete(s.conns, c); s.mu.Unlock() }
func (s *rpcPeer) close() {
	s.mu.Lock()
	s.closed = true
	_ = s.ln.Close()
	for c := range s.conns {
		_ = c.Close()
	}
	s.mu.Unlock()
}
func (s *rpcPeer) serve(ln net.Listener) error {
	defer s.wg.Wait()
	for {
		c, e := ln.Accept()
		if e != nil {
			return nil
		}
		if !s.track(c) {
			return nil
		}
		s.wg.Add(1)
		go func() { defer s.wg.Done(); defer s.drop(c); s.handle(s.wrap(c)) }()
	}
}
func (s *rpcPeer) handle(c *rpcConn) {
	ctx := context.Background()
	for {
		if err := c.SetDeadline(time.Now().Add(30 * time.Second)); err != nil {
			return
		}
		name, kind, seq, e := c.p.ReadMessageBegin(ctx)
		if e != nil {
			return
		}
		if (name != "Forward" && name != "ForwardBatch") || kind != thrift.CALL {
			return
		}
		hdr, data, err := readForwardArgs(ctx, c.p, s.r.batchLimit()+4096)
		if err != nil {
			return
		}
		status := http.StatusBadRequest
		var batchResults []byte
		if name == "ForwardBatch" {
			status, batchResults = s.r.deliverBinaryBatch(data)
		} else if len(hdr) > wire.MaxRouteBytes || len(data) > s.r.t.opts.MaxFrameBytes {
			status = 413
		} else {
			f, err := s.r.decodeRoute(hdr)
			if err == nil {
				select {
				case s.r.inbound <- struct{}{}:
					status = s.r.deliver(f, data)
					<-s.r.inbound
				default:
					status = 429
				}
			}
		}
		if c.p.WriteMessageBegin(ctx, name, thrift.REPLY, seq) != nil {
			return
		}
		if writeForwardResult(ctx, c.p, name == "ForwardBatch", status, batchResults) != nil {
			return
		}
		if c.p.WriteMessageEnd(ctx) != nil || c.p.Flush(ctx) != nil {
			return
		}
	}
}

type rpcResult struct {
	status int
	body   []byte
}

// forward borrows body until this synchronous call returns; it never retains it.
func (s *rpcPeer) forward(ctx context.Context, host, method string, route, body []byte, maxBody int64) (*rpcResult, error) {
	if int64(len(body)) > maxBody {
		return nil, fmt.Errorf("frame too large")
	}
	pool, err := s.acquirePool(ctx, host)
	if err != nil {
		return nil, err
	}
	defer s.releasePool(pool)
	select {
	case pool.slots <- struct{}{}:
		defer func() { <-pool.slots }()
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	c := pool.idle.Take()
	if c != nil {
		s.idleCount.Add(-1)
	} else {
		raw, err := (&net.Dialer{Timeout: s.r.t.opts.OperationTimeout}).DialContext(ctx, "tcp", host)
		if err != nil {
			return nil, err
		}
		if !s.track(raw) {
			return nil, net.ErrClosed
		}
		c = s.wrap(raw)
	}
	ok := false
	defer func() {
		if ok && s.idleCount.Add(1) <= maxRPCIdleConnections {
			if !pool.idle.Put(c) {
				s.idleCount.Add(-1)
				s.drop(c.Conn)
			}
		} else {
			if ok {
				s.idleCount.Add(-1)
			}
			s.drop(c.Conn)
		}
	}()
	deadline := time.Now().Add(s.r.t.opts.OperationTimeout)
	if d, yes := ctx.Deadline(); yes && d.Before(deadline) {
		deadline = d
	}
	if err := c.SetDeadline(deadline); err != nil {
		return nil, err
	}
	// A cancellation closes this connection; never return it to the pool until
	// the cancellation callback has either stopped or finished.
	stopped := make(chan struct{})
	stop := context.AfterFunc(ctx, func() { _ = c.Close(); close(stopped) })
	defer func() {
		if !stop() {
			ok = false
			<-stopped
		}
	}()
	p := c.p
	if err = p.WriteMessageBegin(ctx, method, thrift.CALL, 1); err != nil {
		return nil, err
	}
	if err = writeForwardArgs(ctx, p, route, body); err != nil {
		return nil, err
	}
	if err = p.WriteMessageEnd(ctx); err != nil {
		return nil, err
	}
	if err = p.Flush(ctx); err != nil {
		return nil, err
	}
	result, err := readForwardResponse(ctx, p, method)
	if err != nil {
		return nil, err
	}
	ok = ctx.Err() == nil
	return result, nil
}
