package kafka

// Thrift Binary over framed TCP, preserving the
// existing envelope, delivery statuses and retry/commit path.
import (
	"context"
	"encoding/binary"
	"fmt"
	"github.com/apache/thrift/lib/go/thrift"
	"github.com/nextpkg/courier/internal/binwrite"
	"github.com/nextpkg/courier/internal/reuse"
	"github.com/nextpkg/courier/transport/kafka/internal/wire"
	"io"
	"net"
	"net/http"
	"sync"
	"sync/atomic"
	"time"
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
		c.Close()
		return false
	}
	s.conns[c] = true
	return true
}
func (s *rpcPeer) drop(c net.Conn) { c.Close(); s.mu.Lock(); delete(s.conns, c); s.mu.Unlock() }
func (s *rpcPeer) close() {
	s.mu.Lock()
	s.closed = true
	s.ln.Close()
	for c := range s.conns {
		c.Close()
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
		c.SetDeadline(time.Now().Add(30 * time.Second))
		name, kind, seq, e := c.p.ReadMessageBegin(ctx)
		if e != nil {
			return
		}
		if (name != "Forward" && name != "ForwardBatch") || kind != thrift.CALL {
			return
		}
		if _, e = c.p.ReadStructBegin(ctx); e != nil {
			return
		}
		fields := map[int16][]byte{}
		for {
			_, typ, id, err := c.p.ReadFieldBegin(ctx)
			if err != nil {
				return
			}
			if typ == thrift.STOP {
				break
			}
			if typ != thrift.STRING || id < 1 || id > 2 {
				return
			}
			value, err := readFramedBinary(ctx, c.p, s.r.batchLimit()+4096)
			if err != nil {
				return
			}
			fields[id] = value
			if c.p.ReadFieldEnd(ctx) != nil {
				return
			}
		}
		if c.p.ReadStructEnd(ctx) != nil || c.p.ReadMessageEnd(ctx) != nil {
			return
		}
		status := http.StatusBadRequest
		hdr, data := fields[1], fields[2]
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

// readFramedBinary allocates once after checking the declared field length
// against both the message limit and the bytes already buffered in this frame.
// The returned bytes remain independently owned after the next frame is read.
func readFramedBinary(ctx context.Context, p *thrift.TBinaryProtocol, limit int64) ([]byte, error) {
	size, err := p.ReadI32(ctx)
	if err != nil {
		return nil, err
	}
	if size < 0 {
		return nil, thrift.NewTProtocolExceptionWithType(thrift.NEGATIVE_SIZE, fmt.Errorf("negative binary field size"))
	}
	if int64(size) > limit {
		return nil, thrift.NewTProtocolExceptionWithType(thrift.SIZE_LIMIT, fmt.Errorf("binary field exceeds message limit"))
	}
	frame, ok := p.Transport().(*thrift.TFramedTransport)
	if !ok {
		return nil, fmt.Errorf("binary field requires framed transport")
	}
	if uint64(size) > frame.RemainingBytes() {
		return nil, thrift.NewTProtocolException(io.ErrUnexpectedEOF)
	}
	value := make([]byte, int(size))
	_, err = io.ReadFull(frame, value)
	return value, thrift.NewTProtocolException(err)
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
	var err error
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil, net.ErrClosed
	}
	pool := s.pools[host]
	if pool == nil {
		pool = &rpcPool{make(reuse.Cache[*rpcConn], s.r.t.opts.Workers), make(chan struct{}, s.r.t.opts.Workers)}
		s.pools[host] = pool
	}
	s.mu.Unlock()
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
		if ok && s.idleCount.Add(1) <= 512 {
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
	c.SetDeadline(deadline)
	// A cancellation closes this connection; never return it to the pool until
	// the cancellation callback has either stopped or finished.
	stopped := make(chan struct{})
	stop := context.AfterFunc(ctx, func() { c.Close(); close(stopped) })
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
	name, kind, seq, err := p.ReadMessageBegin(ctx)
	if err != nil {
		return nil, err
	}
	if name != method || kind != thrift.REPLY || seq != 1 {
		return nil, fmt.Errorf("invalid RPC response")
	}
	if _, err = p.ReadStructBegin(ctx); err != nil {
		return nil, err
	}
	_, typ, id, err := p.ReadFieldBegin(ctx)
	if err != nil {
		return nil, err
	}
	if id != 0 {
		return nil, fmt.Errorf("invalid result field")
	}
	var status int32
	var batchResults []byte
	if method == "ForwardBatch" {
		if typ != thrift.STRING {
			return nil, fmt.Errorf("invalid batch result")
		}
		result, e := p.ReadBinary(ctx)
		if e != nil {
			return nil, e
		}
		if len(result) < 4 {
			return nil, fmt.Errorf("missing batch status")
		}
		status = int32(binary.BigEndian.Uint32(result))
		batchResults = result[4:]
	} else {
		if typ != thrift.I32 {
			return nil, fmt.Errorf("invalid result")
		}
		status, err = p.ReadI32(ctx)
		if err != nil {
			return nil, err
		}
	}
	if err = p.ReadFieldEnd(ctx); err != nil {
		return nil, err
	}
	_, typ, _, err = p.ReadFieldBegin(ctx)
	if err != nil || typ != thrift.STOP {
		return nil, fmt.Errorf("invalid result end")
	}
	if err = p.ReadStructEnd(ctx); err != nil {
		return nil, err
	}
	if err = p.ReadMessageEnd(ctx); err != nil {
		return nil, err
	}
	ok = ctx.Err() == nil
	return &rpcResult{status: int(status), body: batchResults}, nil
}

// writeForwardArgs writes only the argument struct; message framing and I/O
// completion stay with the connection owner.
func writeForwardArgs(ctx context.Context, p *thrift.TBinaryProtocol, route, body []byte) error {
	w := binwrite.Writer{Protocol: p, Context: ctx}
	w.Field(1, thrift.STRING)
	w.Binary(route)
	w.Field(2, thrift.STRING)
	w.Binary(body)
	w.Stop()
	return w.Err
}
func writeForwardResult(ctx context.Context, p *thrift.TBinaryProtocol, batch bool, status int, results []byte) error {
	w := binwrite.Writer{Protocol: p, Context: ctx}
	if batch {
		result := binary.BigEndian.AppendUint32(make([]byte, 0, 4+len(results)), uint32(status))
		result = append(result, results...)
		w.Field(0, thrift.STRING)
		w.Binary(result)
	} else {
		w.Field(0, thrift.I32)
		w.Int32(int32(status))
	}
	w.Stop()
	return w.Err
}
