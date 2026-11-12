package kafka

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"

	"github.com/apache/thrift/lib/go/thrift"

	"github.com/nextpkg/courier/internal/binwrite"
)

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

// writeForwardArgs writes only the argument struct; message framing and I/O
// completion stay with the connection owner.
func writeForwardArgs(ctx context.Context, p *thrift.TBinaryProtocol, route, body []byte) error {
	w := binwrite.Writer{Protocol: p, Context: ctx}
	w.BinaryField(forwardRouteFieldID, route)
	w.BinaryField(forwardDataFieldID, body)
	w.Stop()
	return w.Err
}

func writeForwardResult(ctx context.Context, p *thrift.TBinaryProtocol, batch bool, status int, results []byte) error {
	w := binwrite.Writer{Protocol: p, Context: ctx}
	if batch {
		result := binary.BigEndian.AppendUint32(make([]byte, 0, 4+len(results)), uint32(status))
		result = append(result, results...)
		w.BinaryField(forwardSuccessFieldID, result)
	} else {
		w.Int32Field(forwardSuccessFieldID, int32(status))
	}
	w.Stop()
	return w.Err
}

func readForwardResponse(ctx context.Context, p *thrift.TBinaryProtocol, method string) (*rpcResult, error) {
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
	if id != forwardSuccessFieldID {
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
	return &rpcResult{status: int(status), body: batchResults}, nil
}

// readForwardArgs reads the argument struct only; message dispatch and delivery
// stay with the peer. Repeated fields retain the protocol's last-value behavior.
func readForwardArgs(ctx context.Context, p *thrift.TBinaryProtocol, limit int64) ([]byte, []byte, error) {
	if _, err := p.ReadStructBegin(ctx); err != nil {
		return nil, nil, err
	}
	fields := map[int16][]byte{}
	for {
		_, typ, id, err := p.ReadFieldBegin(ctx)
		if err != nil {
			return nil, nil, err
		}
		if typ == thrift.STOP {
			break
		}
		if typ != thrift.STRING || id < forwardRouteFieldID || id > forwardDataFieldID {
			return nil, nil, fmt.Errorf("invalid forward argument")
		}
		value, err := readFramedBinary(ctx, p, limit)
		if err != nil {
			return nil, nil, err
		}
		fields[id] = value
		if err := p.ReadFieldEnd(ctx); err != nil {
			return nil, nil, err
		}
	}
	if err := p.ReadStructEnd(ctx); err != nil {
		return nil, nil, err
	}
	if err := p.ReadMessageEnd(ctx); err != nil {
		return nil, nil, err
	}
	return fields[forwardRouteFieldID], fields[forwardDataFieldID], nil
}
