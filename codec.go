package courier

import (
	"context"
	"fmt"
	"reflect"
	"sync"

	"github.com/apache/thrift/lib/go/thrift"
)

// Override every write fast-path so serialization itself respects the limit.
type limitedBuffer struct {
	*thrift.TMemoryBuffer
	limit    int
	protocol *thrift.TBinaryProtocol
	config   thrift.TConfiguration
}

func (b *limitedBuffer) Write(p []byte) (int, error) {
	if len(p) > b.limit-b.Len() {
		return 0, ErrTooLarge
	}
	return b.TMemoryBuffer.Write(p)
}
func (b *limitedBuffer) WriteString(s string) (int, error) {
	if len(s) > b.limit-b.Len() {
		return 0, ErrTooLarge
	}
	return b.TMemoryBuffer.WriteString(s)
}
func (b *limitedBuffer) WriteByte(v byte) error {
	if b.Len() >= b.limit {
		return ErrTooLarge
	}
	return b.TMemoryBuffer.WriteByte(v)
}

// Each operation exclusively leases a codec and its scratch buffer. Encoded
// results must own their bytes before release; pool reuse is not an async lease.
var codecBuffers sync.Pool

const maxPooledCodecCapacity = 64 << 10

// Discard oversized scratch storage rather than retaining a rare large frame.
// The capacity bound applies per entry; sync.Pool is not a global memory limit.
func releaseBinaryProtocol(b *limitedBuffer) {
	if b.Cap() > maxPooledCodecCapacity {
		return
	}
	b.Reset()
	codecBuffers.Put(b)
}

func binaryProtocol(data []byte, max int) (*limitedBuffer, *thrift.TBinaryProtocol) {
	b, _ := codecBuffers.Get().(*limitedBuffer)
	if b == nil {
		b = &limitedBuffer{TMemoryBuffer: thrift.NewTMemoryBufferLen(len(data))}
	}
	b.Reset()
	b.limit = max
	b.config = thrift.TConfiguration{MaxMessageSize: int32(max), MaxFrameSize: int32(max)}
	if b.protocol == nil {
		b.protocol = thrift.NewTBinaryProtocolConf(b, &b.config)
	}
	_, _ = b.Write(data)
	return b, b.protocol
}

func encodeCall(ctx context.Context, method string, args thrift.TStruct, max int) ([]byte, error) {
	if nilStruct(args) {
		return nil, fmt.Errorf("courier: nil request")
	}
	b, p := binaryProtocol(nil, max)
	defer releaseBinaryProtocol(b)
	if err := p.WriteMessageBegin(ctx, method, thrift.CALL, 0); err != nil {
		return nil, err
	}
	if err := args.Write(ctx, p); err != nil {
		return nil, err
	}
	if err := p.WriteMessageEnd(ctx); err != nil {
		return nil, err
	}
	if b.Len() > max {
		return nil, ErrTooLarge
	}
	return append([]byte(nil), b.Bytes()...), nil
}

func nilStruct(v thrift.TStruct) bool {
	if v == nil {
		return true
	}
	r := reflect.ValueOf(v)
	return r.Kind() == reflect.Pointer && r.IsNil()
}

func decodeResult(ctx context.Context, method string, payload []byte, result thrift.TStruct, max int) error {
	b, p := binaryProtocol(payload, max)
	defer releaseBinaryProtocol(b)
	// Official client validates the method, sequence and application exception.
	c := thrift.NewTStandardClient(p, p)
	if err := c.Recv(ctx, p, 0, method, result); err != nil {
		return err
	}
	if b.Len() != 0 {
		return &RPCError{Code: "protocol", Message: "trailing response bytes"}
	}
	return nil
}
