package courier

import (
	"context"
	"fmt"
	"reflect"

	"github.com/apache/thrift/lib/go/thrift"
)

// Override every write fast-path so serialization itself respects the limit.
type limitedBuffer struct {
	*thrift.TMemoryBuffer
	limit int
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
func binaryProtocol(data []byte, max int) (*limitedBuffer, *thrift.TBinaryProtocol) {
	b := &limitedBuffer{thrift.NewTMemoryBufferLen(len(data)), max}
	_, _ = b.Write(data)
	p := thrift.NewTBinaryProtocolConf(b, &thrift.TConfiguration{MaxMessageSize: int32(max), MaxFrameSize: int32(max)})
	return b, p
}

func encodeCall(ctx context.Context, method string, args thrift.TStruct, max int) ([]byte, error) {
	if nilStruct(args) {
		return nil, fmt.Errorf("courier: nil request")
	}
	b, p := binaryProtocol(nil, max)
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
