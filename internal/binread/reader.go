package binread

import (
	"context"
	"github.com/apache/thrift/lib/go/thrift"
)

// Reader preserves the first value read error. Field routing and validation stay
// with the decoder so reordered and unknown fields retain their protocol rules.
type Reader struct {
	Protocol *thrift.TBinaryProtocol
	Context  context.Context
	Err      error
}

func (r *Reader) String() (v string) {
	if r.Err == nil {
		v, r.Err = r.Protocol.ReadString(r.Context)
	}
	return
}
func (r *Reader) Int32() (v int32) {
	if r.Err == nil {
		v, r.Err = r.Protocol.ReadI32(r.Context)
	}
	return
}
func (r *Reader) Int64() (v int64) {
	if r.Err == nil {
		v, r.Err = r.Protocol.ReadI64(r.Context)
	}
	return
}
func (r *Reader) Binary() (v []byte) {
	if r.Err == nil {
		v, r.Err = r.Protocol.ReadBinary(r.Context)
	}
	return
}
