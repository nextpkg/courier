// Package binwrite provides small typed operations for Thrift binary writers.
package binwrite

import (
	"context"
	"github.com/apache/thrift/lib/go/thrift"
)

// Writer targets TBinaryProtocol only: struct/field names and end markers
// emit no bytes. Field headers, values and the final STOP preserve the IDL order.
// The first write error is retained and prevents all subsequent writes.
type Writer struct {
	Protocol *thrift.TBinaryProtocol
	Context  context.Context
	Err      error
}

func (w *Writer) Field(id int16, kind thrift.TType) {
	if w.Err == nil {
		w.Err = w.Protocol.WriteFieldBegin(w.Context, "", kind, id)
	}
}
func (w *Writer) String(value string) {
	if w.Err == nil {
		w.Err = w.Protocol.WriteString(w.Context, value)
	}
}
func (w *Writer) Int32(value int32) {
	if w.Err == nil {
		w.Err = w.Protocol.WriteI32(w.Context, value)
	}
}
func (w *Writer) Int64(value int64) {
	if w.Err == nil {
		w.Err = w.Protocol.WriteI64(w.Context, value)
	}
}
func (w *Writer) Stop() {
	if w.Err == nil {
		w.Err = w.Protocol.WriteFieldStop(w.Context)
	}
}

func (w *Writer) Binary(value []byte) {
	if w.Err == nil {
		w.Err = w.Protocol.WriteBinary(w.Context, value)
	}
}
