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

// Value applies statically supplied field/value operations without a type switch.
func Value[T any](w *Writer, id int16, value T, field func(*Writer, int16), write func(*Writer, T)) {
	field(w, id)
	write(w, value)
}
func StringField(w *Writer, id int16) { w.Field(id, thrift.STRING) }
func Int32Field(w *Writer, id int16)  { w.Field(id, thrift.I32) }
func Int64Field(w *Writer, id int16)  { w.Field(id, thrift.I64) }

func (w *Writer) StringField(id int16, value string) {
	Value(w, id, value, StringField, (*Writer).String)
}

func (w *Writer) Int32Field(id int16, value int32) {
	Value(w, id, value, Int32Field, (*Writer).Int32)
}

func (w *Writer) Int64Field(id int16, value int64) {
	Value(w, id, value, Int64Field, (*Writer).Int64)
}

func (w *Writer) BinaryField(id int16, value []byte) {
	Value(w, id, value, StringField, (*Writer).Binary)
}
