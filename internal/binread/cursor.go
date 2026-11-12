// Package binread provides checked reads for binary protocol frames.
package binread

import (
	"encoding/binary"

	"github.com/apache/thrift/lib/go/thrift"
)

// Cursor borrows its input. A shape mismatch is sticky and permits a caller to
// fall back to a full decoder; it is not itself a semantic protocol error.
type Cursor struct {
	Data   []byte
	At     int
	Failed bool
}

func (c *Cursor) header(id byte, typ byte, n int) bool {
	if c.Failed {
		return false
	}
	if len(c.Data)-c.At < n+3 || c.Data[c.At] != typ || c.Data[c.At+1] != 0 || c.Data[c.At+2] != id {
		c.Failed = true
		return false
	}
	c.At += 3
	return true
}

// read keeps header and value operations static at each typed call site.
func read[T any](c *Cursor, id byte, header func(*Cursor, byte) bool, value func(*Cursor) T) (v T) {
	if header(c, id) {
		return value(c)
	}
	return
}
func stringHeader(c *Cursor, id byte) bool { return c.header(id, byte(thrift.STRING), 4) }
func int32Header(c *Cursor, id byte) bool  { return c.header(id, byte(thrift.I32), 4) }
func int64Header(c *Cursor, id byte) bool  { return c.header(id, byte(thrift.I64), 8) }
func blobValue(c *Cursor) []byte {
	n := uint64(binary.BigEndian.Uint32(c.Data[c.At:]))
	c.At += 4
	if n > uint64(len(c.Data)-c.At) {
		c.Failed = true
		return nil
	}
	v := c.Data[c.At : c.At+int(n) : c.At+int(n)]
	c.At += int(n)
	return v
}
func int32Value(c *Cursor) int32 {
	v := int32(binary.BigEndian.Uint32(c.Data[c.At:]))
	c.At += 4
	return v
}
func int64Value(c *Cursor) int64 {
	v := int64(binary.BigEndian.Uint64(c.Data[c.At:]))
	c.At += 8
	return v
}
func (c *Cursor) Blob(id byte) []byte { return read(c, id, stringHeader, blobValue) }
func (c *Cursor) Int32(id byte) int32 { return read(c, id, int32Header, int32Value) }
func (c *Cursor) Int64(id byte) int64 { return read(c, id, int64Header, int64Value) }
func (c *Cursor) Done() bool          { return !c.Failed && len(c.Data)-c.At == 1 && c.Data[c.At] == 0 }
