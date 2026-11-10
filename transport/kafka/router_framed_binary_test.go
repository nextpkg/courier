package kafka

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"github.com/apache/thrift/lib/go/thrift"
	"github.com/stretchr/testify/require"
	"testing"
)

func framedBinaryProtocol(fields ...[]byte) *thrift.TBinaryProtocol {
	m := thrift.NewTMemoryBuffer()
	for _, field := range fields {
		frame := binary.BigEndian.AppendUint32(nil, uint32(len(field)))
		frame = append(frame, field...)
		m.Write(frame)
	}
	cfg := &thrift.TConfiguration{MaxFrameSize: 1 << 20, MaxMessageSize: 1 << 20}
	return thrift.NewTBinaryProtocolConf(thrift.NewTFramedTransportConf(m, cfg), cfg)
}
func binaryField(b []byte) []byte {
	return append(binary.BigEndian.AppendUint32(nil, uint32(len(b))), b...)
}
func TestReadFramedBinaryOwnershipAndBounds(t *testing.T) {
	ctx := context.Background()
	for _, size := range []int{0, 1, 512, 513, 32768} {
		t.Run(fmt.Sprint(size), func(t *testing.T) {
			want := bytes.Repeat([]byte{0x42}, size)
			p := framedBinaryProtocol(binaryField(want), binaryField(bytes.Repeat([]byte{0x77}, size)))
			got, err := readFramedBinary(ctx, p, int64(size))
			if err != nil || !bytes.Equal(got, want) {
				t.Fatalf("first read: %v", err)
			}
			next, err := readFramedBinary(ctx, p, int64(size))
			require.NoError(t, err)
			clear(next)
			if !bytes.Equal(got, want) {
				t.Fatal("next frame overwrote retained bytes")
			}
		})
	}
	for _, tt := range []struct {
		name  string
		field []byte
		limit int64
	}{
		{"negative", binary.BigEndian.AppendUint32(nil, 0xffffffff), 100},
		{"limit", binaryField([]byte("abc")), 2},
		{"truncated_length", []byte{0, 0}, 100},
		{"truncated_payload", append(binary.BigEndian.AppendUint32(nil, 10), 'x'), 100},
		{"huge_declaration", binary.BigEndian.AppendUint32(nil, 0x7fffffff), 1 << 20},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := readFramedBinary(ctx, framedBinaryProtocol(tt.field), tt.limit); err == nil {
				t.Fatal("accepted invalid field")
			}
		})
	}
	// A malformed field must not consume bytes belonging to the next message.
	p := framedBinaryProtocol(append(binary.BigEndian.AppendUint32(nil, 5), 'a'), binaryField([]byte("next")))
	if _, err := readFramedBinary(ctx, p, 100); err == nil {
		t.Fatal("accepted cross-frame field")
	}
}

func BenchmarkFramedBinaryRead(b *testing.B) {
	for _, size := range []int{1024, 16384, 65536} {
		payload := binaryField(bytes.Repeat([]byte("x"), size))
		for _, exact := range []bool{false, true} {
			b.Run(fmt.Sprintf("bytes%d/exact%t", size, exact), func(b *testing.B) {
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					p := framedBinaryProtocol(payload)
					var err error
					if exact {
						_, err = readFramedBinary(context.Background(), p, 1<<20)
					} else {
						_, err = p.ReadBinary(context.Background())
					}
					if err != nil {
						b.Fatal(err)
					}
				}
			})
		}
	}
}
