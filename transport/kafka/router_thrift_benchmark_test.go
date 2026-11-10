package kafka

import (
	"context"
	"github.com/apache/thrift/lib/go/thrift"
	"testing"
)

func BenchmarkForwardFields(b *testing.B) {
	ctx := context.Background()
	route, body, results := []byte("route"), []byte("payload"), []byte{0, 0, 0, 204}
	for _, kind := range []string{"args", "result", "batch_result"} {
		b.Run(kind, func(b *testing.B) {
			buffer := thrift.NewTMemoryBufferLen(256)
			p := thrift.NewTBinaryProtocolConf(buffer, nil)
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				buffer.Reset()
				var err error
				if kind == "args" {
					err = writeForwardArgs(ctx, p, route, body)
				} else {
					err = writeForwardResult(ctx, p, kind == "batch_result", 200, results)
				}
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
