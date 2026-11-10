package kafka

import (
	"bytes"
	"fmt"
	"sync"
	"testing"
)

func TestBatchBufferConcurrentOwnership(t *testing.T) {
	var wg sync.WaitGroup
	for worker := 0; worker < 16; worker++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for i := 0; i < 100; i++ {
				msg := []batchMessage{{Instance: fmt.Sprintf("instance-%d-%d", id, i), Expires: 123, Data: bytes.Repeat([]byte{byte(id)}, 4096)}}
				expected := encodeBinaryBatch(msg)
				first := borrowBinaryBatch(msg)
				second := borrowBinaryBatch([]batchMessage{{Instance: "other", Expires: 123, Data: []byte("body")}})
				releaseBinaryBatch(second)
				msg[0].Instance = "changed"
				clear(msg[0].Data)
				if !bytes.Equal(first.data, expected) {
					t.Error("active batch changed while another batch or input was reused")
				}
				decoded, err := decodeBinaryBatch(first.data, 8192)
				if err != nil || len(decoded) != 1 || len(decoded[0].Data) != 4096 {
					t.Errorf("invalid encoded batch: %v", err)
				}
				releaseBinaryBatch(first)
			}
		}(worker)
	}
	wg.Wait()
}

func BenchmarkBinaryBatchBuffer(b *testing.B) {
	messages := make([]batchMessage, 32)
	for i := range messages {
		messages[i] = batchMessage{Instance: "instance", Expires: 123, Data: bytes.Repeat([]byte("d"), 330)}
	}
	b.Run("allocate", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			v := encodeBinaryBatch(messages)
			if len(v) == 0 {
				b.Fatal("empty batch")
			}
		}
	})
	b.Run("reuse", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			v := borrowBinaryBatch(messages)
			if len(v.data) == 0 {
				b.Fatal("empty batch")
			}
			releaseBinaryBatch(v)
		}
	})
}
