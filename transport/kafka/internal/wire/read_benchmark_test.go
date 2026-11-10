package wire

import "testing"

func BenchmarkDecodeFields(b *testing.B) {
	r, _ := EncodeRoute(Route{Endpoint: "http://127.0.0.1:1234", Instance: "instance", Session: "session", ID: "id", Topic: "reply", Partition: 1, Expires: 123})
	batch := EncodeBatch([]BatchMessage{{Instance: "instance", Expires: 123, Data: []byte("payload")}})
	b.Run("route", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			if _, e := DecodeRoute(r); e != nil {
				b.Fatal(e)
			}
		}
	})
	b.Run("batch", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			if _, e := DecodeBatch(batch, 4096); e != nil {
				b.Fatal(e)
			}
		}
	})
}
