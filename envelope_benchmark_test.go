package courier

import "testing"

// BenchmarkEnvelopeEncode measures the encoder including its required owned
// output copy and codec lease, without transport or RPC processing.
func BenchmarkEnvelopeEncode(b *testing.B) {
	for _, kind := range []string{"request", "response", "error"} {
		e := Envelope{Version: 1, Kind: kind, Service: "Orders", Method: "Get", ID: "id", Session: "s", Deadline: 123, Reply: Route{Topic: "reply", Partition: 1}, Payload: []byte("payload")}
		if kind == "error" {
			e.Kind = "response"
			e.Error = &RPCError{Code: "bad", Message: "message"}
		}
		b.Run(kind, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				data, err := encodeBinaryEnvelope(e, 4096)
				if err != nil || len(data) == 0 {
					b.Fatal(err)
				}
			}
		})
	}
}
