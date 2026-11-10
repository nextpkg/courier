package wire

import "testing"

// BenchmarkRouteEncode measures only the borrowed encoder, without pool traffic
// or the independent output copy performed by EncodeRoute.
func BenchmarkRouteEncode(b *testing.B) {
	f := Route{Endpoint: "http://127.0.0.1:1234", Instance: "instance", Session: "session", ID: "id", Topic: "reply", Expires: 123}
	c := getRouteCodec()
	defer putRouteCodec(c)
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		c.b.Reset()
		data, err := encodeBinaryRoute(c, f)
		if err != nil || len(data) == 0 {
			b.Fatal(err)
		}
	}
}
