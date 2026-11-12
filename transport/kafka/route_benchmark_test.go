package kafka

import (
	"testing"
	"time"

	"github.com/nextpkg/courier"
)

func BenchmarkBinaryRoute(b *testing.B) {
	o, err := normalize(Options{Brokers: []string{"localhost:9092"}})
	if err != nil {
		b.Fatal(err)
	}
	r := &replyRouter{t: &Transport{opts: o}, opts: ReplyRouterOptions{AllowInsecureHTTP: true, MaxReplyAge: time.Second}, endpoint: "http://127.0.0.1:1234", instance: "instance"}
	e := courier.Envelope{ID: "request-id", Session: "session", Reply: courier.Route{Topic: o.ReplyTopic}}
	h, err := r.routeHeader(e)
	if err != nil {
		b.Fatal(err)
	}
	b.Run("encode", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			if _, err := r.routeHeader(e); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("verify", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			if _, err := r.decodeRoute(h.Value); err != nil {
				b.Fatal(err)
			}
		}
	})
}
