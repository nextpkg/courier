package courier

import (
	"errors"
	"math"
	"strings"
	"testing"
)

func TestErrorBudgetFastPathMatchesExactBoundary(t *testing.T) {
	for _, field := range []string{"abc", strings.Repeat("a", 100), strings.Repeat("\x00\n\"\\<>&\u2028\ufffd", 30), string([]byte{0xff, 0xfe}), strings.Repeat("x", 2000)} {
		e := Envelope{Version: ProtocolVersion, Kind: "request", Service: field, Method: field, ID: field, Session: field, Reply: Route{Topic: field, Partition: math.MinInt32}}
		expected := e
		expected.Kind = "response"
		expected.Payload = nil
		expected.Deadline = 0
		expected.Error = &RPCError{Code: "method_not_found", Message: strings.Repeat("x", 64)}
		b, err := EncodeEnvelope(expected, DefaultMaxFrameBytes)
		if err != nil {
			t.Fatal(err)
		}
		for _, limit := range []int{-1, 0, 511, 512, len(b) - 1, len(b), len(b) + 1, DefaultMaxFrameBytes} {
			err := checkErrorReplyBudget(e, limit)
			if errors.Is(err, ErrTooLarge) != (len(b) > limit) {
				t.Fatalf("limit=%d exact=%d err=%v", limit, len(b), err)
			}
		}
	}
}

func BenchmarkErrorBudget(b *testing.B) {
	e := Envelope{Version: 1, Kind: "request", Service: "Orders", Method: "Get", ID: strings.Repeat("a", 32), Session: strings.Repeat("b", 32), Reply: Route{Topic: "courier.requests"}}
	b.Run("exact", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			v := e
			v.Kind = "response"
			v.Payload = nil
			v.Deadline = 0
			v.Error = &RPCError{Code: "method_not_found", Message: strings.Repeat("x", 64)}
			if _, err := EncodeEnvelope(v, DefaultMaxFrameBytes); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("bounded", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			if err := checkErrorReplyBudget(e, DefaultMaxFrameBytes); err != nil {
				b.Fatal(err)
			}
		}
	})
}
