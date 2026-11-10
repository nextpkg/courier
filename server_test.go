package courier

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/apache/thrift/lib/go/thrift"
)

type verboseErrorProcessor struct{ thrift.TProcessor }

func (verboseErrorProcessor) ProcessorMap() map[string]thrift.TProcessorFunction {
	return map[string]thrift.TProcessorFunction{"Ping": nil}
}
func (verboseErrorProcessor) Process(ctx context.Context, in, _ thrift.TProtocol) (bool, thrift.TException) {
	if _, _, _, err := in.ReadMessageBegin(ctx); err != nil {
		return false, thrift.WrapTException(err)
	}
	if err := in.Skip(ctx, thrift.STRUCT); err != nil {
		return false, thrift.WrapTException(err)
	}
	return false, thrift.NewTApplicationException(thrift.INTERNAL_ERROR, strings.Repeat("\"", 1024))
}

func TestErrorReplyBudget(t *testing.T) {
	const limit = 512
	payload, err := encodeCall(context.Background(), "Ping", &unit{}, limit)
	if err != nil {
		t.Fatal(err)
	}
	e := Envelope{Version: ProtocolVersion, Kind: "request", Service: "S", Method: "Ping", ID: "id", Session: "session", Deadline: time.Now().Add(time.Minute).UnixNano(), Reply: Route{Topic: "replies", Partition: 0}, Payload: payload}
	// Find the largest admissible metadata budget, including binary framing.
	for {
		next := e
		next.Service += "\""
		if _, err := EncodeEnvelope(next, limit); errors.Is(err, ErrTooLarge) {
			break
		} else if err != nil {
			t.Fatal(err)
		}
		e = next
	}
	s, err := NewServer(&fakeTransport{}, ServerOptions{MaxFrameBytes: limit})
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct{ name, code string }{{"expired", "deadline"}, {"verbose processor error", "protocol"}} {
		t.Run(tc.name, func(t *testing.T) {
			request := e
			if tc.name == "expired" {
				request.Deadline = time.Now().Add(-time.Second).UnixNano()
			}
			data, err := EncodeEnvelope(request, limit)
			if err != nil {
				t.Fatal(err)
			}
			out, err := s.handler(e.Service, verboseErrorProcessor{})(context.Background(), data)
			if err != nil {
				t.Fatal(err)
			}
			reply, err := DecodeEnvelope(out, limit)
			if err != nil || reply.Error == nil || reply.Error.Code != tc.code || reply.ID != e.ID || reply.Session != e.Session || reply.Service != e.Service || reply.Reply != e.Reply {
				t.Fatalf("invalid bounded error response: %+v, %v", reply, err)
			}
		})
	}
	// A raw or old producer can still send an otherwise valid frame without
	// reserving error space; reject it on decode as well as local admission.
	e.Service += "\""
	if _, err := EncodeEnvelope(e, limit); !errors.Is(err, ErrTooLarge) {
		t.Fatalf("oversized metadata admitted: %v", err)
	}
	raw, err := EncodeEnvelope(e, DefaultMaxFrameBytes)
	if err != nil || len(raw) > limit {
		t.Fatalf("invalid raw fixture: size=%d, %v", len(raw), err)
	}
	if _, err := DecodeEnvelope(raw, limit); !errors.Is(err, ErrTooLarge) {
		t.Fatalf("raw metadata bypassed budget: %v", err)
	}
}
