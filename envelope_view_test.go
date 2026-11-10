package courier

import (
	"bytes"
	"encoding/binary"
	"reflect"
	"testing"
)

func replyViewFixture(t testing.TB, rpcerr *RPCError) []byte {
	t.Helper()
	b, err := EncodeEnvelope(Envelope{Version: 1, Kind: "response", Service: "Orders", Method: "Get", ID: "request-id", Session: "session-id", Deadline: 123, Reply: Route{Topic: "replies", Partition: 2}, Payload: []byte{0, 255, 1, 2}, Error: rpcerr}, 4096)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func checkReplyView(t testing.TB, b []byte) {
	t.Helper()
	full, fullErr := DecodeEnvelope(b, 4096)
	view, err := DecodeReplyEnvelopeView(b, 4096)
	valid := fullErr == nil && full.Kind == "response"
	if (err == nil) != valid {
		t.Fatalf("acceptance differs: full=%v kind=%q view=%v", fullErr, full.Kind, err)
	}
	if err != nil {
		return
	}
	owned := view.OwnedEnvelope()
	if !reflect.DeepEqual(owned, full) {
		t.Fatalf("decoded reply differs: %+v vs %+v", owned, full)
	}
	clear(view.Payload)
	clear(b)
	scratch, _ := binaryProtocol(bytes.Repeat([]byte("x"), 200), 4096)
	releaseBinaryProtocol(scratch)
	if !reflect.DeepEqual(owned, full) {
		t.Fatal("owned result changed after input or codec reuse")
	}
}

func TestReplyViewValidationAndOwnership(t *testing.T) {
	for _, e := range []*RPCError{nil, {}, {Code: "deadline", Message: "中文"}} {
		b := replyViewFixture(t, e)
		checkReplyView(t, bytes.Clone(b))
		for n := 0; n < len(b); n++ {
			checkReplyView(t, bytes.Clone(b[:n]))
		}
		checkReplyView(t, append(bytes.Clone(b), 0))
		checkReplyView(t, append(append(bytes.Clone(b[:len(b)-1]), b[4:11]...), 0))
		bad := bytes.Clone(b)
		binary.BigEndian.PutUint32(bad[14:], 0xffffffff)
		checkReplyView(t, bad)
		// Swap Version and Kind, preserving complete Thrift fields.
		reordered := append(bytes.Clone(b[:4]), b[11:26]...)
		reordered = append(reordered, b[4:11]...)
		reordered = append(reordered, b[26:]...)
		checkReplyView(t, reordered)
		extended := append(bytes.Clone(b[:len(b)-1]), 8, 0, 20, 0, 0, 0, 1, 0)
		checkReplyView(t, extended)
		payloadStart := bytes.Index(b, []byte{11, 0, 10})
		if payloadStart < 0 {
			t.Fatal("fixture payload field missing")
		}
		withoutPayload := append(bytes.Clone(b[:payloadStart]), b[payloadStart+11:]...)
		checkReplyView(t, withoutPayload)
	}
	emptyPayload, err := EncodeEnvelope(Envelope{Version: 1, Kind: "response", Service: "S", Method: "M", ID: "id", Session: "s", Reply: Route{Topic: "r"}}, 4096)
	if err != nil {
		t.Fatal(err)
	}
	checkReplyView(t, emptyPayload)
	request, err := EncodeEnvelope(Envelope{Version: 1, Kind: "request", Service: "S", Method: "M", ID: "id", Session: "s", Reply: Route{Topic: "r"}}, 4096)
	if err != nil {
		t.Fatal(err)
	}
	checkReplyView(t, request)
}

func FuzzReplyViewMatchesFullDecoder(f *testing.F) {
	f.Add(replyViewFixture(f, nil))
	f.Add(replyViewFixture(f, &RPCError{Code: "bad", Message: "error"}))
	f.Add([]byte("invalid"))
	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) > 4096 {
			return
		}
		checkReplyView(t, bytes.Clone(data))
	})
}

func BenchmarkReplyEnvelopeView(b *testing.B) {
	wire := replyViewFixture(b, nil)
	b.Run("full", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_, err := DecodeEnvelope(wire, 4096)
			if err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("view", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_, err := DecodeReplyEnvelopeView(wire, 4096)
			if err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("view_owned", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			v, err := DecodeReplyEnvelopeView(wire, 4096)
			if err != nil {
				b.Fatal(err)
			}
			e := v.OwnedEnvelope()
			if e.ID == "" {
				b.Fatal("missing ID")
			}
		}
	})
}
