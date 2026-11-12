package courier

import (
	"bytes"
	"errors"
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBinaryEnvelopeRoundTrip(t *testing.T) {
	for _, kind := range []string{"request", "response"} {
		for _, rpcerr := range []*RPCError{nil, {}, {Code: "deadline", Message: "中文\n\"error"}} {
			e := Envelope{Version: 1, Kind: kind, Service: "S", Method: "M", ID: "id", Session: "s", Reply: Route{Topic: "reply", Partition: 2}, Deadline: 1234, Payload: []byte{0, 255, 1, 2}, Error: rpcerr}
			b, err := EncodeEnvelope(e, 4096)
			require.NoError(t, err)

			got, err := DecodeEnvelope(b, 4096)
			require.NoError(t, err)

			expected := e
			if !reflect.DeepEqual(got, expected) {
				t.Fatalf("roundtrip: %+v", got)
			}
			again, err := EncodeEnvelope(got, 4096)
			if err != nil || !bytes.Equal(b, again) {
				t.Fatal("response encoding format lost")
			}
			for n := 0; n < len(b); n++ {
				if _, err := DecodeEnvelope(b[:n], 4096); err == nil {
					t.Fatalf("truncated input accepted at %d", n)
				}
			}
			if _, err := DecodeEnvelope(append(bytes.Clone(b), 0), 4096); err == nil {
				t.Fatal("trailing bytes accepted")
			}
			if _, err := DecodeEnvelope(b, len(b)-1); !errors.Is(err, ErrTooLarge) {
				t.Fatal("read size bypassed")
			}
			if _, err := EncodeEnvelope(e, 4); err == nil {
				t.Fatal("write size bypassed")
			}
		}
	}
}

func TestBinaryEnvelopeRejectsBadFields(t *testing.T) {
	e := Envelope{Version: 1, Kind: "response", Service: "S", Method: "M", ID: "id", Session: "s", Reply: Route{Topic: "r"}}
	b, err := EncodeEnvelope(e, 512)
	require.NoError(t, err)

	wrongType := bytes.Clone(b)
	wrongType[4] = 11
	if _, err := DecodeEnvelope(wrongType, 512); err == nil {
		t.Fatal("wrong type accepted")
	}
	duplicate := append(bytes.Clone(b[:len(b)-1]), b[4:11]...)
	duplicate = append(duplicate, 0)
	if _, err := DecodeEnvelope(duplicate, 512); err == nil {
		t.Fatal("duplicate version accepted")
	}
	e.Version = 99
	b, err = EncodeEnvelope(e, 512)
	require.NoError(t, err)

	if _, err := DecodeEnvelope(b, 512); err == nil {
		t.Fatal("unknown protocol version accepted")
	}
}

func TestEnvelopeRejectsJSON(t *testing.T) {
	if _, err := DecodeEnvelope([]byte(`{"version":1,"kind":"response","service":"S","method":"M","id":"id","session":"s","reply":{"topic":"r","partition":0}}`), 4096); err == nil {
		t.Fatal("JSON envelope accepted")
	}
}
