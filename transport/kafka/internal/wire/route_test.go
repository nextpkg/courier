package wire

import (
	"bytes"
	"encoding/binary"
	"testing"
)

func TestBinaryRouteStringLengths(t *testing.T) {
	f := Route{Endpoint: "http://localhost:1234", Instance: "i", Session: "s", ID: "id", Topic: "r", Expires: 123}

	encoded, err := EncodeRoute(f)
	if err != nil {
		t.Fatal(err)
	}
	for _, size := range []uint32{0xffffffff, MaxRouteBytes + 1, uint32(len(encoded))} {
		bad := bytes.Clone(encoded)
		// The first field is Endpoint: type byte, field ID, then string length.
		binary.BigEndian.PutUint32(bad[len(RoutePrefix)+3:], size)
		if _, err := DecodeRoute(bad); err == nil {
			t.Fatalf("accepted invalid string length %d", size)
		}
	}
}

func TestBinaryRouteStringsOwnBytes(t *testing.T) {
	f := Route{Endpoint: "http://localhost:1234", Instance: "i", Session: "s", ID: "id", Topic: "r", Expires: 123}

	encoded, err := EncodeRoute(f)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := DecodeRoute(encoded)
	if err != nil {
		t.Fatal(err)
	}
	clear(encoded)
	c := getRouteCodec()
	c.b.Write(bytes.Repeat([]byte{'x'}, MaxRouteBytes))
	putRouteCodec(c)
	if decoded != f {
		t.Fatalf("route changed after input and codec reuse: got %+v, want %+v", decoded, f)
	}
}
