package wire

import (
	"bytes"
	"encoding/binary"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestBinaryRouteStringLengths(t *testing.T) {
	f := Route{Endpoint: "http://localhost:1234", Instance: "i", Session: "s", ID: "id", Topic: "r", Expires: 123}

	encoded, err := EncodeRoute(f)
	require.NoError(t, err)

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
	require.NoError(t, err)

	decoded, err := DecodeRoute(encoded)
	require.NoError(t, err)

	clear(encoded)
	c := getRouteCodec()
	c.b.Write(bytes.Repeat([]byte{'x'}, MaxRouteBytes))
	putRouteCodec(c)
	if decoded != f {
		t.Fatalf("route changed after input and codec reuse: got %+v, want %+v", decoded, f)
	}
}

func TestTruncatedRouteIntegersPreservePartialResult(t *testing.T) {
	f := Route{Endpoint: "http://localhost:1", Instance: "i", Session: "s", ID: "id", Topic: "reply", Partition: 7, Expires: 123}
	data, err := EncodeRoute(f)
	require.NoError(t, err)
	// The tail is I32/header + partition, I64/header + expiry, then STOP.
	for n := 0; n < 4; n++ {
		got, err := DecodeRoute(data[:len(data)-16+n])
		require.Error(t, err)
		require.Equal(t, int32(0), got.Partition)
		require.Equal(t, int64(0), got.Expires)
	}
	for n := 0; n < 8; n++ {
		got, err := DecodeRoute(data[:len(data)-9+n])
		require.Error(t, err)
		require.Equal(t, f.Partition, got.Partition)
		require.Equal(t, int64(0), got.Expires)
	}
}
