package kafka

import (
	"bytes"
	"encoding/binary"
	"github.com/nextpkg/courier/transport/kafka/internal/wire"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestRouteFieldOrderAndOwnership(t *testing.T) {
	r, f, _ := routerFixture(t)

	old, err := wire.EncodeRoute(f)
	require.NoError(t, err)

	got := bytes.Clone(old)
	for i := 0; i < 100; i++ {
		other := f
		other.ID = "different"
		if _, err := wire.EncodeRoute(other); err != nil {
			t.Fatal(err)
		}
	}
	if !bytes.Equal(got, old) {
		t.Fatal("pooled storage escaped")
	}
	// Existing decoders permit reordered fields and unknown future fields.
	firstEnd := 4 + 3 + 4 + int(binary.BigEndian.Uint32(old[7:11]))
	secondEnd := firstEnd + 3 + 4 + int(binary.BigEndian.Uint32(old[firstEnd+3:firstEnd+7]))
	reordered := append(bytes.Clone(old[:4]), old[firstEnd:secondEnd]...)
	reordered = append(reordered, old[4:firstEnd]...)
	reordered = append(reordered, old[secondEnd:]...)
	unknown := append(bytes.Clone(old[:len(old)-1]), 8, 0, 9, 0, 0, 0, 7, 0)
	for _, wire := range [][]byte{old, reordered, unknown} {
		parsed, err := r.decodeRoute(wire)
		if err != nil || parsed != f {
			t.Fatalf("field-order compatibility: %+v %v", parsed, err)
		}
	}
	// Required identity fields cannot be duplicated or omitted.
	duplicate := append(bytes.Clone(old[:len(old)-1]), old[4:firstEnd]...)
	duplicate = append(duplicate, 0)
	if _, err := r.decodeRoute(duplicate); err == nil {
		t.Fatal("duplicate field accepted")
	}
	missing := append(bytes.Clone(old[:4]), old[firstEnd:]...)
	if _, err := r.decodeRoute(missing); err == nil {
		t.Fatal("missing field accepted")
	}
}
