package courier

import (
	"bytes"
	"context"
	"errors"
	"testing"
)

func TestCodecPoolOutputOwnershipAndLimits(t *testing.T) {
	ctx := context.Background()
	first, err := encodeCall(ctx, "first", &unit{}, 1024)
	if err != nil {
		t.Fatal(err)
	}
	saved := bytes.Clone(first)
	for i := 0; i < 100; i++ {
		if _, err := encodeCall(ctx, "another", &unit{}, 1024); err != nil {
			t.Fatal(err)
		}
		b, _ := binaryProtocol(nil, 1)
		if _, err := b.Write([]byte("too long")); !errors.Is(err, ErrTooLarge) {
			t.Fatalf("limit: %v", err)
		}
		releaseBinaryProtocol(b)
	}
	if !bytes.Equal(first, saved) {
		t.Fatal("returned payload changed after buffer reuse")
	}
	if _, err := encodeCall(ctx, "first", &unit{}, 1); err == nil {
		t.Fatal("small frame accepted")
	}
}
