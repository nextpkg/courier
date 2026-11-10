package reuse

import "testing"

func TestCacheBoundaries(t *testing.T) {
	var disabled Cache[int]
	if disabled.Take() != 0 || disabled.Put(1) {
		t.Fatal("nil cache must be disabled")
	}
	cache := make(Cache[int], 1)
	if !cache.Put(7) || cache.Put(8) {
		t.Fatal("capacity must be bounded")
	}
	if cache.Take() != 7 || cache.Take() != 0 {
		t.Fatal("take must preserve values and never wait")
	}
}
