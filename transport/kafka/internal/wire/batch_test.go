package wire

import "testing"

func TestBinaryBatchRejectsMalformedFrames(t *testing.T) {
	good := EncodeBatch([]BatchMessage{{"instance", 123, []byte("body")}})
	for i := 0; i < len(good); i++ {
		if _, err := DecodeBatch(good[:i], 1024); err == nil {
			t.Fatalf("accepted truncated byte %d", i)
		}
	}
	if _, err := DecodeBatch(good, 1); err == nil {
		t.Fatal("accepted oversized item")
	}
	if _, err := DecodeBatch(append(good, 0), 1024); err == nil {
		t.Fatal("accepted trailing data")
	}
	if _, err := DecodeStatuses([]byte{0}, 1); err == nil {
		t.Fatal("accepted partial status")
	}
}
