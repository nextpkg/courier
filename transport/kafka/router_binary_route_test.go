package kafka

import (
	"bytes"
	"encoding/base64"
	"encoding/binary"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestBinaryRouteValidation(t *testing.T) {
	r, f, _ := routerFixture(t)
	{
		b, err := marshalForwardRoute(f)
		if err != nil {
			t.Fatal(err)
		}
		got, err := r.decodeRoute(b)
		if err != nil || got != f {
			t.Fatalf("roundtrip: %+v %v", got, err)
		}
		got.ID = ""
		bad, _ := marshalForwardRoute(got)
		if _, err := r.decodeRoute(bad); err == nil {
			t.Fatal("empty request ID accepted")
		}
		{
			for n := 0; n < len(b); n++ {
				if _, err := r.decodeRoute(b[:n]); err == nil {
					t.Fatalf("truncation accepted %d", n)
				}
			}
			if _, err := r.decodeRoute(append(bytes.Clone(b), 0)); err == nil {
				t.Fatal("trailing data accepted")
			}
		}
	}
}

func TestBinaryRouteHTTPHeader(t *testing.T) {
	r, f, b := routerFixture(t)
	r.inbound = make(chan struct{}, 1)

	hdr, err := marshalForwardRoute(f)
	if err != nil {
		t.Fatal(err)
	}
	req := httptest.NewRequest("POST", "/courier/reply", bytes.NewReader(b))
	req.Header.Set("X-Courier-Route", "base64:"+base64.StdEncoding.EncodeToString(hdr))
	w := httptest.NewRecorder()
	r.serve(w, req)
	if w.Code != http.StatusGone {
		t.Fatalf("expected valid route but absent inbox, got %d", w.Code)
	}
}

func TestBinaryRouteStringLengths(t *testing.T) {
	_, f, _ := routerFixture(t)

	encoded, err := marshalForwardRoute(f)
	if err != nil {
		t.Fatal(err)
	}
	for _, size := range []uint32{0xffffffff, maxRouteBytes + 1, uint32(len(encoded))} {
		bad := bytes.Clone(encoded)
		// The first field is Endpoint: type byte, field ID, then string length.
		binary.BigEndian.PutUint32(bad[len(binaryRoutePrefix)+3:], size)
		if _, err := unmarshalBinaryRoute(bad); err == nil {
			t.Fatalf("accepted invalid string length %d", size)
		}
	}
}

func TestBinaryRouteStringsOwnBytes(t *testing.T) {
	_, f, _ := routerFixture(t)

	encoded, err := marshalForwardRoute(f)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := unmarshalBinaryRoute(encoded)
	if err != nil {
		t.Fatal(err)
	}
	clear(encoded)
	c := getRouteCodec()
	c.b.Write(bytes.Repeat([]byte{'x'}, maxRouteBytes))
	putRouteCodec(c)
	if decoded != f {
		t.Fatalf("route changed after input and codec reuse: got %+v, want %+v", decoded, f)
	}
}

func TestRouteRejectsJSON(t *testing.T) {
	r, _, _ := routerFixture(t)
	if _, err := r.decodeRoute([]byte(`{"endpoint":"http://localhost:1234","instance":"i","session":"s","id":"id","topic":"r","partition":0,"expires":1}`)); err == nil {
		t.Fatal("JSON route accepted")
	}
}
