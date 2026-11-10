package kafka

import (
	"bytes"
	"encoding/base64"
	"github.com/nextpkg/courier/transport/kafka/internal/wire"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestBinaryRouteValidation(t *testing.T) {
	r, f, _ := routerFixture(t)
	{
		b, err := wire.EncodeRoute(f)
		if err != nil {
			t.Fatal(err)
		}
		got, err := r.decodeRoute(b)
		if err != nil || got != f {
			t.Fatalf("roundtrip: %+v %v", got, err)
		}
		got.ID = ""
		bad, _ := wire.EncodeRoute(got)
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

	hdr, err := wire.EncodeRoute(f)
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

func TestRouteRejectsJSON(t *testing.T) {
	r, _, _ := routerFixture(t)
	if _, err := r.decodeRoute([]byte(`{"endpoint":"http://localhost:1234","instance":"i","session":"s","id":"id","topic":"r","partition":0,"expires":1}`)); err == nil {
		t.Fatal("JSON route accepted")
	}
}
