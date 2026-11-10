package kafka

import (
	"bytes"
	"fmt"
	codec "github.com/nextpkg/courier/transport/kafka/internal/wire"
	"github.com/stretchr/testify/require"
	"sync"
	"testing"
)

func TestRouteIdentityOwnershipAndBound(t *testing.T) {
	r, base, _ := routerFixture(t)
	for i := 0; i < 3; i++ {
		want := base
		want.ID = fmt.Sprint(i)
		want.Expires += int64(i)
		wire, err := codec.EncodeRoute(want)
		require.NoError(t, err)

		got, err := r.decodeRoute(wire)
		if err != nil || got != want {
			t.Fatalf("roundtrip: %+v %v", got, err)
		}
		clear(wire)
		if got != want {
			t.Fatal("route aliases input")
		}
	}
	if table := r.identities.Load(); table == nil || len(*table) != 1 {
		t.Fatal("ID or expiry changed the stable cache key")
	}
	for i := 0; i < maxRouteIdentities*3; i++ {
		want := base
		want.Session = fmt.Sprintf("session-%d", i)
		wire, _ := codec.EncodeRoute(want)
		got, err := r.decodeRoute(wire)
		if err != nil || got != want {
			t.Fatalf("cache turnover: %+v %v", got, err)
		}
		if len(*r.identities.Load()) > maxRouteIdentities {
			t.Fatal("unbounded identity cache")
		}
	}
	for _, change := range []func(*codec.Route){
		func(f *codec.Route) { f.ID = "" }, func(f *codec.Route) { f.Instance = "" },
		func(f *codec.Route) { f.Topic = "wrong" }, func(f *codec.Route) { f.Partition = -1 },
		func(f *codec.Route) { f.Expires = 0 }, func(f *codec.Route) { f.Endpoint = "http://localhost:12/path" },
	} {
		invalid := base
		change(&invalid)
		wire, _ := codec.EncodeRoute(invalid)
		if _, err := r.decodeRoute(wire); err == nil {
			t.Fatal("cache accepted invalid route")
		}
	}
}

func TestRouteIdentityConcurrentReadersAndMisses(t *testing.T) {
	r, base, _ := routerFixture(t)
	var wg sync.WaitGroup
	for n := 0; n < 16; n++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			for i := 0; i < 100; i++ {
				want := base
				want.Session = fmt.Sprintf("s-%d", n)
				want.ID = fmt.Sprintf("%d-%d", n, i)
				wire, err := codec.EncodeRoute(want)
				if err != nil {
					t.Error(err)
					return
				}
				got, err := r.decodeRoute(wire)
				if err != nil || got != want {
					t.Errorf("concurrent decode: %+v %v", got, err)
					return
				}
			}
		}(n)
	}
	wg.Wait()
}

func FuzzRouteCanonicalMatchesFullDecoder(f *testing.F) {
	wire, _ := codec.EncodeRoute(codec.Route{Endpoint: "http://127.0.0.1:1", Instance: "i", Session: "s", ID: "id", Topic: "courier.replies", Expires: 1})
	f.Add(wire)
	f.Add([]byte("invalid"))
	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) > codec.MaxRouteBytes || !bytes.HasPrefix(data, codec.RoutePrefix) {
			return
		}
		r, _, _ := routerFixture(t)
		got, err := r.decodeRoute(data)
		expected, decodeErr := codec.DecodeRoute(data)
		if err == nil && (decodeErr != nil || got != expected) {
			t.Fatalf("fast decoder diverged: %+v vs %+v (%v)", got, expected, decodeErr)
		}
	})
}
