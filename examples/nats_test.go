//go:build integration

package examples_test

import (
	"bufio"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"github.com/nextpkg/courier/internal/testcheck"
	"io"
	"os"
	"os/exec"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	sdk "github.com/nats-io/nats.go"
	"github.com/nextpkg/courier"
	"github.com/nextpkg/courier/examples/contract"
	"github.com/nextpkg/courier/examples/gen/demo"
	"github.com/nextpkg/courier/examples/handlers"
	transport "github.com/nextpkg/courier/transport/nats"
	"github.com/stretchr/testify/require"
)

func natsURL(t *testing.T) string {
	t.Helper()
	u := os.Getenv("NATS_URL")
	if u == "" {
		t.Fatal("integration requires NATS_URL pointing at a real broker")
	}
	return u
}
func natsFactory(url, prefix string) contract.Factory {
	return func(t *testing.T) courier.Transport {
		t.Helper()
		conn, err := sdk.Connect(url, sdk.ReconnectWait(20*time.Millisecond), sdk.MaxReconnects(100), sdk.ReconnectBufSize(-1), sdk.FlusherTimeout(time.Second))
		if err != nil {
			t.Fatal(err)
		}
		tr, err := transport.New(conn, transport.Options{Prefix: prefix, PendingMessages: 1024})
		if err != nil {
			conn.Close()
			t.Fatal(err)
		}
		t.Cleanup(func() {
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			if err := tr.Close(ctx); err != nil {
				t.Error(err)
			}
			if s := tr.Stats(); s.Subscriptions != 0 || s.Pending != 0 {
				t.Errorf("NATS resources leaked: %+v", s)
			}
			conn.Close()
		})
		return tr
	}
}
func TestNATSContract(t *testing.T) {
	url := natsURL(t)
	contract.Run(t, func(t *testing.T) courier.Transport {
		prefix := fmt.Sprintf("courier.test.%x", sha256.Sum256([]byte(t.Name())))
		return natsFactory(url, prefix)(t)
	})
}
func TestNATSNoRespondersAndConnectionClose(t *testing.T) {
	tr := natsFactory(natsURL(t), "courier.noresponders")(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	c, err := courier.NewClient(ctx, tr, courier.ClientOptions{})
	require.NoError(t, err)

	defer testcheck.Close(t, c)
	_, err = demo.NewOrdersAsyncClient(c).Get(ctx, "absent", 0)
	if !errors.Is(err, courier.ErrNoResponders) {
		t.Fatalf("no responders: %v", err)
	}
	if err = tr.(*transport.Transport).Close(ctx); err != nil {
		t.Fatal(err)
	}
	// Closing reply delivery must terminate pending calls/admission promptly.
	deadline := time.Now().Add(time.Second)
	for {
		_, err = demo.NewOrdersAsyncClient(c).GetAsync(ctx, "closed", 0)
		if errors.Is(err, courier.ErrClosed) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("client stayed open: %v", err)
		}
		time.Sleep(time.Millisecond)
	}
}
func TestNATSChild(t *testing.T) {
	role := os.Getenv("COURIER_CHILD_ROLE")
	if role == "" {
		return
	}
	code := func() error {
		conn, err := sdk.Connect(os.Getenv("NATS_URL"), sdk.ReconnectBufSize(-1))
		if err != nil {
			return err
		}
		defer conn.Close()
		tr, err := transport.New(conn, transport.Options{Prefix: os.Getenv("COURIER_CHILD_PREFIX")})
		if err != nil {
			return err
		}
		defer testcheck.Close(t, tr)
		s, err := courier.NewServer(tr, courier.ServerOptions{MaxConcurrent: 64})
		if err != nil {
			return err
		}
		defer testcheck.Close(t, s)
		switch role {
		case "orders":
			err = demo.RegisterOrders(s, &handlers.Orders{})
		case "inventory":
			err = demo.RegisterInventory(s, &handlers.Inventory{})
		default:
			return fmt.Errorf("bad child role")
		}
		if err != nil {
			return err
		}
		if err = s.Start(context.Background()); err != nil {
			return err
		}
		fmt.Println("READY")
		scanner := bufio.NewScanner(os.Stdin)
		scanner.Scan()
		return s.Close(context.Background())
	}()
	if code != nil {
		fmt.Fprintln(os.Stderr, code)
		os.Exit(1)
	}
	os.Exit(0)
}
func TestNATSCrossProcess(t *testing.T) {
	url := natsURL(t)
	prefix := fmt.Sprintf("courier.process.%d", time.Now().UnixNano())
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	t.Cleanup(cancel)
	for _, role := range []string{"orders", "orders", "inventory", "inventory"} {
		cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestNATSChild$")
		cmd.Env = append(os.Environ(), "NATS_URL="+url, "COURIER_CHILD_PREFIX="+prefix, "COURIER_CHILD_ROLE="+role)
		cmd.Stderr = os.Stderr
		stdout, err := cmd.StdoutPipe()
		require.NoError(t, err)

		stdin, err := cmd.StdinPipe()
		require.NoError(t, err)

		if err = cmd.Start(); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			_, _ = io.WriteString(stdin, "stop\n")
			_ = stdin.Close()
			if err := cmd.Wait(); err != nil {
				t.Error(err)
			}
		})
		ready := make(chan bool, 1)
		go func() { s := bufio.NewScanner(stdout); ready <- s.Scan() && strings.TrimSpace(s.Text()) == "READY" }()
		select {
		case ok := <-ready:
			if !ok {
				t.Fatal("child failed readiness")
			}
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	}
	var clients []*courier.Client
	for i := 0; i < 3; i++ {
		c, err := courier.NewClient(ctx, natsFactory(url, prefix)(t), courier.ClientOptions{MaxInFlight: 256})
		require.NoError(t, err)

		clients = append(clients, c)
		t.Cleanup(func() { _ = c.Close(context.Background()) })
	}
	var wg sync.WaitGroup
	for i := 0; i < 80; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			c := clients[i%len(clients)]
			id := fmt.Sprint(i)
			switch i % 4 {
			case 0:
				v, err := demo.NewOrdersAsyncClient(c).Get(ctx, id, 0)
				if err != nil || v == nil || v.Source != "Orders" {
					t.Errorf("orders: %v %v", v, err)
				}
			case 1:
				v, err := demo.NewOrdersAsyncClient(c).List(ctx, []string{id})
				if err != nil || len(v) != 1 || v[0].Source != "Orders" {
					t.Errorf("list: %v %v", v, err)
				}
			case 2:
				v, err := demo.NewInventoryAsyncClient(c).Reserve(ctx, id)
				if err != nil || v == nil || v.Source != "Inventory" {
					t.Errorf("inventory: %v %v", v, err)
				}
			case 3:
				v, err := demo.NewInventoryAsyncClient(c).Count(ctx, id)
				if err != nil || v != int64(len(id)) {
					t.Errorf("count: %v %v", v, err)
				}
			}
		}(i)
	}
	wg.Wait()
}

func TestNATSNativeOptionsAndNamespaces(t *testing.T) {
	conn, err := sdk.Connect(natsURL(t), sdk.Name("courier-options-test"), sdk.ReconnectBufSize(-1))
	require.NoError(t, err)

	defer conn.Close()
	if conn.Opts.Name != "courier-options-test" || conn.Opts.ReconnectBufSize != -1 {
		t.Fatal("native configuration was not preserved")
	}
	for _, opts := range []transport.Options{{Prefix: "bad.*"}, {Workers: -1}, {PendingMessages: -1}, {MaxFrameBytes: 1}} {
		if _, err := transport.New(conn, opts); err == nil {
			t.Fatal("invalid options accepted")
		}
	}
	a, err := transport.New(conn, transport.Options{Prefix: "namespace.a"})
	require.NoError(t, err)

	defer testcheck.Close(t, a)
	b, err := transport.New(conn, transport.Options{Prefix: "namespace.b"})
	require.NoError(t, err)

	defer testcheck.Close(t, b)
	s, _ := courier.NewServer(a, courier.ServerOptions{})
	_ = demo.RegisterOrders(s, &handlers.Orders{})
	if err = s.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer testcheck.Close(t, s)
	c, err := courier.NewClient(context.Background(), b, courier.ClientOptions{})
	require.NoError(t, err)

	defer testcheck.Close(t, c)
	_, err = demo.NewOrdersAsyncClient(c).Get(context.Background(), "isolated", 0)
	if !errors.Is(err, courier.ErrNoResponders) {
		t.Fatalf("namespace leakage: %v", err)
	}
	if err = b.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if conn.IsClosed() {
		t.Fatal("adapter closed caller-owned native connection")
	}
}

func TestNATSRepeatedLifecycle(t *testing.T) {
	conn, err := sdk.Connect(natsURL(t), sdk.ReconnectBufSize(-1))
	require.NoError(t, err)

	defer conn.Close()
	baseline := runtime.NumGoroutine()
	for i := 0; i < 30; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		tr, err := transport.New(conn, transport.Options{Prefix: "courier.lifecycle"})
		require.NoError(t, err)

		c, err := courier.NewClient(ctx, tr, courier.ClientOptions{})
		require.NoError(t, err)

		_, err = demo.NewOrdersAsyncClient(c).Get(ctx, "missing", 0)
		if !errors.Is(err, courier.ErrNoResponders) {
			t.Fatal(err)
		}
		if err = c.Close(ctx); err != nil {
			t.Fatal(err)
		}
		if err = tr.Close(ctx); err != nil {
			t.Fatal(err)
		}
		cancel()
		if stats := tr.Stats(); stats.Subscriptions != 0 || stats.Pending != 0 {
			t.Fatalf("adapter retained resources: %+v", stats)
		}
	}
	testcheck.AssertGoroutinesReturn(t, baseline)
}
