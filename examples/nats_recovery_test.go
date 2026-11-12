//go:build integration

package examples_test

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	sdk "github.com/nats-io/nats.go"
	"github.com/stretchr/testify/require"

	"github.com/nextpkg/courier"
	"github.com/nextpkg/courier/examples/gen/demo"
	"github.com/nextpkg/courier/examples/handlers"
	transport "github.com/nextpkg/courier/transport/nats"
)

func TestNATSDisconnectReconnect(t *testing.T) {
	binary := os.Getenv("NATS_SERVER")
	if binary == "" {
		t.Fatal("integration requires NATS_SERVER executable for restart testing")
	}
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	port := l.Addr().(*net.TCPAddr).Port
	_ = l.Close()
	url := "nats://127.0.0.1:" + strconv.Itoa(port)
	logPath := filepath.Join(t.TempDir(), "broker.log")
	var broker *exec.Cmd
	stop := func() {
		if broker != nil {
			_ = broker.Process.Kill()
			_ = broker.Wait()
			broker = nil
		}
	}
	t.Cleanup(stop)
	start := func() {
		t.Helper()
		broker = exec.Command(binary, "-a", "127.0.0.1", "-p", strconv.Itoa(port), "-l", logPath)
		if err := broker.Start(); err != nil {
			t.Fatal(err)
		}
		deadline := time.Now().Add(3 * time.Second)
		for {
			c, err := sdk.Connect(url, sdk.NoReconnect(), sdk.Timeout(100*time.Millisecond))
			if err == nil {
				c.Close()
				return
			}
			if time.Now().After(deadline) {
				t.Fatalf("broker readiness: %v", err)
			}
			time.Sleep(10 * time.Millisecond)
		}
	}
	start()
	open := func() (*sdk.Conn, *transport.Transport) {
		conn, err := sdk.Connect(url, sdk.ReconnectWait(20*time.Millisecond), sdk.MaxReconnects(500), sdk.ReconnectBufSize(-1), sdk.FlusherTimeout(100*time.Millisecond))
		if err != nil {
			t.Fatal(err)
		}
		tr, err := transport.New(conn, transport.Options{Prefix: fmt.Sprintf("recovery.%d", port)})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			_ = tr.Close(ctx)
			conn.Close()
		})
		return conn, tr
	}
	serverConn, serverTr := open()
	clientConn, clientTr := open()
	s, _ := courier.NewServer(serverTr, courier.ServerOptions{})
	r := &handlers.Recorder{Started: make(chan string, 8)}
	_ = demo.RegisterOrders(s, &handlers.Orders{Recorder: r})
	if err = s.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close(context.Background()) })
	c, err := courier.NewClient(context.Background(), clientTr, courier.ClientOptions{})
	require.NoError(t, err)

	t.Cleanup(func() { _ = c.Close(context.Background()) })
	api := demo.NewOrdersAsyncClient(c)
	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()
	pending, err := api.GetAsync(ctx, "interrupted", 500)
	require.NoError(t, err)

	select {
	case <-r.Started:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	stop()
	if _, err = pending.Await(context.Background()); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("inflight interruption=%v", err)
	}
	absentCtx, absentCancel := context.WithTimeout(context.Background(), time.Second)
	defer absentCancel()
	_, err = api.Get(absentCtx, "offline", 0)
	var te *courier.TransportError
	if !errors.As(err, &te) {
		t.Fatalf("offline send=%v", err)
	}
	start()
	deadline := time.Now().Add(3 * time.Second)
	for !serverConn.IsConnected() || !clientConn.IsConnected() {
		if time.Now().After(deadline) {
			t.Fatal("connections did not recover")
		}
		time.Sleep(10 * time.Millisecond)
	}
	if err = serverConn.FlushTimeout(time.Second); err != nil {
		t.Fatal(err)
	}
	if err = clientConn.FlushTimeout(time.Second); err != nil {
		t.Fatal(err)
	}
	v, err := api.Get(context.Background(), "recovered", 0)
	if err != nil || v == nil || v.ID != "recovered" {
		t.Fatalf("recovery=%v %v", v, err)
	}
	// A permanent native connection close must end reply delivery immediately.
	future, err := api.GetAsync(context.Background(), "closing", 500)
	require.NoError(t, err)

	if err = future.Sent(context.Background()); err != nil {
		t.Fatal(err)
	}
	clientConn.Close()
	wait, waitCancel := context.WithTimeout(context.Background(), time.Second)
	defer waitCancel()
	if _, err = future.Await(wait); !errors.Is(err, courier.ErrClosed) {
		t.Fatalf("permanent close=%v", err)
	}
}
