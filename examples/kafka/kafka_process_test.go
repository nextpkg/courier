//go:build integration

package kafka_test

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/nextpkg/courier"
	"github.com/nextpkg/courier/examples/gen/demo"
	"github.com/nextpkg/courier/examples/handlers"
	"github.com/nextpkg/courier/examples/internal/kafkatest"
	"github.com/nextpkg/courier/internal/testcheck"
	transport "github.com/nextpkg/courier/transport/kafka"
)

type processOrders struct{ *handlers.Orders }

func (h processOrders) Get(ctx context.Context, id string, ms int32) (*demo.Item, error) {
	v, e := h.Orders.Get(ctx, id, ms)
	if e == nil {
		v.Values = append(v.Values, int64(os.Getpid()))
	}
	return v, e
}

type processInventory struct{ *handlers.Inventory }

func (h processInventory) Reserve(ctx context.Context, id string) (*demo.Item, error) {
	v, e := h.Inventory.Reserve(ctx, id)
	if e == nil {
		v.Values = append(v.Values, int64(os.Getpid()))
	}
	return v, e
}
func TestKafkaChild(t *testing.T) {
	role := os.Getenv("KAFKA_CHILD_ROLE")
	if role == "" {
		return
	}
	err := func() (runErr error) {
		tr, err := transport.New(context.Background(), transport.Options{Brokers: kafkatest.Brokers(t), Prefix: os.Getenv("KAFKA_CHILD_PREFIX")})
		if err != nil {
			return err
		}
		defer func() {
			stop, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			runErr = errors.Join(runErr, tr.Close(stop))
		}()
		var s *courier.Server
		switch role {
		case "orders":
			s, err = demo.ListenOrders(context.Background(), tr, processOrders{&handlers.Orders{}}, courier.ServerOptions{MaxConcurrent: 128})
		case "inventory":
			s, err = demo.ListenInventory(context.Background(), tr, processInventory{&handlers.Inventory{}}, courier.ServerOptions{MaxConcurrent: 128})
		default:
			return fmt.Errorf("bad child role")
		}
		if err != nil {
			return err
		}
		defer func() {
			stop, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			runErr = errors.Join(runErr, s.Close(stop))
		}()
		fmt.Println("READY")
		scanner := bufio.NewScanner(os.Stdin)
		scanner.Scan()
		return s.Close(context.Background())
	}()
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	os.Exit(0)
}
func TestKafkaCrossProcess(t *testing.T) {
	_, opts := kafkatest.NewEnv(t, 3)
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Second)
	t.Cleanup(cancel)
	expected := map[string]map[int64]bool{"Orders": {}, "Inventory": {}}
	for _, role := range []string{"orders", "orders", "inventory", "inventory"} {
		cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestKafkaChild$")
		cmd.Env = append(os.Environ(), "KAFKA_CHILD_PREFIX="+opts.Prefix, "KAFKA_CHILD_ROLE="+role)
		cmd.Stderr = os.Stderr
		stdout, err := cmd.StdoutPipe()
		require.NoError(t, err)

		stdin, err := cmd.StdinPipe()
		require.NoError(t, err)

		if err = cmd.Start(); err != nil {
			t.Fatal(err)
		}
		name := "Orders"
		if role == "inventory" {
			name = "Inventory"
		}
		expected[name][int64(cmd.Process.Pid)] = true
		t.Cleanup(func() {
			_, _ = io.WriteString(stdin, "stop\n")
			_ = stdin.Close()
			if err := cmd.Wait(); err != nil {
				t.Error(err)
			}
		})
		ready := make(chan bool, 1)
		go func() { scanner := bufio.NewScanner(stdout); ready <- scanner.Scan() && scanner.Text() == "READY" }()
		select {
		case ok := <-ready:
			if !ok {
				t.Fatal("child readiness failed")
			}
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	}
	var clients []*courier.Client
	var orders []*demo.OrdersAsyncClient
	for i := 0; i < 3; i++ {
		api, conn, err := demo.DialOrders(ctx, kafkatest.NewTransport(t, opts), courier.ClientOptions{MaxInFlight: 512, DefaultTimeout: 8 * time.Second})
		require.NoError(t, err)

		t.Cleanup(func() { testcheck.Close(t, conn) })
		clients = append(clients, conn)
		orders = append(orders, api)
	}
	methods := []string{"Orders.Get", "Orders.List", "Inventory.Reserve", "Inventory.Count"}
	completed := make([]map[string]int, len(clients))
	replicas := make([]map[string]map[int64]int, len(clients))
	for i := range clients {
		completed[i] = map[string]int{}
		replicas[i] = map[string]map[int64]int{"Orders": {}, "Inventory": {}}
	}
	t.Logf("topology: services=2 replicas_per_service=2 clients=%d request_partitions=3 reply_partitions=1 prefix=%s", len(clients), opts.Prefix)
	var mu sync.Mutex
	used := map[string]map[int64]bool{"Orders": {}, "Inventory": {}}
	var wg sync.WaitGroup
	for i := 0; i < 240; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			clientIndex := i % len(clients)
			c := clients[clientIndex]
			id := fmt.Sprintf("client-%d/request-%d", clientIndex+1, i)
			var item *demo.Item
			var err error
			switch i % 4 {
			case 0:
				item, err = orders[clientIndex].Get(ctx, id, 0)
			case 1:
				var items []*demo.Item
				items, err = orders[clientIndex].List(ctx, []string{id})
				if err == nil && (len(items) != 1 || items[0] == nil || items[0].ID != id || items[0].Source != "Orders") {
					t.Errorf("List routing: %v", items)
					return
				}
			case 2:
				item, err = demo.NewInventoryAsyncClient(c).Reserve(ctx, id)
			case 3:
				var n int64
				n, err = demo.NewInventoryAsyncClient(c).Count(ctx, id)
				if err == nil && n != int64(len(id)) {
					t.Errorf("Count result: %d", n)
					return
				}
			}
			if err != nil {
				t.Error(err)
				return
			}
			if i%2 == 0 {
				want := "Orders"
				if i%4 == 2 {
					want = "Inventory"
				}
				if item == nil || item.Source != want || item.ID != id || len(item.Values) == 0 {
					t.Errorf("route/result: %v", item)
					return
				}
				pid := item.Values[len(item.Values)-1]
				mu.Lock()
				used[want][pid] = true
				replicas[clientIndex][want][pid]++
				mu.Unlock()
			}
			mu.Lock()
			completed[clientIndex][methods[i%len(methods)]]++
			mu.Unlock()
		}(i)
	}
	wg.Wait()
	for i, client := range clients {
		for _, method := range methods {
			if completed[i][method] != 20 {
				t.Errorf("client %d: %s completed %d calls, want 20", i+1, method, completed[i][method])
			}
		}
		if pending := client.Pending(); pending != 0 {
			t.Errorf("client %d retained %d pending calls", i+1, pending)
		}
		t.Logf("client=%d completed=%v replica_calls=%v pending=%d", i+1, completed[i], replicas[i], client.Pending())
	}
	for name, pids := range expected {
		if len(used[name]) != 2 {
			t.Errorf("both %s replicas not exercised: %v", name, used[name])
		}
		for pid := range used[name] {
			if !pids[pid] {
				t.Errorf("foreign process %d handled %s", pid, name)
			}
		}
	}
	t.Logf("real server processes exercised: %v", used)
}
