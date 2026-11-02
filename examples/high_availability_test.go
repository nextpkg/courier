//go:build integration

package examples_test

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"github.com/nextpkg/courier"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nextpkg/courier/examples/gen/demo"
	"github.com/nextpkg/courier/examples/handlers"
	transport "github.com/nextpkg/courier/transport/kafka"
)

// haOrders records an observable side effect before returning a response.
// A blocked child can be killed at a deterministic point without running defers.
type haOrders struct {
	*handlers.Orders
	directory, block string
}

func (h haOrders) Get(ctx context.Context, id string, delay int32) (*demo.Item, error) {
	mark := func(stage string) error {
		return os.WriteFile(filepath.Join(h.directory, fmt.Sprintf("%s-%d-%s", stage, os.Getpid(), id)), []byte(id), 0600)
	}
	if err := mark("entered"); err != nil {
		return nil, err
	}
	if h.block == "before" {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	value, err := h.Orders.Get(ctx, id, delay)
	if err != nil {
		return nil, err
	}
	if err := mark("effect"); err != nil {
		return nil, err
	}
	if h.block == "after" {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	value.Values = append(value.Values, int64(os.Getpid()))
	return value, nil
}

func TestHATransportChild(t *testing.T) {
	role := os.Getenv("COURIER_HA_ROLE")
	if role == "" {
		return
	}
	var tr courier.Transport
	if strings.HasPrefix(os.Getenv("COURIER_HA_PREFIX"), "courier.natsha.") {
		tr = natsFactory(natsURL(t), os.Getenv("COURIER_HA_PREFIX"))(t)
	} else {
		kt, err := transport.New(context.Background(), transport.Options{Brokers: kafkaBrokers(t), Prefix: os.Getenv("COURIER_HA_PREFIX")})
		if err != nil {
			t.Fatal(err)
		}
		defer closeTestResource(t, kt)
		tr = kt
	}
	if role == "client" {
		api, c, err := demo.DialOrders(context.Background(), tr)
		if err != nil {
			t.Fatal(err)
		}
		defer closeTestResource(t, c)
		f, err := api.GetAsync(context.Background(), "first", 0)
		if err != nil {
			t.Fatal(err)
		}
		if err := f.Sent(context.Background()); err != nil {
			t.Fatal(err)
		}
	} else {
		s, err := demo.ListenOrders(context.Background(), tr, haOrders{&handlers.Orders{}, os.Getenv("COURIER_HA_DIR"), os.Getenv("COURIER_HA_BLOCK")})
		if err != nil {
			t.Fatal(err)
		}
		defer closeTestResource(t, s)
	}
	fmt.Println("READY")
	bufio.NewScanner(os.Stdin).Scan()
}

type haProcess struct {
	cmd   *exec.Cmd
	stdin io.WriteCloser
	done  bool
}

func startHAProcess(t *testing.T, prefix, directory, role, block string) *haProcess {
	t.Helper()
	cmd := exec.Command(os.Args[0], "-test.run=^TestHATransportChild$", "-test.timeout=90s")
	cmd.Env = append(os.Environ(), "COURIER_HA_PREFIX="+prefix, "COURIER_HA_DIR="+directory, "COURIER_HA_ROLE="+role, "COURIER_HA_BLOCK="+block)
	cmd.Stderr = os.Stderr
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	stdin, err := cmd.StdinPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err = cmd.Start(); err != nil {
		t.Fatal(err)
	}
	p := &haProcess{cmd: cmd, stdin: stdin}
	t.Cleanup(func() {
		if !p.done {
			p.stop(t, true)
		}
	})
	ready := make(chan bool, 1)
	go func() {
		s := bufio.NewScanner(stdout)
		ok := false
		for s.Scan() {
			if s.Text() == "READY" {
				if !ok {
					ready <- true
					ok = true
				}
			}
		}
		if !ok {
			ready <- false
		}
	}()
	select {
	case ok := <-ready:
		if !ok {
			t.Fatal("child exited before readiness")
		}
	case <-time.After(20 * time.Second):
		t.Fatal("child readiness timeout")
	}
	return p
}
func (p *haProcess) stop(t *testing.T, abrupt bool) {
	t.Helper()
	if p.done {
		return
	}
	if abrupt {
		if err := p.cmd.Process.Kill(); err != nil {
			t.Error(err)
		}
	} else {
		_, _ = io.WriteString(p.stdin, "stop\n")
	}
	_ = p.stdin.Close()
	err := p.cmd.Wait()
	p.done = true
	if !abrupt && err != nil {
		t.Error(err)
	}
	if abrupt && err == nil {
		t.Error("expected abnormal process exit")
	}
}

func TestKafkaHAProcessScaling(t *testing.T) {
	_, opts := newKafkaEnv(t, 3)
	runHAProcessScaling(t, opts.Prefix, func() courier.Transport { return newKafkaTransport(t, opts) })
}
func TestNATSHAProcessScaling(t *testing.T) {
	prefix := fmt.Sprintf("courier.natsha.%d", time.Now().UnixNano())
	runHAProcessScaling(t, prefix, func() courier.Transport { return natsFactory(natsURL(t), prefix)(t) })
}
func runHAProcessScaling(t *testing.T, prefix string, newTransport func() courier.Transport) {
	dir := t.TempDir()
	first := startHAProcess(t, prefix, dir, "server", "")
	c := kafkaClient(t, newTransport())
	api := demo.NewOrdersAsyncClient(c)
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	var mu sync.Mutex
	successes, failures := 0, 0
	seen := map[int64]int{}
	longest := time.Duration(0)
	for worker := 0; worker < 4; worker++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			for i := 0; ctx.Err() == nil; i++ {
				id := fmt.Sprintf("w%d-%d", worker, i)
				start := time.Now()
				callCtx, done := context.WithTimeout(context.Background(), 20*time.Second)
				v, err := api.Get(callCtx, id, 5)
				done()
				mu.Lock()
				if err != nil || v == nil || v.ID != id || len(v.Values) != 4 {
					failures++
					if failures <= 10 {
						t.Logf("call id=%s error=%v result=%v", id, err, v)
					}
				} else {
					successes++
					seen[v.Values[3]]++
				}
				if elapsed := time.Since(start); elapsed > longest {
					longest = elapsed
				}
				mu.Unlock()
			}
		}(worker)
	}
	defer func() { cancel(); wg.Wait() }()
	awaitPID := func(pid int64) {
		waitFor(t, 15*time.Second, func() bool { mu.Lock(); defer mu.Unlock(); return seen[pid] >= 4 }, "instance did not serve calls")
	}
	awaitPID(int64(first.cmd.Process.Pid))
	for round := 0; round < 2; round++ {
		second := startHAProcess(t, prefix, dir, "server", "")
		awaitPID(int64(second.cmd.Process.Pid))
		first.stop(t, false)
		// Require new completions after the departing process has exited.
		mu.Lock()
		seen[int64(second.cmd.Process.Pid)] = 0
		mu.Unlock()
		awaitPID(int64(second.cmd.Process.Pid))
		first = second
	}
	cancel()
	wg.Wait()
	mu.Lock()
	defer mu.Unlock()
	if failures != 0 || successes == 0 || c.Pending() != 0 {
		t.Fatalf("success=%d failed/mismatched=%d pending=%d", successes, failures, c.Pending())
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	counts := map[string]int{}
	for _, entry := range entries {
		if strings.HasPrefix(entry.Name(), "effect-") {
			data, e := os.ReadFile(filepath.Join(dir, entry.Name()))
			if e != nil {
				t.Fatal(e)
			}
			counts[string(data)]++
		}
	}
	for id, n := range counts {
		if n != 1 {
			t.Fatalf("duplicate %s: %d", id, n)
		}
	}
	if len(counts) != successes {
		t.Fatalf("effects=%d completed=%d", len(counts), successes)
	}
	t.Logf("two 1->2->1 cycles under 4 continuous callers: success=%d failure/mismatch=0 duplicates=0 longest_call=%s pending=0", successes, longest)
}

func TestKafkaHAServerCrash(t *testing.T) {
	for _, stage := range []string{"before", "after"} {
		t.Run(stage, func(t *testing.T) {
			_, opts := newKafkaEnv(t, 1)
			dir := t.TempDir()
			child := startHAProcess(t, opts.Prefix, dir, "server", stage)
			c := kafkaClient(t, newKafkaTransport(t, opts))
			api := demo.NewOrdersAsyncClient(c)
			ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
			defer cancel()
			f, err := api.GetAsync(ctx, "crash", 0)
			if err != nil {
				t.Fatal(err)
			}
			marker := "entered"
			if stage == "after" {
				marker = "effect"
			}
			path := filepath.Join(dir, fmt.Sprintf("%s-%d-crash", marker, child.cmd.Process.Pid))
			waitFor(t, 5*time.Second, func() bool { _, err := os.Stat(path); return err == nil }, "crash point not reached")
			start := time.Now()
			child.stop(t, true)
			replacement := startHAProcess(t, opts.Prefix, dir, "server", "")
			v, err := f.Await(ctx)
			if err != nil || v == nil || v.ID != "crash" || v.Values[3] != int64(replacement.cmd.Process.Pid) {
				t.Fatalf("recovery=%v %v", v, err)
			}
			effects, err := filepath.Glob(filepath.Join(dir, "effect-*-crash"))
			if err != nil {
				t.Fatal(err)
			}
			want := 1
			if stage == "after" {
				want = 2
			}
			if len(effects) != want || c.Pending() != 0 {
				t.Fatalf("effects=%d want=%d pending=%d", len(effects), want, c.Pending())
			}
			t.Logf("SIGKILL stage=%s recovery=%s effect_count=%d pending=0", stage, time.Since(start), len(effects))
		})
	}
}

func TestKafkaHAClientCrash(t *testing.T) {
	_, opts := newKafkaEnv(t, 1)
	started, release := make(chan struct{}), make(chan struct{})
	h := &heldOrders{Orders: &handlers.Orders{}, started: started, release: release}
	kafkaServer(t, newKafkaTransport(t, opts), h)
	child := startHAProcess(t, opts.Prefix, t.TempDir(), "client", "")
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("orphan handler not entered")
	}
	child.stop(t, true)
	tr := newKafkaTransport(t, opts)
	c := kafkaClient(t, tr)
	api := demo.NewOrdersAsyncClient(c)
	close(release)
	v, err := api.Get(context.Background(), "new-client", 0)
	if err != nil || v == nil || v.ID != "new-client" {
		t.Fatalf("new call=%v %v", v, err)
	}
	waitFor(t, 5*time.Second, func() bool { return tr.Stats().ReplyRecords >= 2 }, "old reply not observed")
	if tr.Stats().ReplyMatched != 1 || c.Pending() != 0 {
		t.Fatalf("old reply matched new session: %+v pending=%d", tr.Stats(), c.Pending())
	}
	t.Logf("SIGKILL client with active handler: observed=%d matched=%d pending=0", tr.Stats().ReplyRecords, tr.Stats().ReplyMatched)
}

func TestKafkaHABoundedPressureRecovery(t *testing.T) {
	_, opts := newKafkaEnv(t, 3)
	trace := &kafkaTrace{start: time.Now()}
	opts.Hooks = append(opts.Hooks, trace)
	defer func() {
		if t.Failed() {
			for _, event := range trace.snapshot() {
				t.Log(event)
			}
		}
	}()

	runHABoundedPressureRecovery(t, func() courier.Transport { return newKafkaTransport(t, opts) })
}
func TestNATSHABoundedPressureRecovery(t *testing.T) {
	prefix := fmt.Sprintf("courier.natsha.%d", time.Now().UnixNano())
	runHABoundedPressureRecovery(t, func() courier.Transport { return natsFactory(natsURL(t), prefix)(t) })
}
func runHABoundedPressureRecovery(t *testing.T, newTransport func() courier.Transport) {
	serverTransport := newTransport()
	recorder := &handlers.Recorder{}
	kafkaServer(t, serverTransport, &handlers.Orders{Recorder: recorder})
	tr := newTransport()
	api, c, err := demo.DialOrders(context.Background(), tr, courier.ClientOptions{MaxInFlight: 8, DefaultTimeout: 5 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	defer closeTestResource(t, c)
	for round := 0; round < 3; round++ {
		var futures []*courier.Future[*demo.Item]
		var ids []string
		rejected := 0
		for i := 0; i < 256; i++ {
			f, err := api.GetAsync(context.Background(), fmt.Sprintf("pressure-%d-%d", round, i), 300)
			if errors.Is(err, courier.ErrOverloaded) {
				rejected++
				continue
			}
			if err != nil {
				t.Fatal(err)
			}
			futures = append(futures, f)
			ids = append(ids, fmt.Sprintf("pressure-%d-%d", round, i))
			if c.Pending() > 8 {
				t.Fatal("admission limit exceeded")
			}
		}
		for i, f := range futures {
			v, err := f.Await(context.Background())
			if err != nil || v == nil || v.ID != ids[i] {
				if kt, ok := tr.(*transport.Transport); ok {
					t.Logf("client transport=%+v", kt.Stats())
				}
				if kt, ok := serverTransport.(*transport.Transport); ok {
					t.Logf("server transport=%+v", kt.Stats())
				}
				t.Fatalf("accepted id=%s call_id=%s call=%v error=%v handler_count=%d pending=%d", ids[i], f.ID(), v, err, recorder.Count("Orders.Get:"+ids[i]), c.Pending())
			}
		}
		if rejected == 0 || c.Pending() != 0 {
			t.Fatalf("rejected=%d pending=%d", rejected, c.Pending())
		}
		v, err := api.Get(context.Background(), "recovered", 0)
		if err != nil || v == nil || v.ID != "recovered" {
			t.Fatalf("recovery=%v %v", v, err)
		}
		var mem runtime.MemStats
		runtime.ReadMemStats(&mem)
		t.Logf("round=%d accepted=%d rejected=%d pending=0 heap=%d goroutines=%d recovery=ok", round, len(futures), rejected, mem.HeapAlloc, runtime.NumGoroutine())
	}
}

func TestNATSHAServerCrash(t *testing.T) {
	for _, stage := range []string{"before", "after"} {
		t.Run(stage, func(t *testing.T) {
			prefix := fmt.Sprintf("courier.natsha.%d", time.Now().UnixNano())
			dir := t.TempDir()
			child := startHAProcess(t, prefix, dir, "server", stage)
			c := kafkaClient(t, natsFactory(natsURL(t), prefix)(t))
			api := demo.NewOrdersAsyncClient(c)
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			f, err := api.GetAsync(ctx, "crash", 0)
			if err != nil {
				t.Fatal(err)
			}
			marker := "entered"
			if stage == "after" {
				marker = "effect"
			}
			path := filepath.Join(dir, fmt.Sprintf("%s-%d-crash", marker, child.cmd.Process.Pid))
			waitFor(t, time.Second, func() bool { _, err := os.Stat(path); return err == nil }, "handler not entered")
			start := time.Now()
			child.stop(t, true)
			replacement := startHAProcess(t, prefix, dir, "server", "")
			v, err := api.Get(context.Background(), "new-call", 0)
			if err != nil || v == nil || v.ID != "new-call" || v.Values[3] != int64(replacement.cmd.Process.Pid) {
				t.Fatalf("new call=%v %v", v, err)
			}
			recovery := time.Since(start)
			if _, err = f.Await(context.Background()); !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("old call must time out, got %v", err)
			}
			effects, err := filepath.Glob(filepath.Join(dir, "effect-*-crash"))
			if err != nil {
				t.Fatal(err)
			}
			want := 0
			if stage == "after" {
				want = 1
			}
			if len(effects) != want || c.Pending() != 0 {
				t.Fatalf("effects=%d pending=%d", len(effects), c.Pending())
			}
			t.Logf("SIGKILL stage=%s new_call_recovery=%s old_call=deadline old_effect_count=%d pending=0", stage, recovery, len(effects))
		})
	}
}

func TestNATSHAClientCrash(t *testing.T) {
	prefix := fmt.Sprintf("courier.natsha.%d", time.Now().UnixNano())
	started, release := make(chan struct{}), make(chan struct{})
	h := &heldOrders{Orders: &handlers.Orders{}, started: started, release: release}
	kafkaServer(t, natsFactory(natsURL(t), prefix)(t), h)
	child := startHAProcess(t, prefix, t.TempDir(), "client", "")
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("handler not entered")
	}
	child.stop(t, true)
	c := kafkaClient(t, natsFactory(natsURL(t), prefix)(t))
	api := demo.NewOrdersAsyncClient(c)
	close(release)
	for i := 0; i < 10; i++ {
		id := fmt.Sprint("new-client-", i)
		v, err := api.Get(context.Background(), id, 0)
		if err != nil || v == nil || v.ID != id {
			t.Fatalf("mismatch: %v %v", v, err)
		}
	}
	if c.Pending() != 0 {
		t.Fatal("pending leak")
	}
	t.Log("SIGKILL client: 10 new calls matched after releasing orphan handler; pending=0")
}
