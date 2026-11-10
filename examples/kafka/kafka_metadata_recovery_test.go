//go:build integration

package kafka_test

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/nextpkg/courier/examples/internal/kafkatest"
	"github.com/nextpkg/courier/examples/internal/testutil"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nextpkg/courier/examples/gen/demo"
	"github.com/nextpkg/courier/examples/handlers"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// leaderErrorProxy forwards real Kafka traffic but replaces one successful
// Produce acknowledgement with NOT_LEADER_FOR_PARTITION. The broker retains
// the original record, so successful retry must also preserve idempotence.
type leaderErrorProxy struct {
	injected atomic.Bool
	errors   chan error
}

func readKafkaFrame(r io.Reader) ([]byte, error) {
	var header [4]byte
	if _, err := io.ReadFull(r, header[:]); err != nil {
		return nil, err
	}
	n := int(binary.BigEndian.Uint32(header[:]))
	if n < 4 || n > 32<<20 {
		return nil, fmt.Errorf("invalid Kafka test frame size %d", n)
	}
	frame := make([]byte, n+4)
	copy(frame, header[:])
	_, err := io.ReadFull(r, frame[4:])
	return frame, err
}
func writeKafkaFrame(w io.Writer, frame []byte) error {
	for len(frame) > 0 {
		n, err := w.Write(frame)
		if err != nil {
			return err
		}
		if n == 0 {
			return io.ErrShortWrite
		}
		frame = frame[n:]
	}
	return nil
}
func (p *leaderErrorProxy) Dial(ctx context.Context, network, address string) (net.Conn, error) {
	backend, err := (&net.Dialer{}).DialContext(ctx, network, address)
	if err != nil {
		return nil, err
	}
	client, proxy := net.Pipe()
	var mu sync.Mutex
	versions := map[uint32]int16{}
	go func() {
		defer func() { _ = backend.Close() }()
		defer func() { _ = proxy.Close() }()
		for {
			frame, err := readKafkaFrame(proxy)
			if err != nil {
				return
			}
			if len(frame) < 12 {
				return
			}
			if binary.BigEndian.Uint16(frame[4:6]) == 0 {
				mu.Lock()
				versions[binary.BigEndian.Uint32(frame[8:12])] = int16(binary.BigEndian.Uint16(frame[6:8]))
				mu.Unlock()
			}
			if err := writeKafkaFrame(backend, frame); err != nil {
				return
			}
		}
	}()
	go func() {
		defer func() { _ = backend.Close() }()
		defer func() { _ = proxy.Close() }()
		for {
			frame, err := readKafkaFrame(backend)
			if err != nil {
				return
			}
			correlation := binary.BigEndian.Uint32(frame[4:8])
			mu.Lock()
			version, produce := versions[correlation]
			delete(versions, correlation)
			mu.Unlock()
			if produce && !p.injected.Load() {
				start := 8
				if version >= 9 {
					if len(frame) <= 8 || frame[8] != 0 {
						p.errors <- fmt.Errorf("unexpected response header tags")
						return
					}
					start++
				}
				response := kmsg.NewPtrProduceResponse()
				response.SetVersion(version)
				if err := response.ReadFrom(frame[start:]); err != nil {
					p.errors <- err
					return
				}
				if len(response.Topics) > 0 && len(response.Topics[0].Partitions) > 0 && response.Topics[0].Partitions[0].ErrorCode == 0 && p.injected.CompareAndSwap(false, true) {
					response.Topics[0].Partitions[0].ErrorCode = kerr.NotLeaderForPartition.Code
					frame = append(frame[:start], response.AppendTo(nil)...)
					binary.BigEndian.PutUint32(frame[:4], uint32(len(frame)-4))
				}
			}
			if err := writeKafkaFrame(proxy, frame); err != nil {
				return
			}
		}
	}()
	return client, nil
}

func TestKafkaLeaderMetadataRecovery(t *testing.T) {
	for _, age := range []time.Duration{5 * time.Second, 0} {
		t.Run(fmt.Sprintf("metadata-min-age=%s", age), func(t *testing.T) {
			_, opts := kafkatest.NewEnv(t, 1)
			proxy := &leaderErrorProxy{errors: make(chan error, 4)}
			clientOpts := opts
			clientOpts.Dialer = proxy.Dial
			clientOpts.MetadataMinAge = age
			c := testutil.Client(t, kafkatest.NewTransport(t, clientOpts))
			api := demo.NewOrdersAsyncClient(c)
			f, err := api.GetAsync(context.Background(), "leader-retry", 0)
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			start := time.Now()
			err = f.Sent(ctx)
			if !proxy.injected.Load() {
				t.Fatal("leader error was not injected")
			}
			select {
			case err := <-proxy.errors:
				t.Fatal(err)
			default:
			}
			if age == 5*time.Second {
				if !errors.Is(err, context.DeadlineExceeded) {
					t.Fatalf("5s refresh throttle should exceed 2s probe, got %v", err)
				}
				t.Log("previous 5s throttle reproduced acknowledgement timeout")
				return
			}
			if err != nil {
				t.Fatalf("default metadata refresh did not recover: %v", err)
			}
			t.Logf("leader error recovered and acknowledged in %s", time.Since(start))
			recorder := &handlers.Recorder{}
			testutil.Server(t, kafkatest.NewTransport(t, opts), &handlers.Orders{Recorder: recorder})
			v, err := f.Await(context.Background())
			if err != nil || v == nil || v.ID != "leader-retry" {
				t.Fatalf("result=%v %v", v, err)
			}
			// A later request on the same partition proves all earlier records were read.
			if _, err := api.Get(context.Background(), "barrier", 0); err != nil {
				t.Fatal(err)
			}
			if recorder.Count("Orders.Get:leader-retry") != 1 || c.Pending() != 0 {
				t.Fatalf("retry duplicated execution or leaked pending: count=%d pending=%d", recorder.Count("Orders.Get:leader-retry"), c.Pending())
			}
		})
	}
}
