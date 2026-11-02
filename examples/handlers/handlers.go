// Package handlers contains business handlers shared by all transports.
package handlers

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/nextpkg/courier/examples/gen/demo"
)

type Recorder struct {
	mu      sync.Mutex
	counts  map[string]int
	Started chan string
}

func (r *Recorder) Record(key string) {
	if r == nil {
		return
	}
	r.mu.Lock()
	if r.counts == nil {
		r.counts = map[string]int{}
	}
	r.counts[key]++
	r.mu.Unlock()
	if r.Started != nil {
		select {
		case r.Started <- key:
		default:
		}
	}
}
func (r *Recorder) Count(key string) int { r.mu.Lock(); defer r.mu.Unlock(); return r.counts[key] }

type Orders struct{ Recorder *Recorder }

func (h *Orders) Ping(context.Context) error { h.Recorder.Record("Orders.Ping"); return nil }

func (h *Orders) Get(ctx context.Context, id string, delayMS int32) (*demo.Item, error) {
	h.Recorder.Record("Orders.Get:" + id)
	if delayMS > 0 {
		timer := time.NewTimer(time.Duration(delayMS) * time.Millisecond)
		defer timer.Stop()
		select {
		case <-timer.C:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	switch id {
	case "reject":
		return nil, &demo.Rejected{Reason: "business rejection"}
	case "error":
		return nil, errors.New("business failure")
	case "panic":
		panic("test panic")
	case "missing":
		return nil, nil
	}
	return &demo.Item{ID: id, Source: "Orders", Values: []int64{1, 2, 3}}, nil
}
func (h *Orders) List(ctx context.Context, ids []string) ([]*demo.Item, error) {
	h.Recorder.Record("Orders.List")
	out := make([]*demo.Item, 0, len(ids))
	for _, id := range ids {
		out = append(out, &demo.Item{ID: id, Source: "Orders", Values: []int64{1, 2, 3}})
	}
	return out, nil
}

type Inventory struct{ Recorder *Recorder }

func (h *Inventory) Count(ctx context.Context, id string) (int64, error) {
	h.Recorder.Record("Inventory.Count:" + id)
	return int64(len(id)), nil
}
func (h *Inventory) Reserve(ctx context.Context, id string) (*demo.Item, error) {
	h.Recorder.Record("Inventory.Reserve:" + id)
	return &demo.Item{ID: id, Source: "Inventory", Values: []int64{4, 5}}, nil
}
