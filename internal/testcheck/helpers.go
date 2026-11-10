// Package testcheck provides shared test cleanup and synchronous polling.
package testcheck

import (
	"context"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"runtime"
	"testing"
	"time"
)

func AssertGoroutinesReturn(t *testing.T, baseline int) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for {
		n := runtime.NumGoroutine()
		if n <= baseline+4 {
			return
		}
		if time.Now().After(deadline) {
			stack := make([]byte, 256<<10)
			nbytes := runtime.Stack(stack, true)
			require.FailNowf(t, "goroutines did not return", "baseline=%d now=%d\n%s", baseline, n, stack[:nbytes])
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// Close uses a fresh deadline even when a request context has expired.
// Cleanup failures are nonfatal so later resources are still closed.
func Close(t *testing.T, resource interface{ Close(context.Context) error }) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := resource.Close(ctx); err != nil {
		assert.NoError(t, err, "resource cleanup")
	}
}

// WaitFor evaluates the condition on the calling test goroutine.
func WaitFor(t *testing.T, d time.Duration, condition func() bool, message string) {
	t.Helper()
	Poll(t, d, 10*time.Millisecond, condition, message)
}

// Poll keeps conditions on the caller goroutine; no assertion runs in a hidden worker.
func Poll(t *testing.T, d, interval time.Duration, condition func() bool, message string) {
	t.Helper()
	end := time.Now().Add(d)
	for {
		if condition() {
			return
		}
		if time.Now().After(end) {
			require.FailNow(t, message)
		}
		time.Sleep(interval)
	}
}
