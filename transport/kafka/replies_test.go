package kafka

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestInboxCancellationWhileReaderIsStarting(t *testing.T) {
	tr := &Transport{
		opts: Options{ReplyTopic: "replies", ReplyPartitions: []int32{0}, MaxSubscriptions: 4, StartupTimeout: time.Second},
		ctx:  context.Background(), subs: make(map[*subscription]bool), inboxGate: make(chan struct{}, 1),
	}
	tr.inboxGate <- struct{}{} // Another registration owns reader startup.
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	done := make(chan error, 1)
	go func() { _, err := tr.OpenInbox(ctx, "cancelled", func([]byte) {}); done <- err }()
	select {
	case err := <-done:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("expected deadline: %v", err)
		}
	case <-time.After(time.Second):
		<-tr.inboxGate
		t.Fatal("cancelled inbox remained blocked on another startup")
	}
	if tr.Stats().Subscriptions != 0 {
		t.Fatal("cancelled registration leaked")
	}
}
