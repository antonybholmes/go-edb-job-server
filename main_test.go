package main

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/antonybholmes/go-jobs"
)

type retryPublisher struct {
	done  chan struct{}
	calls int
}

func (p *retryPublisher) PublishOne(_ context.Context, _ jobs.MessageSender) (bool, error) {
	p.calls++
	if p.calls == 1 {
		return false, errors.New("temporary failure")
	}
	if p.calls == 2 {
		close(p.done)
	}
	return false, nil
}

func TestOutboxRetriesAndStops(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	publisher := &retryPublisher{done: make(chan struct{})}
	stopped := make(chan struct{})
	go func() {
		runOutbox(ctx, publisher, &jobs.FakeSender{}, time.Millisecond)
		close(stopped)
	}()
	select {
	case <-publisher.done:
		cancel()
	case <-time.After(time.Second):
		t.Fatal("outbox did not retry")
	}
	select {
	case <-stopped:
	case <-time.After(time.Second):
		t.Fatal("outbox did not stop after cancellation")
	}
}
