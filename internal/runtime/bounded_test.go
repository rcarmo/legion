package runtime

import (
	"context"
	"encoding/json"
	"testing"
	"time"
)

type fakeInvoker struct {
	delay  time.Duration
	output json.RawMessage
}

func (f fakeInvoker) Invoke(ctx context.Context, r Request) (Result, error) {
	select {
	case <-ctx.Done():
		return Result{}, ctx.Err()
	case <-time.After(f.delay):
		return Result{CallID: r.CallID, Output: f.output}, nil
	}
}
func TestBoundedInvokerLimits(t *testing.T) {
	l := DefaultLimits()
	l.MaxInputBytes = 2
	l.MaxOutputBytes = 2
	b := NewBoundedInvoker(fakeInvoker{output: json.RawMessage(`null`)}, l)
	if _, e := b.Invoke(context.Background(), Request{FunctionName: "f", Args: json.RawMessage(`{}`)}); e == nil {
		t.Fatal("expected output limit")
	}
	if _, e := b.Invoke(context.Background(), Request{FunctionName: "f", Args: json.RawMessage(`{"x":1}`)}); e == nil {
		t.Fatal("expected input limit")
	}
}
func TestBoundedInvokerTimeoutAndConcurrency(t *testing.T) {
	l := DefaultLimits()
	l.Timeout = 20 * time.Millisecond
	l.MaxConcurrentPerFunction = 1
	b := NewBoundedInvoker(fakeInvoker{delay: 100 * time.Millisecond, output: json.RawMessage(`null`)}, l)
	done := make(chan error, 1)
	go func() {
		_, e := b.Invoke(context.Background(), Request{FunctionName: "f", Args: json.RawMessage(`{}`)})
		done <- e
	}()
	time.Sleep(5 * time.Millisecond)
	if _, e := b.Invoke(context.Background(), Request{FunctionName: "f", Args: json.RawMessage(`{}`)}); e == nil {
		t.Fatal("expected busy")
	}
	if e := <-done; e == nil {
		t.Fatal("expected timeout")
	}
}
