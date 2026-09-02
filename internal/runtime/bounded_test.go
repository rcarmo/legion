package runtime

import (
	"context"
	"encoding/json"
	"errors"
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

type observed struct{ calls, rejects int }

func (o *observed) Record(string, uint64, bool) { o.calls++ }
func (o *observed) Reject()                     { o.rejects++ }

func TestObservedInvokerCountsLoadShedding(t *testing.T) {
	limits := DefaultLimits()
	limits.MaxRequestsPerWindow = 1
	inner := NewBoundedInvoker(fakeInvoker{output: json.RawMessage(`{}`)}, limits)
	observer := &observed{}
	invoker := ObservedInvoker{Inner: inner, Observer: observer}
	req := Request{FunctionName: "echo", Args: json.RawMessage(`{}`)}
	if _, err := invoker.Invoke(context.Background(), req); err != nil {
		t.Fatal(err)
	}
	if _, err := invoker.Invoke(context.Background(), req); err == nil {
		t.Fatal("rate-limited invocation accepted")
	}
	if observer.calls != 2 || observer.rejects != 1 {
		t.Fatalf("calls=%d rejects=%d", observer.calls, observer.rejects)
	}
}

func TestBoundedInvokerRateLimit(t *testing.T) {
	l := DefaultLimits()
	l.MaxRequestsPerWindow = 1
	l.RateWindow = time.Minute
	b := NewBoundedInvoker(fakeInvoker{output: json.RawMessage(`{}`)}, l)
	if _, err := b.Invoke(context.Background(), Request{FunctionName: "f", Args: json.RawMessage(`{}`)}); err != nil {
		t.Fatal(err)
	}
	_, err := b.Invoke(context.Background(), Request{FunctionName: "f", Args: json.RawMessage(`{}`)})
	var limit LimitError
	if !errors.As(err, &limit) || limit.Kind != LimitRate || limit.RetryAfter <= 0 {
		t.Fatalf("error=%v", err)
	}
	if _, err = b.Invoke(context.Background(), Request{FunctionName: "other", Args: json.RawMessage(`{}`)}); err != nil {
		t.Fatal(err)
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
