package runtime

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// BoundedInvoker enforces limits shared by all runtime backends.
type BoundedInvoker struct {
	inner      Invoker
	limits     Limits
	mu         sync.Mutex
	semaphores map[string]chan struct{}
	rate       *WindowLimiter
}

func NewBoundedInvoker(inner Invoker, limits Limits) *BoundedInvoker {
	if limits.Timeout <= 0 || limits.MaxInputBytes <= 0 || limits.MaxOutputBytes <= 0 || limits.MaxConcurrentPerFunction <= 0 {
		defaults := DefaultLimits()
		if limits.Timeout <= 0 {
			limits.Timeout = defaults.Timeout
		}
		if limits.MaxInputBytes <= 0 {
			limits.MaxInputBytes = defaults.MaxInputBytes
		}
		if limits.MaxOutputBytes <= 0 {
			limits.MaxOutputBytes = defaults.MaxOutputBytes
		}
		if limits.MaxConcurrentPerFunction <= 0 {
			limits.MaxConcurrentPerFunction = defaults.MaxConcurrentPerFunction
		}
		if limits.MaxMemoryBytes == 0 {
			limits.MaxMemoryBytes = defaults.MaxMemoryBytes
		}
	}
	return &BoundedInvoker{inner: inner, limits: limits, semaphores: map[string]chan struct{}{}, rate: NewWindowLimiter(limits.MaxRequestsPerWindow, limits.RateWindow)}
}
func (b *BoundedInvoker) semaphore(name string) chan struct{} {
	b.mu.Lock()
	defer b.mu.Unlock()
	s := b.semaphores[name]
	if s == nil {
		s = make(chan struct{}, b.limits.MaxConcurrentPerFunction)
		b.semaphores[name] = s
	}
	return s
}
func (b *BoundedInvoker) Invoke(ctx context.Context, req Request) (Result, error) {
	if len(req.Args) > b.limits.MaxInputBytes {
		return Result{}, LimitError{Function: req.FunctionName, Kind: LimitInput, Limit: b.limits.MaxInputBytes}
	}
	if retry, ok := b.rate.Check(req.FunctionName); !ok {
		return Result{}, LimitError{Function: req.FunctionName, Kind: LimitRate, RetryAfter: retry}
	}
	s := b.semaphore(req.FunctionName)
	select {
	case s <- struct{}{}:
		defer func() { <-s }()
	default:
		return Result{}, LimitError{Function: req.FunctionName, Kind: LimitBusy, RetryAfter: 100 * time.Millisecond}
	}
	callCtx, cancel := context.WithTimeout(ctx, b.limits.Timeout)
	defer cancel()
	type response struct {
		r Result
		e error
	}
	ch := make(chan response, 1)
	go func() { r, e := b.inner.Invoke(callCtx, req); ch <- response{r, e} }()
	select {
	case <-callCtx.Done():
		return Result{}, fmt.Errorf("%w: %v", LimitError{Function: req.FunctionName, Kind: LimitTimeout}, callCtx.Err())
	case out := <-ch:
		if out.e != nil {
			return out.r, out.e
		}
		if len(out.r.Output) > b.limits.MaxOutputBytes {
			return Result{}, LimitError{Function: req.FunctionName, Kind: LimitOutput, Limit: b.limits.MaxOutputBytes}
		}
		return out.r, nil
	}
}
