package runtime

import (
	"context"
	"fmt"
	"sync"
)

// BoundedInvoker enforces limits shared by all runtime backends.
type BoundedInvoker struct {
	inner      Invoker
	limits     Limits
	mu         sync.Mutex
	semaphores map[string]chan struct{}
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
	return &BoundedInvoker{inner: inner, limits: limits, semaphores: map[string]chan struct{}{}}
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
		return Result{}, fmt.Errorf("function %s input exceeds %d bytes", req.FunctionName, b.limits.MaxInputBytes)
	}
	s := b.semaphore(req.FunctionName)
	select {
	case s <- struct{}{}:
		defer func() { <-s }()
	default:
		return Result{}, fmt.Errorf("function %s is busy", req.FunctionName)
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
		return Result{}, fmt.Errorf("function %s timed out: %w", req.FunctionName, callCtx.Err())
	case out := <-ch:
		if out.e != nil {
			return out.r, out.e
		}
		if len(out.r.Output) > b.limits.MaxOutputBytes {
			return Result{}, fmt.Errorf("function %s output exceeds %d bytes", req.FunctionName, b.limits.MaxOutputBytes)
		}
		return out.r, nil
	}
}
