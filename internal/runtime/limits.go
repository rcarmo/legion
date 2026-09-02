package runtime

import (
	"fmt"
	"sync"
	"time"
)

type LimitKind string

const (
	LimitInput   LimitKind = "input"
	LimitOutput  LimitKind = "output"
	LimitBusy    LimitKind = "busy"
	LimitRate    LimitKind = "rate"
	LimitTimeout LimitKind = "timeout"
)

type LimitError struct {
	Function   string
	Kind       LimitKind
	Limit      int
	RetryAfter time.Duration
}

func (e LimitError) Error() string {
	switch e.Kind {
	case LimitInput, LimitOutput:
		return fmt.Sprintf("function %s %s exceeds %d bytes", e.Function, e.Kind, e.Limit)
	case LimitBusy:
		return fmt.Sprintf("function %s is busy", e.Function)
	case LimitRate:
		return fmt.Sprintf("function %s rate limit exceeded", e.Function)
	case LimitTimeout:
		return fmt.Sprintf("function %s timed out", e.Function)
	default:
		return fmt.Sprintf("function %s limit exceeded", e.Function)
	}
}

type fixedWindow struct {
	Started time.Time
	Count   int
}

type WindowLimiter struct {
	Max    int
	Window time.Duration
	mu     sync.Mutex
	items  map[string]fixedWindow
}

func NewWindowLimiter(max int, window time.Duration) *WindowLimiter {
	return &WindowLimiter{Max: max, Window: window, items: map[string]fixedWindow{}}
}

func (l *WindowLimiter) Check(key string) (time.Duration, bool) {
	if l == nil || l.Max <= 0 || l.Window <= 0 {
		return 0, true
	}
	now := time.Now()
	l.mu.Lock()
	defer l.mu.Unlock()
	item := l.items[key]
	if item.Started.IsZero() || now.Sub(item.Started) >= l.Window {
		item = fixedWindow{Started: now}
	}
	if item.Count >= l.Max {
		return max(l.Window-now.Sub(item.Started), time.Millisecond), false
	}
	item.Count++
	l.items[key] = item
	if len(l.items) > 1024 {
		for name, candidate := range l.items {
			if now.Sub(candidate.Started) >= l.Window {
				delete(l.items, name)
			}
		}
	}
	return 0, true
}
