package api

import (
	"sync"
	"sync/atomic"
	"time"
)

type window struct {
	started time.Time
	count   int
}

type SessionLimiter struct {
	Max        int
	Window     time.Duration
	mu         sync.Mutex
	items      map[string]window
	rejections atomic.Uint64
}

func NewSessionLimiter(max int, duration time.Duration) *SessionLimiter {
	return &SessionLimiter{Max: max, Window: duration, items: map[string]window{}}
}
func (l *SessionLimiter) Check(key string) (time.Duration, bool) {
	if l == nil || l.Max <= 0 || l.Window <= 0 {
		return 0, true
	}
	now := time.Now()
	l.mu.Lock()
	defer l.mu.Unlock()
	item := l.items[key]
	if item.started.IsZero() || now.Sub(item.started) >= l.Window {
		item = window{started: now}
	}
	if item.count >= l.Max {
		l.rejections.Add(1)
		return max(l.Window-now.Sub(item.started), time.Millisecond), false
	}
	item.count++
	l.items[key] = item
	return 0, true
}
func (l *SessionLimiter) Rejections() uint64 {
	if l == nil {
		return 0
	}
	return l.rejections.Load()
}
