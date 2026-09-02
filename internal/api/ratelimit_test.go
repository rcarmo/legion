package api

import (
	"testing"
	"time"
)

func TestSessionWindowsAreIndependentAndReset(t *testing.T) {
	limiter := NewSessionLimiter(1, 20*time.Millisecond)
	if _, ok := limiter.Check("a"); !ok {
		t.Fatal("first rejected")
	}
	if _, ok := limiter.Check("a"); ok {
		t.Fatal("second accepted")
	}
	if _, ok := limiter.Check("b"); !ok {
		t.Fatal("independent session rejected")
	}
	time.Sleep(25 * time.Millisecond)
	if _, ok := limiter.Check("a"); !ok {
		t.Fatal("window did not reset")
	}
	if limiter.Rejections() != 1 {
		t.Fatalf("rejections=%d", limiter.Rejections())
	}
}
