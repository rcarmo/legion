package core

import (
	"errors"
	"fmt"
)

var (
	ErrSessionNotFound       = errors.New("session not found")
	ErrSessionExists         = errors.New("session already exists")
	ErrTamperEvident         = errors.New("tamper-evident chain broken")
	ErrPendingReconciliation = errors.New("pending reconciliation")
	ErrToolNotFound          = errors.New("tool not found")
)

type BudgetExceededError struct{ Field string }

func (e BudgetExceededError) Error() string { return "budget exceeded: " + e.Field }

type ChainError struct {
	RunID RunID
	Seq   SeqNum
}

func (e ChainError) Error() string {
	return fmt.Sprintf("%v at run=%s seq=%d", ErrTamperEvident, e.RunID, e.Seq)
}
func (e ChainError) Unwrap() error { return ErrTamperEvident }
