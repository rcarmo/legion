package agent

import (
	"fmt"

	"github.com/rcarmo/legion/internal/core"
)

// turnState makes model/tool event ordering explicit and rejects driver bugs
// before they can produce a log that recovery cannot interpret.
type turnState struct {
	phase core.TurnPhase
}

func newTurnState() *turnState {
	return &turnState{phase: core.PhaseSetup}
}

func (s *turnState) transition(expected, next core.TurnPhase) error {
	if s.phase != expected {
		return fmt.Errorf("invalid turn phase transition: %s -> %s (expected %s)", s.phase, next, expected)
	}
	s.phase = next
	return nil
}
