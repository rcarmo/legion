package core

import (
	"crypto/sha256"
	"encoding/json"
)

func HashEnvelope(e TurnEnvelope) [32]byte {
	content := struct {
		Seq       SeqNum    `json:"seq"`
		PrevHash  [32]byte  `json:"prev_hash"`
		Event     TurnEvent `json:"event"`
		CreatedAt int64     `json:"created_at"`
	}{e.Seq, e.PrevHash, e.Event, e.CreatedAt}
	b, _ := json.Marshal(content)
	return sha256.Sum256(b)
}
func VerifyChain(log []TurnEnvelope, runID RunID) error {
	for i, e := range log {
		var want [32]byte
		if i > 0 {
			want = HashEnvelope(log[i-1])
		}
		if e.PrevHash != want {
			return ChainError{runID, e.Seq}
		}
	}
	return nil
}
