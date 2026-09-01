package core

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
)

// canonicalJSONValue converts structs and RawMessage payloads into recursive
// maps while retaining JSON numbers verbatim. encoding/json sorts map keys,
// matching serde_json::Value's default lexical object ordering in Rust.
func canonicalJSONValue(value any) any {
	encoded, _ := json.Marshal(value)
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.UseNumber()
	var canonical any
	_ = decoder.Decode(&canonical)
	return canonical
}

func HashEnvelope(e TurnEnvelope) [32]byte {
	// run_id is deliberately omitted so copied history remains valid after fork.
	content := map[string]any{
		"seq":        e.Seq,
		"prev_hash":  e.PrevHash,
		"event":      canonicalJSONValue(e.Event),
		"created_at": e.CreatedAt,
	}
	encoded, _ := json.Marshal(content)
	return sha256.Sum256(encoded)
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
