package core

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"testing"

	"github.com/google/uuid"
)

func ptr[T any](v T) *T { return &v }
func testConfig() RunConfig {
	return RunConfig{SystemPrompt: ptr("test"), Model: "faux/test", Budget: Budget{}, Tools: []string{"echo"}}
}

func TestRustCompatibleEventJSON(t *testing.T) {
	e := ToolCallIntent("echo", "call-1", EffectRead, map[string]any{"message": "hello"})
	got, err := json.Marshal(e)
	if err != nil {
		t.Fatal(err)
	}
	want := `{"kind":{"kind":"tool_call_intent","tool_name":"echo","call_id":"call-1","effect":"read"},"payload":{"arguments":{"message":"hello"}},"payload_cid":null,"model":null,"tokens_in":null,"tokens_out":null,"wall_ms":null}`
	if string(got) != want {
		t.Fatalf("wire mismatch\ngot  %s\nwant %s", got, want)
	}
	var round TurnEvent
	if err := json.Unmarshal(got, &round); err != nil {
		t.Fatal(err)
	}
	if round.Kind.CallID != "call-1" {
		t.Fatalf("roundtrip: %#v", round)
	}
}
func TestHashEnvelopeMatchesRustGoldenVector(t *testing.T) {
	envelope := TurnEnvelope{
		RunID: uuid.Nil, Seq: 7, Event: NewUserMessage("hello"), CreatedAt: 1_700_000_000_000,
	}
	hash := HashEnvelope(envelope)
	got := hex.EncodeToString(hash[:])
	const want = "4b44647ff56102406c2e2af6878b8e13933c6b4c55f6e698dec8e7429b5480f1"
	if got != want {
		t.Fatalf("Rust hash mismatch: got %s want %s", got, want)
	}
}

func TestCanonicalHashRetainsLargePayloadIntegers(t *testing.T) {
	event := NewUserMessage("hello")
	event.Payload = json.RawMessage(`{"value":9007199254740993}`)
	envelope := TurnEnvelope{Seq: 1, Event: event, CreatedAt: 1}
	first := HashEnvelope(envelope)
	event.Payload = json.RawMessage(`{"value":9007199254740992}`)
	envelope.Event = event
	second := HashEnvelope(envelope)
	if first == second {
		t.Fatal("canonical hash rounded distinct large integers")
	}
}

func TestBudgetOrderAndTerminalStatus(t *testing.T) {
	s := BudgetState{Turns: 1, ToolCalls: 1}
	if got := s.ExceededBy(Budget{MaxTurns: ptr(uint32(1)), MaxToolCalls: ptr(uint32(1))}); got != "max_turns" {
		t.Fatal(got)
	}
	if !StatusCompleted.IsTerminal() {
		t.Fatal("completed not terminal")
	}
}
func TestMemoryStoreAppendForkAndChain(t *testing.T) {
	ctx := context.Background()
	s := NewMemoryEventStore()
	id := uuid.New()
	if err := s.CreateSession(ctx, id, testConfig()); err != nil {
		t.Fatal(err)
	}
	for _, v := range []string{"a", "b", "c"} {
		if _, err := s.Append(ctx, id, NewUserMessage(v)); err != nil {
			t.Fatal(err)
		}
	}
	log, err := s.ReadLog(ctx, id)
	if err != nil || len(log) != 3 {
		t.Fatalf("log=%d err=%v", len(log), err)
	}
	fork, err := s.Fork(ctx, id, 1)
	if err != nil {
		t.Fatal(err)
	}
	flog, err := s.ReadLog(ctx, fork)
	if err != nil || len(flog) != 2 {
		t.Fatalf("fork=%d err=%v", len(flog), err)
	}
	bad := append([]TurnEnvelope(nil), log...)
	bad[1].PrevHash = [32]byte{}
	if !errors.Is(VerifyChain(bad, id), ErrTamperEvident) {
		t.Fatal("tampering not detected")
	}
}
func TestEchoToolRegistry(t *testing.T) {
	r := EchoToolRegistry{}
	got, err := r.Dispatch(context.Background(), "echo", json.RawMessage(`{"ok":true}`))
	if err != nil || string(got) != `{"ok":true}` {
		t.Fatalf("%s %v", got, err)
	}
	if _, err = r.Dispatch(context.Background(), "missing", nil); !errors.Is(err, ErrToolNotFound) {
		t.Fatal(err)
	}
}
