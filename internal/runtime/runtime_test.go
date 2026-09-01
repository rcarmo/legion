package runtime

import (
	"bytes"
	"encoding/json"
	"testing"
)

func TestRuntimeManifestValues(t *testing.T) {
	for _, runtime := range []Kind{WASM, Bun, Joker} {
		manifest := Manifest{Name: "hello", Runtime: runtime, Version: "1.0.0", Parameters: json.RawMessage(`{"type":"object"}`)}
		encoded, err := json.Marshal(manifest)
		if err != nil {
			t.Fatal(err)
		}
		wireRuntime := runtime
		if runtime == Joker {
			wireRuntime = Bun
			if !bytes.Contains(encoded, []byte(`"executor":"joker"`)) {
				t.Fatalf("manifest=%s", encoded)
			}
		}
		if !bytes.Contains(encoded, []byte(`"runtime":"`+wireRuntime+`"`)) {
			t.Fatalf("manifest=%s", encoded)
		}
		var decoded Manifest
		if err = json.Unmarshal(encoded, &decoded); err != nil {
			t.Fatal(err)
		}
		if decoded.Runtime != runtime {
			t.Fatalf("got %q want %q", decoded.Runtime, runtime)
		}
	}
}
