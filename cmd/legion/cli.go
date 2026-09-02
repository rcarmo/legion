package main

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
)

func runCLI(args []string) (bool, error) {
	if len(args) == 0 {
		return false, nil
	}
	base := os.Getenv("LEGION_URL")
	if base == "" {
		base = "http://127.0.0.1:8080"
	}
	var method, p string
	var body []byte
	switch args[0] {
	case "session":
		if len(args) < 2 {
			return true, fmt.Errorf("usage: legion session list|new|status|send|history")
		}
		switch args[1] {
		case "list":
			method, p = "GET", "/sessions"
		case "new":
			method, p = "POST", "/sessions"
			body = []byte(`{"model":"faux/test","budget":{},"tools":[]}`)
			if len(args) > 2 {
				body = []byte(args[2])
			}
		case "status":
			if len(args) < 3 {
				return true, fmt.Errorf("session status requires id")
			}
			method, p = "GET", "/sessions/"+args[2]
		case "history":
			if len(args) < 3 {
				return true, fmt.Errorf("session history requires id")
			}
			method, p = "GET", "/sessions/"+args[2]+"/log"
		case "reconcile":
			if len(args) != 4 || (args[3] != "skip" && args[3] != "retry") {
				return true, fmt.Errorf("usage: legion session reconcile ID skip|retry")
			}
			method, p = "POST", "/sessions/"+args[2]+"/reconcile"
			body, _ = json.Marshal(map[string]string{"action": args[3]})
		case "send":
			if len(args) < 4 {
				return true, fmt.Errorf("session send requires id and message")
			}
			method, p = "POST", "/sessions/"+args[2]+"/messages"
			body, _ = json.Marshal(map[string]string{"content": strings.Join(args[3:], " ")})
		default:
			return true, fmt.Errorf("unknown session command %q", args[1])
		}
	case "cluster":
		if len(args) < 2 {
			return true, fmt.Errorf("usage: legion cluster health|peers|leader|self")
		}
		method = "GET"
		field := args[1]
		if field == "peers" {
			p = "/namespace/cluster/peers"
		} else {
			p = "/cluster/" + field
		}
	case "deploy":
		if len(args) < 2 {
			return true, fmt.Errorf("usage: legion deploy push|register|route|promote")
		}
		method = "POST"
		switch args[1] {
		case "push":
			if len(args) == 3 {
				artifact, err := os.ReadFile(args[2])
				if err != nil {
					return true, err
				}
				body, p = artifact, "/deploy/push"
			} else if len(args) == 5 {
				artifact, err := os.ReadFile(args[4])
				if err != nil {
					return true, err
				}
				job := map[string]any{"name": args[2], "runtime": args[3]}
				if args[3] == "wasm" {
					job["wasm_b64"] = base64.StdEncoding.EncodeToString(artifact)
				} else {
					job["code"] = string(artifact)
				}
				body, _ = json.Marshal(job)
				p = "/deploy/register"
			} else {
				return true, fmt.Errorf("usage: legion deploy push PATH | NAME wasm|bun|joker PATH")
			}
		case "register":
			if len(args) != 5 {
				return true, fmt.Errorf("usage: legion deploy register NAME CID wasm|bun|joker")
			}
			body, _ = json.Marshal(map[string]any{"name": args[2], "cid": args[3], "runtime": args[4]})
			p = "/deploy/register"
		case "route":
			if len(args) != 5 {
				return true, fmt.Errorf("usage: legion deploy route NAME CID WEIGHT")
			}
			weight, err := strconv.ParseUint(args[4], 10, 16)
			if err != nil || weight > 10000 {
				return true, fmt.Errorf("weight must be 0..10000")
			}
			body, _ = json.Marshal(map[string]any{"name": args[2], "artifact_cid": args[3], "weight": weight})
			p = "/deploy/route"
		case "promote":
			if len(args) < 3 || len(args) > 4 {
				return true, fmt.Errorf("usage: legion deploy promote NAME [CID]")
			}
			request := map[string]any{"name": args[2]}
			if len(args) == 4 {
				request["artifact_cid"] = args[3]
			}
			body, _ = json.Marshal(request)
			p = "/deploy/promote"
		default:
			return true, fmt.Errorf("unknown deploy command %q", args[1])
		}
	case "call":
		if len(args) < 2 {
			return true, fmt.Errorf("call requires function name")
		}
		method, p = "POST", "/functions/"+args[1]+"/invoke"
		if len(args) > 2 {
			body = []byte(args[2])
		} else {
			body, _ = io.ReadAll(os.Stdin)
			if len(body) == 0 {
				body = []byte("{}")
			}
		}
	default:
		return false, nil
	}
	req, err := http.NewRequest(method, strings.TrimRight(base, "/")+p, bytes.NewReader(body))
	if err != nil {
		return true, err
	}
	req.Header.Set("Content-Type", "application/json")
	if key := os.Getenv("LEGION_API_KEY"); key != "" {
		req.Header.Set("Authorization", "Bearer "+key)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return true, err
	}
	defer resp.Body.Close()
	out, _ := io.ReadAll(resp.Body)
	if resp.StatusCode/100 != 2 {
		return true, fmt.Errorf("API %s: %s", resp.Status, string(out))
	}
	fmt.Println(string(out))
	return true, nil
}
