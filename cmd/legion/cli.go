package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
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
