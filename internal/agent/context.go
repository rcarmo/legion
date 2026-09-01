package agent

import (
	"encoding/json"

	goai "github.com/rcarmo/go-ai"
	"github.com/rcarmo/legion/internal/core"
)

func buildContext(log []core.TurnEnvelope, config core.RunConfig) *goai.Context {
	ctx := &goai.Context{}
	if config.SystemPrompt != nil {
		ctx.SystemPrompt = *config.SystemPrompt
	}
	for i, e := range log {
		switch e.Event.Kind.Kind {
		case "user_message":
			var p struct {
				Content string `json:"content"`
			}
			_ = json.Unmarshal(e.Event.Payload, &p)
			ctx.Messages = append(ctx.Messages, goai.UserMessage(p.Content))
		case "assistant_message":
			var p struct {
				Message *goai.Message `json:"message"`
				Content string        `json:"content"`
			}
			_ = json.Unmarshal(e.Event.Payload, &p)
			if p.Message != nil {
				ctx.Messages = append(ctx.Messages, *p.Message)
			} else {
				ctx.Messages = append(ctx.Messages, goai.Message{Role: goai.RoleAssistant, Content: []goai.ContentBlock{{Type: "text", Text: p.Content}}})
			}
		case "tool_result":
			name := ""
			for j := i - 1; j >= 0; j-- {
				k := log[j].Event.Kind
				if k.Kind == "tool_call_intent" && k.CallID == e.Event.Kind.CallID {
					name = k.ToolName
					break
				}
			}
			var value any
			_ = json.Unmarshal(e.Event.Payload, &value)
			b, _ := json.Marshal(value)
			goai.AppendToolResult(ctx, e.Event.Kind.CallID, name, string(b), isError(value))
		}
	}
	return ctx
}
func isError(v any) bool {
	m, ok := v.(map[string]any)
	if !ok {
		return false
	}
	_, ok = m["error"]
	return ok
}
