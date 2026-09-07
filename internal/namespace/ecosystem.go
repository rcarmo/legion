package namespace

import (
	"encoding/json"
	"io"
	"net/http"

	"github.com/rcarmo/legion/internal/ecosystem"
)

func ecosystemError(w http.ResponseWriter, status int, err string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]string{"error": err})
}
func (a REST) agents(w http.ResponseWriter, r *http.Request) {
	if a.Agents == nil {
		ecosystemError(w, 503, "agent registry unavailable")
		return
	}
	switch r.Method {
	case http.MethodGet:
		b, err := json.Marshal(a.Agents.Profiles())
		writeHTTP(w, b, err)
	case http.MethodDelete:
		if !a.Agents.Unregister(r.PathValue("name")) {
			ecosystemError(w, 404, "agent not found")
			return
		}
		w.WriteHeader(http.StatusNoContent)
	case http.MethodPost:
		body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, 1<<20))
		if err != nil {
			ecosystemError(w, 400, err.Error())
			return
		}
		if r.PathValue("name") != "" {
			b, err := a.Agents.Dispatch(r.Context(), "agent."+r.PathValue("name"), body)
			if err != nil {
				ecosystemError(w, 422, err.Error())
				return
			}
			writeHTTP(w, b, nil)
			return
		}
		var profile ecosystem.AgentProfile
		if err = json.Unmarshal(body, &profile); err != nil {
			ecosystemError(w, 400, err.Error())
			return
		}
		if err = a.Agents.Register(profile); err != nil {
			ecosystemError(w, 400, err.Error())
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(map[string]bool{"registered": true})
	}
}
func (a REST) workflow(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, 1<<20))
	if err != nil {
		ecosystemError(w, 400, err.Error())
		return
	}
	var workflow ecosystem.Workflow
	if err = json.Unmarshal(body, &workflow); err != nil {
		ecosystemError(w, 400, err.Error())
		return
	}
	tools := a.Tools
	if tools == nil && a.Agents != nil {
		tools = a.Agents
	}
	if tools == nil {
		ecosystemError(w, 503, "workflow tools unavailable")
		return
	}
	result, err := (ecosystem.WorkflowRunner{Tools: tools}).Run(r.Context(), workflow)
	if err != nil {
		ecosystemError(w, 422, err.Error())
		return
	}
	b, err := json.Marshal(result)
	writeHTTP(w, b, err)
}
