package agent

import (
	"testing"

	goai "github.com/rcarmo/go-ai"
)

func TestBuiltinInferenceRegistration(t *testing.T) {
	model := goai.GetModel(goai.ProviderOpenCode, "nemotron-3.5-lightning-free")
	if model == nil {
		t.Fatal("built-in model catalogue was not registered")
	}
	for _, api := range []goai.Api{goai.ApiOpenAICompletions, goai.ApiOpenAIResponses, goai.ApiAnthropicMessages, goai.ApiGoogleGenerativeAI} {
		if goai.GetApiProvider(api) == nil {
			t.Fatalf("wire protocol %q is unavailable", api)
		}
	}
}
