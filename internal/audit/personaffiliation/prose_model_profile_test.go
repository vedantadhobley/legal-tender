package personaffiliation

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"
)

// Explicit research controls, not a model-ID routing policy. Discovery must
// validate them; the prompt, schema, source windows and validator stay fixed.
type modelProfile struct {
	Model           string `json:"model"`
	ReasoningEffort string `json:"reasoning_effort"`
	MaxTokens       int    `json:"max_tokens"`
}

func checkModelProfile(raw []byte, p modelProfile) error {
	var inventory struct {
		Data []struct {
			ID           string
			Capabilities struct {
				Reasoning struct{ Supported []string }   `json:"reasoning_effort"`
				Format    struct{ Supported []string }   `json:"response_format"`
				Tokens    struct{ Minimum, Maximum int } `json:"max_tokens"`
			}
		}
	}
	if err := json.Unmarshal(raw, &inventory); err != nil {
		return err
	}
	for _, v := range inventory.Data { // Only data[] is ready, not configured_models.
		c := v.Capabilities
		if v.ID == p.Model && p.Model != "" && p.MaxTokens > 0 &&
			slices.Contains(c.Reasoning.Supported, p.ReasoningEffort) &&
			slices.Contains(c.Format.Supported, "json_schema") &&
			p.MaxTokens >= c.Tokens.Minimum && p.MaxTokens <= c.Tokens.Maximum {
			return nil
		}
	}
	return fmt.Errorf("requested model is not ready with the required controls")
}

func TestProseModelProfiles(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join(fixtureDir, "prose-model-v1/models.json"))
	if err != nil {
		t.Fatal(err)
	}
	for _, p := range []modelProfile{{"gemma-4-12b", "none", 2048}, {"gpt-oss-120b", "medium", 4096}} {
		if err := checkModelProfile(raw, p); err != nil {
			t.Fatal(p, err)
		}
	}
	for _, p := range []modelProfile{{"missing", "none", 2048}, {"gpt-oss-120b", "none", 4096}, {"gemma-4-12b", "none", 0}, {"gpt-oss-120b", "medium", 12289}} {
		if err := checkModelProfile(raw, p); err == nil {
			t.Fatal("unsupported profile passed", p)
		}
	}
	if err := checkModelProfile(bytes.Replace(raw, []byte(`"data"`), []byte(`"unavailable"`), 1), modelProfile{"gemma-4-12b", "none", 2048}); err == nil {
		t.Fatal("configured-only model treated as ready")
	}
	// Only explicitly declared model controls may differ across the experiment.
	for _, c := range proseModelCases(t) {
		var first, second map[string]json.RawMessage
		json.Unmarshal(proseModelRequest(c, modelProfile{"gemma-4-12b", "none", 2048}), &first)
		json.Unmarshal(proseModelRequest(c, modelProfile{"gpt-oss-120b", "medium", 4096}), &second)
		for _, key := range []string{"model", "reasoning_effort", "max_tokens"} {
			delete(first, key)
			delete(second, key)
		}
		a, _ := json.Marshal(first)
		b, _ := json.Marshal(second)
		if !bytes.Equal(a, b) {
			t.Fatal("comparison changed the task", c.ID)
		}
	}
}

func TestProseModelRequestTimeout(t *testing.T) {
	for raw, want := range map[string]time.Duration{"": 90 * time.Second, "10": 10 * time.Second, "900": 15 * time.Minute, "1800": 30 * time.Minute} {
		got, err := proseModelRequestTimeout(raw)
		if err != nil || got != want {
			t.Fatal(raw, got, err)
		}
	}
	for _, raw := range []string{"bad", "0", "9", "1801"} {
		if got, err := proseModelRequestTimeout(raw); err == nil || got != 0 {
			t.Fatal("invalid timeout passed", raw)
		}
	}
}
