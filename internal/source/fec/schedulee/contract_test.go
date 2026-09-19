package schedulee

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

func TestRecordSchemaMatchesCompiledColumns(t *testing.T) {
	t.Parallel()
	contractDirectory := filepath.Clean(filepath.Join(fixtureDirectory(t), "../.."))
	content, err := os.ReadFile(filepath.Join(contractDirectory, "record.schema.json"))
	if err != nil {
		t.Fatal(err)
	}
	var schema struct {
		Required   []string                   `json:"required"`
		Properties map[string]json.RawMessage `json:"properties"`
	}
	if err := json.Unmarshal(content, &schema); err != nil {
		t.Fatal(err)
	}
	if len(schema.Required) != FieldCount || len(schema.Properties) != FieldCount {
		t.Fatalf("record schema width = (%d required, %d properties); want %d", len(schema.Required), len(schema.Properties), FieldCount)
	}
	for index, column := range columns {
		if schema.Required[index] != column.Name {
			t.Fatalf("required field %d = %q; want %q", index, schema.Required[index], column.Name)
		}
		if _, ok := schema.Properties[column.Name]; !ok {
			t.Fatalf("record schema lacks property %q", column.Name)
		}
	}
}

func TestContractFixtureDigests(t *testing.T) {
	t.Parallel()
	contractDirectory := filepath.Clean(filepath.Join(fixtureDirectory(t), "../.."))
	content, err := os.ReadFile(filepath.Join(contractDirectory, "contract.json"))
	if err != nil {
		t.Fatal(err)
	}
	var contract struct {
		ContractID string `json:"contract_id"`
		Status     string `json:"status"`
		Fixtures   []struct {
			Path   string `json:"path"`
			SHA256 string `json:"sha256"`
		} `json:"fixtures"`
	}
	if err := json.Unmarshal(content, &contract); err != nil {
		t.Fatal(err)
	}
	if contract.ContractID != "fec/schedule-e" || contract.Status != "accepted" || len(contract.Fixtures) != 6 {
		t.Fatalf("unexpected contract header: %+v", contract)
	}
	for _, fixture := range contract.Fixtures {
		physical, err := os.ReadFile(filepath.Join(contractDirectory, fixture.Path))
		if err != nil {
			t.Fatalf("read %s: %v", fixture.Path, err)
		}
		digest := sha256.Sum256(physical)
		if actual := hex.EncodeToString(digest[:]); actual != fixture.SHA256 {
			t.Fatalf("%s digest = %s; want %s", fixture.Path, actual, fixture.SHA256)
		}
	}
}
