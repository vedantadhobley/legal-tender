package scheduleb

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"runtime"
	"testing"
)

func TestCompiledSchemaMatchesArchiveObservation(t *testing.T) {
	content, err := os.ReadFile(filepath.Join(contractDirectory(t), "fixtures/archive/dump-2026-08-30.json"))
	if err != nil {
		t.Fatal(err)
	}
	var observation struct {
		Relation struct {
			FieldCount int `json:"field_count"`
			Fields     []struct {
				Ordinal      int    `json:"ordinal"`
				Name         string `json:"name"`
				PostgresType string `json:"postgres_type"`
				Nullable     bool   `json:"nullable"`
			} `json:"fields"`
		} `json:"relation"`
	}
	if err := json.Unmarshal(content, &observation); err != nil {
		t.Fatal(err)
	}
	if observation.Relation.FieldCount != FieldCount || len(observation.Relation.Fields) != FieldCount {
		t.Fatalf("archive width = (%d, %d listed); want %d", observation.Relation.FieldCount, len(observation.Relation.Fields), FieldCount)
	}
	for index, field := range observation.Relation.Fields {
		column := columns[index]
		if field.Ordinal != index+1 || field.Name != column.Name || field.Nullable != column.Nullable {
			t.Fatalf("archive field %d = (%d, %q, nullable=%t); compiled = (%d, %q, nullable=%t)", index, field.Ordinal, field.Name, field.Nullable, index+1, column.Name, column.Nullable)
		}
		kind, precision, scale := kindFromPostgres(field.PostgresType)
		if kind != column.Kind || precision != column.Precision || scale != column.Scale {
			t.Fatalf("archive field %s type = %q => (%v,%d,%d); compiled = (%v,%d,%d)", field.Name, field.PostgresType, kind, precision, scale, column.Kind, column.Precision, column.Scale)
		}
	}
}

func TestRecordSchemaMatchesCompiledColumns(t *testing.T) {
	content, err := os.ReadFile(filepath.Join(contractDirectory(t), "record.schema.json"))
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
	content, err := os.ReadFile(filepath.Join(contractDirectory(t), "contract.json"))
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
	if contract.ContractID != "fec/schedule-b" || contract.Status != "accepted" || len(contract.Fixtures) != 1 {
		t.Fatalf("unexpected contract header: %+v", contract)
	}
	for _, fixture := range contract.Fixtures {
		physical, err := os.ReadFile(filepath.Join(contractDirectory(t), fixture.Path))
		if err != nil {
			t.Fatal(err)
		}
		digest := sha256.Sum256(physical)
		if actual := hex.EncodeToString(digest[:]); actual != fixture.SHA256 {
			t.Fatalf("%s digest = %s; want %s", fixture.Path, actual, fixture.SHA256)
		}
	}
}

func kindFromPostgres(postgresType string) (Kind, int, int) {
	switch postgresType {
	case "numeric(14,2)":
		return KindDecimal, 14, 2
	case "numeric(4,0)":
		return KindInteger, 4, 0
	case "numeric(7,0)":
		return KindInteger, 7, 0
	case "numeric(19,0)":
		return KindInteger, 19, 0
	case "timestamp without time zone":
		return KindTimestamp, 0, 0
	default:
		return KindText, 0, 0
	}
}

func contractDirectory(t *testing.T) string {
	t.Helper()
	_, source, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("resolve source path")
	}
	return filepath.Clean(filepath.Join(filepath.Dir(source), "../../../../contracts/sources/fec/schedule-b/v1"))
}
