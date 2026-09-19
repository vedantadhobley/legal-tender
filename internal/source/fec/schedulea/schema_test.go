// Schema tests bind compiled field order to the exact archive observation.
package schedulea

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

func TestCompiledSchemaMatchesArchiveObservation(t *testing.T) {
	t.Parallel()
	path := filepath.Join(fixtureDirectory(t), "../archive/dump-2026-08-23.json")
	evidence, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read archive observation: %v", err)
	}
	var observation struct {
		ParentRelation struct {
			FieldCount int `json:"field_count"`
			Fields     []struct {
				Ordinal      int    `json:"ordinal"`
				Name         string `json:"name"`
				PostgresType string `json:"postgres_type"`
				Nullable     bool   `json:"nullable"`
			} `json:"fields"`
		} `json:"parent_relation"`
	}
	if err := json.Unmarshal(evidence, &observation); err != nil {
		t.Fatalf("decode archive observation: %v", err)
	}
	if observation.ParentRelation.FieldCount != FieldCount || len(observation.ParentRelation.Fields) != FieldCount {
		t.Fatalf("archive width = (%d, %d listed); want %d", observation.ParentRelation.FieldCount, len(observation.ParentRelation.Fields), FieldCount)
	}

	for index, field := range observation.ParentRelation.Fields {
		column := columns[index]
		if field.Ordinal != index+1 || field.Name != column.Name || field.Nullable != column.Nullable {
			t.Fatalf("archive field %d = (%d, %q, nullable=%t); compiled = (%d, %q, nullable=%t)", index, field.Ordinal, field.Name, field.Nullable, index+1, column.Name, column.Nullable)
		}
		if got, want := kindFromPostgres(field.PostgresType), column.Kind; got != want {
			t.Fatalf("archive field %s kind = %v from %q; compiled = %v", field.Name, got, field.PostgresType, want)
		}
	}
}

func kindFromPostgres(postgresType string) Kind {
	switch postgresType {
	case "numeric(14,2)":
		return KindDecimal
	case "numeric(4,0)", "numeric(7,0)", "numeric(19,0)":
		return KindInteger
	case "timestamp without time zone":
		return KindTimestamp
	case "boolean":
		return KindBoolean
	default:
		return KindText
	}
}
