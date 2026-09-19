package schedulee

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

func TestCompiledSchemaMatchesArchiveObservation(t *testing.T) {
	t.Parallel()
	path := filepath.Clean(filepath.Join(fixtureDirectory(t), "../archive/dump-2026-08-30.json"))
	content, err := os.ReadFile(path)
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
