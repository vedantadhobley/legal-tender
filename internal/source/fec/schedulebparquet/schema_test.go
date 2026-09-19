package schedulebparquet

import (
	"encoding/json"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/vedantadhobley/legal-tender/internal/source/fec/scheduleb"
)

func TestPhysicalSchemaMatchesMachineContract(t *testing.T) {
	t.Parallel()
	schema, err := NewSchema()
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(repositoryRoot(t), "contracts", "facts", "fec", "schedule-b", "columnar", "v1", "physical-schema.json")
	file, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = file.Close() }()
	var contract struct {
		SchemaVersion string `json:"schema_version"`
		ColumnCount   int    `json:"column_count"`
		SourceColumns struct {
			Count int `json:"count"`
		} `json:"source_columns"`
		AddedColumns []struct {
			Name string `json:"name"`
		} `json:"added_columns"`
	}
	decoder := json.NewDecoder(file)
	if err := decoder.Decode(&contract); err != nil {
		t.Fatal(err)
	}
	if contract.SchemaVersion != PhysicalSchemaVersion || contract.ColumnCount != schema.ColumnCount() || contract.SourceColumns.Count != scheduleb.FieldCount {
		t.Fatalf("physical contract drift: %+v schema_columns=%d", contract, schema.ColumnCount())
	}
	columns := scheduleb.Columns()
	for _, column := range columns {
		if _, ok := schema.ColumnIndex(column.Name); !ok {
			t.Fatalf("source column %s is absent from Parquet schema", column.Name)
		}
	}
	wantAdded := []string{
		ColumnSourceRowOrdinal, ColumnSourceRawByteOffset, ColumnSourceRawByteLength,
		ColumnDisbursementAmountMinorUnits, ColumnDisbursementAmountSourceScale, ColumnDisbursementAmountState,
		ColumnBundledRefundMinorUnits, ColumnBundledRefundSourceScale, ColumnBundledRefundState,
		ColumnDisbursementDate, ColumnDisbursementAtLocal, ColumnCommunicationDate, ColumnCommunicationAtLocal,
		ColumnPublisherLoadedAtLocal, ColumnReportYear, ColumnTwoYearTransactionPeriod, ColumnMemoedSubtotal,
	}
	if len(contract.AddedColumns) != len(wantAdded) {
		t.Fatalf("added contract columns = %d; want %d", len(contract.AddedColumns), len(wantAdded))
	}
	for index, name := range wantAdded {
		if contract.AddedColumns[index].Name != name {
			t.Fatalf("added contract column %d = %q; want %q", index, contract.AddedColumns[index].Name, name)
		}
		if _, ok := schema.ColumnIndex(name); !ok {
			t.Fatalf("added column %s is absent from Parquet schema", name)
		}
	}
}

func repositoryRoot(t *testing.T) string {
	t.Helper()
	_, filename, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("resolve test source path")
	}
	return filepath.Clean(filepath.Join(filepath.Dir(filename), "..", "..", "..", ".."))
}
