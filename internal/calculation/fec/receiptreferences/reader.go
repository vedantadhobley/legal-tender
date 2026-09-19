package receiptreferences

import (
	"github.com/parquet-go/parquet-go"
	"github.com/vedantadhobley/legal-tender/internal/storage/narrowparquet"
)

// The generic schema-conversion Rows method reconstructs source-width rows,
// including placeholders for unselected columns, before projecting them. For
// this strictly flat schema, the converted chunks already contain the exact
// selected columns, with correctly remapped indexes and definition levels.
// Read those chunks directly; keep the library's typed row reconstruction.
func newNarrowReferenceReader(p *parquet.File) (referenceRowReader, error) {
	return narrowparquet.New[Row](p)
}
