// PostgreSQL COPY text decoding preserves Schedule E rows before normalization.
package schedulee

import (
	"io"

	"github.com/vedantadhobley/legal-tender/internal/source/fec/copytext"
)

var (
	// ErrFieldCount marks a physical row whose width differs from the contract.
	ErrFieldCount = copytext.ErrFieldCount
	// ErrInvalidEscape marks an incomplete or invalid COPY text escape.
	ErrInvalidEscape = copytext.ErrInvalidEscape
	// ErrMissingLineFeed marks a final physical row without its required LF.
	ErrMissingLineFeed = copytext.ErrMissingLineFeed
)

// Field is one decoded COPY source value.
type Field = copytext.Field

// Row is one decoded processed Schedule E source row.
type Row = copytext.Row

// Decoder streams processed Schedule E COPY text rows.
type Decoder = copytext.Decoder

// NewDecoder creates an exact-width processed Schedule E decoder.
func NewDecoder(reader io.Reader) *Decoder {
	return copytext.NewDecoder(reader, FieldCount)
}
