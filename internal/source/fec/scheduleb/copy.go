// PostgreSQL COPY text decoding preserves Schedule B rows before normalization.
package scheduleb

import (
	"io"

	"github.com/vedantadhobley/legal-tender/internal/source/fec/copytext"
)

var (
	ErrFieldCount      = copytext.ErrFieldCount
	ErrInvalidEscape   = copytext.ErrInvalidEscape
	ErrMissingLineFeed = copytext.ErrMissingLineFeed
)

type Field = copytext.Field
type Row = copytext.Row
type Decoder = copytext.Decoder

// NewDecoder creates an exact-width processed Schedule B decoder.
func NewDecoder(reader io.Reader) *Decoder { return copytext.NewDecoder(reader, FieldCount) }
