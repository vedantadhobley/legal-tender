package schedulea

import fecmoney "github.com/vedantadhobley/legal-tender/internal/source/fec/money"

// ParseUSDMinorUnits converts one plain signed decimal source lexeme to
// checked integer cents. Extra fractional zeroes preserve their source scale
// without inventing sub-cent precision.
func ParseUSDMinorUnits(raw string) (minorUnits string, sourceScale int, issue string) {
	return fecmoney.ParseUSDMinorUnits(raw)
}
