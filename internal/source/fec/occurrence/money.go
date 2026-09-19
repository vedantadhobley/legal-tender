package occurrence

import "github.com/vedantadhobley/legal-tender/internal/source/fec/schedulea"

// parseUSDMinorUnits converts one plain signed decimal lexeme to checked
// integer cents. Extra fractional zeroes preserve their source scale without
// inventing sub-cent precision.
func parseUSDMinorUnits(raw string) (minorUnits string, sourceScale int, issue string) {
	return schedulea.ParseUSDMinorUnits(raw)
}
