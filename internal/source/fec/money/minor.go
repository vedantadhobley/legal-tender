// Package money owns source-agnostic exact money lexeme conversion for FEC
// contracts. It does not apply any counting or amendment policy.
package money

import (
	"strconv"
	"strings"
)

// ParseUSDMinorUnits converts one plain signed decimal source lexeme to
// checked integer cents. Extra fractional zeroes preserve their source scale
// without inventing sub-cent precision.
func ParseUSDMinorUnits(raw string) (minorUnits string, sourceScale int, issue string) {
	negative := strings.HasPrefix(raw, "-")
	digits := strings.TrimPrefix(raw, "-")
	parts := strings.Split(digits, ".")
	if len(parts) > 2 || parts[0] == "" {
		return "", 0, "invalid_syntax"
	}
	for _, character := range parts[0] {
		if character < '0' || character > '9' {
			return "", 0, "invalid_syntax"
		}
	}
	fraction := ""
	if len(parts) == 2 {
		fraction = parts[1]
		if fraction == "" {
			return "", 0, "invalid_syntax"
		}
		for _, character := range fraction {
			if character < '0' || character > '9' {
				return "", 0, "invalid_syntax"
			}
		}
	}
	sourceScale = len(fraction)
	if len(fraction) > 2 {
		for _, character := range fraction[2:] {
			if character != '0' {
				return "", sourceScale, "minor_unit_precision"
			}
		}
		fraction = fraction[:2]
	}
	fraction += strings.Repeat("0", 2-len(fraction))
	minorDigits := strings.TrimLeft(parts[0]+fraction, "0")
	if minorDigits == "" {
		minorDigits = "0"
	}
	if negative && minorDigits != "0" {
		minorDigits = "-" + minorDigits
	}
	minor, err := strconv.ParseInt(minorDigits, 10, 64)
	if err != nil {
		return "", sourceScale, "minor_unit_overflow"
	}
	return strconv.FormatInt(minor, 10), sourceScale, ""
}
