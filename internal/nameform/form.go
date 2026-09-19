// Package nameform contains shared, non-identifying text transformations.
package nameform

import (
	"strings"
	"unicode"
)

// Normalize preserves token order, accents and suffixes.
func Normalize(s string) string {
	var b strings.Builder
	for _, r := range s {
		if unicode.IsLetter(r) || unicode.IsNumber(r) || unicode.IsMark(r) {
			b.WriteRune(unicode.ToUpper(r))
		} else {
			b.WriteByte(' ')
		}
	}
	return strings.Join(strings.Fields(b.String()), " ")
}

// SplitLegalSuffix removes at most one complete trailing designator. This is
// a bounded proposal vocabulary, not permission to discard corporate words.
func SplitLegalSuffix(s string) (stem, suffix, family string) {
	i := strings.LastIndexByte(s, ' ')
	suffix = s[i+1:]
	switch strings.ToUpper(suffix) {
	case "CORP", "CORPORATION":
		family = "corporation"
	case "INC", "INCORPORATED":
		family = "incorporated"
	case "LTD", "LIMITED":
		family = "limited"
	case "LLC":
		family = "llc"
	default:
		return s, "", ""
	}
	if i < 0 {
		return "", suffix, family
	}
	return s[:i], suffix, family
}

// DigitLetterBoundaries separates letters and numbers, never arbitrary words.
func DigitLetterBoundaries(s string) string {
	var b strings.Builder
	var previous byte
	for _, r := range s {
		var class byte
		switch {
		case unicode.IsLetter(r):
			class = 'L'
		case unicode.IsNumber(r):
			class = 'N'
		case unicode.IsMark(r):
			b.WriteRune(r)
			continue
		}
		if previous != 0 && class != 0 && previous != class {
			b.WriteByte(' ')
		}
		b.WriteRune(r)
		previous = class
	}
	return b.String()
}
