package personaffiliation

import (
	"fmt"
	"slices"
	"strings"
	"unicode"
	"unicode/utf8"

	screen "github.com/vedantadhobley/legal-tender/internal/calculation/personaffiliation"
)

// Research-only lexical coordinates, not names, entities or semantic tokens.
// Offsets stay in Go. A producer sees explicit token IDs and original token text.
type citationToken struct {
	ID    int    `json:"id"`
	Text  string `json:"text"`
	Start int    `json:"-"`
	End   int    `json:"-"`
}
type citationTokenEntry struct {
	Entry  int             `json:"entry"`
	Text   string          `json:"text"`
	Tokens []citationToken `json:"tokens"`
}
type citationCatalog struct {
	Case    modelCase
	Entries []citationTokenEntry
}

// Like the existing interpretation helper, this requires reader-verified input.
// It does not authenticate caller-invented HTML spans against a source body.
func newCitationCatalog(c modelCase) (citationCatalog, error) {
	fail := func() (citationCatalog, error) {
		return citationCatalog{}, fmt.Errorf("invalid or unbounded citation source")
	}
	if c.Source.Validate() != nil || len(c.Entries) == 0 || len(c.Entries) > 32 {
		return fail()
	}
	c.Entries = slices.Clone(c.Entries)
	out := citationCatalog{Case: c, Entries: []citationTokenEntry{}}
	previous, size, count := -1, 0, 0
	for _, e := range c.Entries {
		if e.ID <= previous || e.Text == "" || !utf8.ValidString(e.Text) || (e.Kind != "text" && e.Kind != "heading") {
			return fail()
		}
		previous = e.ID
		size += len(e.Text)
		if size > 64<<10 {
			return fail()
		}
		entry := citationTokenEntry{Entry: e.ID, Text: e.Text, Tokens: []citationToken{}}
		start := -1
		add := func(a, b int) {
			entry.Tokens = append(entry.Tokens, citationToken{ID: len(entry.Tokens), Text: e.Text[a:b], Start: a, End: b})
			count++
		}
		flush := func(end int) {
			if start >= 0 {
				add(start, end)
				start = -1
			}
		}
		for i, r := range e.Text {
			if unicode.IsLetter(r) || unicode.IsNumber(r) || unicode.IsMark(r) {
				if start < 0 {
					start = i
				}
			} else {
				flush(i)
				if !unicode.IsSpace(r) {
					add(i, i+utf8.RuneLen(r))
				}
			}
			if count > 4096 {
				return fail()
			}
		}
		flush(len(e.Text))
		if len(entry.Tokens) == 0 || count > 4096 {
			return fail()
		}
		out.Entries = append(out.Entries, entry)
	}
	return out, nil
}

// First and Last are inclusive IDs supplied in the source catalog, not offsets
// to count. A range never crosses entries or joins disjoint pieces of a sentence.
type citationRange struct {
	Entry int `json:"entry"`
	First int `json:"first_token"`
	Last  int `json:"last_token"`
}

func (c citationCatalog) selectRange(ref citationRange) (screen.ProseCitation, error) {
	for i, e := range c.Entries {
		if e.Entry != ref.Entry {
			continue
		}
		if ref.First < 0 || ref.Last < ref.First || ref.Last >= len(e.Tokens) {
			break
		}
		a, b := e.Tokens[ref.First].Start, e.Tokens[ref.Last].End
		return screen.ProseCitation{Entry: e.Entry, HTML: c.Case.Entries[i].Span,
			Matched: screen.ProseText{Text: e.Text[a:b], Start: a, End: b}}, nil
	}
	return screen.ProseCitation{}, fmt.Errorf("invalid citation token reference")
}

type citationOption struct {
	Evidence screen.ProseCitation `json:"evidence"`
	Range    *citationRange       `json:"token_range"` // nil when an exact substring cuts a token
}
type citationOptions struct {
	State   string           `json:"state"`
	Matches []citationOption `json:"matches"`
}

// Offline diagnostic only. Enumerate every exact occurrence of a saved query;
// do not repair text, consume model offsets, or choose an occurrence automatically.
func (c citationCatalog) literalOptions(entry int, text string) (citationOptions, error) {
	if text == "" || !utf8.ValidString(text) || len(text) > 64<<10 {
		return citationOptions{}, fmt.Errorf("invalid literal query")
	}
	for i, e := range c.Entries {
		if e.Entry != entry {
			continue
		}
		out := citationOptions{State: "not_found", Matches: []citationOption{}}
		for pos := 0; pos < len(e.Text); {
			n := strings.Index(e.Text[pos:], text)
			if n < 0 {
				break
			}
			a, b := pos+n, pos+n+len(text)
			option := citationOption{Evidence: screen.ProseCitation{Entry: entry, HTML: c.Case.Entries[i].Span,
				Matched: screen.ProseText{Text: e.Text[a:b], Start: a, End: b}}}
			first, last := -1, -1
			for _, token := range e.Tokens {
				if token.Start == a {
					first = token.ID
				}
				if token.End == b {
					last = token.ID
				}
			}
			if first >= 0 && last >= first {
				option.Range = &citationRange{Entry: entry, First: first, Last: last}
			}
			out.Matches = append(out.Matches, option)
			if len(out.Matches) > 64 {
				return citationOptions{}, fmt.Errorf("literal occurrence budget exceeded")
			}
			pos = a + 1 // Include overlapping occurrences; valid UTF-8 query cannot start inside a rune.
		}
		if len(out.Matches) == 1 {
			out.State = "unique"
		} else if len(out.Matches) > 1 {
			out.State = "ambiguous"
		}
		return out, nil
	}
	return citationOptions{}, fmt.Errorf("unknown citation entry")
}
