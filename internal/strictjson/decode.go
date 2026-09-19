// Package strictjson validates lossless, closed JSON input contracts.
package strictjson

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"strconv"
	"unicode/utf8"
)

// encoding/json otherwise replaces invalid UTF-8/surrogates and accepts
// duplicate object keys with last-wins semantics. Neither is lossless evidence.
func Decode(raw []byte, target any) error {
	if !utf8.Valid(raw) || !json.Valid(raw) || !validSurrogates(raw) {
		return errors.New("invalid JSON encoding or framing")
	}
	d := json.NewDecoder(bytes.NewReader(raw))
	d.UseNumber()
	if err := checkValue(d, 0); err != nil {
		return err
	}
	if _, err := d.Token(); err != io.EOF {
		return errors.New("trailing JSON value")
	}
	if target == nil {
		return nil
	}
	d = json.NewDecoder(bytes.NewReader(raw))
	d.DisallowUnknownFields()
	if err := d.Decode(target); err != nil {
		// Do not echo arbitrary manifest keys, URLs, or source values in errors.
		return errors.New("JSON does not match the closed contract")
	}
	return nil
}

func checkValue(d *json.Decoder, depth int) error {
	if depth > 32 {
		return errors.New("JSON nesting budget exceeded")
	}
	t, err := d.Token()
	if err != nil {
		return errors.New("invalid JSON token")
	}
	switch t {
	case json.Delim('{'):
		keys := make(map[string]bool)
		for d.More() {
			t, err := d.Token()
			if err != nil {
				return errors.New("invalid object key")
			}
			k, ok := t.(string)
			if !ok || keys[k] {
				return errors.New("duplicate or invalid object key")
			}
			keys[k] = true
			if err := checkValue(d, depth+1); err != nil {
				return err
			}
		}
		_, err = d.Token()
	case json.Delim('['):
		for d.More() {
			if err := checkValue(d, depth+1); err != nil {
				return err
			}
		}
		_, err = d.Token()
	}
	return err
}

func validSurrogates(raw []byte) bool {
	for i := 0; i < len(raw); i++ {
		if raw[i] != '\\' {
			continue
		}
		i++
		if i >= len(raw) {
			return false
		}
		if raw[i] != 'u' {
			continue
		} // Skip escaped backslashes and quotes.
		if i+4 >= len(raw) {
			return false
		}
		n, err := strconv.ParseUint(string(raw[i+1:i+5]), 16, 16)
		if err != nil {
			return false
		}
		i += 4
		if n >= 0xdc00 && n <= 0xdfff {
			return false
		}
		if n < 0xd800 || n > 0xdbff {
			continue
		}
		if i+6 >= len(raw) || raw[i+1] != '\\' || raw[i+2] != 'u' {
			return false
		}
		low, err := strconv.ParseUint(string(raw[i+3:i+7]), 16, 16)
		if err != nil || low < 0xdc00 || low > 0xdfff {
			return false
		}
		i += 6
	}
	return true
}
