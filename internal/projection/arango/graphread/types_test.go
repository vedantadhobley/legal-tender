package graphread

import (
	"bytes"
	"testing"
)

func TestCanonicalRetainsExactValuesAndNestedOrdering(t *testing.T) {
	a := []byte(`{"z":{"z":9007199254740993,"a":null},"a":"-103"}`)
	b := []byte(`{"a":"-103","z":{"a":null,"z":9007199254740993}}`)
	x, e := Canonical(a)
	if e != nil {
		t.Fatal(e)
	}
	y, e := Canonical(b)
	if e != nil || !bytes.Equal(x, y) {
		t.Fatal(string(x), string(y), e)
	}
	for _, bad := range []string{`{"a":1,"a":2}`, `{"z":{"a":1,"a":2}}`, `{} {}`, `{"a":"\ud800"}`} {
		if _, e := Canonical([]byte(bad)); e == nil {
			t.Fatal("accepted invalid JSON", bad)
		}
	}
	for _, other := range []string{`{"a":"-103","z":{"a":"","z":9007199254740993}}`, `{"a":"-103","z":{"a":null,"z":9007199254740992}}`, `{"a":-103,"z":{"a":null,"z":9007199254740993}}`} {
		v, e := Canonical([]byte(other))
		if e != nil || bytes.Equal(x, v) {
			t.Fatal("collapsed distinct source values")
		}
	}
}

func TestPageBounds(t *testing.T) {
	for _, id := range []string{"C00000001", "H0ZZ00001", "S0ZZ00001", "P00000001"} {
		if ValidPage(id, "", 1) != nil {
			t.Fatal(id)
		}
	}
	for _, id := range []string{"", "C1", "entities/C00000001", "C00000001' REMOVE"} {
		if ValidPage(id, "", 1) == nil {
			t.Fatal(id)
		}
	}
	if ValidPage("C00000001", "", 0) == nil || ValidPage("C00000001", "", 11) == nil || ValidPage("C00000001", "arbitrary", 1) == nil {
		t.Fatal("accepted unsafe page")
	}
}
