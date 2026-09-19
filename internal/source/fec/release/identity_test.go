package release

import (
	"strings"
	"testing"
)

func TestPublicReleaseIdentityContract(t *testing.T) {
	if !ValidReleaseID("fec-" + strings.Repeat("a", 64)) {
		t.Fatal("valid namespaced release rejected")
	}
	for _, value := range []string{"", strings.Repeat("a", 64), "fec-", "fec-" + strings.Repeat("g", 64), "../fec-" + strings.Repeat("a", 64)} {
		if ValidReleaseID(value) {
			t.Fatal("invalid release identity accepted", value)
		}
	}
}
