package flowreconciliation

import (
	"encoding/json"
	"testing"

	"github.com/vedantadhobley/legal-tender/internal/source/fec/occurrence"
)

func TestReferenceContextCannotBeSuppliedAsSerializedApproval(t *testing.T) {
	b := Bundle{BundleID: "bundle"}
	var nilContext *ReferenceContext
	var decoded ReferenceContext
	if err := json.Unmarshal([]byte(`{"bundleID":"bundle","bundleSHA":"sha","proofs":[{"policy":"passed"}]}`), &decoded); err != nil {
		t.Fatal(err)
	}
	for _, c := range []*ReferenceContext{nilContext, {}, &decoded} {
		if _, _, _, err := c.References(b, "sha"); err == nil {
			t.Fatal("unverified capability accepted")
		}
	}
}

func TestReferenceContextPinsBundleAndOwnsProofs(t *testing.T) {
	b := Bundle{BundleID: "bundle"}
	c := ReferenceContext{bundleID: b.BundleID, bundleSHA: "sha", committee: FactReference{FactSetID: "master"}, linkages: FactReference{FactSetID: "linkage"}, proofs: make([]occurrence.ClassicReferenceProof, 3)}
	master, linkage, proofs, err := c.References(b, "sha")
	if err != nil || master.FactSetID != "master" || linkage.FactSetID != "linkage" {
		t.Fatal(err)
	}
	proofs[0].ProofID = "mutated"
	_, _, again, err := c.References(b, "sha")
	if err != nil || again[0].ProofID != "" {
		t.Fatal("caller mutated verified capability")
	}
	if _, _, _, err := c.References(b, "other"); err == nil {
		t.Fatal("different bundle digest accepted")
	}
	b.BundleID = "other"
	if _, _, _, err := c.References(b, "sha"); err == nil {
		t.Fatal("different bundle accepted")
	}
}
