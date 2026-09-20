package candidateevidence

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"

	"github.com/vedantadhobley/legal-tender/internal/strictjson"
)

const maxDossierBytes = 32 << 20

// OpenDossier reads and authenticates one immutable candidate dossier. It
// verifies the compact document itself; it does not reopen the dossier's
// complete parent publications.
func OpenDossier(ctx context.Context, path, expectedID string) (Dossier, string, error) {
	if err := ctx.Err(); err != nil {
		return Dossier{}, "", err
	}
	if !digest(expectedID) {
		return Dossier{}, "", fmt.Errorf("expected dossier ID required")
	}
	f, err := os.Open(path)
	if err != nil {
		return Dossier{}, "", err
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		return Dossier{}, "", err
	}
	if !info.Mode().IsRegular() || info.Size() > maxDossierBytes {
		return Dossier{}, "", fmt.Errorf("dossier must be a regular file no larger than 32 MiB")
	}
	raw, err := io.ReadAll(io.LimitReader(f, maxDossierBytes+1))
	if err != nil {
		return Dossier{}, "", err
	}
	if len(raw) > maxDossierBytes {
		return Dossier{}, "", fmt.Errorf("dossier exceeds size limit")
	}
	var dossier Dossier
	if err := strictjson.Decode(raw, &dossier); err != nil {
		return Dossier{}, "", err
	}
	if err := verifyDossier(dossier, expectedID); err != nil {
		return Dossier{}, "", err
	}
	h := sha256.Sum256(raw)
	return dossier, hex.EncodeToString(h[:]), ctx.Err()
}

func verifyDossier(dossier Dossier, expectedID string) error {
	if dossier.SchemaVersion != DossierVersion || dossier.Policy != DossierPolicy || dossier.DossierID != expectedID ||
		!digest(dossier.ExecutableSHA256) || dossier.CandidateID == "" || dossier.Cycle == "" || dossier.CandidateName.EntityID != dossier.CandidateID ||
		dossier.TerminalPolicy != nil || dossier.AllocationPolicy != nil || dossier.TerminalAmount != nil || dossier.TerminalEligible {
		return fmt.Errorf("dossier version, scope or attribution boundary mismatch")
	}
	for _, value := range []string{
		dossier.Inputs.ParentReportID,
		dossier.Inputs.ParentReportSHA256,
		dossier.Inputs.ParentEvidenceID,
		dossier.Inputs.ReceiptFactSetID,
		dossier.Inputs.ReceiptManifestSHA256,
		dossier.Inputs.CommitteeFlowCalculationSetID,
		dossier.Inputs.CandidateInterpretationSetID,
		dossier.Inputs.CandidateInterpretationManifestSHA,
		dossier.Inputs.CandidateInterpretationArtifactSHA,
	} {
		if !digest(value) {
			return fmt.Errorf("dossier contains an invalid content identity")
		}
	}
	if len(dossier.Receipts.CandidateLinkedCommittees) == 0 || dossier.OutsideSpending.ViewsAreNonAdditive != true {
		return fmt.Errorf("dossier lacks required evidence populations")
	}
	copy := dossier
	copy.DossierID = ""
	id, err := contentID(copy)
	if err != nil || id != expectedID {
		return fmt.Errorf("dossier content identity mismatch")
	}
	return nil
}
