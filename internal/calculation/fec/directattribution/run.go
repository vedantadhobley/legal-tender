package directattribution

import (
	"context"
	"fmt"
	"path/filepath"

	participants "github.com/vedantadhobley/legal-tender/internal/calculation/fec/receiptparticipants"
	"github.com/vedantadhobley/legal-tender/internal/calculation/fec/receipts"
)

type Options struct {
	StorageRoot         string
	ScheduleAFacts      string
	ParticipantManifest string
	ExpectedParticipant string
	ReceiptBundle       string
	BuildSHA256         string
	ExpectedCalculation string
	Workers             int
	Progress            func(string)
}

func Run(ctx context.Context, options Options) (Result, error) {
	var out Result
	if options.StorageRoot == "" || options.ScheduleAFacts == "" || options.ParticipantManifest == "" || options.ReceiptBundle == "" ||
		!digest(options.ExpectedParticipant) || !digest(options.BuildSHA256) || options.ExpectedCalculation != "" && !digest(options.ExpectedCalculation) || options.Workers < 1 || options.Workers > 8 {
		return out, fmt.Errorf("exact storage, Schedule A, participant, receipt bundle, build identity, optional replay identity, and 1..8 workers required")
	}
	progress := options.Progress
	if progress == nil {
		progress = func(string) {}
	}
	progress("verifying participant publication, Schedule A ancestry, and receipt fact bundle")
	inspector, err := participants.OpenInspector(ctx, options.StorageRoot, options.ScheduleAFacts, options.ParticipantManifest, options.ExpectedParticipant)
	if err != nil {
		return out, err
	}
	scope := inspector.Scope()
	bundle, bundleSHA, err := receipts.LoadPublishedFactBundle(options.StorageRoot, options.ReceiptBundle)
	if err != nil {
		return out, err
	}
	if filepath.Base(options.ReceiptBundle) != bundle.BundleID+".json" {
		return out, fmt.Errorf("exact immutable receipt fact bundle required")
	}
	var scheduleRef, linkageRef *receipts.FactSetReference
	for index := range bundle.InputFactSets {
		reference := &bundle.InputFactSets[index]
		switch reference.Role {
		case "schedule_a_receipts":
			scheduleRef = reference
		case "candidate_committee_linkage":
			linkageRef = reference
		}
	}
	if scheduleRef == nil || linkageRef == nil || bundle.Cycle != scope.Cycle || bundle.SourceReleaseID != scope.SourceReleaseID ||
		scheduleRef.FactSetID != scope.FactSetID || scheduleRef.ManifestSHA256 != scope.ManifestSHA256 {
		return out, fmt.Errorf("participant publication and receipt fact bundle do not share exact Schedule A ancestry")
	}
	linkagePath := filepath.Join(options.StorageRoot, "facts", "fec", "classic", "candidate-committee-linkage", "manifests", linkageRef.FactSetID+".json")
	linkages, linkageManifest, linkageSHA, err := receipts.LoadPublishedLinkageFacts(ctx, options.StorageRoot, linkagePath)
	if err != nil {
		return out, err
	}
	if linkageManifest.FactSetID != linkageRef.FactSetID || linkageSHA != linkageRef.ManifestSHA256 || linkageManifest.Cycle != scope.Cycle || linkageManifest.SourceReleaseID != scope.SourceReleaseID {
		return out, fmt.Errorf("candidate-committee linkage facts differ from receipt bundle")
	}
	collector, err := NewCollector(linkages, options.Workers)
	if err != nil {
		return out, err
	}
	progress("classifying every source-grain participant through the accepted direct/earmark predicate")
	verified, err := inspector.Scan(ctx, options.Workers, collector.Observe, progress)
	if err != nil {
		return out, err
	}
	if verified.Rows != scope.Rows {
		return out, fmt.Errorf("verified participant census differs from exact source scope")
	}
	census, candidates, err := collector.Finish(scope.Rows)
	if err != nil {
		return out, err
	}
	out = Result{SchemaVersion: SchemaVersion, CalculationPolicy: CalculationPolicy, TerminalPolicy: TerminalPolicy, AllocationPolicy: AllocationPolicy,
		ExecutableSHA256: options.BuildSHA256, Cycle: scope.Cycle, State: "complete_cycle_partial_source_appearance_attribution",
		IdentityBoundary: "reported_source_appearance_not_resolved_person_or_organization",
		Inputs: Inputs{ParticipantCalculationID: scope.ParticipantID, ParticipantManifestSHA: scope.ParticipantSHA256,
			ScheduleAFactSetID: scope.FactSetID, ScheduleAManifestSHA: scope.ManifestSHA256, SourceReleaseID: scope.SourceReleaseID,
			ReceiptBundleID: bundle.BundleID, ReceiptBundleSHA: bundleSHA, LinkageFactSetID: linkageRef.FactSetID, LinkageManifestSHA: linkageSHA,
			SourceRows: scope.Rows, LinkageFacts: uint64(len(linkages))},
		Predicate: predicate(), Authorization: collector.Authorization(), Census: census, Candidates: candidates,
		AppearanceTerminalEligible: true, ResolvedEntityTerminalEligible: false, CommitteeChainAllocationPerformed: false,
		Limitations: []string{"reported_source_appearances_are_not_deduplicated_people_or_organizations", "employer_and_occupation_are_not_donor_identity", "committee_chain_amounts_remain_unresolved", "unitemized_receipts_and_opening_cash_not_source_attributed", "chronological_cash_availability_not_established", "independent_expenditures_are_separate"},
	}
	out.CalculationID, err = identity(out)
	if err != nil {
		return Result{}, err
	}
	if options.ExpectedCalculation != "" && out.CalculationID != options.ExpectedCalculation {
		return Result{}, fmt.Errorf("direct source-appearance attribution replay identity differs")
	}
	return out, ctx.Err()
}
