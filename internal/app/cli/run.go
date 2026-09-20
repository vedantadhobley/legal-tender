// Package cli dispatches independently runnable Legal Tender Go operations.
package cli

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/vedantadhobley/legal-tender/internal/audit/fecclassicflow"
	"github.com/vedantadhobley/legal-tender/internal/audit/feccommitteeflow"
	"github.com/vedantadhobley/legal-tender/internal/audit/fecflowmastergaps"
	"github.com/vedantadhobley/legal-tender/internal/audit/fecmastergaps"
	"github.com/vedantadhobley/legal-tender/internal/audit/fecschedulea"
	"github.com/vedantadhobley/legal-tender/internal/audit/fecscheduleab"
	"github.com/vedantadhobley/legal-tender/internal/audit/fecschedulealayout"
	"github.com/vedantadhobley/legal-tender/internal/audit/fecscheduleb"
	feccandidateresolution "github.com/vedantadhobley/legal-tender/internal/calculation/fec/candidateresolution"
	feccommitteeflows "github.com/vedantadhobley/legal-tender/internal/calculation/fec/committeeflows"
	feccommitteeidentity "github.com/vedantadhobley/legal-tender/internal/calculation/fec/committeeidentity"
	fecindependentexpenditures "github.com/vedantadhobley/legal-tender/internal/calculation/fec/independentexpenditures"
	fecreceipts "github.com/vedantadhobley/legal-tender/internal/calculation/fec/receipts"
	projectionreceipts "github.com/vedantadhobley/legal-tender/internal/projection/arango/candidatereceipts"
	projectioncommitteeflows "github.com/vedantadhobley/legal-tender/internal/projection/arango/committeeflows"
	projectionindependentexpenditures "github.com/vedantadhobley/legal-tender/internal/projection/arango/independentexpenditures"
	fecoccurrence "github.com/vedantadhobley/legal-tender/internal/source/fec/occurrence"
	fecrelease "github.com/vedantadhobley/legal-tender/internal/source/fec/release"
	"github.com/vedantadhobley/legal-tender/internal/source/fec/schedulea"
	"github.com/vedantadhobley/legal-tender/internal/source/fec/scheduleb"
	"github.com/vedantadhobley/legal-tender/internal/source/fec/schedulee"
)

const usage = `usage:
  legal-tender pipeline entities extract-company-page [options]
  legal-tender pipeline entities extract-role-evidence [options]
  legal-tender pipeline entities query-relationships [options]
  legal-tender pipeline entities capture-organizations [options]
  legal-tender pipeline entities replay-organizations [options]
  legal-tender pipeline entities evaluate-organizations [options]
  legal-tender pipeline entities capture-organization-registry [options]
  legal-tender pipeline entities corroborate-organizations [options]
  legal-tender pipeline entities capture-issuer-directory [options]
  legal-tender pipeline entities discover-issuer-organizations [options]
  legal-tender pipeline entities capture-issuer-filing [options]
  legal-tender pipeline entities inspect-issuer-filing [options]
  legal-tender pipeline fec build-organization-queries [options]
  legal-tender pipeline entities plan-affiliation-discovery [options]
  legal-tender pipeline entities capture-affiliation-candidates [options]
  legal-tender pipeline entities replay-affiliation-candidates [options]
  legal-tender pipeline entities assess-affiliation-candidates [options]
  legal-tender pipeline entities enrich-affiliations [options]
  legal-tender pipeline entities plan-employer-registry [options]
  legal-tender pipeline entities capture-registry-names [options]
  legal-tender pipeline entities replay-registry-names [options]
  legal-tender serve committee-flow-evidence [options]
  legal-tender pipeline fec discover [options]
  legal-tender pipeline fec plan-release [options]
  legal-tender pipeline fec review-release-storage [options]
  legal-tender pipeline fec acquire [options]
  legal-tender pipeline fec stage-release [options]
  legal-tender pipeline fec publish-release [options]
  legal-tender pipeline fec publish-classic-occurrences [options]
  legal-tender pipeline fec publish-classic-facts [options]
  legal-tender pipeline fec publish-schedule-a-occurrences [options]
  legal-tender pipeline fec publish-schedule-a-compact-occurrences [options]
  legal-tender pipeline fec publish-schedule-a-facts [options]
  legal-tender pipeline fec publish-schedule-a-columnar-facts [options]
  legal-tender pipeline fec publish-schedule-b-columnar-facts [options]
  legal-tender pipeline fec publish-schedule-e-occurrences [options]
  legal-tender pipeline fec publish-schedule-e-facts [options]
  legal-tender pipeline fec publish-effective-independent-expenditures [options]
  legal-tender pipeline fec publish-independent-expenditure-candidate-resolution [options]
  legal-tender pipeline fec publish-independent-expenditure-candidate-interpretations [options]
  legal-tender pipeline fec publish-resolved-independent-expenditures [options]
  legal-tender pipeline fec publish-independent-expenditure-projection-bundle [options]
  legal-tender pipeline fec publish-resolved-independent-expenditure-projection-bundle [options]
  legal-tender pipeline fec publish-candidate-itemized-receipts-fact-bundle [options]
  legal-tender pipeline fec publish-candidate-itemized-receipts [options]
  legal-tender pipeline fec publish-candidate-itemized-receipts-compact [options]
  legal-tender pipeline fec publish-receiver-committee-flows [options]
  legal-tender pipeline fec publish-receiver-committee-flow-projection-bundle [options]
  legal-tender pipeline fec publish-receiver-flow-committee-identities [options]
  legal-tender pipeline fec publish-receiver-committee-flow-projection-bundle-v2 [options]
  legal-tender pipeline fec probe-arango-candidate-receipts [options]
  legal-tender pipeline fec probe-arango-receiver-committee-flows [options]
  legal-tender pipeline fec probe-arango-receiver-committee-flows-v2 [options]
  legal-tender pipeline fec probe-arango-independent-expenditures [options]
  legal-tender pipeline fec probe-arango-resolved-independent-expenditures [options]
  legal-tender pipeline fec probe-candidate-itemized-receipts [options]
  legal-tender pipeline fec probe-receiver-committee-flows [options]
  legal-tender pipeline fec benchmark-schedule-a-layout [options]
  legal-tender pipeline fec verify-schedule-a [options]
  legal-tender pipeline fec verify-schedule-b [options]
  legal-tender pipeline fec verify-schedule-e [options]
  legal-tender pipeline fec verify-committee-summary [options]
  legal-tender pipeline fec publish-committee-summary [options]
  legal-tender pipeline fec calculate-committee-summary-assertions [options]
  legal-tender pipeline fec review-summary-receipt-compatibility [options]
  legal-tender pipeline fec profile-receipt-report-scope [options]
  legal-tender pipeline fec review-funding-report-lines [options]
  legal-tender pipeline fec review-report-metadata [options]
  legal-tender pipeline fec review-report-scope [options]
  legal-tender pipeline fec compare-report-total-receipts [options]
  legal-tender pipeline fec review-report-unitemized [options]
  legal-tender pipeline fec review-report-period-membership [options]
  legal-tender pipeline fec review-report-field-binding [options]
  legal-tender pipeline fec review-report-window [options]
  legal-tender pipeline fec compare-summary-report-window [options]
  legal-tender pipeline fec compare-summary-report-window-v2 [options]
  legal-tender pipeline fec compare-receipt-report-window [options]
  legal-tender pipeline fec compare-receipt-families [options]
  legal-tender pipeline fec review-receipt-family-absence [options]
  legal-tender pipeline fec compare-receipt-family-window [options]
  legal-tender pipeline fec compare-receipt-family-summary [options]
  legal-tender pipeline fec capture-report-metadata [options]
  legal-tender pipeline fec audit-classic-flows [options]
  legal-tender pipeline fec audit-receipt-master-gaps [options]
  legal-tender pipeline fec audit-receiver-flow-master-gaps [options]
  legal-tender pipeline fec audit-schedule-a-overlap [options]
  legal-tender pipeline fec audit-schedule-ab-alignment [options]
  legal-tender pipeline fec audit-schedule-b-overlap [options]
  legal-tender pipeline fec audit-schedule-b-semantics [options]
  legal-tender pipeline fec audit-pre-attribution-interpretations [options]
  legal-tender pipeline fec calculate-disbursement-reporting [options]
  legal-tender pipeline fec reconcile-committee-flows [options]
  legal-tender pipeline fec publish-committee-flow-reconciliation [options]
  legal-tender pipeline fec publish-committee-flow-comparison-candidates [options]
  legal-tender pipeline fec publish-committee-flow-evidence-bundle [options]
  legal-tender pipeline fec verify-committee-flow-evidence-bundle [options]
  legal-tender pipeline fec probe-arango-committee-flow-evidence [options]
  legal-tender pipeline fec review-committee-flows [options]
  legal-tender pipeline fec trace-candidate-committee-receipts [options]
  legal-tender pipeline fec build-candidate-evidence [options]
  legal-tender pipeline fec build-candidate-dossier [options]
  legal-tender pipeline fec compare-terminal-policies [options]
  legal-tender pipeline fec calculate-direct-source-attribution [options]
  legal-tender pipeline fec inspect-candidate-connection [options]
  legal-tender pipeline fec benchmark-receipt-reference-index [options]
  legal-tender pipeline fec join-receipt-references [options]
  legal-tender pipeline fec build-receipt-reference-topology [options]
  legal-tender pipeline fec publish-receipt-participants [options]
  legal-tender pipeline fec publish-receipt-conduit-associations [options]
  legal-tender pipeline fec publish-shared-receipt-conduit-associations [options]
  legal-tender pipeline fec publish-shared-conduit-generation [options]
  legal-tender pipeline fec inspect-funding-recovery-inventory [options]
  legal-tender pipeline fec plan-funding-recovery [options]
  legal-tender pipeline fec verify-funding-recovery-files [options]
  legal-tender pipeline fec review-release-stage-evidence [options]
  legal-tender pipeline fec validate-shared-conduit-queries [options]
  legal-tender pipeline fec profile-shared-receipt-references [options]
  legal-tender pipeline fec review-shared-receipt-references [options]
  legal-tender pipeline fec benchmark-arango-receipt-participants [options]
  legal-tender pipeline fec publish-arango-receipt-participants [options]
  legal-tender pipeline fec inspect-receipt-candidate-connection [options]
  legal-tender pipeline fec validate-receipt-candidate-connections [options]
  legal-tender pipeline fec verify-funding-generation [options]
  legal-tender pipeline fec inspect-funding-neighborhood [options]
  legal-tender pipeline fec validate-funding-neighborhoods [options]
  legal-tender pipeline fec inspect-funding-paths [options]
  legal-tender pipeline fec validate-funding-paths [options]
  legal-tender pipeline fec assess-terminal-sources [options]
  legal-tender pipeline fec profile-terminal-receipt-roles [options]
  legal-tender pipeline fec build-reported-identity-assertions [options]
  legal-tender pipeline fec benchmark-receipt-participants [options]
  legal-tender pipeline fec inspect-receipt-participant [options]
  legal-tender pipeline fec calculate-committee-funding-basis [options]
  legal-tender pipeline fec list-funding-receipts [options]
  legal-tender pipeline fec assess-candidate-funding-basis [options]
  legal-tender pipeline fec review-funding-component [options]
  legal-tender pipeline fec review-funding-report [options]
  legal-tender pipeline fec audit-funding-coverage [options]
  legal-tender pipeline fec inspect-funding-receipts [options]

Commands emit versioned JSON results on stdout. Diagnostics go to stderr.
`

// Run dispatches one command and returns its process exit code.
func Run(args []string, stdout, stderr io.Writer) int {
	return RunContext(context.Background(), args, stdout, stderr)
}

// RunContext dispatches one command under the caller's cancellation boundary
// and returns its process exit code.
func RunContext(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	if len(args) == 0 || isHelp(args[0]) {
		_, _ = io.WriteString(stderr, usage)
		if len(args) > 0 {
			return 0
		}
		return 2
	}
	if len(args) >= 3 && args[0] == "pipeline" && args[1] == "entities" {
		return runOrganizations(ctx, args[2], args[3:], stdout, stderr)
	}
	if len(args) < 3 || args[0] != "pipeline" || args[1] != "fec" {
		if len(args) >= 2 && args[0] == "serve" && args[1] == "committee-flow-evidence" {
			return runServeCommitteeFlow(ctx, args[2:], stdout, stderr)
		}
		fmt.Fprintf(stderr, "unknown command: %s\n\n%s", strings.Join(args, " "), usage)
		return 2
	}

	switch args[2] {
	case "build-organization-queries":
		return runOrganizations(ctx, args[2], args[3:], stdout, stderr)
	case "review-report-period-membership":
		return runReportPeriods(ctx, args[3:], stdout, stderr)
	case "review-report-field-binding":
		return runReportBinding(ctx, args[3:], stdout, stderr)
	case "review-report-window":
		return runReportWindow(ctx, args[3:], stdout, stderr)
	case "compare-summary-report-window":
		return runSummaryWindow(ctx, args[3:], stdout, stderr)
	case "compare-summary-report-window-v2":
		return runSummaryWindowVersion(ctx, args[2], args[3:], stdout, stderr)
	case "compare-receipt-report-window":
		return runReceiptWindow(ctx, args[3:], stdout, stderr)
	case "compare-receipt-families":
		return runReceiptComparison(ctx, args[2], args[3:], stdout, stderr)
	case "review-receipt-family-absence":
		return runReceiptComparison(ctx, args[2], args[3:], stdout, stderr)
	case "compare-receipt-family-window":
		return runReceiptComparison(ctx, args[2], args[3:], stdout, stderr)
	case "compare-receipt-family-summary":
		return runReceiptComparison(ctx, args[2], args[3:], stdout, stderr)
	case "review-report-unitemized":
		return runReportUnitemized(ctx, args[3:], stdout, stderr)
	case "compare-report-total-receipts":
		return runReportTotalReceipts(ctx, args[3:], stdout, stderr)
	case "review-report-scope":
		return runReportScope(ctx, args[3:], stdout, stderr)
	case "capture-report-metadata":
		return runCaptureReportMetadata(ctx, args[3:], stdout, stderr)
	case "review-report-metadata":
		return runReportMetadata(ctx, args[3:], stdout, stderr)
	case "verify-committee-summary":
		return runVerifyCommitteeSummary(ctx, args[3:], stdout, stderr)
	case "publish-committee-summary":
		return runPublishCommitteeSummary(ctx, args[3:], stdout, stderr)
	case "calculate-committee-summary-assertions":
		return runSummaryAssertions(ctx, args[3:], stdout, stderr)
	case "review-summary-receipt-compatibility":
		return runSummaryReceiptReview(ctx, args[3:], stdout, stderr)
	case "profile-receipt-report-scope":
		return runReceiptReportProfile(ctx, args[3:], stdout, stderr)
	case "review-funding-report-lines":
		return runReportLines(ctx, args[3:], stdout, stderr)
	case "calculate-committee-funding-basis", "list-funding-receipts", "assess-candidate-funding-basis", "review-funding-component", "inspect-funding-receipts", "review-funding-report", "audit-funding-coverage":
		return runFundingBasis(ctx, args[2], args[3:], stdout, stderr)
	case "trace-candidate-committee-receipts":
		return runCandidateUpstream(ctx, args[3:], stdout, stderr)
	case "build-candidate-evidence":
		return runCandidateEvidence(ctx, args[3:], stdout, stderr)
	case "build-candidate-dossier":
		return runCandidateDossier(ctx, args[3:], stdout, stderr)
	case "compare-terminal-policies":
		return runTerminalPolicyComparison(ctx, args[3:], stdout, stderr)
	case "calculate-direct-source-attribution":
		return runDirectSourceAttribution(ctx, args[3:], stdout, stderr)
	case "inspect-candidate-connection":
		return runCandidateConnection(ctx, args[3:], stdout, stderr)
	case "benchmark-receipt-reference-index":
		return runReceiptIndexBenchmark(ctx, args[3:], stdout, stderr)
	case "join-receipt-references":
		return runReceiptReferences(ctx, args[3:], stdout, stderr)
	case "build-receipt-reference-topology":
		return runReceiptReferenceTopology(ctx, args[3:], stdout, stderr)
	case "publish-receipt-participants", "benchmark-receipt-participants", "inspect-receipt-participant":
		return runReceiptParticipants(ctx, args[2], args[3:], stdout, stderr)
	case "publish-receipt-conduit-associations":
		return runReceiptConduits(ctx, args[3:], stdout, stderr)
	case "publish-shared-receipt-conduit-associations":
		return runReceiptConduitCommand(ctx, args[3:], stdout, stderr, false, true)
	case "publish-shared-conduit-generation":
		return runSharedConduitGeneration(ctx, args[3:], stdout, stderr)
	case "inspect-funding-recovery-inventory":
		return runFundingRecovery(ctx, args[3:], stdout, stderr)
	case "plan-funding-recovery":
		return runFundingRecoveryPlan(ctx, args[3:], stdout, stderr)
	case "verify-funding-recovery-files":
		return runFundingRecoveryVerify(ctx, args[3:], stdout, stderr)
	case "review-release-stage-evidence":
		return runStageEvidenceReview(ctx, args[3:], stdout, stderr)
	case "validate-shared-conduit-queries":
		return runSharedConduitQueries(ctx, args[3:], stdout, stderr)
	case "profile-shared-receipt-references":
		return runReceiptConduitCommand(ctx, args[3:], stdout, stderr, true, false)
	case "review-shared-receipt-references":
		return runSharedReferenceReview(ctx, args[3:], stdout, stderr)
	case "benchmark-arango-receipt-participants":
		return runReceiptGraph(ctx, args[3:], stdout, stderr)
	case "publish-arango-receipt-participants":
		return runReceiptGraphMode(ctx, args[3:], stdout, stderr, true)
	case "inspect-receipt-candidate-connection":
		return runReceiptConnection(ctx, args[3:], stdout, stderr)
	case "validate-receipt-candidate-connections":
		return runReceiptConnectionMode(ctx, args[3:], stdout, stderr, true)
	case "verify-funding-generation":
		return runFundingGeneration(ctx, args[3:], stdout, stderr)
	case "inspect-funding-neighborhood":
		return runFundingNeighborhood(ctx, args[3:], stdout, stderr, false)
	case "inspect-funding-paths":
		return runFundingPaths(ctx, args[3:], stdout, stderr, false)
	case "inspect-funding-window-paths":
		return runFundingWindow(ctx, args[3:], stdout, stderr)
	case "inspect-funding-window-connections":
		return runFundingWindowQuery(ctx, args[3:], stdout, stderr, true)
	case "validate-funding-paths":
		return runFundingPaths(ctx, args[3:], stdout, stderr, true)
	case "assess-terminal-sources":
		return runTerminalAssessment(ctx, args[3:], stdout, stderr)
	case "profile-terminal-receipt-roles":
		return runTerminalEvidence(ctx, args[3:], stdout, stderr, true)
	case "build-reported-identity-assertions":
		return runIdentityAssertions(ctx, args[3:], stdout, stderr)
	case "validate-funding-neighborhoods":
		return runFundingNeighborhood(ctx, args[3:], stdout, stderr, true)
	case "probe-arango-committee-flow-evidence":
		return runCommitteeFlowGraph(ctx, args[3:], stdout, stderr)
	case "discover":
		return runDiscover(ctx, args[3:], stdout, stderr)
	case "plan-release":
		return runPlanRelease(args[3:], stdout, stderr)
	case "review-release-storage":
		return runReviewReleaseStorage(args[3:], stdout, stderr)
	case "acquire":
		return runAcquire(ctx, args[3:], stdout, stderr)
	case "stage-release":
		return runStageRelease(ctx, args[3:], stdout, stderr)
	case "publish-release":
		return runPublishRelease(ctx, args[3:], stdout, stderr)
	case "publish-classic-occurrences":
		return runPublishClassicOccurrences(ctx, args[3:], stdout, stderr)
	case "publish-classic-facts":
		return runPublishClassicFacts(ctx, args[3:], stdout, stderr)
	case "publish-schedule-a-occurrences":
		return runPublishScheduleAOccurrences(ctx, args[3:], stdout, stderr)
	case "publish-schedule-a-compact-occurrences":
		return runPublishScheduleACompactOccurrences(ctx, args[3:], stdout, stderr)
	case "publish-schedule-a-facts":
		return runPublishScheduleAFacts(ctx, args[3:], stdout, stderr)
	case "publish-schedule-a-columnar-facts":
		return runPublishScheduleAColumnarFacts(ctx, args[3:], stdout, stderr)
	case "publish-schedule-b-columnar-facts":
		return runPublishScheduleBColumnarFacts(ctx, args[3:], stdout, stderr)
	case "publish-schedule-e-occurrences":
		return runPublishScheduleEOccurrences(ctx, args[3:], stdout, stderr)
	case "publish-schedule-e-facts":
		return runPublishScheduleEFacts(ctx, args[3:], stdout, stderr)
	case "publish-effective-independent-expenditures":
		return runPublishEffectiveIndependentExpenditures(ctx, args[3:], stdout, stderr)
	case "publish-independent-expenditure-candidate-resolution":
		return runPublishIndependentExpenditureCandidateResolution(ctx, args[3:], stdout, stderr)
	case "publish-independent-expenditure-candidate-interpretations":
		return runPublishCandidateInterpretations(ctx, args[3:], stdout, stderr)
	case "publish-resolved-independent-expenditures":
		return runPublishResolvedIndependentExpenditures(ctx, args[3:], stdout, stderr)
	case "publish-independent-expenditure-projection-bundle":
		return runPublishIndependentExpenditureProjectionBundle(ctx, args[3:], stdout, stderr)
	case "publish-resolved-independent-expenditure-projection-bundle":
		return runPublishResolvedIndependentExpenditureProjectionBundle(ctx, args[3:], stdout, stderr)
	case "publish-candidate-itemized-receipts-fact-bundle":
		return runPublishCandidateItemizedReceiptsFactBundle(ctx, args[3:], stdout, stderr)
	case "publish-candidate-itemized-receipts":
		return runPublishCandidateItemizedReceipts(ctx, args[3:], stdout, stderr)
	case "publish-candidate-itemized-receipts-compact":
		return runPublishCandidateItemizedReceiptsCompact(ctx, args[3:], stdout, stderr)
	case "publish-receiver-committee-flows":
		return runPublishReceiverCommitteeFlows(ctx, args[3:], stdout, stderr)
	case "publish-receiver-committee-flow-projection-bundle":
		return runPublishReceiverCommitteeFlowProjectionBundle(ctx, args[3:], stdout, stderr)
	case "publish-receiver-flow-committee-identities":
		return runPublishReceiverFlowCommitteeIdentities(ctx, args[3:], stdout, stderr)
	case "publish-receiver-committee-flow-projection-bundle-v2":
		return runPublishReceiverCommitteeFlowProjectionBundleV2(ctx, args[3:], stdout, stderr)
	case "probe-arango-candidate-receipts":
		return runProbeArangoCandidateReceipts(ctx, args[3:], stdout, stderr)
	case "probe-arango-receiver-committee-flows":
		return runProbeArangoReceiverCommitteeFlows(ctx, args[3:], stdout, stderr)
	case "probe-arango-receiver-committee-flows-v2":
		return runProbeArangoReceiverCommitteeFlowsV2(ctx, args[3:], stdout, stderr)
	case "probe-arango-independent-expenditures":
		return runProbeArangoIndependentExpenditures(ctx, args[3:], stdout, stderr)
	case "probe-arango-resolved-independent-expenditures":
		return runProbeArangoResolvedIndependentExpenditures(ctx, args[3:], stdout, stderr)
	case "probe-candidate-itemized-receipts":
		return runProbeCandidateItemizedReceipts(ctx, args[3:], stdout, stderr)
	case "probe-receiver-committee-flows":
		return runProbeReceiverCommitteeFlows(ctx, args[3:], stdout, stderr)
	case "benchmark-schedule-a-layout":
		return runBenchmarkScheduleALayout(ctx, args[3:], stdout, stderr)
	case "verify-schedule-a":
		return runVerifyScheduleA(ctx, args[3:], stdout, stderr)
	case "verify-schedule-b":
		return runVerifyScheduleB(ctx, args[3:], stdout, stderr)
	case "verify-schedule-e":
		return runVerifyScheduleE(ctx, args[3:], stdout, stderr)
	case "audit-classic-flows":
		return runAuditClassicFlows(ctx, args[3:], stdout, stderr)
	case "audit-receipt-master-gaps":
		return runAuditReceiptMasterGaps(ctx, args[3:], stdout, stderr)
	case "audit-receiver-flow-master-gaps":
		return runAuditReceiverFlowMasterGaps(ctx, args[3:], stdout, stderr)
	case "audit-schedule-a-overlap":
		return runAuditScheduleAOverlap(ctx, args[3:], stdout, stderr)
	case "audit-schedule-ab-alignment":
		return runAuditScheduleABAlignment(ctx, args[3:], stdout, stderr)
	case "audit-schedule-b-overlap":
		return runAuditScheduleBOverlap(ctx, args[3:], stdout, stderr)
	case "audit-schedule-b-semantics":
		return runAuditScheduleBSemantics(ctx, args[3:], stdout, stderr)
	case "audit-pre-attribution-interpretations":
		return runAuditPreAttributionInterpretations(ctx, args[3:], stdout, stderr)
	case "calculate-disbursement-reporting":
		return calculateDisbursementReporting(ctx, args[3:], stdout, stderr)
	case "reconcile-committee-flows":
		return runReconcileCommitteeFlows(ctx, args[3:], stdout, stderr)
	case "publish-committee-flow-reconciliation":
		return runCommitteeFlowCalculation(ctx, args[3:], stdout, stderr, true)
	case "publish-committee-flow-comparison-candidates":
		return runPublishCommitteeFlowComparisonCandidates(ctx, args[3:], stdout, stderr)
	case "publish-committee-flow-evidence-bundle":
		return runCommitteeFlowBundle(ctx, args[3:], stdout, stderr, false)
	case "verify-committee-flow-evidence-bundle":
		return runCommitteeFlowBundle(ctx, args[3:], stdout, stderr, true)
	case "review-committee-flows":
		return runReviewCommitteeFlows(ctx, args[3:], stdout, stderr)
	default:
		fmt.Fprintf(stderr, "unknown FEC command %q\n\n%s", args[2], usage)
		return 2
	}
}

func runAuditScheduleABAlignment(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	flags := flag.NewFlagSet("audit-schedule-ab-alignment", flag.ContinueOnError)
	flags.SetOutput(stderr)
	storageRoot := flags.String("storage-root", "", "root of Legal Tender durable storage")
	cycle := flags.String("cycle", "", "FEC two-year transaction period")
	scheduleAFacts := flags.String("schedule-a-facts", "", "published Schedule A columnar fact manifest")
	scheduleARelease := flags.String("schedule-a-release", "", "exact source-release manifest referenced by the Schedule A facts")
	scheduleBDump := flags.String("schedule-b-dump", "", "pinned official Schedule B PostgreSQL custom archive")
	scheduleBObservation := flags.String("schedule-b-observation", "", "pinned Schedule B archive observation JSON")
	workDir := flags.String("work-dir", "", "parent for automatically removed Schedule B uniqueness shards")
	workers := flags.Int("workers", 0, "Schedule A Parquet scan workers; zero uses a bounded machine default")
	maxScheduleBRows := flags.Uint64("max-schedule-b-rows", 0, "stop after this many Schedule B rows; zero audits the complete relation")
	pgRestore := flags.String("pg-restore", "pg_restore", "pg_restore executable used to stream Schedule B")
	if err := flags.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 2
	}
	if flags.NArg() != 0 {
		fmt.Fprintf(stderr, "unexpected positional arguments: %s\n", strings.Join(flags.Args(), " "))
		return 2
	}
	if *storageRoot == "" || *scheduleAFacts == "" || *scheduleARelease == "" ||
		*scheduleBDump == "" || *scheduleBObservation == "" {
		_, _ = io.WriteString(stderr, "--storage-root, --schedule-a-facts, --schedule-a-release, --schedule-b-dump, and --schedule-b-observation are required\n")
		return 2
	}
	if !validPeriod(*cycle) {
		_, _ = io.WriteString(stderr, "--cycle must be a four-digit even year\n")
		return 2
	}
	if *workers < 0 {
		_, _ = io.WriteString(stderr, "--workers cannot be negative\n")
		return 2
	}
	result, auditErr := fecscheduleab.Audit(ctx, fecscheduleab.Options{
		StorageRoot: *storageRoot, ScheduleAFactManifestPath: *scheduleAFacts,
		ScheduleAReleaseManifestPath: *scheduleARelease, ScheduleBDumpPath: *scheduleBDump,
		ScheduleBObservationPath: *scheduleBObservation, Cycle: *cycle, WorkDir: *workDir,
		Workers: *workers, MaxScheduleBRows: *maxScheduleBRows, PGRestorePath: *pgRestore,
		Progress: func(message string) { fmt.Fprintln(stderr, message) },
	})
	if err := encodeJSON(stdout, result); err != nil {
		fmt.Fprintf(stderr, "encode Schedule A/B alignment result: %v\n", err)
		return 1
	}
	if auditErr != nil {
		fmt.Fprintf(stderr, "Schedule A/B alignment audit failed: %v\n", auditErr)
		return 1
	}
	return 0
}

type repeatedStringFlag []string

func (values *repeatedStringFlag) String() string { return strings.Join(*values, ",") }

func (values *repeatedStringFlag) Set(value string) error {
	if value == "" {
		return errors.New("path must not be empty")
	}
	*values = append(*values, value)
	return nil
}

func runAuditReceiptMasterGaps(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	flags := flag.NewFlagSet("audit-receipt-master-gaps", flag.ContinueOnError)
	flags.SetOutput(stderr)
	cycle := flags.String("cycle", "", "FEC two-year cycle used by the receipt calculation")
	storageRoot := flags.String("storage-root", "", "root of Legal Tender durable storage")
	calculation := flags.String("calculation", "", "exact compact candidate-receipt calculation manifest")
	candidateFacts := flags.String("candidate-facts", "", "exact current-cycle candidate-master fact manifest")
	committeeFacts := flags.String("committee-facts", "", "exact current-cycle committee-master fact manifest")
	var candidateComparisons repeatedStringFlag
	var committeeComparisons repeatedStringFlag
	var candidateHistory repeatedStringFlag
	var committeeHistory repeatedStringFlag
	flags.Var(&candidateComparisons, "candidate-comparison-facts", "different-release same-cycle candidate-master fact manifest; repeat if needed")
	flags.Var(&committeeComparisons, "committee-comparison-facts", "different-release same-cycle committee-master fact manifest; repeat if needed")
	flags.Var(&candidateHistory, "candidate-history-facts", "other-cycle candidate-master fact manifest; repeat for each cycle")
	flags.Var(&committeeHistory, "committee-history-facts", "other-cycle committee-master fact manifest; repeat for each cycle")
	if err := flags.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 2
	}
	if flags.NArg() != 0 {
		fmt.Fprintf(stderr, "unexpected positional arguments: %s\n", strings.Join(flags.Args(), " "))
		return 2
	}
	if !validPeriod(*cycle) || *storageRoot == "" {
		_, _ = io.WriteString(stderr, "--cycle must be a four-digit even year and --storage-root is required\n")
		return 2
	}
	if *calculation == "" {
		*calculation = filepath.Join(*storageRoot, "calculations", "fec", "candidate-itemized-individual-receipts", "compact", "current", *cycle+".json")
	}
	if *candidateFacts == "" {
		*candidateFacts = filepath.Join(*storageRoot, "facts", "fec", "classic", "candidate-master", "current", *cycle+".json")
	}
	if *committeeFacts == "" {
		*committeeFacts = filepath.Join(*storageRoot, "facts", "fec", "classic", "committee-master", "current", *cycle+".json")
	}
	if len(candidateComparisons) == 0 || len(committeeComparisons) == 0 || len(candidateHistory) == 0 || len(committeeHistory) == 0 {
		_, _ = io.WriteString(stderr, "provide same-cycle --candidate-comparison-facts and --committee-comparison-facts, then repeat the history flags for every audited other cycle\n")
		return 2
	}

	report, auditErr := fecmastergaps.Audit(ctx, fecmastergaps.Options{
		StorageRoot: *storageRoot, Cycle: *cycle,
		CalculationManifestPath: *calculation,
		CandidateManifestPath:   *candidateFacts, CommitteeManifestPath: *committeeFacts,
		CandidateComparisonManifestPaths: append([]string(nil), candidateComparisons...),
		CommitteeComparisonManifestPaths: append([]string(nil), committeeComparisons...),
		CandidateHistoryManifestPaths:    append([]string(nil), candidateHistory...),
		CommitteeHistoryManifestPaths:    append([]string(nil), committeeHistory...),
		Progress:                         func(message string) { fmt.Fprintln(stderr, message) },
	})
	if err := encodeJSON(stdout, report); err != nil {
		fmt.Fprintf(stderr, "encode result: %v\n", err)
		return 1
	}
	if auditErr != nil {
		fmt.Fprintf(stderr, "receipt-master-gap audit failed: %v\n", auditErr)
		return 1
	}
	return 0
}

func runAuditReceiverFlowMasterGaps(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	flags := flag.NewFlagSet("audit-receiver-flow-master-gaps", flag.ContinueOnError)
	flags.SetOutput(stderr)
	cycle := flags.String("cycle", "", "FEC two-year cycle used by the receiver-flow graph")
	storageRoot := flags.String("storage-root", "", "root of Legal Tender durable storage")
	bundle := flags.String("projection-bundle", "", "exact receiver-flow projection-readiness bundle")
	linkageFacts := flags.String("linkage-facts", "", "same-release candidate-committee-linkage fact manifest")
	allCandidateSummary := flags.String("all-candidates-summary-facts", "", "same-release all-candidates summary fact manifest")
	currentCampaignSummary := flags.String("current-campaigns-summary-facts", "", "same-release current-campaigns summary fact manifest")
	traceSourceReceipts := flags.Bool("trace-source-receipts", false, "scan exact Schedule A facts for rows behind IDs absent from every audited master")
	var committeeComparisons repeatedStringFlag
	var committeeHistory repeatedStringFlag
	var rawHistory repeatedStringFlag
	flags.Var(&committeeComparisons, "committee-comparison-facts", "different-release same-cycle committee-master fact manifest; repeat if needed")
	flags.Var(&committeeHistory, "committee-history-facts", "other-cycle normalized committee-master fact manifest; repeat for each cycle")
	flags.Var(&rawHistory, "historical-committee-archive", "official historical committee-master archive as CYCLE=PATH; repeat for each cycle")
	if err := flags.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 2
	}
	if flags.NArg() != 0 {
		fmt.Fprintf(stderr, "unexpected positional arguments: %s\n", strings.Join(flags.Args(), " "))
		return 2
	}
	if !validPeriod(*cycle) || *storageRoot == "" {
		_, _ = io.WriteString(stderr, "--cycle must be a four-digit even year and --storage-root is required\n")
		return 2
	}
	if *bundle == "" {
		*bundle = filepath.Join(*storageRoot, "bundles", "fec", "receiver-reported-committee-flow-projection", "current", *cycle+".json")
	}
	if *linkageFacts == "" {
		*linkageFacts = filepath.Join(*storageRoot, "facts", "fec", "classic", "candidate-committee-linkage", "current", *cycle+".json")
	}
	if *allCandidateSummary == "" {
		*allCandidateSummary = filepath.Join(*storageRoot, "facts", "fec", "classic", "all-candidates-summary", "current", *cycle+".json")
	}
	if *currentCampaignSummary == "" {
		*currentCampaignSummary = filepath.Join(*storageRoot, "facts", "fec", "classic", "current-campaigns-summary", "current", *cycle+".json")
	}
	archives := make([]fecflowmastergaps.RawHistoryArchiveInput, 0, len(rawHistory))
	for _, specification := range rawHistory {
		archiveCycle, archivePath, found := strings.Cut(specification, "=")
		if !found || !validPeriod(archiveCycle) || archivePath == "" {
			fmt.Fprintf(stderr, "invalid --historical-committee-archive %q; expected CYCLE=PATH\n", specification)
			return 2
		}
		archives = append(archives, fecflowmastergaps.RawHistoryArchiveInput{Cycle: archiveCycle, Path: archivePath})
	}
	if len(committeeComparisons) == 0 || len(committeeHistory)+len(archives) == 0 {
		_, _ = io.WriteString(stderr, "provide at least one --committee-comparison-facts and one normalized or raw history input\n")
		return 2
	}

	report, auditErr := fecflowmastergaps.Audit(ctx, fecflowmastergaps.Options{
		StorageRoot: *storageRoot, Cycle: *cycle, ReadinessBundlePath: *bundle,
		CommitteeComparisonManifestPaths: append([]string(nil), committeeComparisons...),
		CommitteeHistoryManifestPaths:    append([]string(nil), committeeHistory...),
		LinkageManifestPath:              *linkageFacts,
		SummaryManifestPaths:             []string{*allCandidateSummary, *currentCampaignSummary},
		RawHistoryArchives:               archives,
		TraceSourceReceipts:              *traceSourceReceipts,
		Progress:                         func(message string) { fmt.Fprintln(stderr, message) },
	})
	if err := encodeJSON(stdout, report); err != nil {
		fmt.Fprintf(stderr, "encode result: %v\n", err)
		return 1
	}
	if auditErr != nil {
		fmt.Fprintf(stderr, "receiver-flow-master-gap audit failed: %v\n", auditErr)
		return 1
	}
	return 0
}

func runProbeReceiverCommitteeFlows(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	flags := flag.NewFlagSet("probe-receiver-committee-flows", flag.ContinueOnError)
	flags.SetOutput(stderr)
	cycle := flags.String("cycle", "", "FEC two-year cycle to inspect")
	storageRoot := flags.String("storage-root", "", "root of Legal Tender durable storage")
	manifest := flags.String("schedule-a-facts", "", "exact Schedule A columnar fact manifest")
	workers := flags.Int("workers", 0, "parallel Parquet readers; zero uses up to 16 logical CPUs")
	topEdges := flags.Int("top-edges", 50, "number of exact-match edge groups to retain in diagnostic output")
	if err := flags.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 2
	}
	if flags.NArg() != 0 {
		fmt.Fprintf(stderr, "unexpected positional arguments: %s\n", strings.Join(flags.Args(), " "))
		return 2
	}
	if !validPeriod(*cycle) || *storageRoot == "" || *workers < 0 || *topEdges <= 0 {
		_, _ = io.WriteString(stderr, "--cycle and --storage-root are required; workers must be nonnegative and top-edges positive\n")
		return 2
	}
	if *manifest == "" {
		*manifest = filepath.Join(*storageRoot, "facts", "fec", "schedule-a", "columnar", "current", *cycle+".json")
	}
	result, probeErr := feccommitteeflow.Probe(ctx, feccommitteeflow.Options{
		StorageRoot: *storageRoot, ManifestPath: *manifest, Cycle: *cycle,
		Workers: *workers, TopEdges: *topEdges,
		Progress: func(message string) { fmt.Fprintln(stderr, message) },
	})
	if err := encodeJSON(stdout, result); err != nil {
		fmt.Fprintf(stderr, "encode result: %v\n", err)
		return 1
	}
	if probeErr != nil {
		fmt.Fprintf(stderr, "receiver-committee-flow probe failed: %v\n", probeErr)
		return 1
	}
	return 0
}

func runPublishReceiverCommitteeFlows(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	flags := flag.NewFlagSet("publish-receiver-committee-flows", flag.ContinueOnError)
	flags.SetOutput(stderr)
	cycle := flags.String("cycle", "", "FEC two-year cycle to calculate")
	storageRoot := flags.String("storage-root", "", "root of Legal Tender durable storage")
	columnarFacts := flags.String("schedule-a-facts", "", "exact Schedule A columnar fact manifest; defaults to the cycle pointer")
	currentPath := flags.String("current", "", "path to this cycle's active receiver-flow calculation manifest")
	runID := flags.String("run-id", "", "stable orchestration run identity")
	workers := flags.Int("workers", 0, "parallel Parquet readers; zero uses up to 16 logical CPUs")
	if err := flags.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 2
	}
	if flags.NArg() != 0 {
		fmt.Fprintf(stderr, "unexpected positional arguments: %s\n", strings.Join(flags.Args(), " "))
		return 2
	}
	if !validPeriod(*cycle) || *storageRoot == "" || *runID == "" || *workers < 0 {
		_, _ = io.WriteString(stderr, "--cycle must be a four-digit even year; --storage-root and --run-id are required; workers must be nonnegative\n")
		return 2
	}
	if *columnarFacts == "" {
		*columnarFacts = filepath.Join(*storageRoot, "facts", "fec", "schedule-a", "columnar", "current", *cycle+".json")
	}
	manifest, err := feccommitteeflows.Publish(
		ctx,
		feccommitteeflows.PublishInput{ColumnarFactManifestPath: *columnarFacts},
		*runID,
		feccommitteeflows.PublishOptions{
			StorageRoot: *storageRoot, CurrentManifestPath: *currentPath, Workers: *workers, Clock: time.Now,
			Progress: func(message string) { fmt.Fprintln(stderr, message) },
		},
	)
	if err != nil {
		fmt.Fprintf(stderr, "publish receiver-reported committee flows: %v\n", err)
		return 1
	}
	if manifest.Cycle != *cycle {
		fmt.Fprintf(stderr, "receiver-flow calculation returned cycle %s, expected %s\n", manifest.Cycle, *cycle)
		return 1
	}
	if err := encodeJSON(stdout, manifest); err != nil {
		fmt.Fprintf(stderr, "encode receiver-flow calculation manifest: %v\n", err)
		return 1
	}
	return 0
}

func runPublishReceiverCommitteeFlowProjectionBundle(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	flags := flag.NewFlagSet("publish-receiver-committee-flow-projection-bundle", flag.ContinueOnError)
	flags.SetOutput(stderr)
	cycle := flags.String("cycle", "", "FEC two-year cycle to bundle")
	storageRoot := flags.String("storage-root", "", "root of Legal Tender durable storage")
	calculation := flags.String("calculation", "", "receiver-flow calculation manifest; defaults to the cycle pointer")
	committeeFacts := flags.String("committee-facts", "", "committee-master fact manifest; defaults to the cycle pointer")
	currentPath := flags.String("current", "", "path to this cycle's active receiver-flow projection bundle")
	runID := flags.String("run-id", "", "stable orchestration run identity")
	if err := flags.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 2
	}
	if flags.NArg() != 0 {
		fmt.Fprintf(stderr, "unexpected positional arguments: %s\n", strings.Join(flags.Args(), " "))
		return 2
	}
	if !validPeriod(*cycle) || *storageRoot == "" || *runID == "" {
		_, _ = io.WriteString(stderr, "--cycle must be a four-digit even year; --storage-root and --run-id are required\n")
		return 2
	}
	if *calculation == "" {
		*calculation = filepath.Join(*storageRoot, "calculations", "fec", "receiver-reported-committee-flows", "current", *cycle+".json")
	}
	if *committeeFacts == "" {
		*committeeFacts = filepath.Join(*storageRoot, "facts", "fec", "classic", "committee-master", "current", *cycle+".json")
	}
	manifest, err := feccommitteeflows.PublishProjectionBundle(ctx, feccommitteeflows.ProjectionBundleInput{
		CalculationManifestPath: *calculation,
		CommitteeManifestPath:   *committeeFacts,
	}, *runID, feccommitteeflows.ProjectionBundleOptions{
		StorageRoot: *storageRoot, CurrentManifestPath: *currentPath,
		ExpectedCycle: *cycle, Clock: time.Now,
	})
	if err != nil {
		fmt.Fprintf(stderr, "publish receiver-flow projection bundle: %v\n", err)
		return 1
	}
	if err := encodeJSON(stdout, manifest); err != nil {
		fmt.Fprintf(stderr, "encode receiver-flow projection bundle: %v\n", err)
		return 1
	}
	return 0
}

func runPublishReceiverFlowCommitteeIdentities(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	flags := flag.NewFlagSet("publish-receiver-flow-committee-identities", flag.ContinueOnError)
	flags.SetOutput(stderr)
	cycle := flags.String("cycle", "", "FEC two-year cycle to classify")
	storageRoot := flags.String("storage-root", "", "root of Legal Tender durable storage")
	bundle := flags.String("projection-bundle", "", "exact v1 receiver-flow projection-readiness bundle")
	linkageFacts := flags.String("linkage-facts", "", "same-release candidate-committee-linkage fact manifest used by the audit gate")
	allCandidateSummary := flags.String("all-candidates-summary-facts", "", "same-release all-candidates summary fact manifest used by the audit gate")
	currentCampaignSummary := flags.String("current-campaigns-summary-facts", "", "same-release current-campaigns summary fact manifest used by the audit gate")
	currentPath := flags.String("current", "", "path to this cycle's active committee-identity calculation manifest")
	runID := flags.String("run-id", "", "stable orchestration run identity")
	var committeeComparisons repeatedStringFlag
	var committeeHistory repeatedStringFlag
	var rawHistory repeatedStringFlag
	flags.Var(&committeeComparisons, "committee-comparison-facts", "different-release same-cycle committee-master fact manifest; repeat if needed")
	flags.Var(&committeeHistory, "committee-history-facts", "other-cycle normalized committee-master fact manifest; repeat for each cycle")
	flags.Var(&rawHistory, "historical-committee-archive", "official historical committee-master archive as CYCLE=PATH; repeat for each cycle")
	if err := flags.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 2
	}
	if flags.NArg() != 0 {
		fmt.Fprintf(stderr, "unexpected positional arguments: %s\n", strings.Join(flags.Args(), " "))
		return 2
	}
	if !validPeriod(*cycle) || *storageRoot == "" || *runID == "" {
		_, _ = io.WriteString(stderr, "--cycle must be a four-digit even year; --storage-root and --run-id are required\n")
		return 2
	}
	if *bundle == "" {
		*bundle = filepath.Join(*storageRoot, "bundles", "fec", "receiver-reported-committee-flow-projection", "current", *cycle+".json")
	}
	if *linkageFacts == "" {
		*linkageFacts = filepath.Join(*storageRoot, "facts", "fec", "classic", "candidate-committee-linkage", "current", *cycle+".json")
	}
	if *allCandidateSummary == "" {
		*allCandidateSummary = filepath.Join(*storageRoot, "facts", "fec", "classic", "all-candidates-summary", "current", *cycle+".json")
	}
	if *currentCampaignSummary == "" {
		*currentCampaignSummary = filepath.Join(*storageRoot, "facts", "fec", "classic", "current-campaigns-summary", "current", *cycle+".json")
	}
	archives := make([]fecflowmastergaps.RawHistoryArchiveInput, 0, len(rawHistory))
	for _, specification := range rawHistory {
		archiveCycle, archivePath, found := strings.Cut(specification, "=")
		if !found || !validPeriod(archiveCycle) || archivePath == "" {
			fmt.Fprintf(stderr, "invalid --historical-committee-archive %q; expected CYCLE=PATH\n", specification)
			return 2
		}
		archives = append(archives, fecflowmastergaps.RawHistoryArchiveInput{Cycle: archiveCycle, Path: archivePath})
	}
	if len(committeeComparisons) == 0 || len(committeeHistory)+len(archives) == 0 {
		_, _ = io.WriteString(stderr, "provide at least one --committee-comparison-facts and one normalized or raw history input\n")
		return 2
	}
	manifest, err := feccommitteeidentity.Publish(ctx, feccommitteeidentity.PublishInput{
		ReadinessBundlePath:              *bundle,
		CommitteeComparisonManifestPaths: append([]string(nil), committeeComparisons...),
		CommitteeHistoryManifestPaths:    append([]string(nil), committeeHistory...),
		RawHistoryArchives:               archives, LinkageManifestPath: *linkageFacts,
		SummaryManifestPaths: []string{*allCandidateSummary, *currentCampaignSummary},
	}, *runID, feccommitteeidentity.PublishOptions{
		StorageRoot: *storageRoot, Cycle: *cycle, CurrentManifestPath: *currentPath,
		Clock: time.Now, Progress: func(message string) { fmt.Fprintln(stderr, message) },
	})
	if err != nil {
		fmt.Fprintf(stderr, "publish receiver-flow committee identities: %v\n", err)
		return 1
	}
	if err := encodeJSON(stdout, manifest); err != nil {
		fmt.Fprintf(stderr, "encode committee-identity manifest: %v\n", err)
		return 1
	}
	return 0
}

func runPublishReceiverCommitteeFlowProjectionBundleV2(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	flags := flag.NewFlagSet("publish-receiver-committee-flow-projection-bundle-v2", flag.ContinueOnError)
	flags.SetOutput(stderr)
	cycle := flags.String("cycle", "", "FEC two-year cycle to bundle")
	storageRoot := flags.String("storage-root", "", "root of Legal Tender durable storage")
	baseBundle := flags.String("base-bundle", "", "exact shipped v1 receiver-flow readiness bundle")
	identity := flags.String("identity-calculation", "", "exact committee-identity coverage calculation")
	currentPath := flags.String("current", "", "path to this cycle's active v2 receiver-flow bundle")
	runID := flags.String("run-id", "", "stable orchestration run identity")
	if err := flags.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 2
	}
	if flags.NArg() != 0 {
		fmt.Fprintf(stderr, "unexpected positional arguments: %s\n", strings.Join(flags.Args(), " "))
		return 2
	}
	if !validPeriod(*cycle) || *storageRoot == "" || *runID == "" {
		_, _ = io.WriteString(stderr, "--cycle must be a four-digit even year; --storage-root and --run-id are required\n")
		return 2
	}
	if *baseBundle == "" {
		*baseBundle = filepath.Join(*storageRoot, "bundles", "fec", "receiver-reported-committee-flow-projection", "current", *cycle+".json")
	}
	if *identity == "" {
		*identity = filepath.Join(*storageRoot, "calculations", "fec", "receiver-flow-committee-identity-coverage", "current", *cycle+".json")
	}
	manifest, err := feccommitteeidentity.PublishProjectionBundle(ctx, feccommitteeidentity.ProjectionBundleInput{
		BaseBundlePath: *baseBundle, IdentityCalculationPath: *identity,
	}, *runID, feccommitteeidentity.ProjectionBundleOptions{
		StorageRoot: *storageRoot, CurrentManifestPath: *currentPath,
		ExpectedCycle: *cycle, Clock: time.Now,
	})
	if err != nil {
		fmt.Fprintf(stderr, "publish v2 receiver-flow projection bundle: %v\n", err)
		return 1
	}
	if err := encodeJSON(stdout, manifest); err != nil {
		fmt.Fprintf(stderr, "encode v2 receiver-flow projection bundle: %v\n", err)
		return 1
	}
	return 0
}

func runAuditClassicFlows(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	flags := flag.NewFlagSet("audit-classic-flows", flag.ContinueOnError)
	flags.SetOutput(stderr)
	pas2Path := flags.String("pas2", "", "path to the classic cycle pas2 ZIP")
	othPath := flags.String("oth", "", "path to the classic cycle oth ZIP")
	period := flags.String("period", "", "expected FEC two-year period")
	if err := flags.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 2
	}
	if flags.NArg() != 0 {
		fmt.Fprintf(stderr, "unexpected positional arguments: %s\n", strings.Join(flags.Args(), " "))
		return 2
	}
	if *pas2Path == "" || *othPath == "" {
		_, _ = io.WriteString(stderr, "--pas2 and --oth are required\n")
		return 2
	}
	if !validPeriod(*period) {
		_, _ = io.WriteString(stderr, "--period must be a four-digit even year\n")
		return 2
	}

	result, auditErr := fecclassicflow.Audit(ctx, fecclassicflow.Options{
		Pas2Path: *pas2Path,
		OthPath:  *othPath,
		Period:   *period,
		Progress: func(message string) {
			fmt.Fprintln(stderr, message)
		},
	})
	if err := encodeJSON(stdout, result); err != nil {
		fmt.Fprintf(stderr, "encode result: %v\n", err)
		return 1
	}
	if auditErr != nil {
		fmt.Fprintf(stderr, "classic-flow audit failed: %v\n", auditErr)
		return 1
	}
	return 0
}

func runProbeArangoCandidateReceipts(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	flags := flag.NewFlagSet("probe-arango-candidate-receipts", flag.ContinueOnError)
	flags.SetOutput(stderr)
	cycle := flags.String("cycle", "", "FEC two-year cycle to project")
	storageRoot := flags.String("storage-root", "", "root of Legal Tender durable storage")
	factBundle := flags.String("fact-bundle", "", "exact ready candidate-receipt fact-bundle manifest")
	calculation := flags.String("calculation", "", "exact published compact candidate-receipt calculation manifest")
	candidateFacts := flags.String("candidate-facts", "", "exact published candidate-master fact manifest")
	committeeFacts := flags.String("committee-facts", "", "exact published committee-master fact manifest")
	endpoint := flags.String("endpoint", "", "ArangoDB HTTP endpoint")
	username := flags.String("username", "", "ArangoDB username; defaults to ARANGO_USER or root")
	passwordEnv := flags.String("password-env", "ARANGO_PASSWORD", "environment variable containing the ArangoDB password")
	runID := flags.String("run-id", "", "stable probe run identity")
	batchSize := flags.Int("batch-size", 5_000, "maximum documents per ArangoDB import request")
	queryRepetitions := flags.Int("query-repetitions", 10, "measured executions per representative query")
	if err := flags.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 2
	}
	if flags.NArg() != 0 {
		fmt.Fprintf(stderr, "unexpected positional arguments: %s\n", strings.Join(flags.Args(), " "))
		return 2
	}
	if *cycle == "" || *storageRoot == "" || *endpoint == "" || *runID == "" {
		_, _ = io.WriteString(stderr, "--cycle, --storage-root, --endpoint, and --run-id are required\n")
		return 2
	}
	if *factBundle == "" {
		*factBundle = filepath.Join(*storageRoot, "bundles", "fec", "candidate-itemized-individual-receipts", "current", *cycle+".json")
	}
	if *calculation == "" {
		*calculation = filepath.Join(*storageRoot, "calculations", "fec", "candidate-itemized-individual-receipts", "compact", "current", *cycle+".json")
	}
	if *candidateFacts == "" {
		*candidateFacts = filepath.Join(*storageRoot, "facts", "fec", "classic", "candidate-master", "current", *cycle+".json")
	}
	if *committeeFacts == "" {
		*committeeFacts = filepath.Join(*storageRoot, "facts", "fec", "classic", "committee-master", "current", *cycle+".json")
	}
	if *username == "" {
		*username = os.Getenv("ARANGO_USER")
		if *username == "" {
			*username = "root"
		}
	}
	password := os.Getenv(*passwordEnv)
	if password == "" {
		fmt.Fprintf(stderr, "ArangoDB password environment variable %s is not set\n", *passwordEnv)
		return 2
	}
	result, err := projectionreceipts.Run(ctx, projectionreceipts.Input{
		StorageRoot: *storageRoot, Cycle: *cycle,
		FactBundleManifestPath: *factBundle, CalculationManifestPath: *calculation,
		CandidateManifestPath: *candidateFacts, CommitteeManifestPath: *committeeFacts,
		Endpoint: *endpoint, Username: *username, Password: password, RunID: *runID,
		BatchSize: *batchSize, QueryRepetitions: *queryRepetitions,
	}, projectionreceipts.Options{
		Clock: time.Now, Progress: func(message string) { fmt.Fprintln(stderr, message) },
	})
	if err != nil {
		fmt.Fprintf(stderr, "probe ArangoDB candidate-receipt projection: %v\n", err)
		return 1
	}
	if err := encodeJSON(stdout, result); err != nil {
		fmt.Fprintf(stderr, "encode ArangoDB candidate-receipt probe result: %v\n", err)
		return 1
	}
	return 0
}

func runProbeArangoReceiverCommitteeFlows(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	flags := flag.NewFlagSet("probe-arango-receiver-committee-flows", flag.ContinueOnError)
	flags.SetOutput(stderr)
	cycle := flags.String("cycle", "", "FEC two-year cycle to project")
	storageRoot := flags.String("storage-root", "", "root of Legal Tender durable storage")
	projectionBundle := flags.String("projection-bundle", "", "exact ready receiver-flow projection bundle")
	endpoint := flags.String("endpoint", "", "ArangoDB HTTP endpoint")
	username := flags.String("username", "", "ArangoDB username; defaults to ARANGO_USER or root")
	passwordEnv := flags.String("password-env", "ARANGO_PASSWORD", "environment variable containing the ArangoDB password")
	runID := flags.String("run-id", "", "stable probe run identity")
	batchSize := flags.Int("batch-size", 5_000, "maximum documents per ArangoDB import request")
	queryRepetitions := flags.Int("query-repetitions", 10, "measured executions per representative query")
	if err := flags.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 2
	}
	if flags.NArg() != 0 {
		fmt.Fprintf(stderr, "unexpected positional arguments: %s\n", strings.Join(flags.Args(), " "))
		return 2
	}
	if !validPeriod(*cycle) || *storageRoot == "" || *endpoint == "" || *runID == "" {
		_, _ = io.WriteString(stderr, "--cycle, --storage-root, --endpoint, and --run-id are required\n")
		return 2
	}
	if *projectionBundle == "" {
		*projectionBundle = filepath.Join(*storageRoot, "bundles", "fec", "receiver-reported-committee-flow-projection", "current", *cycle+".json")
	}
	if *username == "" {
		*username = os.Getenv("ARANGO_USER")
		if *username == "" {
			*username = "root"
		}
	}
	password := os.Getenv(*passwordEnv)
	if password == "" {
		fmt.Fprintf(stderr, "ArangoDB password environment variable %s is not set\n", *passwordEnv)
		return 2
	}
	result, err := projectioncommitteeflows.Run(ctx, projectioncommitteeflows.Input{
		StorageRoot: *storageRoot, Cycle: *cycle, ReadinessBundlePath: *projectionBundle,
		Endpoint: *endpoint, Username: *username, Password: password, RunID: *runID,
		BatchSize: *batchSize, QueryRepetitions: *queryRepetitions,
	}, projectioncommitteeflows.Options{
		Clock: time.Now, Progress: func(message string) { fmt.Fprintln(stderr, message) },
	})
	if err != nil {
		fmt.Fprintf(stderr, "probe ArangoDB receiver-flow projection: %v\n", err)
		return 1
	}
	if err := encodeJSON(stdout, result); err != nil {
		fmt.Fprintf(stderr, "encode ArangoDB receiver-flow result: %v\n", err)
		return 1
	}
	return 0
}

func runProbeArangoReceiverCommitteeFlowsV2(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	flags := flag.NewFlagSet("probe-arango-receiver-committee-flows-v2", flag.ContinueOnError)
	flags.SetOutput(stderr)
	cycle := flags.String("cycle", "", "FEC two-year cycle to project")
	storageRoot := flags.String("storage-root", "", "root of Legal Tender durable storage")
	projectionBundle := flags.String("projection-bundle", "", "exact ready v2 receiver-flow projection bundle")
	endpoint := flags.String("endpoint", "", "ArangoDB HTTP endpoint")
	username := flags.String("username", "", "ArangoDB username; defaults to ARANGO_USER or root")
	passwordEnv := flags.String("password-env", "ARANGO_PASSWORD", "environment variable containing the ArangoDB password")
	runID := flags.String("run-id", "", "stable probe run identity")
	batchSize := flags.Int("batch-size", 5_000, "maximum documents per ArangoDB import request")
	queryRepetitions := flags.Int("query-repetitions", 10, "measured executions per representative query")
	if err := flags.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 2
	}
	if flags.NArg() != 0 {
		fmt.Fprintf(stderr, "unexpected positional arguments: %s\n", strings.Join(flags.Args(), " "))
		return 2
	}
	if !validPeriod(*cycle) || *storageRoot == "" || *endpoint == "" || *runID == "" {
		_, _ = io.WriteString(stderr, "--cycle, --storage-root, --endpoint, and --run-id are required\n")
		return 2
	}
	if *projectionBundle == "" {
		*projectionBundle = filepath.Join(*storageRoot, "bundles", "fec", "receiver-reported-committee-flow-projection", "v2", "current", *cycle+".json")
	}
	if *username == "" {
		*username = os.Getenv("ARANGO_USER")
		if *username == "" {
			*username = "root"
		}
	}
	password := os.Getenv(*passwordEnv)
	if password == "" {
		fmt.Fprintf(stderr, "ArangoDB password environment variable %s is not set\n", *passwordEnv)
		return 2
	}
	result, err := projectioncommitteeflows.RunV2(ctx, projectioncommitteeflows.InputV2{
		StorageRoot: *storageRoot, Cycle: *cycle, ReadinessBundlePath: *projectionBundle,
		Endpoint: *endpoint, Username: *username, Password: password, RunID: *runID,
		BatchSize: *batchSize, QueryRepetitions: *queryRepetitions,
	}, projectioncommitteeflows.Options{
		Clock: time.Now, Progress: func(message string) { fmt.Fprintln(stderr, message) },
	})
	if err != nil {
		fmt.Fprintf(stderr, "probe ArangoDB receiver-flow v2 projection: %v\n", err)
		return 1
	}
	if err := encodeJSON(stdout, result); err != nil {
		fmt.Fprintf(stderr, "encode ArangoDB receiver-flow v2 result: %v\n", err)
		return 1
	}
	return 0
}

func runProbeArangoIndependentExpenditures(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	flags := flag.NewFlagSet("probe-arango-independent-expenditures", flag.ContinueOnError)
	flags.SetOutput(stderr)
	cycle := flags.String("cycle", "", "FEC two-year cycle to project")
	storageRoot := flags.String("storage-root", "", "root of Legal Tender durable storage")
	projectionBundle := flags.String("projection-bundle", "", "exact ready independent-expenditure projection bundle")
	calculation := flags.String("calculation", "", "exact published effective independent-expenditure calculation manifest")
	candidateFacts := flags.String("candidate-facts", "", "exact published candidate-master fact manifest")
	committeeFacts := flags.String("committee-facts", "", "exact published committee-master fact manifest")
	endpoint := flags.String("endpoint", "", "ArangoDB HTTP endpoint")
	username := flags.String("username", "", "ArangoDB username; defaults to ARANGO_USER or root")
	passwordEnv := flags.String("password-env", "ARANGO_PASSWORD", "environment variable containing the ArangoDB password")
	runID := flags.String("run-id", "", "stable probe run identity")
	batchSize := flags.Int("batch-size", 5_000, "maximum documents per ArangoDB import request")
	queryRepetitions := flags.Int("query-repetitions", 10, "measured executions per representative query")
	if err := flags.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 2
	}
	if flags.NArg() != 0 {
		fmt.Fprintf(stderr, "unexpected positional arguments: %s\n", strings.Join(flags.Args(), " "))
		return 2
	}
	if *cycle == "" || *storageRoot == "" || *endpoint == "" || *runID == "" {
		_, _ = io.WriteString(stderr, "--cycle, --storage-root, --endpoint, and --run-id are required\n")
		return 2
	}
	directInputs := *calculation != "" || *candidateFacts != "" || *committeeFacts != ""
	if *projectionBundle != "" && directInputs {
		_, _ = io.WriteString(stderr, "--projection-bundle cannot be combined with direct manifest flags\n")
		return 2
	}
	if *projectionBundle == "" && !directInputs {
		*projectionBundle = filepath.Join(*storageRoot, "bundles", "fec", "independent-expenditure-projection", "current", *cycle+".json")
	}
	if *projectionBundle == "" && (*calculation == "" || *candidateFacts == "" || *committeeFacts == "") {
		_, _ = io.WriteString(stderr, "--projection-bundle or all three direct manifest flags are required\n")
		return 2
	}
	if *username == "" {
		*username = os.Getenv("ARANGO_USER")
		if *username == "" {
			*username = "root"
		}
	}
	password := os.Getenv(*passwordEnv)
	if password == "" {
		fmt.Fprintf(stderr, "ArangoDB password environment variable %s is not set\n", *passwordEnv)
		return 2
	}
	result, err := projectionindependentexpenditures.Run(ctx, projectionindependentexpenditures.Input{
		StorageRoot: *storageRoot, Cycle: *cycle,
		ReadinessBundlePath:     *projectionBundle,
		CalculationManifestPath: *calculation,
		CandidateManifestPath:   *candidateFacts, CommitteeManifestPath: *committeeFacts,
		Endpoint: *endpoint, Username: *username, Password: password, RunID: *runID,
		BatchSize: *batchSize, QueryRepetitions: *queryRepetitions,
	}, projectionindependentexpenditures.Options{
		Clock: time.Now, Progress: func(message string) { fmt.Fprintln(stderr, message) },
	})
	if err != nil {
		fmt.Fprintf(stderr, "probe ArangoDB independent-expenditure projection: %v\n", err)
		return 1
	}
	if err := encodeJSON(stdout, result); err != nil {
		fmt.Fprintf(stderr, "encode ArangoDB independent-expenditure probe result: %v\n", err)
		return 1
	}
	return 0
}

func runProbeArangoResolvedIndependentExpenditures(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	flags := flag.NewFlagSet("probe-arango-resolved-independent-expenditures", flag.ContinueOnError)
	flags.SetOutput(stderr)
	cycle := flags.String("cycle", "", "FEC two-year cycle to project")
	storageRoot := flags.String("storage-root", "", "root of Legal Tender durable storage")
	projectionBundle := flags.String("projection-bundle", "", "exact ready resolved independent-expenditure projection bundle")
	aggregate := flags.String("aggregate", "", "exact published resolved independent-expenditure calculation manifest")
	resolution := flags.String("candidate-resolution", "", "exact candidate-resolution manifest; derived from the aggregate when omitted")
	candidateFacts := flags.String("candidate-facts", "", "exact published candidate-master fact manifest")
	committeeFacts := flags.String("committee-facts", "", "exact published committee-master fact manifest")
	endpoint := flags.String("endpoint", "", "ArangoDB HTTP endpoint")
	username := flags.String("username", "", "ArangoDB username; defaults to ARANGO_USER or root")
	passwordEnv := flags.String("password-env", "ARANGO_PASSWORD", "environment variable containing the ArangoDB password")
	runID := flags.String("run-id", "", "stable probe run identity")
	batchSize := flags.Int("batch-size", 5_000, "maximum documents per ArangoDB import request")
	queryRepetitions := flags.Int("query-repetitions", 10, "measured executions per representative query")
	if err := flags.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 2
	}
	if flags.NArg() != 0 {
		fmt.Fprintf(stderr, "unexpected positional arguments: %s\n", strings.Join(flags.Args(), " "))
		return 2
	}
	if *cycle == "" || *storageRoot == "" || *endpoint == "" || *runID == "" {
		_, _ = io.WriteString(stderr, "--cycle, --storage-root, --endpoint, and --run-id are required\n")
		return 2
	}
	directInputs := *aggregate != "" || *resolution != "" || *candidateFacts != "" || *committeeFacts != ""
	if *projectionBundle != "" && directInputs {
		_, _ = io.WriteString(stderr, "--projection-bundle cannot be combined with direct manifest flags\n")
		return 2
	}
	if *projectionBundle == "" && !directInputs {
		*projectionBundle = filepath.Join(*storageRoot, "bundles", "fec", "resolved-independent-expenditure-projection", "current", *cycle+".json")
	}
	if *projectionBundle == "" && (*aggregate == "" || *candidateFacts == "" || *committeeFacts == "") {
		_, _ = io.WriteString(stderr, "--projection-bundle or aggregate, candidate, and committee manifest flags are required\n")
		return 2
	}
	if *username == "" {
		*username = os.Getenv("ARANGO_USER")
		if *username == "" {
			*username = "root"
		}
	}
	password := os.Getenv(*passwordEnv)
	if password == "" {
		fmt.Fprintf(stderr, "ArangoDB password environment variable %s is not set\n", *passwordEnv)
		return 2
	}
	result, err := projectionindependentexpenditures.RunResolved(ctx, projectionindependentexpenditures.ResolvedInput{
		StorageRoot: *storageRoot, Cycle: *cycle, ReadinessBundlePath: *projectionBundle,
		AggregateManifestPath: *aggregate, ResolutionManifestPath: *resolution,
		CandidateManifestPath: *candidateFacts, CommitteeManifestPath: *committeeFacts,
		Endpoint: *endpoint, Username: *username, Password: password, RunID: *runID,
		BatchSize: *batchSize, QueryRepetitions: *queryRepetitions,
	}, projectionindependentexpenditures.Options{
		Clock: time.Now, Progress: func(message string) { fmt.Fprintln(stderr, message) },
	})
	if err != nil {
		fmt.Fprintf(stderr, "probe ArangoDB resolved independent-expenditure projection: %v\n", err)
		return 1
	}
	if err := encodeJSON(stdout, result); err != nil {
		fmt.Fprintf(stderr, "encode ArangoDB resolved independent-expenditure probe result: %v\n", err)
		return 1
	}
	return 0
}

func runPublishIndependentExpenditureProjectionBundle(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	flags := flag.NewFlagSet("publish-independent-expenditure-projection-bundle", flag.ContinueOnError)
	flags.SetOutput(stderr)
	cycle := flags.String("cycle", "", "FEC two-year cycle to coordinate")
	calculation := flags.String("calculation", "", "optional exact effective independent-expenditure calculation manifest; defaults to the cycle pointer")
	candidateFacts := flags.String("candidate-facts", "", "optional exact candidate-master fact manifest; defaults to the cycle pointer")
	committeeFacts := flags.String("committee-facts", "", "optional exact committee-master fact manifest; defaults to the cycle pointer")
	storageRoot := flags.String("storage-root", "", "root of Legal Tender durable storage")
	currentPath := flags.String("current", "", "path to this cycle's active projection-bundle manifest")
	runID := flags.String("run-id", "", "stable orchestration run identity")
	if err := flags.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 2
	}
	if flags.NArg() != 0 {
		fmt.Fprintf(stderr, "unexpected positional arguments: %s\n", strings.Join(flags.Args(), " "))
		return 2
	}
	if *cycle == "" || *storageRoot == "" || *runID == "" {
		_, _ = io.WriteString(stderr, "--cycle, --storage-root, and --run-id are required\n")
		return 2
	}
	if *calculation == "" {
		*calculation = filepath.Join(*storageRoot, "calculations", "fec", "effective-independent-expenditures", "current", *cycle+".json")
	}
	if *candidateFacts == "" {
		*candidateFacts = filepath.Join(*storageRoot, "facts", "fec", "classic", "candidate-master", "current", *cycle+".json")
	}
	if *committeeFacts == "" {
		*committeeFacts = filepath.Join(*storageRoot, "facts", "fec", "classic", "committee-master", "current", *cycle+".json")
	}
	manifest, err := fecindependentexpenditures.PublishProjectionBundle(ctx, fecindependentexpenditures.ProjectionBundleInput{
		CalculationManifestPath: *calculation,
		CandidateManifestPath:   *candidateFacts,
		CommitteeManifestPath:   *committeeFacts,
	}, *runID, fecindependentexpenditures.ProjectionBundleOptions{
		StorageRoot: *storageRoot, CurrentManifestPath: *currentPath,
		ExpectedCycle: *cycle, Clock: time.Now,
	})
	if err != nil {
		fmt.Fprintf(stderr, "publish independent-expenditure projection bundle: %v\n", err)
		return 1
	}
	if err := encodeJSON(stdout, manifest); err != nil {
		fmt.Fprintf(stderr, "encode independent-expenditure projection bundle manifest: %v\n", err)
		return 1
	}
	return 0
}

func runPublishResolvedIndependentExpenditureProjectionBundle(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	flags := flag.NewFlagSet("publish-resolved-independent-expenditure-projection-bundle", flag.ContinueOnError)
	flags.SetOutput(stderr)
	cycle := flags.String("cycle", "", "FEC two-year cycle to coordinate")
	aggregate := flags.String("aggregate", "", "optional exact resolved independent-expenditure calculation manifest; defaults to the cycle pointer")
	candidateFacts := flags.String("candidate-facts", "", "optional exact candidate-master fact manifest; defaults to the cycle pointer")
	committeeFacts := flags.String("committee-facts", "", "optional exact committee-master fact manifest; defaults to the cycle pointer")
	storageRoot := flags.String("storage-root", "", "root of Legal Tender durable storage")
	currentPath := flags.String("current", "", "path to this cycle's active resolved projection-bundle manifest")
	runID := flags.String("run-id", "", "stable orchestration run identity")
	if err := flags.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 2
	}
	if flags.NArg() != 0 {
		fmt.Fprintf(stderr, "unexpected positional arguments: %s\n", strings.Join(flags.Args(), " "))
		return 2
	}
	if *cycle == "" || *storageRoot == "" || *runID == "" {
		_, _ = io.WriteString(stderr, "--cycle, --storage-root, and --run-id are required\n")
		return 2
	}
	if *aggregate == "" {
		*aggregate = filepath.Join(*storageRoot, "calculations", "fec", "resolved-independent-expenditures", "current", *cycle+".json")
	}
	if *candidateFacts == "" {
		*candidateFacts = filepath.Join(*storageRoot, "facts", "fec", "classic", "candidate-master", "current", *cycle+".json")
	}
	if *committeeFacts == "" {
		*committeeFacts = filepath.Join(*storageRoot, "facts", "fec", "classic", "committee-master", "current", *cycle+".json")
	}
	manifest, err := feccandidateresolution.PublishResolvedProjectionBundle(
		ctx,
		feccandidateresolution.ResolvedProjectionBundleInput{
			AggregateManifestPath: *aggregate, CandidateManifestPath: *candidateFacts, CommitteeManifestPath: *committeeFacts,
		},
		*runID,
		feccandidateresolution.ResolvedProjectionBundleOptions{
			StorageRoot: *storageRoot, CurrentManifestPath: *currentPath, ExpectedCycle: *cycle, Clock: time.Now,
		},
	)
	if err != nil {
		fmt.Fprintf(stderr, "publish resolved independent-expenditure projection bundle: %v\n", err)
		return 1
	}
	if err := encodeJSON(stdout, manifest); err != nil {
		fmt.Fprintf(stderr, "encode resolved independent-expenditure projection bundle manifest: %v\n", err)
		return 1
	}
	return 0
}

func runPublishCandidateItemizedReceiptsCompact(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	flags := flag.NewFlagSet("publish-candidate-itemized-receipts-compact", flag.ContinueOnError)
	flags.SetOutput(stderr)
	factBundle := flags.String("fact-bundle", "", "path to the exact ready fact-bundle manifest")
	columnarFacts := flags.String("columnar-facts", "", "path to the exact published Schedule A columnar fact manifest")
	linkageFacts := flags.String("linkage-facts", "", "path to the exact published candidate-committee linkage fact manifest")
	allCandidatesFacts := flags.String("all-candidates-summary-facts", "", "path to the exact published all-candidates summary fact manifest")
	currentCampaignsFacts := flags.String("current-campaigns-summary-facts", "", "path to the exact published current-campaigns summary fact manifest")
	storageRoot := flags.String("storage-root", "", "root of Legal Tender durable storage")
	currentPath := flags.String("current", "", "path to this cycle's active compact calculation manifest")
	runID := flags.String("run-id", "", "stable orchestration run identity")
	if err := flags.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 2
	}
	if flags.NArg() != 0 {
		fmt.Fprintf(stderr, "unexpected positional arguments: %s\n", strings.Join(flags.Args(), " "))
		return 2
	}
	if *storageRoot == "" || *runID == "" {
		_, _ = io.WriteString(stderr, "--storage-root and --run-id are required\n")
		return 2
	}
	directInputsPresent := *columnarFacts != "" || *linkageFacts != "" || *allCandidatesFacts != "" || *currentCampaignsFacts != ""
	if *factBundle != "" && directInputsPresent {
		_, _ = io.WriteString(stderr, "--fact-bundle cannot be combined with direct fact-manifest flags\n")
		return 2
	}
	if *factBundle == "" && (*columnarFacts == "" || *linkageFacts == "" || *allCandidatesFacts == "" || *currentCampaignsFacts == "") {
		_, _ = io.WriteString(stderr, "--fact-bundle or all four direct fact-manifest flags are required\n")
		return 2
	}
	options := fecreceipts.CompactPublishOptions{
		StorageRoot: *storageRoot, CurrentManifestPath: *currentPath, Clock: time.Now,
		Progress: func(message string) { fmt.Fprintln(stderr, message) },
	}
	var manifest fecreceipts.CompactManifest
	var err error
	if *factBundle != "" {
		manifest, err = fecreceipts.PublishCompactFromFactBundle(ctx, *factBundle, *runID, options)
	} else {
		manifest, err = fecreceipts.PublishCompact(ctx, fecreceipts.CompactPublishInput{
			ColumnarFactManifestPath: *columnarFacts, LinkageFactManifestPath: *linkageFacts,
			AllCandidatesFactManifestPath: *allCandidatesFacts, CurrentCampaignsManifestPath: *currentCampaignsFacts,
		}, *runID, options)
	}
	if err != nil {
		fmt.Fprintf(stderr, "publish compact candidate itemized-individual receipts: %v\n", err)
		return 1
	}
	if err := encodeJSON(stdout, manifest); err != nil {
		fmt.Fprintf(stderr, "encode compact candidate itemized-individual receipt manifest: %v\n", err)
		return 1
	}
	return 0
}

func runPublishCandidateItemizedReceiptsFactBundle(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	flags := flag.NewFlagSet("publish-candidate-itemized-receipts-fact-bundle", flag.ContinueOnError)
	flags.SetOutput(stderr)
	cycle := flags.String("cycle", "", "FEC two-year cycle to coordinate")
	columnarFacts := flags.String("columnar-facts", "", "optional exact Schedule A columnar fact manifest; defaults to the cycle pointer")
	linkageFacts := flags.String("linkage-facts", "", "optional exact linkage fact manifest; defaults to the cycle pointer")
	allCandidatesFacts := flags.String("all-candidates-summary-facts", "", "optional exact all-candidates summary fact manifest; defaults to the cycle pointer")
	currentCampaignsFacts := flags.String("current-campaigns-summary-facts", "", "optional exact current-campaigns summary fact manifest; defaults to the cycle pointer")
	storageRoot := flags.String("storage-root", "", "root of Legal Tender durable storage")
	currentPath := flags.String("current", "", "path to this cycle's active fact-bundle manifest")
	runID := flags.String("run-id", "", "stable orchestration run identity")
	if err := flags.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 2
	}
	if flags.NArg() != 0 {
		fmt.Fprintf(stderr, "unexpected positional arguments: %s\n", strings.Join(flags.Args(), " "))
		return 2
	}
	if *cycle == "" || *storageRoot == "" || *runID == "" {
		_, _ = io.WriteString(stderr, "--cycle, --storage-root, and --run-id are required\n")
		return 2
	}
	if *columnarFacts == "" {
		*columnarFacts = filepath.Join(*storageRoot, "facts", "fec", "schedule-a", "columnar", "current", *cycle+".json")
	}
	if *linkageFacts == "" {
		*linkageFacts = filepath.Join(*storageRoot, "facts", "fec", "classic", "candidate-committee-linkage", "current", *cycle+".json")
	}
	if *allCandidatesFacts == "" {
		*allCandidatesFacts = filepath.Join(*storageRoot, "facts", "fec", "classic", "all-candidates-summary", "current", *cycle+".json")
	}
	if *currentCampaignsFacts == "" {
		*currentCampaignsFacts = filepath.Join(*storageRoot, "facts", "fec", "classic", "current-campaigns-summary", "current", *cycle+".json")
	}
	manifest, err := fecreceipts.PublishFactBundle(ctx, fecreceipts.FactBundleInput{
		ColumnarFactManifestPath:      *columnarFacts,
		LinkageFactManifestPath:       *linkageFacts,
		AllCandidatesFactManifestPath: *allCandidatesFacts,
		CurrentCampaignsManifestPath:  *currentCampaignsFacts,
	}, *runID, fecreceipts.FactBundleOptions{
		StorageRoot: *storageRoot, CurrentManifestPath: *currentPath,
		ExpectedCycle: *cycle, Clock: time.Now,
	})
	if err != nil {
		fmt.Fprintf(stderr, "publish candidate receipt fact bundle: %v\n", err)
		return 1
	}
	if err := encodeJSON(stdout, manifest); err != nil {
		fmt.Fprintf(stderr, "encode candidate receipt fact bundle manifest: %v\n", err)
		return 1
	}
	return 0
}

func runPublishScheduleACompactOccurrences(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	flags := flag.NewFlagSet("publish-schedule-a-compact-occurrences", flag.ContinueOnError)
	flags.SetOutput(stderr)
	releasePath := flags.String("release", "", "path to the exact published coordinated FEC release manifest")
	cycle := flags.String("cycle", "", "selected two-year Schedule A period")
	storageRoot := flags.String("storage-root", "", "root of Legal Tender durable storage")
	currentPath := flags.String("current", "", "path to this cycle's active compact occurrence manifest")
	runID := flags.String("run-id", "", "stable orchestration run identity")
	partitions := flags.Int("partitions", 512, "bounded-memory compact natural-key partition count")
	if err := flags.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 2
	}
	if flags.NArg() != 0 {
		fmt.Fprintf(stderr, "unexpected positional arguments: %s\n", strings.Join(flags.Args(), " "))
		return 2
	}
	if *releasePath == "" || *cycle == "" || *storageRoot == "" || *runID == "" {
		_, _ = io.WriteString(stderr, "--release, --cycle, --storage-root, and --run-id are required\n")
		return 2
	}
	if *partitions <= 0 || *partitions > 4096 {
		_, _ = io.WriteString(stderr, "--partitions must be between 1 and 4096\n")
		return 2
	}
	releaseManifest, releaseDigest, err := decodeJSONFileWithSHA256[fecrelease.ReleaseManifest](*releasePath)
	if err != nil {
		fmt.Fprintf(stderr, "read source release manifest: %v\n", err)
		return 1
	}
	manifest, err := fecoccurrence.PublishScheduleACompactOccurrences(
		ctx, releaseManifest, releaseDigest, *cycle, *runID,
		fecoccurrence.Options{
			StorageRoot: *storageRoot, CurrentManifestPath: *currentPath, Clock: time.Now,
			ShardCount: *partitions, Progress: func(message string) { fmt.Fprintln(stderr, message) },
		},
	)
	if err != nil {
		fmt.Fprintf(stderr, "publish compact Schedule A occurrences: %v\n", err)
		return 1
	}
	if err := encodeJSON(stdout, manifest); err != nil {
		fmt.Fprintf(stderr, "encode compact Schedule A occurrence manifest: %v\n", err)
		return 1
	}
	return 0
}

func runPublishScheduleAColumnarFacts(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	flags := flag.NewFlagSet("publish-schedule-a-columnar-facts", flag.ContinueOnError)
	flags.SetOutput(stderr)
	releasePath := flags.String("release", "", "path to the exact published coordinated FEC release manifest")
	occurrencePath := flags.String("occurrences", "", "path to the exact published Schedule A occurrence manifest")
	storageRoot := flags.String("storage-root", "", "root of Legal Tender durable storage")
	currentPath := flags.String("current", "", "path to this cycle's active columnar fact manifest")
	runID := flags.String("run-id", "", "stable orchestration run identity")
	rowsPerShard := flags.Uint64("rows-per-shard", 1_000_000, "deterministic source-row range per Parquet shard")
	rowsPerRowGroup := flags.Uint64("rows-per-row-group", 128_000, "maximum facts per Parquet row group")
	if err := flags.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 2
	}
	if flags.NArg() != 0 {
		fmt.Fprintf(stderr, "unexpected positional arguments: %s\n", strings.Join(flags.Args(), " "))
		return 2
	}
	if *releasePath == "" || *occurrencePath == "" || *storageRoot == "" || *runID == "" {
		_, _ = io.WriteString(stderr, "--release, --occurrences, --storage-root, and --run-id are required\n")
		return 2
	}
	releaseManifest, releaseDigest, err := decodeJSONFileWithSHA256[fecrelease.ReleaseManifest](*releasePath)
	if err != nil {
		fmt.Fprintf(stderr, "read source release manifest: %v\n", err)
		return 1
	}
	manifest, err := fecoccurrence.PublishScheduleAColumnarFacts(
		ctx, releaseManifest, releaseDigest, *occurrencePath, *runID,
		fecoccurrence.Options{
			StorageRoot: *storageRoot, CurrentManifestPath: *currentPath, Clock: time.Now,
			RowsPerColumnarShard: *rowsPerShard, RowsPerColumnarRowGroup: *rowsPerRowGroup,
			Progress: func(message string) { fmt.Fprintln(stderr, message) },
		},
	)
	if err != nil {
		fmt.Fprintf(stderr, "publish Schedule A columnar facts: %v\n", err)
		return 1
	}
	if err := encodeJSON(stdout, manifest); err != nil {
		fmt.Fprintf(stderr, "encode Schedule A columnar fact manifest: %v\n", err)
		return 1
	}
	return 0
}

func runPublishScheduleBColumnarFacts(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	flags := flag.NewFlagSet("publish-schedule-b-columnar-facts", flag.ContinueOnError)
	flags.SetOutput(stderr)
	releasePath := flags.String("release", "", "path to the exact published coordinated FEC release manifest")
	storageRoot := flags.String("storage-root", "", "root of Legal Tender durable storage")
	currentPath := flags.String("current", "", "path to this cycle's active columnar fact manifest")
	cycle := flags.String("cycle", "", "selected FEC two-year transaction period")
	runID := flags.String("run-id", "", "stable orchestration run identity")
	pgRestore := flags.String("pg-restore", "pg_restore", "pg_restore executable used to stream the selected relation")
	workDir := flags.String("work-dir", "", "parent for automatically removed SUB_ID uniqueness shards")
	rowsPerShard := flags.Uint64("rows-per-shard", 1_000_000, "deterministic source-row range per Parquet shard")
	rowsPerRowGroup := flags.Uint64("rows-per-row-group", 128_000, "maximum facts per Parquet row group")
	if err := flags.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 2
	}
	if flags.NArg() != 0 {
		fmt.Fprintf(stderr, "unexpected positional arguments: %s\n", strings.Join(flags.Args(), " "))
		return 2
	}
	if *releasePath == "" || *storageRoot == "" || *cycle == "" || *runID == "" {
		_, _ = io.WriteString(stderr, "--release, --storage-root, --cycle, and --run-id are required\n")
		return 2
	}
	releaseManifest, releaseDigest, err := decodeJSONFileWithSHA256[fecrelease.ReleaseManifest](*releasePath)
	if err != nil {
		fmt.Fprintf(stderr, "read source release manifest: %v\n", err)
		return 1
	}
	manifest, err := fecoccurrence.PublishScheduleBColumnarFacts(ctx, releaseManifest, releaseDigest, *cycle, *runID, fecoccurrence.ScheduleBColumnarOptions{
		StorageRoot: *storageRoot, CurrentManifestPath: *currentPath, PGRestorePath: *pgRestore, WorkDir: *workDir, Clock: time.Now,
		RowsPerShard: *rowsPerShard, RowsPerRowGroup: *rowsPerRowGroup,
		Progress: func(message string) { fmt.Fprintln(stderr, message) },
	})
	if err != nil {
		fmt.Fprintf(stderr, "publish Schedule B columnar facts: %v\n", err)
		return 1
	}
	if err := encodeJSON(stdout, manifest); err != nil {
		fmt.Fprintf(stderr, "encode Schedule B columnar fact manifest: %v\n", err)
		return 1
	}
	return 0
}

func runBenchmarkScheduleALayout(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	flags := flag.NewFlagSet("benchmark-schedule-a-layout", flag.ContinueOnError)
	flags.SetOutput(stderr)
	source := flags.String("source", "", "path to one zstd-compressed Schedule A COPY relation")
	cycle := flags.String("cycle", "", "selected FEC two-year cycle")
	rows := flags.Uint64("rows", 0, "bounded sequential row count")
	totalRows := flags.Uint64("source-total-rows", 0, "complete relation row count for extrapolation")
	sourceSHA256 := flags.String("source-sha256", "", "optional expected complete compressed source SHA-256")
	output := flags.String("output", "", "new benchmark output directory")
	rowsPerFile := flags.Uint64("rows-per-file", 1_000_000, "Parquet ordinal-range shard size")
	rowsPerRowGroup := flags.Uint64("rows-per-row-group", 128_000, "Parquet row-group limit")
	if err := flags.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 2
	}
	if flags.NArg() != 0 {
		fmt.Fprintf(stderr, "unexpected positional arguments: %s\n", strings.Join(flags.Args(), " "))
		return 2
	}
	if *source == "" || *cycle == "" || *rows == 0 || *totalRows == 0 || *output == "" {
		_, _ = io.WriteString(stderr, "--source, --cycle, --rows, --source-total-rows, and --output are required\n")
		return 2
	}
	result, err := fecschedulealayout.Run(ctx, fecschedulealayout.Options{
		SourcePath: *source, Cycle: *cycle, Rows: *rows, ExpectedTotalRows: *totalRows,
		ExpectedSourceSHA256: *sourceSHA256, OutputDirectory: *output,
		RowsPerFile: *rowsPerFile, RowsPerRowGroup: *rowsPerRowGroup, Clock: time.Now,
		Progress: func(message string) { fmt.Fprintln(stderr, message) },
	})
	if err != nil {
		fmt.Fprintf(stderr, "benchmark Schedule A layout: %v\n", err)
		return 1
	}
	if err := encodeJSON(stdout, result); err != nil {
		fmt.Fprintf(stderr, "encode Schedule A layout benchmark: %v\n", err)
		return 1
	}
	return 0
}

func runProbeCandidateItemizedReceipts(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	flags := flag.NewFlagSet("probe-candidate-itemized-receipts", flag.ContinueOnError)
	flags.SetOutput(stderr)
	cycle := flags.String("cycle", "", "FEC two-year cycle to probe")
	release := flags.String("release", "", "path to the exact published coordinated FEC release manifest")
	occurrences := flags.String("occurrences", "", "path to the exact published Schedule A occurrence manifest")
	linkageFacts := flags.String("linkage-facts", "", "path to the exact published candidate-committee linkage fact manifest")
	allCandidatesFacts := flags.String("all-candidates-summary-facts", "", "path to the exact published all-candidates summary fact manifest")
	currentCampaignsFacts := flags.String("current-campaigns-summary-facts", "", "path to the exact published current-campaigns summary fact manifest")
	storageRoot := flags.String("storage-root", "", "root of Legal Tender durable storage")
	runID := flags.String("run-id", "", "stable probe run identity")
	if err := flags.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 2
	}
	if flags.NArg() != 0 {
		fmt.Fprintf(stderr, "unexpected positional arguments: %s\n", strings.Join(flags.Args(), " "))
		return 2
	}
	if *cycle == "" || *release == "" || *occurrences == "" || *linkageFacts == "" || *allCandidatesFacts == "" || *currentCampaignsFacts == "" || *storageRoot == "" || *runID == "" {
		_, _ = io.WriteString(stderr, "--cycle, --release, --occurrences, --linkage-facts, --all-candidates-summary-facts, --current-campaigns-summary-facts, --storage-root, and --run-id are required\n")
		return 2
	}
	manifest, err := fecreceipts.ProbeDirect(ctx, fecreceipts.DirectProbeInput{
		Cycle: *cycle, ReleaseManifestPath: *release, OccurrenceManifestPath: *occurrences,
		LinkageFactManifestPath: *linkageFacts, AllCandidatesFactManifestPath: *allCandidatesFacts,
		CurrentCampaignsManifestPath: *currentCampaignsFacts,
	}, *runID, fecreceipts.DirectProbeOptions{
		StorageRoot: *storageRoot, Clock: time.Now,
		Progress: func(message string) { fmt.Fprintln(stderr, message) },
	})
	if err != nil {
		fmt.Fprintf(stderr, "probe candidate itemized-individual receipts: %v\n", err)
		return 1
	}
	if err := encodeJSON(stdout, manifest); err != nil {
		fmt.Fprintf(stderr, "encode direct receipt probe manifest: %v\n", err)
		return 1
	}
	return 0
}

func runPublishCandidateItemizedReceipts(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	flags := flag.NewFlagSet("publish-candidate-itemized-receipts", flag.ContinueOnError)
	flags.SetOutput(stderr)
	scheduleAFacts := flags.String("schedule-a-facts", "", "path to the exact published Schedule A fact manifest")
	linkageFacts := flags.String("linkage-facts", "", "path to the exact published candidate-committee linkage fact manifest")
	allCandidatesFacts := flags.String("all-candidates-summary-facts", "", "path to the exact published all-candidates summary fact manifest")
	currentCampaignsFacts := flags.String("current-campaigns-summary-facts", "", "path to the exact published current-campaigns summary fact manifest")
	storageRoot := flags.String("storage-root", "", "root of Legal Tender durable storage")
	currentPath := flags.String("current", "", "path to this cycle's active calculation manifest")
	runID := flags.String("run-id", "", "stable orchestration run identity")
	if err := flags.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 2
	}
	if flags.NArg() != 0 {
		fmt.Fprintf(stderr, "unexpected positional arguments: %s\n", strings.Join(flags.Args(), " "))
		return 2
	}
	if *scheduleAFacts == "" || *linkageFacts == "" || *allCandidatesFacts == "" || *currentCampaignsFacts == "" || *storageRoot == "" || *runID == "" {
		_, _ = io.WriteString(stderr, "--schedule-a-facts, --linkage-facts, --all-candidates-summary-facts, --current-campaigns-summary-facts, --storage-root, and --run-id are required\n")
		return 2
	}
	manifest, err := fecreceipts.Publish(ctx, fecreceipts.PublishInput{
		ScheduleAFactManifestPath: *scheduleAFacts, LinkageFactManifestPath: *linkageFacts,
		AllCandidatesFactManifestPath: *allCandidatesFacts, CurrentCampaignsManifestPath: *currentCampaignsFacts,
	}, *runID, fecreceipts.PublishOptions{
		StorageRoot: *storageRoot, CurrentManifestPath: *currentPath, Clock: time.Now,
		Progress: func(message string) { fmt.Fprintln(stderr, message) },
	})
	if err != nil {
		fmt.Fprintf(stderr, "publish candidate itemized-individual receipts: %v\n", err)
		return 1
	}
	if err := encodeJSON(stdout, manifest); err != nil {
		fmt.Fprintf(stderr, "encode candidate itemized-individual receipt manifest: %v\n", err)
		return 1
	}
	return 0
}

func runPublishScheduleAFacts(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	flags := flag.NewFlagSet("publish-schedule-a-facts", flag.ContinueOnError)
	flags.SetOutput(stderr)
	releasePath := flags.String("release", "", "path to the exact published coordinated FEC release manifest")
	occurrencePath := flags.String("occurrences", "", "path to the exact published Schedule A occurrence manifest")
	storageRoot := flags.String("storage-root", "", "root of Legal Tender durable storage")
	currentPath := flags.String("current", "", "path to this cycle's active fact manifest")
	runID := flags.String("run-id", "", "stable orchestration run identity")
	if err := flags.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 2
	}
	if flags.NArg() != 0 {
		fmt.Fprintf(stderr, "unexpected positional arguments: %s\n", strings.Join(flags.Args(), " "))
		return 2
	}
	if *releasePath == "" || *occurrencePath == "" || *storageRoot == "" || *runID == "" {
		_, _ = io.WriteString(stderr, "--release, --occurrences, --storage-root, and --run-id are required\n")
		return 2
	}
	releaseManifest, releaseDigest, err := decodeJSONFileWithSHA256[fecrelease.ReleaseManifest](*releasePath)
	if err != nil {
		fmt.Fprintf(stderr, "read source release manifest: %v\n", err)
		return 1
	}
	manifest, err := fecoccurrence.PublishScheduleAFacts(
		ctx,
		releaseManifest,
		releaseDigest,
		*occurrencePath,
		*runID,
		fecoccurrence.Options{
			StorageRoot:         *storageRoot,
			CurrentManifestPath: *currentPath,
			Clock:               time.Now,
			Progress: func(message string) {
				fmt.Fprintln(stderr, message)
			},
		},
	)
	if err != nil {
		fmt.Fprintf(stderr, "publish Schedule A facts: %v\n", err)
		return 1
	}
	if err := encodeJSON(stdout, manifest); err != nil {
		fmt.Fprintf(stderr, "encode Schedule A fact manifest: %v\n", err)
		return 1
	}
	return 0
}

func runPublishScheduleEOccurrences(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	flags := flag.NewFlagSet("publish-schedule-e-occurrences", flag.ContinueOnError)
	flags.SetOutput(stderr)
	releasePath := flags.String("release", "", "path to the exact published coordinated FEC release manifest")
	cycle := flags.String("cycle", "", "selected two-year Schedule E period")
	storageRoot := flags.String("storage-root", "", "root of Legal Tender durable storage")
	currentPath := flags.String("current", "", "path to this cycle's active occurrence manifest")
	runID := flags.String("run-id", "", "stable orchestration run identity")
	if err := flags.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 2
	}
	if flags.NArg() != 0 {
		fmt.Fprintf(stderr, "unexpected positional arguments: %s\n", strings.Join(flags.Args(), " "))
		return 2
	}
	if *releasePath == "" || *cycle == "" || *storageRoot == "" || *runID == "" {
		_, _ = io.WriteString(stderr, "--release, --cycle, --storage-root, and --run-id are required\n")
		return 2
	}
	releaseManifest, releaseDigest, err := decodeJSONFileWithSHA256[fecrelease.ReleaseManifest](*releasePath)
	if err != nil {
		fmt.Fprintf(stderr, "read source release manifest: %v\n", err)
		return 1
	}
	manifest, err := fecoccurrence.PublishScheduleEOccurrences(
		ctx, releaseManifest, releaseDigest, *cycle, *runID,
		fecoccurrence.Options{
			StorageRoot: *storageRoot, CurrentManifestPath: *currentPath, Clock: time.Now,
			Progress: func(message string) { fmt.Fprintln(stderr, message) },
		},
	)
	if err != nil {
		fmt.Fprintf(stderr, "publish Schedule E occurrences: %v\n", err)
		return 1
	}
	if err := encodeJSON(stdout, manifest); err != nil {
		fmt.Fprintf(stderr, "encode Schedule E occurrence manifest: %v\n", err)
		return 1
	}
	return 0
}

func runPublishScheduleEFacts(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	flags := flag.NewFlagSet("publish-schedule-e-facts", flag.ContinueOnError)
	flags.SetOutput(stderr)
	releasePath := flags.String("release", "", "path to the exact published coordinated FEC release manifest")
	occurrencePath := flags.String("occurrences", "", "path to the exact published Schedule E occurrence manifest")
	storageRoot := flags.String("storage-root", "", "root of Legal Tender durable storage")
	currentPath := flags.String("current", "", "path to this cycle's active fact manifest")
	runID := flags.String("run-id", "", "stable orchestration run identity")
	if err := flags.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 2
	}
	if flags.NArg() != 0 {
		fmt.Fprintf(stderr, "unexpected positional arguments: %s\n", strings.Join(flags.Args(), " "))
		return 2
	}
	if *releasePath == "" || *occurrencePath == "" || *storageRoot == "" || *runID == "" {
		_, _ = io.WriteString(stderr, "--release, --occurrences, --storage-root, and --run-id are required\n")
		return 2
	}
	releaseManifest, releaseDigest, err := decodeJSONFileWithSHA256[fecrelease.ReleaseManifest](*releasePath)
	if err != nil {
		fmt.Fprintf(stderr, "read source release manifest: %v\n", err)
		return 1
	}
	manifest, err := fecoccurrence.PublishScheduleEFacts(
		ctx, releaseManifest, releaseDigest, *occurrencePath, *runID,
		fecoccurrence.Options{
			StorageRoot: *storageRoot, CurrentManifestPath: *currentPath, Clock: time.Now,
			Progress: func(message string) { fmt.Fprintln(stderr, message) },
		},
	)
	if err != nil {
		fmt.Fprintf(stderr, "publish Schedule E facts: %v\n", err)
		return 1
	}
	if err := encodeJSON(stdout, manifest); err != nil {
		fmt.Fprintf(stderr, "encode Schedule E fact manifest: %v\n", err)
		return 1
	}
	return 0
}

func runPublishEffectiveIndependentExpenditures(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	flags := flag.NewFlagSet("publish-effective-independent-expenditures", flag.ContinueOnError)
	flags.SetOutput(stderr)
	scheduleEFacts := flags.String("schedule-e-facts", "", "path to the exact published Schedule E fact manifest")
	storageRoot := flags.String("storage-root", "", "root of Legal Tender durable storage")
	currentPath := flags.String("current", "", "path to this cycle's active effective independent-expenditure manifest")
	runID := flags.String("run-id", "", "stable orchestration run identity")
	if err := flags.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 2
	}
	if flags.NArg() != 0 {
		fmt.Fprintf(stderr, "unexpected positional arguments: %s\n", strings.Join(flags.Args(), " "))
		return 2
	}
	if *scheduleEFacts == "" || *storageRoot == "" || *runID == "" {
		_, _ = io.WriteString(stderr, "--schedule-e-facts, --storage-root, and --run-id are required\n")
		return 2
	}
	manifest, err := fecindependentexpenditures.Publish(
		ctx,
		fecindependentexpenditures.PublishInput{ScheduleEFactManifestPath: *scheduleEFacts},
		*runID,
		fecindependentexpenditures.PublishOptions{
			StorageRoot: *storageRoot, CurrentManifestPath: *currentPath, Clock: time.Now,
			Progress: func(message string) { fmt.Fprintln(stderr, message) },
		},
	)
	if err != nil {
		fmt.Fprintf(stderr, "publish effective independent expenditures: %v\n", err)
		return 1
	}
	if err := encodeJSON(stdout, manifest); err != nil {
		fmt.Fprintf(stderr, "encode effective independent-expenditure manifest: %v\n", err)
		return 1
	}
	return 0
}

func runPublishIndependentExpenditureCandidateResolution(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	flags := flag.NewFlagSet("publish-independent-expenditure-candidate-resolution", flag.ContinueOnError)
	flags.SetOutput(stderr)
	cycle := flags.String("cycle", "", "FEC two-year cycle to resolve")
	effective := flags.String("effective", "", "exact effective independent-expenditure manifest; defaults to the cycle pointer")
	candidateFacts := flags.String("candidate-facts", "", "exact candidate-master fact manifest; defaults to the cycle pointer")
	storageRoot := flags.String("storage-root", "", "root of Legal Tender durable storage")
	currentPath := flags.String("current", "", "path to this cycle's active candidate-resolution manifest")
	runID := flags.String("run-id", "", "stable orchestration run identity")
	if err := flags.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 2
	}
	if flags.NArg() != 0 {
		fmt.Fprintf(stderr, "unexpected positional arguments: %s\n", strings.Join(flags.Args(), " "))
		return 2
	}
	if !validPeriod(*cycle) || *storageRoot == "" || *runID == "" {
		_, _ = io.WriteString(stderr, "--cycle must be a four-digit even year; --storage-root and --run-id are required\n")
		return 2
	}
	if *effective == "" {
		*effective = filepath.Join(*storageRoot, "calculations", "fec", "effective-independent-expenditures", "current", *cycle+".json")
	}
	if *candidateFacts == "" {
		*candidateFacts = filepath.Join(*storageRoot, "facts", "fec", "classic", "candidate-master", "current", *cycle+".json")
	}
	manifest, err := feccandidateresolution.Publish(
		ctx,
		feccandidateresolution.PublishInput{
			EffectiveManifestPath: *effective,
			CandidateManifestPath: *candidateFacts,
		},
		*runID,
		feccandidateresolution.PublishOptions{
			StorageRoot: *storageRoot, CurrentManifestPath: *currentPath, Clock: time.Now,
			Progress: func(message string) { fmt.Fprintln(stderr, message) },
		},
	)
	if err != nil {
		fmt.Fprintf(stderr, "publish independent-expenditure candidate resolution: %v\n", err)
		return 1
	}
	if manifest.Cycle != *cycle {
		fmt.Fprintf(stderr, "candidate resolution returned cycle %s, expected %s\n", manifest.Cycle, *cycle)
		return 1
	}
	if err := encodeJSON(stdout, manifest); err != nil {
		fmt.Fprintf(stderr, "encode independent-expenditure candidate-resolution manifest: %v\n", err)
		return 1
	}
	return 0
}

func runPublishResolvedIndependentExpenditures(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	flags := flag.NewFlagSet("publish-resolved-independent-expenditures", flag.ContinueOnError)
	flags.SetOutput(stderr)
	cycle := flags.String("cycle", "", "FEC two-year cycle to group")
	resolution := flags.String("candidate-resolution", "", "exact candidate-resolution manifest; defaults to the cycle pointer")
	storageRoot := flags.String("storage-root", "", "root of Legal Tender durable storage")
	currentPath := flags.String("current", "", "path to this cycle's active resolved independent-expenditure manifest")
	runID := flags.String("run-id", "", "stable orchestration run identity")
	if err := flags.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 2
	}
	if flags.NArg() != 0 {
		fmt.Fprintf(stderr, "unexpected positional arguments: %s\n", strings.Join(flags.Args(), " "))
		return 2
	}
	if !validPeriod(*cycle) || *storageRoot == "" || *runID == "" {
		_, _ = io.WriteString(stderr, "--cycle must be a four-digit even year; --storage-root and --run-id are required\n")
		return 2
	}
	if *resolution == "" {
		*resolution = filepath.Join(
			*storageRoot, "calculations", "fec", "independent-expenditure-candidate-resolution", "current", *cycle+".json",
		)
	}
	manifest, err := feccandidateresolution.PublishAggregate(
		ctx,
		feccandidateresolution.AggregatePublishInput{CandidateResolutionManifestPath: *resolution},
		*runID,
		feccandidateresolution.AggregatePublishOptions{
			StorageRoot: *storageRoot, CurrentManifestPath: *currentPath, Clock: time.Now,
			Progress: func(message string) { fmt.Fprintln(stderr, message) },
		},
	)
	if err != nil {
		fmt.Fprintf(stderr, "publish resolved independent expenditures: %v\n", err)
		return 1
	}
	if manifest.Cycle != *cycle {
		fmt.Fprintf(stderr, "resolved independent expenditures returned cycle %s, expected %s\n", manifest.Cycle, *cycle)
		return 1
	}
	if err := encodeJSON(stdout, manifest); err != nil {
		fmt.Fprintf(stderr, "encode resolved independent-expenditure manifest: %v\n", err)
		return 1
	}
	return 0
}

func runPublishClassicFacts(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	flags := flag.NewFlagSet("publish-classic-facts", flag.ContinueOnError)
	flags.SetOutput(stderr)
	releasePath := flags.String("release", "", "path to the exact published coordinated FEC release manifest")
	occurrencePath := flags.String("occurrences", "", "path to the exact published classic occurrence manifest")
	storageRoot := flags.String("storage-root", "", "root of Legal Tender durable storage")
	currentPath := flags.String("current", "", "path to this dataset and cycle's active fact manifest")
	runID := flags.String("run-id", "", "stable orchestration run identity")
	if err := flags.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 2
	}
	if flags.NArg() != 0 {
		fmt.Fprintf(stderr, "unexpected positional arguments: %s\n", strings.Join(flags.Args(), " "))
		return 2
	}
	if *releasePath == "" || *occurrencePath == "" || *storageRoot == "" || *runID == "" {
		_, _ = io.WriteString(stderr, "--release, --occurrences, --storage-root, and --run-id are required\n")
		return 2
	}
	releaseManifest, releaseDigest, err := decodeJSONFileWithSHA256[fecrelease.ReleaseManifest](*releasePath)
	if err != nil {
		fmt.Fprintf(stderr, "read source release manifest: %v\n", err)
		return 1
	}
	manifest, err := fecoccurrence.PublishClassicFacts(
		ctx,
		releaseManifest,
		releaseDigest,
		*occurrencePath,
		*runID,
		fecoccurrence.Options{
			StorageRoot:         *storageRoot,
			CurrentManifestPath: *currentPath,
			Clock:               time.Now,
			Progress: func(message string) {
				fmt.Fprintln(stderr, message)
			},
		},
	)
	if err != nil {
		fmt.Fprintf(stderr, "publish classic FEC facts: %v\n", err)
		return 1
	}
	if err := encodeJSON(stdout, manifest); err != nil {
		fmt.Fprintf(stderr, "encode classic fact manifest: %v\n", err)
		return 1
	}
	return 0
}

func runPublishClassicOccurrences(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	flags := flag.NewFlagSet("publish-classic-occurrences", flag.ContinueOnError)
	flags.SetOutput(stderr)
	releasePath := flags.String("release", "", "path to the exact published coordinated FEC release manifest")
	dataset := flags.String("dataset", "", "classic FEC dataset contract name")
	cycle := flags.String("cycle", "", "selected two-year source period")
	storageRoot := flags.String("storage-root", "", "root of Legal Tender durable storage")
	currentPath := flags.String("current", "", "path to this dataset and cycle's active occurrence manifest")
	runID := flags.String("run-id", "", "stable orchestration run identity")
	shards := flags.Int("shards", 64, "bounded-memory natural-key shard count")
	if err := flags.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 2
	}
	if flags.NArg() != 0 {
		fmt.Fprintf(stderr, "unexpected positional arguments: %s\n", strings.Join(flags.Args(), " "))
		return 2
	}
	if *releasePath == "" || *dataset == "" || *cycle == "" || *storageRoot == "" || *runID == "" {
		_, _ = io.WriteString(stderr, "--release, --dataset, --cycle, --storage-root, and --run-id are required\n")
		return 2
	}
	if *shards <= 0 {
		_, _ = io.WriteString(stderr, "--shards must be greater than zero\n")
		return 2
	}
	releaseManifest, releaseDigest, err := decodeJSONFileWithSHA256[fecrelease.ReleaseManifest](*releasePath)
	if err != nil {
		fmt.Fprintf(stderr, "read source release manifest: %v\n", err)
		return 1
	}
	manifest, err := fecoccurrence.PublishClassic(
		ctx,
		releaseManifest,
		releaseDigest,
		*dataset,
		*cycle,
		*runID,
		fecoccurrence.Options{
			StorageRoot:         *storageRoot,
			CurrentManifestPath: *currentPath,
			Clock:               time.Now,
			ShardCount:          *shards,
			Progress: func(message string) {
				fmt.Fprintln(stderr, message)
			},
		},
	)
	if err != nil {
		fmt.Fprintf(stderr, "publish classic FEC occurrences: %v\n", err)
		return 1
	}
	if err := encodeJSON(stdout, manifest); err != nil {
		fmt.Fprintf(stderr, "encode classic occurrence manifest: %v\n", err)
		return 1
	}
	return 0
}

func runDiscover(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	flags := flag.NewFlagSet("discover", flag.ContinueOnError)
	flags.SetOutput(stderr)
	timeout := flags.Duration("timeout", 30*time.Second, "timeout for each metadata request")
	concurrency := flags.Int("concurrency", 4, "maximum simultaneous metadata requests")
	inventoryVersion := flags.String("inventory-version", fecrelease.ActiveInventoryVersion, "committed FEC inventory version")
	if err := flags.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 2
	}
	if flags.NArg() != 0 {
		fmt.Fprintf(stderr, "unexpected positional arguments: %s\n", strings.Join(flags.Args(), " "))
		return 2
	}
	if *timeout <= 0 {
		_, _ = io.WriteString(stderr, "--timeout must be greater than zero\n")
		return 2
	}
	if *concurrency <= 0 {
		_, _ = io.WriteString(stderr, "--concurrency must be greater than zero\n")
		return 2
	}

	inventory, known := fecrelease.InventoryForVersion(*inventoryVersion)
	if !known {
		fmt.Fprintf(stderr, "unknown FEC inventory version %q\n", *inventoryVersion)
		return 2
	}
	result, err := fecrelease.Discover(
		ctx,
		&http.Client{Timeout: *timeout},
		inventory,
		time.Now,
		fecrelease.DiscoverOptions{MaxConcurrent: *concurrency},
	)
	if err != nil {
		fmt.Fprintf(stderr, "discover FEC metadata: %v\n", err)
		return 1
	}
	if err := encodeJSON(stdout, result); err != nil {
		fmt.Fprintf(stderr, "encode discovery: %v\n", err)
		return 1
	}
	return 0
}

func runPlanRelease(args []string, stdout, stderr io.Writer) int {
	flags := flag.NewFlagSet("plan-release", flag.ContinueOnError)
	flags.SetOutput(stderr)
	observationsPath := flags.String("observations", "", "path to saved metadata discovery JSON")
	currentPath := flags.String("current", "", "path to the prior published release manifest")
	if err := flags.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 2
	}
	if flags.NArg() != 0 {
		fmt.Fprintf(stderr, "unexpected positional arguments: %s\n", strings.Join(flags.Args(), " "))
		return 2
	}
	if *observationsPath == "" {
		_, _ = io.WriteString(stderr, "--observations is required\n")
		return 2
	}

	discovery, err := decodeJSONFile[fecrelease.Discovery](*observationsPath)
	if err != nil {
		fmt.Fprintf(stderr, "read observations: %v\n", err)
		return 1
	}
	var current *fecrelease.ReleaseManifest
	if *currentPath != "" {
		manifest, err := decodeJSONFile[fecrelease.ReleaseManifest](*currentPath)
		if err != nil {
			fmt.Fprintf(stderr, "read current release: %v\n", err)
			return 1
		}
		current = &manifest
	}
	inventory, known := fecrelease.InventoryForVersion(discovery.InventoryVersion)
	if !known {
		fmt.Fprintf(stderr, "unknown FEC inventory version %q\n", discovery.InventoryVersion)
		return 1
	}
	result := fecrelease.Plan(inventory, discovery, current, time.Now())
	if err := encodeJSON(stdout, result); err != nil {
		fmt.Fprintf(stderr, "encode release plan: %v\n", err)
		return 1
	}
	if result.Status == fecrelease.PlanInvalid {
		_, _ = io.WriteString(stderr, "release plan input is invalid\n")
		return 1
	}
	return 0
}

func runAcquire(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	flags := flag.NewFlagSet("acquire", flag.ContinueOnError)
	flags.SetOutput(stderr)
	planPath := flags.String("plan", "", "path to an update_available release plan")
	currentPath := flags.String("current", "", "path to the prior published release manifest")
	storageRoot := flags.String("storage-root", "", "root of Legal Tender durable storage")
	runID := flags.String("run-id", "", "stable orchestration run identity")
	concurrency := flags.Int("concurrency", 3, "maximum simultaneous body requests")
	requestTimeout := flags.Duration("request-timeout", 12*time.Hour, "timeout for each body or metadata request")
	pgRestorePath := flags.String("pg-restore", "pg_restore", "pg_restore executable used for dump TOC validation")
	if err := flags.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 2
	}
	if flags.NArg() != 0 {
		fmt.Fprintf(stderr, "unexpected positional arguments: %s\n", strings.Join(flags.Args(), " "))
		return 2
	}
	if *planPath == "" || *storageRoot == "" || *runID == "" {
		_, _ = io.WriteString(stderr, "--plan, --storage-root, and --run-id are required\n")
		return 2
	}
	if *concurrency <= 0 || *requestTimeout <= 0 {
		_, _ = io.WriteString(stderr, "--concurrency and --request-timeout must be greater than zero\n")
		return 2
	}

	planContent, err := os.ReadFile(*planPath)
	if err != nil {
		fmt.Fprintf(stderr, "read release plan: %v\n", err)
		return 1
	}
	plan, err := decodeJSONBytes[fecrelease.ReleasePlan](planContent)
	if err != nil {
		fmt.Fprintf(stderr, "read release plan: %v\n", err)
		return 1
	}
	if plan.Status != fecrelease.PlanUpdateAvailable {
		fmt.Fprintf(stderr, "release plan status %q does not authorize acquisition\n", plan.Status)
		return 1
	}
	inventory, known := fecrelease.InventoryForVersion(plan.InventoryVersion)
	if !known {
		fmt.Fprintf(stderr, "unknown FEC inventory version %q\n", plan.InventoryVersion)
		return 1
	}
	if issues := fecrelease.ValidatePlan(inventory, plan); len(issues) != 0 {
		fmt.Fprintf(stderr, "invalid release plan: %s\n", issues[0].Message)
		return 1
	}
	if !fecrelease.ValidAcquisitionRunID(*runID) {
		_, _ = io.WriteString(stderr, "--run-id contains unsupported characters\n")
		return 2
	}
	var current *fecrelease.ReleaseManifest
	if *currentPath != "" {
		manifest, err := decodeJSONFile[fecrelease.ReleaseManifest](*currentPath)
		if err != nil {
			fmt.Fprintf(stderr, "read current release: %v\n", err)
			return 1
		}
		current = &manifest
	}
	planDigest := fmt.Sprintf("%x", sha256.Sum256(planContent))
	result, acquisitionErr := fecrelease.Acquire(
		ctx,
		&http.Client{Timeout: *requestTimeout},
		inventory,
		plan,
		current,
		planDigest,
		*runID,
		fecrelease.AcquisitionOptions{
			StorageRoot:   *storageRoot,
			MaxConcurrent: *concurrency,
			Clock:         time.Now,
			PGRestorePath: *pgRestorePath,
			Progress: func(message string) {
				fmt.Fprintln(stderr, message)
			},
		},
	)
	if err := encodeJSON(stdout, result); err != nil {
		fmt.Fprintf(stderr, "encode acquisition result: %v\n", err)
		return 1
	}
	if acquisitionErr != nil {
		fmt.Fprintf(stderr, "acquire FEC release: %v\n", acquisitionErr)
		return 1
	}
	return 0
}

func runStageRelease(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	flags := flag.NewFlagSet("stage-release", flag.ContinueOnError)
	flags.SetOutput(stderr)
	planPath := flags.String("plan", "", "path to the exact update_available release plan")
	acquisitionPath := flags.String("acquisition", "", "path to the exact acquired result")
	currentPath := flags.String("current", "", "path to the prior published release manifest")
	storageRoot := flags.String("storage-root", "", "root of Legal Tender durable storage")
	runID := flags.String("run-id", "", "stable orchestration run identity")
	pgRestorePath := flags.String("pg-restore", "pg_restore", "pg_restore executable used for selective relation extraction")
	if err := flags.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 2
	}
	if flags.NArg() != 0 {
		fmt.Fprintf(stderr, "unexpected positional arguments: %s\n", strings.Join(flags.Args(), " "))
		return 2
	}
	if *planPath == "" || *acquisitionPath == "" || *storageRoot == "" || *runID == "" {
		_, _ = io.WriteString(stderr, "--plan, --acquisition, --storage-root, and --run-id are required\n")
		return 2
	}
	plan, planDigest, err := decodeJSONFileWithSHA256[fecrelease.ReleasePlan](*planPath)
	if err != nil {
		fmt.Fprintf(stderr, "read release plan: %v\n", err)
		return 1
	}
	inventory, known := fecrelease.InventoryForVersion(plan.InventoryVersion)
	if !known {
		fmt.Fprintf(stderr, "unknown FEC inventory version %q\n", plan.InventoryVersion)
		return 1
	}
	acquisition, acquisitionDigest, err := decodeJSONFileWithSHA256[fecrelease.AcquisitionResult](*acquisitionPath)
	if err != nil {
		fmt.Fprintf(stderr, "read acquisition result: %v\n", err)
		return 1
	}
	var current *fecrelease.ReleaseManifest
	if *currentPath != "" {
		manifest, err := decodeJSONFile[fecrelease.ReleaseManifest](*currentPath)
		if err != nil {
			fmt.Fprintf(stderr, "read current release: %v\n", err)
			return 1
		}
		current = &manifest
	}
	result, stageErr := fecrelease.Stage(
		ctx,
		inventory,
		plan,
		planDigest,
		acquisition,
		acquisitionDigest,
		current,
		*runID,
		fecrelease.StageOptions{
			StorageRoot:   *storageRoot,
			Clock:         time.Now,
			PGRestorePath: *pgRestorePath,
			Progress: func(message string) {
				fmt.Fprintln(stderr, message)
			},
		},
	)
	if err := encodeJSON(stdout, result); err != nil {
		fmt.Fprintf(stderr, "encode staged release: %v\n", err)
		return 1
	}
	if stageErr != nil {
		fmt.Fprintf(stderr, "stage FEC release: %v\n", stageErr)
		return 1
	}
	return 0
}

func runPublishRelease(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	flags := flag.NewFlagSet("publish-release", flag.ContinueOnError)
	flags.SetOutput(stderr)
	planPath := flags.String("plan", "", "path to the exact update_available release plan")
	acquisitionPath := flags.String("acquisition", "", "path to the exact acquired result")
	stagePath := flags.String("stage", "", "path to the exact staged release")
	currentPath := flags.String("current", "", "path to the active release manifest")
	storageRoot := flags.String("storage-root", "", "root of Legal Tender durable storage")
	runID := flags.String("run-id", "", "stable orchestration run identity")
	if err := flags.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 2
	}
	if flags.NArg() != 0 {
		fmt.Fprintf(stderr, "unexpected positional arguments: %s\n", strings.Join(flags.Args(), " "))
		return 2
	}
	if *planPath == "" || *acquisitionPath == "" || *stagePath == "" || *storageRoot == "" || *runID == "" {
		_, _ = io.WriteString(stderr, "--plan, --acquisition, --stage, --storage-root, and --run-id are required\n")
		return 2
	}
	plan, planDigest, planContent, err := decodeJSONFileWithSHA256AndContent[fecrelease.ReleasePlan](*planPath)
	if err != nil {
		fmt.Fprintf(stderr, "read release plan: %v\n", err)
		return 1
	}
	inventory, known := fecrelease.InventoryForVersion(plan.InventoryVersion)
	if !known {
		fmt.Fprintf(stderr, "unknown FEC inventory version %q\n", plan.InventoryVersion)
		return 1
	}
	acquisition, acquisitionDigest, acquisitionContent, err := decodeJSONFileWithSHA256AndContent[fecrelease.AcquisitionResult](*acquisitionPath)
	if err != nil {
		fmt.Fprintf(stderr, "read acquisition result: %v\n", err)
		return 1
	}
	stage, stageDigest, stageContent, err := decodeJSONFileWithSHA256AndContent[fecrelease.StageResult](*stagePath)
	if err != nil {
		fmt.Fprintf(stderr, "read staged release: %v\n", err)
		return 1
	}
	for _, input := range []struct {
		category string
		digest   string
		content  []byte
	}{
		{category: "plans", digest: planDigest, content: planContent},
		{category: "acquisitions", digest: acquisitionDigest, content: acquisitionContent},
		{category: "stages", digest: stageDigest, content: stageContent},
	} {
		if _, err := preserveReleaseControlInput(*storageRoot, input.category, input.digest, input.content); err != nil {
			fmt.Fprintf(stderr, "preserve %s release control input: %v\n", input.category, err)
			return 1
		}
	}
	manifest, err := fecrelease.Publish(
		ctx,
		inventory,
		plan,
		planDigest,
		acquisition,
		acquisitionDigest,
		stage,
		stageDigest,
		*runID,
		fecrelease.PublishOptions{StorageRoot: *storageRoot, CurrentManifestPath: *currentPath, Clock: time.Now},
	)
	if err != nil {
		fmt.Fprintf(stderr, "publish FEC release: %v\n", err)
		return 1
	}
	if err := encodeJSON(stdout, manifest); err != nil {
		fmt.Fprintf(stderr, "encode release manifest: %v\n", err)
		return 1
	}
	return 0
}

func runPublishScheduleAOccurrences(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	flags := flag.NewFlagSet("publish-schedule-a-occurrences", flag.ContinueOnError)
	flags.SetOutput(stderr)
	releasePath := flags.String("release", "", "path to the exact published coordinated FEC release manifest")
	cycle := flags.String("cycle", "", "selected two-year Schedule A period")
	storageRoot := flags.String("storage-root", "", "root of Legal Tender durable storage")
	currentPath := flags.String("current", "", "path to this cycle's active occurrence manifest")
	runID := flags.String("run-id", "", "stable orchestration run identity")
	shards := flags.Int("shards", 512, "bounded-memory natural-key shard count")
	if err := flags.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 2
	}
	if flags.NArg() != 0 {
		fmt.Fprintf(stderr, "unexpected positional arguments: %s\n", strings.Join(flags.Args(), " "))
		return 2
	}
	if *releasePath == "" || *cycle == "" || *storageRoot == "" || *runID == "" {
		_, _ = io.WriteString(stderr, "--release, --cycle, --storage-root, and --run-id are required\n")
		return 2
	}
	if *shards <= 0 {
		_, _ = io.WriteString(stderr, "--shards must be greater than zero\n")
		return 2
	}
	releaseManifest, releaseDigest, err := decodeJSONFileWithSHA256[fecrelease.ReleaseManifest](*releasePath)
	if err != nil {
		fmt.Fprintf(stderr, "read source release manifest: %v\n", err)
		return 1
	}
	manifest, err := fecoccurrence.Publish(
		ctx,
		releaseManifest,
		releaseDigest,
		*cycle,
		*runID,
		fecoccurrence.Options{
			StorageRoot:         *storageRoot,
			CurrentManifestPath: *currentPath,
			Clock:               time.Now,
			ShardCount:          *shards,
			Progress: func(message string) {
				fmt.Fprintln(stderr, message)
			},
		},
	)
	if err != nil {
		fmt.Fprintf(stderr, "publish Schedule A occurrences: %v\n", err)
		return 1
	}
	if err := encodeJSON(stdout, manifest); err != nil {
		fmt.Fprintf(stderr, "encode occurrence manifest: %v\n", err)
		return 1
	}
	return 0
}

func decodeJSONFile[T any](path string) (T, error) {
	var value T
	content, err := os.ReadFile(path)
	if err != nil {
		return value, err
	}
	return decodeJSONBytes[T](content)
}

func decodeJSONFileWithSHA256[T any](path string) (T, string, error) {
	value, digest, _, err := decodeJSONFileWithSHA256AndContent[T](path)
	return value, digest, err
}

func decodeJSONFileWithSHA256AndContent[T any](path string) (T, string, []byte, error) {
	var value T
	content, err := os.ReadFile(path)
	if err != nil {
		return value, "", nil, err
	}
	value, err = decodeJSONBytes[T](content)
	if err != nil {
		return value, "", nil, err
	}
	return value, fmt.Sprintf("%x", sha256.Sum256(content)), content, nil
}

func decodeJSONBytes[T any](content []byte) (T, error) {
	var value T
	decoder := json.NewDecoder(bytes.NewReader(content))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&value); err != nil {
		return value, err
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		if err == nil {
			return value, errors.New("multiple JSON values")
		}
		return value, err
	}
	return value, nil
}

func encodeJSON(writer io.Writer, value any) error {
	encoder := json.NewEncoder(writer)
	encoder.SetIndent("", "  ")
	return encoder.Encode(value)
}

func runAuditScheduleAOverlap(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	flags := flag.NewFlagSet("audit-schedule-a-overlap", flag.ContinueOnError)
	flags.SetOutput(stderr)
	scheduleAPath := flags.String("schedule-a", "", "path to a processed Schedule A .copy.zst extract")
	indivPath := flags.String("indiv", "", "path to the classic cycle indiv ZIP")
	othPath := flags.String("oth", "", "path to the classic cycle oth ZIP")
	period := flags.String("period", "", "expected two_year_transaction_period")
	workDir := flags.String("work-dir", "", "parent for automatically removed duplicate-check shards")
	maxRows := flags.Uint64("max-schedule-rows", 0, "stop after this many Schedule A rows; zero audits the complete stream")
	if err := flags.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 2
	}
	if flags.NArg() != 0 {
		fmt.Fprintf(stderr, "unexpected positional arguments: %s\n", strings.Join(flags.Args(), " "))
		return 2
	}
	if *scheduleAPath == "" || *indivPath == "" || *othPath == "" {
		_, _ = io.WriteString(stderr, "--schedule-a, --indiv, and --oth are required\n")
		return 2
	}
	if !validPeriod(*period) {
		_, _ = io.WriteString(stderr, "--period must be a four-digit even year\n")
		return 2
	}

	result, auditErr := fecschedulea.Audit(ctx, fecschedulea.Options{
		ScheduleAPath:   *scheduleAPath,
		IndivPath:       *indivPath,
		OthPath:         *othPath,
		Period:          *period,
		WorkDir:         *workDir,
		MaxScheduleRows: *maxRows,
		Progress: func(message string) {
			fmt.Fprintln(stderr, message)
		},
	})
	encoder := json.NewEncoder(stdout)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(result); err != nil {
		fmt.Fprintf(stderr, "encode result: %v\n", err)
		return 1
	}
	if auditErr != nil {
		fmt.Fprintf(stderr, "overlap audit failed: %v\n", auditErr)
		return 1
	}
	return 0
}

func runVerifyScheduleA(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	flags := flag.NewFlagSet("verify-schedule-a", flag.ContinueOnError)
	flags.SetOutput(stderr)
	input := flags.String("input", "", "path to a .copy.zst extract")
	period := flags.String("period", "", "expected two_year_transaction_period")
	expectedRows := flags.Uint64("expected-rows", 0, "complete-stream row count")
	expectedBytes := flags.Uint64("expected-bytes", 0, "complete uncompressed byte count")
	expectedSHA256 := flags.String("expected-sha256", "", "complete uncompressed SHA-256")
	expectedCompressedBytes := flags.Uint64("expected-compressed-bytes", 0, "compressed byte count")
	expectedCompressedSHA256 := flags.String("expected-compressed-sha256", "", "compressed SHA-256")
	maxRows := flags.Uint64("max-rows", 0, "stop after this many rows; zero verifies the complete stream")
	if err := flags.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 2
	}
	if flags.NArg() != 0 {
		fmt.Fprintf(stderr, "unexpected positional arguments: %s\n", strings.Join(flags.Args(), " "))
		return 2
	}
	if *input == "" {
		_, _ = io.WriteString(stderr, "--input is required\n")
		return 2
	}
	if *period != "" && !validPeriod(*period) {
		_, _ = io.WriteString(stderr, "--period must be a four-digit even year\n")
		return 2
	}

	file, err := os.Open(*input)
	if err != nil {
		fmt.Fprintf(stderr, "open input: %v\n", err)
		return 1
	}
	defer func() { _ = file.Close() }()

	result, verifyErr := schedulea.VerifyZstd(ctx, file, schedulea.VerifyOptions{
		ExpectedPeriod:             *period,
		ExpectedRows:               *expectedRows,
		ExpectedUncompressedBytes:  *expectedBytes,
		ExpectedUncompressedSHA256: *expectedSHA256,
		ExpectedCompressedBytes:    *expectedCompressedBytes,
		ExpectedCompressedSHA256:   *expectedCompressedSHA256,
		MaxRows:                    *maxRows,
	})
	encoder := json.NewEncoder(stdout)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(result); err != nil {
		fmt.Fprintf(stderr, "encode result: %v\n", err)
		return 1
	}
	if verifyErr != nil {
		fmt.Fprintf(stderr, "verification failed: %v\n", verifyErr)
		return 1
	}
	return 0
}

func runVerifyScheduleB(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	flags := flag.NewFlagSet("verify-schedule-b", flag.ContinueOnError)
	flags.SetOutput(stderr)
	input := flags.String("input", "", "path to data-row-only COPY text, or - for stdin")
	dump := flags.String("dump", "", "path to the official PostgreSQL custom archive")
	pgRestore := flags.String("pg-restore", "pg_restore", "pg_restore executable used with --dump")
	period := flags.String("period", "", "expected two_year_transaction_period")
	relation := flags.String("relation", "", "schema-qualified Schedule B partition; derived from period by default")
	workDir := flags.String("work-dir", "", "parent for automatically removed SUB_ID uniqueness shards")
	maxRows := flags.Uint64("max-rows", 0, "stop after this many rows; zero verifies the complete relation")
	if err := flags.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 2
	}
	if flags.NArg() != 0 {
		fmt.Fprintf(stderr, "unexpected positional arguments: %s\n", strings.Join(flags.Args(), " "))
		return 2
	}
	if (*input == "") == (*dump == "") {
		_, _ = io.WriteString(stderr, "exactly one of --input or --dump is required\n")
		return 2
	}
	if !validPeriod(*period) {
		_, _ = io.WriteString(stderr, "--period must be a four-digit even year\n")
		return 2
	}
	if *relation == "" {
		startYear, _ := strconv.Atoi(*period)
		*relation = fmt.Sprintf("disclosure.fec_fitem_sched_b_%d_%s", startYear-1, *period)
	}

	source, err := openScheduleBInput(ctx, *input, *dump, *pgRestore, *relation, *period)
	if err != nil {
		fmt.Fprintf(stderr, "open Schedule B input: %v\n", err)
		return 1
	}
	defer source.Close()

	result, verifyErr := scheduleb.Verify(ctx, source.Reader, scheduleb.VerifyOptions{
		ExpectedPeriod: *period, WorkDir: *workDir, MaxRows: *maxRows,
		Progress: func(message string) { fmt.Fprintln(stderr, message) },
	})
	if verifyErr != nil || !result.Complete {
		source.Close()
	}
	if extractionErr := source.Wait(); extractionErr != nil && verifyErr == nil && result.Complete {
		verifyErr = extractionErr
	}
	if err := encodeJSON(stdout, result); err != nil {
		fmt.Fprintf(stderr, "encode result: %v\n", err)
		return 1
	}
	if verifyErr != nil {
		fmt.Fprintf(stderr, "Schedule B verification failed: %v\n", verifyErr)
		return 1
	}
	return 0
}

type scheduleBInput struct {
	Reader     io.Reader
	close      func()
	extraction <-chan error
}

func openScheduleBInput(ctx context.Context, input, dump, pgRestore, relation, period string) (*scheduleBInput, error) {
	if dump != "" {
		reader, writer := io.Pipe()
		done := make(chan error, 1)
		go func() {
			_, err := fecrelease.ExtractRelation(ctx, pgRestore, dump, relation, period, writer)
			_ = writer.CloseWithError(err)
			done <- err
			close(done)
		}()
		return &scheduleBInput{Reader: reader, close: func() { _ = reader.Close() }, extraction: done}, nil
	}
	if input == "-" {
		return &scheduleBInput{Reader: os.Stdin}, nil
	}
	file, err := os.Open(input)
	if err != nil {
		return nil, err
	}
	return &scheduleBInput{Reader: file, close: func() { _ = file.Close() }}, nil
}

func (input *scheduleBInput) Close() {
	if input.close != nil {
		input.close()
	}
}

func (input *scheduleBInput) Wait() error {
	if input.extraction == nil {
		return nil
	}
	return <-input.extraction
}

func runAuditScheduleBOverlap(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	flags := flag.NewFlagSet("audit-schedule-b-overlap", flag.ContinueOnError)
	flags.SetOutput(stderr)
	input := flags.String("input", "", "path to data-row-only Schedule B COPY text, or - for stdin")
	dump := flags.String("dump", "", "path to the official Schedule B PostgreSQL custom archive")
	pgRestore := flags.String("pg-restore", "pg_restore", "pg_restore executable used with --dump")
	pas2 := flags.String("pas2", "", "path to the classic cycle pas2 ZIP")
	oth := flags.String("oth", "", "path to the classic cycle oth ZIP")
	period := flags.String("period", "", "expected two_year_transaction_period")
	relation := flags.String("relation", "", "schema-qualified Schedule B partition; derived from period by default")
	workDir := flags.String("work-dir", "", "parent for automatically removed SUB_ID uniqueness shards")
	maxRows := flags.Uint64("max-rows", 0, "stop after this many Schedule B rows; zero audits the complete relation")
	if err := flags.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 2
	}
	if flags.NArg() != 0 {
		fmt.Fprintf(stderr, "unexpected positional arguments: %s\n", strings.Join(flags.Args(), " "))
		return 2
	}
	if (*input == "") == (*dump == "") || *pas2 == "" || *oth == "" {
		_, _ = io.WriteString(stderr, "exactly one of --input or --dump, plus --pas2 and --oth, is required\n")
		return 2
	}
	if !validPeriod(*period) {
		_, _ = io.WriteString(stderr, "--period must be a four-digit even year\n")
		return 2
	}
	if *relation == "" {
		startYear, _ := strconv.Atoi(*period)
		*relation = fmt.Sprintf("disclosure.fec_fitem_sched_b_%d_%s", startYear-1, *period)
	}
	source, err := openScheduleBInput(ctx, *input, *dump, *pgRestore, *relation, *period)
	if err != nil {
		fmt.Fprintf(stderr, "open Schedule B input: %v\n", err)
		return 1
	}
	defer source.Close()
	result, auditErr := fecscheduleb.Audit(ctx, source.Reader, fecscheduleb.Options{
		Pas2Path: *pas2, OthPath: *oth, Period: *period, WorkDir: *workDir, MaxRows: *maxRows,
		Progress: func(message string) { fmt.Fprintln(stderr, message) },
	})
	if auditErr != nil || !result.Complete {
		source.Close()
	}
	if extractionErr := source.Wait(); extractionErr != nil && auditErr == nil && result.Complete {
		auditErr = extractionErr
	}
	if err := encodeJSON(stdout, result); err != nil {
		fmt.Fprintf(stderr, "encode result: %v\n", err)
		return 1
	}
	if auditErr != nil {
		fmt.Fprintf(stderr, "Schedule B overlap audit failed: %v\n", auditErr)
		return 1
	}
	return 0
}

func runVerifyScheduleE(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	flags := flag.NewFlagSet("verify-schedule-e", flag.ContinueOnError)
	flags.SetOutput(stderr)
	input := flags.String("input", "", "path to data-row-only COPY text, or - for stdin")
	cycle := flags.String("cycle", "", "optional expected election_cycle")
	expectedRows := flags.Uint64("expected-rows", 0, "complete-stream row count")
	expectedBytes := flags.Uint64("expected-bytes", 0, "complete-stream byte count")
	expectedSHA256 := flags.String("expected-sha256", "", "complete-stream SHA-256")
	maxRows := flags.Uint64("max-rows", 0, "stop after this many rows; zero verifies the complete stream")
	if err := flags.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 2
	}
	if flags.NArg() != 0 {
		fmt.Fprintf(stderr, "unexpected positional arguments: %s\n", strings.Join(flags.Args(), " "))
		return 2
	}
	if *input == "" {
		_, _ = io.WriteString(stderr, "--input is required\n")
		return 2
	}
	if *cycle != "" && !validPeriod(*cycle) {
		_, _ = io.WriteString(stderr, "--cycle must be a four-digit even year\n")
		return 2
	}

	var source io.Reader = os.Stdin
	var closeSource func()
	if *input != "-" {
		file, err := os.Open(*input)
		if err != nil {
			fmt.Fprintf(stderr, "open input: %v\n", err)
			return 1
		}
		source = file
		closeSource = func() { _ = file.Close() }
	}
	if closeSource != nil {
		defer closeSource()
	}

	result, verifyErr := schedulee.Verify(ctx, source, schedulee.VerifyOptions{
		ExpectedCycle:  *cycle,
		ExpectedRows:   *expectedRows,
		ExpectedBytes:  *expectedBytes,
		ExpectedSHA256: *expectedSHA256,
		MaxRows:        *maxRows,
	})
	encoder := json.NewEncoder(stdout)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(result); err != nil {
		fmt.Fprintf(stderr, "encode result: %v\n", err)
		return 1
	}
	if verifyErr != nil {
		fmt.Fprintf(stderr, "verification failed: %v\n", verifyErr)
		return 1
	}
	return 0
}

func validPeriod(period string) bool {
	if len(period) != 4 {
		return false
	}
	for _, character := range period {
		if character < '0' || character > '9' {
			return false
		}
	}
	return (period[3]-'0')%2 == 0
}

func isHelp(argument string) bool {
	return argument == "help" || argument == "-h" || argument == "--help"
}
