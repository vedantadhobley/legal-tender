package cli

import (
	"bytes"
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/vedantadhobley/legal-tender/internal/audit/fecpreattribution"
)

func runAuditPreAttributionInterpretations(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	flags := flag.NewFlagSet("audit-pre-attribution-interpretations", flag.ContinueOnError)
	flags.SetOutput(stderr)
	storageRoot := flags.String("storage-root", "", "root of Legal Tender durable storage")
	candidateResolution := flags.String("candidate-resolution", "", "exact published candidate-resolution manifest")
	receiverFlows := flags.String("receiver-flows", "", "exact published receiver-flow manifest")
	scheduleBSemantics := flags.String("schedule-b-semantics", "", "complete Schedule B semantics audit result")
	committeeFlowResult := flags.String("committee-flow-result", "", "published committee-flow reconciliation result")
	committeeFlowEvidence := flags.String("committee-flow-evidence-root", "", "root containing the reconciliation evidence artifacts")
	output := flags.String("output", "", "optional path for the atomically retained successful result")
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
	if *storageRoot == "" || *candidateResolution == "" || *receiverFlows == "" || *scheduleBSemantics == "" ||
		*committeeFlowResult == "" || *committeeFlowEvidence == "" {
		_, _ = io.WriteString(stderr, "--storage-root, --candidate-resolution, --receiver-flows, --schedule-b-semantics, --committee-flow-result, and --committee-flow-evidence-root are required\n")
		return 2
	}
	result, auditErr := fecpreattribution.Audit(ctx, fecpreattribution.Options{
		StorageRoot: *storageRoot, CandidateResolutionPath: *candidateResolution,
		ReceiverFlowPath: *receiverFlows, ScheduleBSemanticsPath: *scheduleBSemantics,
		CommitteeFlowResultPath: *committeeFlowResult, CommitteeFlowEvidenceRoot: *committeeFlowEvidence,
		Progress: func(message string) { fmt.Fprintln(stderr, message) },
	})
	var encoded bytes.Buffer
	if err := encodeJSON(&encoded, result); err != nil {
		fmt.Fprintf(stderr, "encode pre-attribution interpretation audit: %v\n", err)
		return 1
	}
	if _, err := stdout.Write(encoded.Bytes()); err != nil {
		fmt.Fprintf(stderr, "write pre-attribution interpretation audit: %v\n", err)
		return 1
	}
	if auditErr != nil {
		fmt.Fprintf(stderr, "pre-attribution interpretation audit failed: %v\n", auditErr)
		return 1
	}
	if *output != "" {
		if err := writeAuditResult(*output, encoded.Bytes()); err != nil {
			fmt.Fprintf(stderr, "retain pre-attribution interpretation audit: %v\n", err)
			return 1
		}
	}
	return 0
}

func writeAuditResult(path string, content []byte) error {
	directory := filepath.Dir(path)
	if err := os.MkdirAll(directory, 0o750); err != nil {
		return err
	}
	if existing, err := os.ReadFile(path); err == nil {
		if !bytes.Equal(existing, content) {
			return fmt.Errorf("audit result collision at %s", path)
		}
		return nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	temporary, err := os.CreateTemp(directory, ".pre-attribution-*.json")
	if err != nil {
		return err
	}
	temporaryPath := temporary.Name()
	defer func() { _ = os.Remove(temporaryPath) }()
	if err := temporary.Chmod(0o640); err != nil {
		_ = temporary.Close()
		return err
	}
	if _, err := temporary.Write(content); err != nil {
		_ = temporary.Close()
		return err
	}
	if err := temporary.Sync(); err != nil {
		_ = temporary.Close()
		return err
	}
	if err := temporary.Close(); err != nil {
		return err
	}
	if err := os.Link(temporaryPath, path); err != nil {
		if !errors.Is(err, os.ErrExist) {
			return err
		}
		existing, readErr := os.ReadFile(path)
		if readErr != nil {
			return readErr
		}
		if !bytes.Equal(existing, content) {
			return fmt.Errorf("audit result collision at %s", path)
		}
	}
	dir, err := os.Open(directory)
	if err != nil {
		return err
	}
	defer dir.Close()
	return dir.Sync()
}
