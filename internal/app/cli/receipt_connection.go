package cli

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"

	"github.com/vedantadhobley/legal-tender/internal/projection/arango/receiptgraph"
)

func runReceiptConnection(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	return runReceiptConnectionMode(ctx, args, stdout, stderr, false)
}

func runReceiptConnectionMode(ctx context.Context, args []string, stdout, stderr io.Writer, gate bool) int {
	name := "inspect-receipt-candidate-connection"
	if gate {
		name = "validate-receipt-candidate-connections"
	}
	f := flag.NewFlagSet(name, flag.ContinueOnError)
	f.SetOutput(stderr)
	var o receiptgraph.ConnectionOptions
	var passwordEnv, expectedGate string
	f.StringVar(&o.Manifest, "graph-manifest", "", "exact completed full-cycle receipt graph manifest")
	f.StringVar(&o.ManifestSHA256, "expected-graph-sha256", "", "required completed manifest SHA256")
	f.StringVar(&o.FlowBundle, "flow-bundle", "", "exact published committee-flow evidence bundle")
	if gate {
		f.StringVar(&expectedGate, "expected-gate-id", "", "optional exact proof identity required on whole-gate replay")
	} else {
		f.StringVar(&o.Candidate, "candidate", "", "candidate ID; no built-in selection")
		f.Uint64Var(&o.Ordinal, "source-row-ordinal", 0, "receipt occurrence in the completed graph's pinned fact set")
	}
	registerReceiptSourceFlags(f, &o.Sources, &passwordEnv)
	if err := f.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 2
	}
	if f.NArg() != 0 || !gate && (o.Ordinal == 0 || o.Candidate == "") {
		fmt.Fprintln(stderr, "exact source ordinal and candidate required")
		return 2
	}
	for _, value := range []string{o.Manifest, o.ManifestSHA256, o.FlowBundle, o.Sources.StorageRoot, o.Sources.Participants, o.Sources.ParticipantID, o.Sources.Conduits, o.Sources.ConduitID, o.Sources.Facts, o.Sources.Committees, o.Sources.Candidates, o.Sources.Linkages, o.Sources.Endpoint} {
		if value == "" {
			fmt.Fprintln(stderr, "completed graph and all exact source/flow inputs required")
			return 2
		}
	}
	o.Sources.Password = os.Getenv(passwordEnv)
	if o.Sources.Password == "" {
		fmt.Fprintln(stderr, "configured Arango password required")
		return 2
	}
	var err error
	o.BuildSHA256, err = executableDigest(ctx)
	if err != nil {
		fmt.Fprintln(stderr, "executable identity:", err)
		return 1
	}
	o.Sources.Progress = func(s string) { fmt.Fprintln(stderr, s) }
	var out any
	if gate {
		out, err = receiptgraph.ValidateConnections(ctx, receiptgraph.ConnectionGateOptions{ConnectionOptions: o, ExpectedGateID: expectedGate})
	} else {
		out, err = receiptgraph.InspectConnection(ctx, o)
	}
	if err != nil {
		fmt.Fprintln(stderr, "receipt connection:", err)
		return 1
	}
	if err = encodeJSON(stdout, out); err != nil {
		fmt.Fprintln(stderr, err)
		return 1
	}
	return 0
}

func registerReceiptSourceFlags(f *flag.FlagSet, o *receiptgraph.Options, passwordEnv *string) {
	f.StringVar(&o.StorageRoot, "storage-root", "", "read-only published source storage")
	f.StringVar(&o.Participants, "participant-manifest", "", "exact participant manifest")
	f.StringVar(&o.ParticipantID, "expected-participant-id", "", "participant identity")
	f.StringVar(&o.Conduits, "conduit-manifest", "", "exact conduit manifest")
	f.StringVar(&o.ConduitID, "expected-conduit-id", "", "conduit identity")
	f.StringVar(&o.Facts, "schedule-a-facts", "", "exact Schedule A fact manifest")
	f.StringVar(&o.Committees, "committee-facts", "", "exact committee master")
	f.StringVar(&o.Candidates, "candidate-facts", "", "exact candidate master")
	f.StringVar(&o.Linkages, "linkage-facts", "", "exact candidate authorization facts")
	f.StringVar(&o.Endpoint, "endpoint", "", "Arango endpoint; credentials separate")
	f.StringVar(&o.Username, "username", "root", "configured Arango user")
	f.StringVar(passwordEnv, "password-env", "ARANGO_PASSWORD", "configured password environment variable")
}
