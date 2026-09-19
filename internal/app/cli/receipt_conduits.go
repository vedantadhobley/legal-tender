package cli

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"

	"github.com/vedantadhobley/legal-tender/internal/calculation/fec/receiptconduits"
)

func runReceiptConduits(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	return runReceiptConduitCommand(ctx, args, stdout, stderr, false, false)
}

func runReceiptConduitCommand(ctx context.Context, args []string, stdout, stderr io.Writer, profile, groupMode bool) int {
	command := "publish-receipt-conduit-associations"
	if profile {
		command = "profile-shared-receipt-references"
	}
	if groupMode {
		command = "publish-shared-receipt-conduit-associations"
	}
	f := flag.NewFlagSet(command, flag.ContinueOnError)
	f.SetOutput(stderr)
	var o receiptconduits.Options
	if groupMode {
		f.StringVar(&o.GroupBaseline, "baseline-manifest", "", "exact accepted v1 conduit manifest.json")
		f.StringVar(&o.GroupBaselineID, "expected-baseline-id", "", "required accepted v1 conduit identity")
	}
	f.StringVar(&o.Participants, "participant-manifest", "", "exact participant manifest.json")
	f.StringVar(&o.ParticipantID, "expected-participant-id", "", "required complete participant identity")
	f.StringVar(&o.Topology, "topology-manifest", "", "exact reference endpoint manifest.json")
	f.StringVar(&o.TopologyID, "expected-topology-id", "", "required reference topology identity")
	f.StringVar(&o.OutputDirectory, "output-dir", "", "new isolated output directory")
	f.IntVar(&o.Workers, "workers", 8, "bounded independent shard workers, 1..8")
	f.IntVar(&o.RunRows, "run-rows", 100000, "maximum records per sort run, 1..100000")
	f.IntVar(&o.FanIn, "merge-fan-in", 8, "bounded merge fan-in, 2..16")
	f.Uint64Var(&o.MaxWorkspaceBytes, "max-workspace-bytes", 8<<30, "shared workspace cap, at most 32GiB; 1MiB per metadata file reserved separately")
	if err := f.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 2
	}
	if f.NArg() != 0 || o.Participants == "" || o.ParticipantID == "" || o.Topology == "" || o.TopologyID == "" || o.OutputDirectory == "" || (groupMode && (o.GroupBaseline == "" || o.GroupBaselineID == "")) {
		fmt.Fprintln(stderr, "exact participant/topology inputs, identities and new output required")
		return 2
	}
	var err error
	o.BuildSHA256, err = executableDigest(ctx)
	if err != nil {
		fmt.Fprintln(stderr, err)
		return 1
	}
	o.Progress = func(s string) { fmt.Fprintln(stderr, s) }
	var r any
	if profile {
		r, err = receiptconduits.ProfileSharedReferences(ctx, o)
	} else {
		r, err = receiptconduits.Run(ctx, o)
	}
	if err != nil {
		fmt.Fprintln(stderr, command+":", err)
		return 1
	}
	if err = encodeJSON(stdout, r); err != nil {
		fmt.Fprintln(stderr, err)
		return 1
	}
	return 0
}
