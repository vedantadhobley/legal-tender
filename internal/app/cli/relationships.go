package cli

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/vedantadhobley/legal-tender/internal/source/wikimedia"
)

func runRelationships(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	f := flag.NewFlagSet("query-relationships", flag.ContinueOnError)
	f.SetOutput(stderr)
	var body, pin, ids, entity, observed string
	f.StringVar(&body, "body", "", "retained wbgetentities JSON body; offline only")
	f.StringVar(&pin, "expected-body-sha256", "", "required exact response-body digest")
	f.StringVar(&ids, "ids", "", "complete expected response QIDs, comma-separated; at most twenty")
	f.StringVar(&entity, "entity", "", "source-qualified wikidata:QID; match either source endpoint")
	f.StringVar(&observed, "observed-at", "", "optional acquisition timestamp (RFC3339), caller-supplied, not role validity")
	if err := f.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 2
	}
	if f.NArg() != 0 || body == "" || !wikimedia.Digest(pin) || ids == "" || entity == "" {
		fmt.Fprintln(stderr, "body, exact body SHA-256, expected IDs and source-qualified entity are required")
		return 2
	}
	if err := ctx.Err(); err != nil {
		fmt.Fprintln(stderr, err)
		return 1
	}
	st, err := os.Lstat(body)
	if err != nil || !st.Mode().IsRegular() || st.Size() <= 0 || st.Size() > wikimedia.MaxBody {
		fmt.Fprintln(stderr, "relationship query requires a bounded regular body file")
		return 1
	}
	file, err := os.Open(body)
	if err != nil {
		fmt.Fprintln(stderr, "cannot open relationship body")
		return 1
	}
	raw, err := io.ReadAll(io.LimitReader(file, wikimedia.MaxBody+1))
	closeErr := file.Close()
	if err != nil || closeErr != nil {
		fmt.Fprintln(stderr, "cannot read relationship body")
		return 1
	}
	evidence, err := wikimedia.QueryRelationships(raw, pin, strings.Split(ids, ","), entity, observed)
	if err != nil {
		fmt.Fprintln(stderr, "relationship query:", err)
		return 1
	}
	build, err := executableDigest(ctx)
	if err != nil {
		fmt.Fprintln(stderr, err)
		return 1
	}
	result := struct {
		BuildSHA256 string                      `json:"build_sha256"`
		Evidence    wikimedia.RelationshipQuery `json:"evidence"`
	}{build, evidence}
	if err := encodeJSON(stdout, result); err != nil {
		fmt.Fprintln(stderr, err)
		return 1
	}
	return 0
}
