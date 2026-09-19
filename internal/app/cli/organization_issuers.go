package cli

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"

	org "github.com/vedantadhobley/legal-tender/internal/calculation/organizationresolution"
	"github.com/vedantadhobley/legal-tender/internal/source/sec"
	"github.com/vedantadhobley/legal-tender/internal/source/wikimedia"
)

func runOrganizationIssuers(ctx context.Context, command string, args []string, stdout, stderr io.Writer) int {
	f := flag.NewFlagSet(command, flag.ContinueOnError)
	f.SetOutput(stderr)
	var output, agent, queries, queryPin, capture, pin string
	if command == "capture-issuer-directory" {
		f.StringVar(&output, "output", "", "new SEC bulk snapshot directory")
		f.StringVar(&agent, "user-agent", "", "declared SEC user agent with reachable contact; defaults to SEC_USER_AGENT")
	} else {
		f.StringVar(&queries, "queries", "", "saved source-backed organization queries")
		f.StringVar(&queryPin, "expected-queries-sha256", "", "required exact query-file digest")
		f.StringVar(&capture, "issuer-capture", "", "saved SEC bulk snapshot; offline")
		f.StringVar(&pin, "expected-issuer-sha256", "", "required SEC capture.json digest")
	}
	if err := f.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 2
	}
	// Read only this source's configuration; do not load or expose the rest of
	// the environment. Apply after parsing so --help never prints the contact.
	if command == "capture-issuer-directory" && agent == "" {
		agent = os.Getenv("SEC_USER_AGENT")
	}
	if f.NArg() != 0 || (command == "capture-issuer-directory" && (output == "" || agent == "")) ||
		(command != "capture-issuer-directory" && (queries == "" || capture == "" || !wikimedia.Digest(queryPin) || !wikimedia.Digest(pin))) {
		fmt.Fprintln(stderr, "capture requires a new output and declared user agent; discovery requires exact query and issuer capture pins")
		return 2
	}
	build, err := executableDigest(ctx)
	if err != nil {
		fmt.Fprintln(stderr, err)
		return 1
	}
	var result any
	code := 0
	if command == "capture-issuer-directory" {
		pin, err = sec.Capture(ctx, sec.Options{Directory: output, BuildSHA256: build, UserAgent: agent})
		if err == nil {
			var r sec.Replay
			r, err = sec.Read(output, pin)
			// Do not duplicate the entire directory in stdout.
			result = struct {
				CaptureSHA256 string       `json:"capture_sha256"`
				Source        sec.Manifest `json:"source"`
				Rows          int          `json:"directory_rows"`
				Issue         string       `json:"issue,omitempty"`
			}{pin, r.Manifest, len(r.Rows), r.Issue}
			if r.Issue != "" {
				code = 1
			}
		}
	} else {
		var file *os.File
		file, err = os.Open(queries)
		if err == nil {
			var b []byte
			b, err = io.ReadAll(io.LimitReader(file, wikimedia.MaxBody+1))
			file.Close()
			if err == nil {
				var r org.IssuerResult
				r, err = org.DiscoverIssuers(b, queryPin, capture, pin, build)
				result = r
				if !r.SourceUsable {
					code = 1
				}
			}
		}
	}
	if err != nil {
		fmt.Fprintln(stderr, "issuer evidence:", err)
		return 1
	}
	if err = encodeJSON(stdout, result); err != nil {
		fmt.Fprintln(stderr, err)
		return 1
	}
	return code
}
