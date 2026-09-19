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

func runIssuerFiling(ctx context.Context, command string, args []string, stdout, stderr io.Writer) int {
	f := flag.NewFlagSet(command, flag.ContinueOnError)
	f.SetOutput(stderr)
	var ref sec.FilingReference
	f.StringVar(&ref.CIK, "cik", "", "exact ten-digit filer CIK")
	f.StringVar(&ref.Accession, "accession", "", "explicit official accession; no automatic latest selection")
	f.StringVar(&ref.Document, "document", "", "primary inline-XBRL document basename")
	var output, agent, capture, pin, queries, queryPin, directory, directoryPin string
	if command == "capture-issuer-filing" {
		f.StringVar(&output, "output", "", "new private filing capture directory")
		f.StringVar(&agent, "user-agent", "", "declared request contact; defaults to SEC_USER_AGENT")
	} else {
		f.StringVar(&capture, "filing-capture", "", "saved filing capture; offline")
		f.StringVar(&pin, "expected-filing-sha256", "", "exact filing capture.json digest")
		f.StringVar(&queries, "queries", "", "original source-backed FEC query file")
		f.StringVar(&queryPin, "expected-queries-sha256", "", "exact query file digest")
		f.StringVar(&directory, "issuer-capture", "", "exact SEC directory capture")
		f.StringVar(&directoryPin, "expected-issuer-sha256", "", "exact directory capture.json digest")
	}
	if err := f.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 2
	}
	if agent == "" {
		agent = os.Getenv("SEC_USER_AGENT")
	}
	if f.NArg() != 0 || ref.Validate() != nil || (command == "capture-issuer-filing" && (output == "" || agent == "")) ||
		(command == "inspect-issuer-filing" && (capture == "" || queries == "" || directory == "" || !wikimedia.Digest(pin) || !wikimedia.Digest(queryPin) || !wikimedia.Digest(directoryPin))) {
		fmt.Fprintln(stderr, "valid explicit filing reference and complete capture/inspection inputs required")
		return 2
	}
	build, err := executableDigest(ctx)
	if err != nil {
		fmt.Fprintln(stderr, err)
		return 1
	}
	var result any
	code := 0
	if command == "capture-issuer-filing" {
		pin, err = sec.CaptureFiling(ctx, sec.Options{Directory: output, BuildSHA256: build, UserAgent: agent}, ref)
		if err == nil {
			var filing sec.FilingIdentity
			filing, err = sec.ReadFiling(output, pin, ref)
			result = filing
			if !filing.SourceUsable {
				code = 1
			}
		}
	} else {
		var raw []byte
		var file *os.File
		file, err = os.Open(queries)
		if err == nil {
			raw, err = io.ReadAll(io.LimitReader(file, wikimedia.MaxBody+1))
			file.Close()
		}
		if err == nil {
			var d org.IssuerResult
			d, err = org.DiscoverIssuers(raw, queryPin, directory, directoryPin, build)
			if err == nil {
				var filing sec.FilingIdentity
				filing, err = sec.ReadFiling(capture, pin, ref)
				if err == nil {
					result, err = org.CompareIssuerFiling(d, filing, build)
					if !d.SourceUsable || !filing.SourceUsable {
						code = 1
					}
				}
			}
		}
	}
	if err != nil {
		fmt.Fprintln(stderr, "filed issuer evidence:", err)
		return 1
	}
	if err = encodeJSON(stdout, result); err != nil {
		fmt.Fprintln(stderr, err)
		return 1
	}
	return code
}
