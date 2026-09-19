package reportperiod

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"os"
	"path/filepath"

	"github.com/vedantadhobley/legal-tender/internal/source/fec/reportmetadata"
	"github.com/vedantadhobley/legal-tender/internal/source/fec/reportscope"
	"github.com/vedantadhobley/legal-tender/internal/strictjson"
)

const DocumentSetVersion = "legal-tender.fec.report-document-set.v1"
const MaxWindowDocuments = 64
const MaxWindowDocumentBytes = 16 << 20

type DocumentReference struct {
	SourceURL string                  `json:"source_url"`
	Body      reportmetadata.Artifact `json:"body"`
	Headers   reportmetadata.Artifact `json:"headers"`
}

type DocumentSet struct {
	Version   string              `json:"version"`
	Documents []DocumentReference `json:"documents"`
}

func readDocumentSet(ctx context.Context, path string) (reportmetadata.Artifact, []reportscope.Request, error) {
	var identity reportmetadata.Artifact
	if err := ctx.Err(); err != nil {
		return identity, nil, err
	}
	f, err := os.Open(path)
	if err != nil {
		return identity, nil, errors.New("cannot open document-set descriptor")
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil || !info.Mode().IsRegular() || info.Size() > 128<<10 {
		return identity, nil, errors.New("invalid document-set descriptor size or type")
	}
	raw, err := io.ReadAll(io.LimitReader(f, (128<<10)+1))
	if err != nil || len(raw) > 128<<10 {
		return identity, nil, errors.New("document-set descriptor exceeds read budget")
	}
	var set DocumentSet
	if err := strictjson.Decode(raw, &set); err != nil {
		return identity, nil, err
	}
	if set.Version != DocumentSetVersion || set.Documents == nil || len(set.Documents) > MaxWindowDocuments {
		return identity, nil, errors.New("invalid document-set version or document count")
	}
	h := sha256.Sum256(raw)
	identity = reportmetadata.Artifact{Path: path, SHA256: hex.EncodeToString(h[:]), Bytes: int64(len(raw))}
	requests := make([]reportscope.Request, 0, len(set.Documents))
	var total int64
	for _, d := range set.Documents {
		if err := ctx.Err(); err != nil {
			return identity, nil, err
		}
		paths := []string{}
		for i, a := range []reportmetadata.Artifact{d.Body, d.Headers} {
			cap := int64(reportscope.MaxBodyBytes)
			if i == 1 {
				cap = 128 << 10
			}
			if a.Path == "" || a.Bytes <= 0 || a.Bytes > cap {
				return identity, nil, errors.New("invalid document artifact declaration")
			}
			total += a.Bytes
			if total > MaxWindowDocumentBytes {
				return identity, nil, errors.New("document-set artifact budget exceeded")
			}
			p := a.Path
			if !filepath.IsAbs(p) {
				p = filepath.Join(filepath.Dir(path), p)
			}
			stat, err := os.Stat(p)
			if err != nil || !stat.Mode().IsRegular() || stat.Size() != a.Bytes {
				return identity, nil, errors.New("document artifact size or type mismatch")
			}
			paths = append(paths, p)
		}
		requests = append(requests, reportscope.Request{SourceURL: d.SourceURL, BodyPath: paths[0], BodySHA256: d.Body.SHA256, HeadersPath: paths[1], HeadersSHA256: d.Headers.SHA256})
	}
	return identity, requests, nil
}
