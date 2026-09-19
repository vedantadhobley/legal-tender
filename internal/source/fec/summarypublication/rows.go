package summarypublication

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"

	"github.com/vedantadhobley/legal-tender/internal/source/fec/committeesummary"
	"github.com/vedantadhobley/legal-tender/internal/storage/artifact"
)

// buildRows creates an unpublished content-addressed artifact. Publication
// additionally requires exact release membership. Corpus tests can exercise
// this boundary without inventing a coordinated release for research captures.
func buildRows(ctx context.Context, root string, input io.ReadSeeker, expected committeesummary.Expected) (artifact.Descriptor, committeesummary.Verification, error) {
	writer, err := artifact.NewWriter(ctx, root, filepath.Join(root, basePath, "staging"), basePath, "facts")
	if err != nil {
		return artifact.Descriptor{}, committeesummary.Verification{}, err
	}
	defer writer.Abort()
	verification, err := committeesummary.Verify(ctx, input, expected, func(row *committeesummary.Record) error {
		return writer.WriteJSON(newFact(expected, row))
	})
	if err != nil {
		return artifact.Descriptor{}, committeesummary.Verification{}, err
	}
	descriptor, err := writer.Finalize()
	if err != nil {
		return descriptor, verification, err
	}
	if _, err := input.Seek(0, io.SeekStart); err != nil {
		return descriptor, verification, err
	}
	if err := verifyRows(ctx, root, input, expected, descriptor, verification); err != nil {
		return descriptor, verification, err
	}
	return descriptor, verification, nil
}

// verifyRows compares every stored field and provenance value against a fresh
// raw-source read. EOF verifies both compressed and uncompressed identities.
func verifyRows(ctx context.Context, root string, input io.Reader, expected committeesummary.Expected, descriptor artifact.Descriptor, want committeesummary.Verification) error {
	if descriptor.CompressedBytes == 0 || descriptor.UncompressedBytes == 0 || descriptor.RecordCount != want.Rows || descriptor.Compression != "zstd" || len(descriptor.CompressedSHA256) != 64 {
		return fmt.Errorf("invalid committee-summary fact artifact")
	}
	canonical := filepath.ToSlash(filepath.Join(basePath, "facts", "sha256", descriptor.CompressedSHA256[:2], descriptor.CompressedSHA256+".jsonl.zst"))
	if descriptor.StorageKey != canonical {
		return fmt.Errorf("noncanonical committee-summary fact artifact")
	}
	reader, err := artifact.Open[Fact](ctx, root, descriptor)
	if err != nil {
		return err
	}
	defer reader.Abort()
	got, err := committeesummary.Verify(ctx, input, expected, func(row *committeesummary.Record) error {
		stored, ok, err := reader.Next()
		if err != nil {
			return err
		}
		if !ok || !reflect.DeepEqual(stored, newFact(expected, row)) {
			return fmt.Errorf("committee-summary fact readback mismatch at ordinal %d", row.Ordinal)
		}
		return nil
	})
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(got, want) {
		return fmt.Errorf("committee-summary verification readback mismatch")
	}
	if _, ok, err := reader.Next(); err != nil {
		return err
	} else if ok {
		return fmt.Errorf("extra committee-summary facts")
	}
	return nil
}

func openSource(root string, expected committeesummary.Expected) (*os.File, error) {
	if err := committeesummary.ValidateExpected(expected); err != nil {
		return nil, err
	}
	file, err := os.Open(filepath.Join(root, "raw", "fec", "artifacts", "sha256", expected.SHA256[:2], expected.SHA256))
	if err != nil {
		return nil, err
	}
	info, err := file.Stat()
	if err != nil || !info.Mode().IsRegular() || info.Size() != expected.Bytes {
		_ = file.Close()
		return nil, fmt.Errorf("committee-summary raw artifact has invalid size or type")
	}
	return file, nil
}
