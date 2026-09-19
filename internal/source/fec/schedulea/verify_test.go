// Stream verification tests cover complete, partial, and failed conservation.
package schedulea

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/klauspost/compress/zstd"
)

func TestVerifyZstdComplete(t *testing.T) {
	t.Parallel()
	physical, compressed := compressedFixture(t, "negative-adjustment.copy", 1)
	uncompressedDigest := sha256.Sum256(physical)
	compressedDigest := sha256.Sum256(compressed)

	result, err := VerifyZstd(context.Background(), bytes.NewReader(compressed), VerifyOptions{
		ExpectedPeriod:             "2026",
		ExpectedRows:               1,
		ExpectedUncompressedBytes:  uint64(len(physical)),
		ExpectedUncompressedSHA256: hex.EncodeToString(uncompressedDigest[:]),
		ExpectedCompressedBytes:    uint64(len(compressed)),
		ExpectedCompressedSHA256:   hex.EncodeToString(compressedDigest[:]),
	})
	if err != nil {
		t.Fatalf("VerifyZstd returned error: %v", err)
	}
	if !result.Complete || result.Rows != 1 || result.ValidRows != 1 || result.InvalidRows != 0 {
		t.Fatalf("unexpected result: %+v", result)
	}
	for _, check := range result.Checks {
		if !check.Passed {
			t.Fatalf("check failed: %+v", check)
		}
	}
}

func TestVerifyZstdReportsFailedExpectation(t *testing.T) {
	t.Parallel()
	_, compressed := compressedFixture(t, "negative-adjustment.copy", 1)
	result, err := VerifyZstd(context.Background(), bytes.NewReader(compressed), VerifyOptions{
		ExpectedPeriod: "2026",
		ExpectedRows:   2,
	})
	if !IsVerifyError(err) {
		t.Fatalf("error = %v; want VerifyError", err)
	}
	var verifyError *VerifyError
	if !errors.As(err, &verifyError) || verifyError.FailedChecks != 1 {
		t.Fatalf("verification error = %+v; want one failed check", verifyError)
	}
	if !result.Complete {
		t.Fatal("mismatched expectation should still complete the stream")
	}
}

func TestVerifyZstdPartialPassMakesNoWholeStreamClaims(t *testing.T) {
	t.Parallel()
	_, compressed := compressedFixture(t, "negative-adjustment.copy", 2)
	result, err := VerifyZstd(context.Background(), bytes.NewReader(compressed), VerifyOptions{
		ExpectedPeriod: "2026",
		MaxRows:        1,
	})
	if err != nil {
		t.Fatalf("VerifyZstd returned error: %v", err)
	}
	if result.Complete || result.Rows != 1 {
		t.Fatalf("partial result = %+v", result)
	}
	if result.CompressedSHA256 != "" || result.UncompressedSHA256 != "" {
		t.Fatal("partial pass published a whole-stream digest")
	}
}

func TestVerifyZstdHonorsCanceledContext(t *testing.T) {
	t.Parallel()
	_, compressed := compressedFixture(t, "negative-adjustment.copy", 20000)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := VerifyZstd(ctx, bytes.NewReader(compressed), VerifyOptions{ExpectedPeriod: "2026"})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("error = %v; want context.Canceled", err)
	}
}

func TestVerifyZstdRowsObservationsRemainProvisional(t *testing.T) {
	_, compressed := compressedFixture(t, "negative-adjustment.copy", 2)
	seen := 0
	v, err := VerifyZstdRows(context.Background(), bytes.NewReader(compressed), VerifyOptions{ExpectedPeriod: "2026", ExpectedRows: 3}, func(row *Row) error { seen++; return nil })
	if !IsVerifyError(err) || seen != 2 || !v.Complete {
		t.Fatal("callbacks bypassed whole-stream checks", v, err)
	}
	_, err = VerifyZstdRows(context.Background(), bytes.NewReader(compressed), VerifyOptions{ExpectedPeriod: "2024"}, func(row *Row) error { t.Fatal("invalid row visited"); return nil })
	if !IsVerifyError(err) {
		t.Fatal(err)
	}
	want := errors.New("observer failed")
	v, err = VerifyZstdRows(context.Background(), bytes.NewReader(compressed), VerifyOptions{ExpectedPeriod: "2026"}, func(row *Row) error { return want })
	if !errors.Is(err, want) || v.Complete || v.CompressedSHA256 != "" {
		t.Fatal("observer failure published complete verification", v, err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = VerifyZstdRows(ctx, bytes.NewReader(compressed), VerifyOptions{}, func(row *Row) error { t.Fatal("canceled observer ran"); return nil })
	if !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}

func compressedFixture(t *testing.T, name string, copies int) ([]byte, []byte) {
	t.Helper()
	physical, err := os.ReadFile(filepath.Join(fixtureDirectory(t), name))
	if err != nil {
		t.Fatalf("read fixture: %v", err)
	}
	physical = bytes.Repeat(physical, copies)
	var compressed bytes.Buffer
	encoder, err := zstd.NewWriter(&compressed, zstd.WithEncoderConcurrency(1))
	if err != nil {
		t.Fatalf("create zstd writer: %v", err)
	}
	if _, err := encoder.Write(physical); err != nil {
		t.Fatalf("compress fixture: %v", err)
	}
	if err := encoder.Close(); err != nil {
		t.Fatalf("close zstd writer: %v", err)
	}
	return physical, compressed.Bytes()
}
