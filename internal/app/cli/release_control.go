package cli

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

var releaseControlCategories = map[string]struct{}{
	"plans":        {},
	"acquisitions": {},
	"stages":       {},
}

// preserveReleaseControlInput writes the exact bytes consumed by release
// publication before the active pointer can advance. This makes direct CLI
// use as durable as the Dagster adapter and prevents later shell redirection
// from destroying an input named by the published manifest.
func preserveReleaseControlInput(storageRoot, category, expectedSHA256 string, content []byte) (string, error) {
	if _, ok := releaseControlCategories[category]; !ok {
		return "", fmt.Errorf("unsupported release control category %q", category)
	}
	actual := fmt.Sprintf("%x", sha256.Sum256(content))
	if actual != expectedSHA256 {
		return "", fmt.Errorf("release control input SHA-256 %s does not match %s", actual, expectedSHA256)
	}
	target := filepath.Join(storageRoot, "control", "fec", "release", category, expectedSHA256+".json")
	if err := os.MkdirAll(filepath.Dir(target), 0o750); err != nil {
		return "", err
	}
	if err := validateExistingReleaseControlInput(target, content); err == nil {
		return target, nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return "", err
	}

	temporary, err := os.CreateTemp(filepath.Dir(target), ".pending-*")
	if err != nil {
		return "", err
	}
	temporaryPath := temporary.Name()
	defer func() { _ = os.Remove(temporaryPath) }()
	if err := temporary.Chmod(0o640); err != nil {
		_ = temporary.Close()
		return "", err
	}
	if _, err := temporary.Write(content); err != nil {
		_ = temporary.Close()
		return "", err
	}
	if err := temporary.Sync(); err != nil {
		_ = temporary.Close()
		return "", err
	}
	if err := temporary.Close(); err != nil {
		return "", err
	}
	if err := os.Link(temporaryPath, target); err != nil {
		if !errors.Is(err, os.ErrExist) {
			return "", err
		}
		if err := validateExistingReleaseControlInput(target, content); err != nil {
			return "", err
		}
	}
	directory, err := os.Open(filepath.Dir(target))
	if err != nil {
		return "", err
	}
	defer directory.Close()
	if err := directory.Sync(); err != nil {
		return "", err
	}
	return target, nil
}

func validateExistingReleaseControlInput(path string, expected []byte) error {
	content, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	if !bytes.Equal(content, expected) {
		return fmt.Errorf("immutable release control artifact %s has unexpected bytes", path)
	}
	return nil
}
