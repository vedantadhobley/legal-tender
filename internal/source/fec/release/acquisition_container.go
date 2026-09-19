package release

import (
	"archive/zip"
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"

	"github.com/vedantadhobley/legal-tender/internal/source/fec/committeesummary"
)

const maxPGRestoreOutputBytes = 4 << 20

func validateContainer(ctx context.Context, source SourceSpec, path, pgRestorePath string) (string, error) {
	if source.ArtifactFormat == ArtifactFormatCommitteeSummaryCSV {
		if source.SourceContract != committeesummary.SourceContract || len(source.Periods) != 1 {
			return "", fmt.Errorf("committee summary requires its reviewed contract and one cycle")
		}
		file, err := os.Open(path)
		if err != nil {
			return "", err
		}
		defer func() { _ = file.Close() }()
		info, err := file.Stat()
		if err != nil {
			return "", err
		}
		if !info.Mode().IsRegular() || info.Size() <= 0 || info.Size() > committeesummary.MaxArtifactBytes {
			return "", fmt.Errorf("committee summary artifact exceeds the reviewed size boundary")
		}
		digest, err := fileSHA256(ctx, path)
		if err != nil {
			return "", err
		}
		result, err := committeesummary.Verify(ctx, file, committeesummary.Expected{Cycle: source.Periods[0], Bytes: info.Size(), SHA256: digest}, nil)
		if err != nil {
			return "", err
		}
		return fmt.Sprintf("committee_summary_csv_v1:rows=%d:rows_with_issues=%d", result.Rows, result.RowsWithIssues), nil
	}
	if len(source.SelectedMembers) != 0 {
		return validateZIP(path, source.SelectedMembers)
	}
	selectedRelations := selectedRelationNames(source)
	if len(selectedRelations) != 0 {
		return validatePostgresDump(ctx, path, pgRestorePath, selectedRelations)
	}
	return "", fmt.Errorf("source has no selected archive members or relations")
}

func validateZIP(path string, selectedMembers []string) (string, error) {
	archive, err := zip.OpenReader(path)
	if err != nil {
		return "", err
	}
	defer func() { _ = archive.Close() }()
	byName := make(map[string][]*zip.File, len(archive.File))
	for _, member := range archive.File {
		byName[member.Name] = append(byName[member.Name], member)
	}
	for _, selected := range selectedMembers {
		matches := byName[selected]
		if len(matches) != 1 {
			return "", fmt.Errorf("selected ZIP member %q occurs %d times", selected, len(matches))
		}
		reader, err := matches[0].Open()
		if err != nil {
			return "", err
		}
		_, copyErr := io.Copy(io.Discard, reader)
		closeErr := reader.Close()
		if copyErr != nil {
			return "", fmt.Errorf("validate ZIP member %q: %w", selected, copyErr)
		}
		if closeErr != nil {
			return "", fmt.Errorf("close ZIP member %q: %w", selected, closeErr)
		}
	}
	return fmt.Sprintf("zip_members_crc:%d", len(selectedMembers)), nil
}

func validatePostgresDump(ctx context.Context, path, pgRestorePath string, selectedRelations []string) (string, error) {
	file, err := os.Open(path)
	if err != nil {
		return "", err
	}
	magic := make([]byte, 5)
	_, readErr := io.ReadFull(file, magic)
	closeErr := file.Close()
	if readErr != nil {
		return "", readErr
	}
	if closeErr != nil {
		return "", closeErr
	}
	if !bytes.Equal(magic, []byte("PGDMP")) {
		return "", fmt.Errorf("processed schedule artifact has no PGDMP header")
	}
	if pgRestorePath == "" {
		pgRestorePath = "pg_restore"
	}
	command := exec.CommandContext(ctx, pgRestorePath, "--list", path)
	var stdout limitedBuffer
	var stderr limitedBuffer
	command.Stdout = &stdout
	command.Stderr = &stderr
	if err := command.Run(); err != nil {
		return "", fmt.Errorf("pg_restore --list: %w: %s", err, strings.TrimSpace(stderr.String()))
	}
	listing := stdout.String()
	for _, relation := range selectedRelations {
		parts := strings.SplitN(relation, ".", 2)
		if len(parts) != 2 || !tocContainsRelation(listing, parts[0], parts[1]) {
			return "", fmt.Errorf("pg_restore TOC does not contain selected relation %q", relation)
		}
	}
	return fmt.Sprintf("pg_restore_toc_relations:%d", len(selectedRelations)), nil
}

func tocContainsRelation(listing, schema, relation string) bool {
	for _, line := range strings.Split(listing, "\n") {
		fields := strings.Fields(line)
		for index := 0; index+1 < len(fields); index++ {
			if fields[index] == schema && fields[index+1] == relation {
				return true
			}
		}
	}
	return false
}

type limitedBuffer struct {
	bytes.Buffer
}

func (buffer *limitedBuffer) Write(content []byte) (int, error) {
	originalLength := len(content)
	remaining := maxPGRestoreOutputBytes - buffer.Len()
	if remaining > 0 {
		if len(content) > remaining {
			content = content[:remaining]
		}
		_, _ = buffer.Buffer.Write(content)
	}
	return originalLength, nil
}
