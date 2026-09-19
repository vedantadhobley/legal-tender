package release

import (
	"archive/zip"
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"github.com/vedantadhobley/legal-tender/internal/source/fec/schedulea"
	"github.com/vedantadhobley/legal-tender/internal/source/fec/scheduleb"
	"github.com/vedantadhobley/legal-tender/internal/source/fec/schedulee"
)

func extractSelectedMember(ctx context.Context, artifactPath, selectedMember string, destination io.Writer) error {
	archive, err := zip.OpenReader(artifactPath)
	if err != nil {
		return err
	}
	defer func() { _ = archive.Close() }()
	var match *zip.File
	for _, member := range archive.File {
		if member.Name != selectedMember {
			continue
		}
		if match != nil {
			return fmt.Errorf("selected ZIP member %q occurs more than once", selectedMember)
		}
		match = member
	}
	if match == nil {
		return fmt.Errorf("selected ZIP member %q is absent", selectedMember)
	}
	reader, err := match.Open()
	if err != nil {
		return err
	}
	_, copyErr := io.Copy(destination, &contextReader{ctx: ctx, reader: reader})
	closeErr := reader.Close()
	if copyErr != nil {
		return fmt.Errorf("extract ZIP member %q: %w", selectedMember, copyErr)
	}
	if closeErr != nil {
		return fmt.Errorf("close ZIP member %q: %w", selectedMember, closeErr)
	}
	return nil
}

func extractSelectedRelation(ctx context.Context, pgRestorePath, artifactPath, relation, period string, destination io.Writer) (uint64, error) {
	if pgRestorePath == "" {
		pgRestorePath = "pg_restore"
	}
	arguments, err := pgRestoreRelationArguments(artifactPath, relation)
	if err != nil {
		return 0, err
	}
	commandContext, cancel := context.WithCancel(ctx)
	defer cancel()
	command := exec.CommandContext(commandContext, pgRestorePath, arguments...)
	stdout, err := command.StdoutPipe()
	if err != nil {
		return 0, err
	}
	var stderr limitedBuffer
	command.Stderr = &stderr
	if err := command.Start(); err != nil {
		return 0, err
	}
	rows, sections, parseErr := copySelectedRelation(commandContext, stdout, relation, period, destination)
	if parseErr != nil {
		cancel()
	}
	waitErr := command.Wait()
	if parseErr != nil {
		return 0, parseErr
	}
	if waitErr != nil {
		return 0, fmt.Errorf("pg_restore selected relation %q: %w: %s", relation, waitErr, strings.TrimSpace(stderr.String()))
	}
	if sections != 1 {
		return 0, fmt.Errorf("pg_restore emitted %d COPY sections for %q; want exactly one", sections, relation)
	}
	return rows, nil
}

// ExtractRelation streams one exact data-row-only relation from a PostgreSQL
// custom archive. The COPY header must match a compiled source schema.
func ExtractRelation(ctx context.Context, pgRestorePath, artifactPath, relation, period string, destination io.Writer) (uint64, error) {
	return extractSelectedRelation(ctx, pgRestorePath, artifactPath, relation, period, destination)
}

func pgRestoreRelationArguments(artifactPath, relation string) ([]string, error) {
	schema, table, qualified := strings.Cut(relation, ".")
	if !qualified || schema == "" || table == "" || strings.Contains(table, ".") {
		return nil, fmt.Errorf("relation %q is not a schema-qualified table name", relation)
	}
	return []string{
		"--data-only",
		"--schema=" + schema,
		"--table=" + table,
		"--file=-",
		artifactPath,
	}, nil
}

func copySelectedRelation(ctx context.Context, source io.Reader, relation, _ string, destination io.Writer) (uint64, int, error) {
	reader := bufio.NewReaderSize(source, 256*1024)
	headerPrefix := "COPY " + relation + " ("
	expectedHeader, err := expectedRelationCOPYHeader(relation)
	if err != nil {
		return 0, 0, err
	}
	inside := false
	sections := 0
	var rows uint64
	for {
		line, err := reader.ReadBytes('\n')
		if len(line) != 0 {
			switch {
			case !inside && bytes.HasPrefix(line, []byte(headerPrefix)) && bytes.HasSuffix(line, []byte(") FROM stdin;\n")):
				if !bytes.Equal(line, expectedHeader) {
					return rows, sections, fmt.Errorf("COPY columns for %q do not match the compiled ordered source schema", relation)
				}
				sections++
				if sections > 1 {
					return rows, sections, fmt.Errorf("multiple COPY sections for %q", relation)
				}
				inside = true
			case inside && bytes.Equal(line, []byte("\\.\n")):
				inside = false
			case inside:
				if _, writeErr := destination.Write(line); writeErr != nil {
					return rows, sections, writeErr
				}
				rows++
				if rows&0x3fff == 0 {
					if contextErr := ctx.Err(); contextErr != nil {
						return rows, sections, contextErr
					}
				}
			}
		}
		if err != nil {
			if err == io.EOF {
				break
			}
			return rows, sections, err
		}
	}
	if inside {
		return rows, sections, fmt.Errorf("COPY section for %q has no terminator", relation)
	}
	return rows, sections, nil
}

func expectedRelationCOPYHeader(relation string) ([]byte, error) {
	var fieldNames []string
	switch {
	case strings.HasPrefix(relation, "disclosure.fec_fitem_sched_a_"):
		columns := schedulea.Columns()
		fieldNames = make([]string, len(columns))
		for index, column := range columns {
			fieldNames[index] = column.Name
		}
	case strings.HasPrefix(relation, "disclosure.fec_fitem_sched_b_"):
		columns := scheduleb.Columns()
		fieldNames = make([]string, len(columns))
		for index, column := range columns {
			fieldNames[index] = column.Name
		}
	case relation == "disclosure.fec_fitem_sched_e":
		columns := schedulee.Columns()
		fieldNames = make([]string, len(columns))
		for index, column := range columns {
			fieldNames[index] = column.Name
		}
	default:
		return nil, fmt.Errorf("relation %q has no compiled ordered source schema", relation)
	}
	return []byte("COPY " + relation + " (" + strings.Join(fieldNames, ", ") + ") FROM stdin;\n"), nil
}

func openAcquisitionArtifact(storageRoot string, artifact AcquisitionArtifact) (string, error) {
	path, err := resolveStorageKey(storageRoot, artifact.StorageKey)
	if err != nil {
		return "", err
	}
	info, err := os.Stat(path)
	if err != nil {
		return "", err
	}
	if !info.Mode().IsRegular() || info.Size() != artifact.ByteCount || filepathDigest(path) != artifact.SHA256 {
		return "", fmt.Errorf("acquisition artifact does not match immutable metadata")
	}
	return path, nil
}

func filepathDigest(path string) string {
	base := strings.TrimSuffix(strings.TrimSuffix(filepath.Base(path), ".copy.zst"), ".zst")
	return base
}
