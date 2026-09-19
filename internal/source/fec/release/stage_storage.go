package release

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
)

type stageCheckpoint struct {
	CandidateReleaseID string         `json:"candidate_release_id"`
	PlanSHA256         string         `json:"plan_sha256"`
	AcquisitionSHA256  string         `json:"acquisition_sha256"`
	RunID              string         `json:"run_id"`
	Outputs            []StagedOutput `json:"outputs"`
}

func stageCheckpointPath(storageRoot, candidateReleaseID, runID string) string {
	return filepath.Join(storageRoot, "raw", "fec", "stages", candidateReleaseID, runID+".checkpoint.json")
}

func stageStatePath(storageRoot, candidateReleaseID, runID string) string {
	return filepath.Join(storageRoot, "raw", "fec", "stages", candidateReleaseID, runID+".json")
}

func stageTemporaryDirectory(storageRoot, candidateReleaseID, runID string) string {
	return filepath.Join(scheduleAStorageRoot(storageRoot), "staging", candidateReleaseID, "selected", runID)
}

func stagedStorageKey(sourceID, digest string) string {
	parts := []string{"raw", "fec"}
	name := digest + ".zst"
	if family := processedScheduleFamily(sourceID); family != "" {
		parts = append(parts, family, "extracts")
		name = digest + ".copy.zst"
	} else {
		parts = append(parts, "selected")
	}
	parts = append(parts, "sha256", digest[:2], name)
	return filepath.ToSlash(filepath.Join(parts...))
}

func outputIdentity(output StagedOutput) string {
	return output.SourceID + "\x00" + output.SelectionKind + "\x00" + output.Selection
}

func readStageCheckpoint(path, candidateReleaseID, planSHA256, acquisitionSHA256, runID string) (map[string]StagedOutput, error) {
	content, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return map[string]StagedOutput{}, nil
	}
	if err != nil {
		return nil, err
	}
	var checkpoint stageCheckpoint
	decoder := json.NewDecoder(strings.NewReader(string(content)))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&checkpoint); err != nil {
		return nil, fmt.Errorf("decode stage checkpoint: %w", err)
	}
	if checkpoint.CandidateReleaseID != candidateReleaseID || checkpoint.PlanSHA256 != planSHA256 ||
		checkpoint.AcquisitionSHA256 != acquisitionSHA256 || checkpoint.RunID != runID {
		return nil, fmt.Errorf("stage checkpoint belongs to different inputs")
	}
	outputs := make(map[string]StagedOutput, len(checkpoint.Outputs))
	for _, output := range checkpoint.Outputs {
		identity := outputIdentity(output)
		if _, duplicate := outputs[identity]; duplicate {
			return nil, fmt.Errorf("duplicate stage checkpoint output %q", identity)
		}
		outputs[identity] = output
	}
	return outputs, nil
}

func writeStageCheckpoint(path, candidateReleaseID, planSHA256, acquisitionSHA256, runID string, outputs []StagedOutput) error {
	return writeAtomicJSON(path, stageCheckpoint{
		CandidateReleaseID: candidateReleaseID,
		PlanSHA256:         planSHA256,
		AcquisitionSHA256:  acquisitionSHA256,
		RunID:              runID,
		Outputs:            outputs,
	})
}

func readCompletedStage(path, candidateReleaseID, planSHA256, acquisitionSHA256, runID string) (*StageResult, error) {
	content, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var result StageResult
	decoder := json.NewDecoder(strings.NewReader(string(content)))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&result); err != nil {
		return nil, fmt.Errorf("decode prior stage state: %w", err)
	}
	if result.CandidateReleaseID != candidateReleaseID || result.PlanSHA256 != planSHA256 ||
		result.AcquisitionSHA256 != acquisitionSHA256 || result.RunID != runID || result.Status != StageStaged {
		return nil, fmt.Errorf("stage state exists for different or incomplete inputs")
	}
	return &result, nil
}

func writeAtomicJSON(path string, value any) error {
	content, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return err
	}
	return writeAtomicBytes(path, append(content, '\n'))
}

func writeAtomicBytes(path string, content []byte) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
		return err
	}
	temporary, err := os.CreateTemp(filepath.Dir(path), ".pending-*")
	if err != nil {
		return err
	}
	temporaryPath := temporary.Name()
	defer func() { _ = os.Remove(temporaryPath) }()
	if err := temporary.Chmod(0o640); err != nil {
		_ = temporary.Close()
		return err
	}
	if _, err := temporary.Write(content); err != nil {
		_ = temporary.Close()
		return err
	}
	if err := temporary.Sync(); err != nil {
		_ = temporary.Close()
		return err
	}
	if err := temporary.Close(); err != nil {
		return err
	}
	if err := os.Rename(temporaryPath, path); err != nil {
		return err
	}
	return syncDirectory(filepath.Dir(path))
}

func syncDirectory(path string) error {
	directory, err := os.Open(path)
	if err != nil {
		return err
	}
	defer func() { _ = directory.Close() }()
	return directory.Sync()
}

func fileSHA256(ctx context.Context, path string) (string, error) {
	file, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer func() { _ = file.Close() }()
	hasher := sha256.New()
	if _, err := io.Copy(hasher, &contextReader{ctx: ctx, reader: file}); err != nil {
		return "", err
	}
	return hex.EncodeToString(hasher.Sum(nil)), nil
}

type contextReader struct {
	ctx    context.Context
	reader io.Reader
}

func (reader *contextReader) Read(destination []byte) (int, error) {
	if err := reader.ctx.Err(); err != nil {
		return 0, err
	}
	return reader.reader.Read(destination)
}
