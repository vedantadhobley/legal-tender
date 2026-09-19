package occurrence

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
)

// LoadPublishedScheduleBColumnarManifest verifies the immutable manifest and
// every backing shard before exposing the facts to calculations and audits.
func LoadPublishedScheduleBColumnarManifest(ctx context.Context, storageRoot, path string) (ScheduleBColumnarManifest, string, error) {
	var manifest ScheduleBColumnarManifest
	if storageRoot == "" || path == "" {
		return manifest, "", fmt.Errorf("storage root and Schedule B fact manifest path are required")
	}
	if err := requirePathInside(storageRoot, path); err != nil {
		return manifest, "", err
	}
	content, err := os.ReadFile(path)
	if err != nil {
		return manifest, "", err
	}
	if err := json.Unmarshal(content, &manifest); err != nil {
		return manifest, "", err
	}
	if err := validateScheduleBColumnarManifest(manifest); err != nil {
		return manifest, "", err
	}
	immutablePath := filepath.Join(storageRoot, scheduleBColumnarBase(), "manifests", manifest.FactSetID+".json")
	immutable, err := readScheduleBColumnarManifestIfPresent(immutablePath)
	if err != nil {
		return manifest, "", err
	}
	if immutable == nil || !reflect.DeepEqual(manifest, *immutable) {
		return manifest, "", fmt.Errorf("Schedule B pointer differs from immutable manifest")
	}
	for _, shard := range manifest.Shards {
		if err := ctx.Err(); err != nil {
			return manifest, "", err
		}
		shardPath, err := resolveStorageKey(storageRoot, shard.StorageKey)
		if err != nil {
			return manifest, "", err
		}
		info, err := os.Stat(shardPath)
		if err != nil {
			return manifest, "", err
		}
		if !info.Mode().IsRegular() || info.Size() < 0 || uint64(info.Size()) != shard.Bytes {
			return manifest, "", fmt.Errorf("Schedule B shard %d size mismatch", shard.Index)
		}
		digest, err := scheduleBColumnarFileSHA256(ctx, shardPath)
		if err != nil {
			return manifest, "", err
		}
		if digest != shard.SHA256 {
			return manifest, "", fmt.Errorf("Schedule B shard %d digest mismatch", shard.Index)
		}
	}
	// Hash the bytes we decoded, not a mutable pointer reread after validation.
	digest := sha256.Sum256(content)
	return manifest, hex.EncodeToString(digest[:]), nil
}
