# FEC Bulk Data Caching Implementation - Phase 1

## Summary

Implemented persistent disk-based caching for FEC bulk data downloads to eliminate redundant downloads during testing and development.

## Problem Solved

**Before**: Every pipeline run downloaded 7-9GB of FEC data, taking 5-10 minutes and consuming bandwidth unnecessarily.

**After**: First run downloads everything, subsequent runs use cached files (instant startup).

---

## Implementation Details

### 1. Cache Storage Location

**Path**: `/home/vedanta/legal-tender/data/fec_cache/` (host machine)

**Mounted to**: `/app/cache/fec_bulk` (inside Docker containers)

**Files stored with naming convention**: `{cycle}_{filename}`
- Examples: `2024_cn.txt`, `2024_indiv24.zip`, `2026_cm.txt`

### 2. Code Changes

#### `src/api/fec_bulk_data.py`

**Added**:
- `CACHE_DIR = "/app/cache/fec_bulk"` constant
- `force_refresh` parameter to all download and load methods
- Environment variable support: `FEC_FORCE_REFRESH=true`
- Automatic cache check before download
- Automatic cache save after successful download
- Logging for cache hits: `📂 Using cached {filename}` vs `⬇️ Downloading {filename}`

**Modified functions**:
1. `download_bulk_file(cycle, filename, force_refresh=False)`
   - Checks cache first
   - Downloads only if not cached or force_refresh=True
   - Saves to cache after download
   
2. `download_bulk_zip(cycle, filename, force_refresh=False)`
   - Same logic for ZIP files
   - Binary mode for cache reads/writes

3. All `FECBulkDataCache.load_*()` methods now accept `force_refresh` parameter:
   - `load_cycle(cycle, force_refresh=False)`
   - `load_candidate_summaries(cycle, force_refresh=False)`
   - `load_committee_summaries(cycle, force_refresh=False)`
   - `load_pac_summaries(cycle, force_refresh=False)`
   - `load_individual_contributions(cycle, max_records=None, force_refresh=False)`
   - `load_independent_expenditures(cycle, max_records=None, force_refresh=False)`
   - `load_committee_transfers(cycle, max_records=None, force_refresh=False)`

#### `docker-compose.yml`

**Added volume mounts** to both `dagster-webserver` and `dagster-daemon`:
```yaml
volumes:
  - ./data/fec_cache:/app/cache/fec_bulk
```

#### `.gitignore`

**Added**:
```
# FEC bulk data cache (large files, don't commit to git)
data/fec_cache/
```

### 3. Documentation

Created:
- `data/fec_cache/README.md` - Comprehensive cache directory documentation
- `test_cache.py` - Quick test script to verify caching works

---

## How to Use

### Normal Operation (Use Cache)

```bash
# First run: Downloads everything (~7-9GB, 5-10 minutes)
docker compose up

# Subsequent runs: Uses cache (instant)
docker compose up
```

### Force Refresh (Ignore Cache)

**Option 1: Environment Variable**
```bash
FEC_FORCE_REFRESH=true docker compose up
```

**Option 2: Delete Cache Manually**
```bash
rm -rf data/fec_cache/*
docker compose up
```

**Option 3: Programmatic (in Python code)**
```python
cache = get_cache()
cache.load_cycle(2024, force_refresh=True)
```

### Test Caching

```bash
# Run test script
python test_cache.py

# Check cache directory
ls -lh data/fec_cache/
du -sh data/fec_cache/
```

---

## Cache Behavior

### First Pipeline Run
1. Downloads all FEC bulk files from FEC.gov
2. Saves files to `data/fec_cache/` on host
3. Loads into memory for processing
4. **Duration**: 5-10 minutes (download time)

### Subsequent Pipeline Runs
1. Checks if files exist in cache
2. Loads from cache (no download)
3. Loads into memory for processing
4. **Duration**: ~30 seconds (disk read time)

### Memory vs Disk
- **Disk cache**: Persistent across container restarts
- **Memory cache**: Lives in `FECBulkDataCache` instance, cleared on restart
- **Double check**: Code checks memory first, then disk, then downloads

---

## File Sizes

| File | Size | Purpose |
|------|------|---------|
| `cn.txt` | ~10MB | Candidate master |
| `cm.txt` | ~10MB | Committee master |
| `ccl.txt` | ~10MB | Candidate-committee links |
| `weball24.zip` | ~50MB | Candidate financial summaries |
| `webl24.zip` | ~100MB | Committee financial summaries |
| `webk24.zip` | ~50MB | PAC/party summaries |
| `indiv24.zip` | ~4.3GB | Individual contributions |
| `oppexp24.zip` | ~2-3GB | Super PAC spending (AIPAC!) |
| `pas224.zip` | ~1-2GB | Committee transfers |
| **TOTAL** | **~7-9GB** | Per cycle |

---

## Troubleshooting

### Cache Corruption
If you get parse errors, delete the specific file:
```bash
rm data/fec_cache/2024_indiv24.zip
docker compose restart dagster-webserver dagster-daemon
```

### Disk Space Issues
Check cache size:
```bash
du -sh data/fec_cache
# Expected: ~7-9GB per cycle
```

### Permission Issues
Ensure Docker can write:
```bash
chmod -R 755 data/fec_cache
```

### Cache Not Working
Check logs for cache-related messages:
```bash
docker compose logs -f dagster-webserver | grep -E "📂|⬇️|💾"
```

You should see:
- `📂 Using cached {filename}` - Cache hit (good!)
- `⬇️ Downloading {filename}` - Cache miss, downloading
- `💾 Cached to {path}` - Saved to cache

---

## Future Enhancements (Phase 2)

Planned features:
1. **Age Detection**: Warn if cache > 7 days old
2. **Smart Refresh**: Check FEC's `Last-Modified` header before download
3. **Selective Refresh**: Only download changed files
4. **Cache Manifest**: JSON file with metadata (download time, checksums, sizes)
5. **Automated Cron**: Daily job to check and refresh stale files

---

## Testing Results

### Expected Behavior

**First Run**:
```
⬇️  Downloading cn.txt from https://www.fec.gov/files/bulk-downloads/2024/cn.txt...
✅ Downloaded cn.txt (12345 chars)
💾 Cached to /app/cache/fec_bulk/2024_cn.txt
```

**Second Run**:
```
📂 Using cached cn.txt from /app/cache/fec_bulk/2024_cn.txt
   Loaded 12345 chars from cache
```

---

## Benefits

✅ **No redundant downloads** - Saves bandwidth and time
✅ **Fast testing cycles** - Instant startup after first run
✅ **Persistent across restarts** - Cache survives container rebuilds
✅ **Manual control** - Easy to force refresh when needed
✅ **Future-ready** - Easy to add smart refresh logic later
✅ **Transparent** - Clear logging shows cache hits vs misses
✅ **Flexible** - Works with environment variable or parameter

---

## Developer Notes

- Cache is per-cycle (2024, 2026, etc.)
- Files are never automatically deleted
- Manual cleanup required for old cycles
- No expiration logic yet (Phase 2)
- Force refresh parameter propagates through all load methods
- Environment variable takes precedence over parameter

---

## Next Steps

1. ✅ **Phase 1 Complete**: Basic caching with force refresh
2. 🔜 **Test with full pipeline**: Run finance_mapping job
3. 🔜 **Validate cache hits**: Check logs for `📂` messages
4. 🔜 **Monitor disk usage**: Ensure cache doesn't grow unbounded
5. 🔜 **Phase 2**: Add age warnings and smart refresh

---

## Questions?

- **How do I clear the cache?** `rm -rf data/fec_cache/*`
- **How do I force a fresh download?** `FEC_FORCE_REFRESH=true docker compose up`
- **Where are files stored?** `data/fec_cache/` on your host machine
- **How much disk space needed?** ~10GB (7-9GB for files + overhead)
- **Does cache expire?** Not yet, manual cleanup required (Phase 2 will add auto-expiration)
