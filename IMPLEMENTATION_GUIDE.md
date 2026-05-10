# NestVault — Implementation Guide for Alternative Clients

This document describes the full API contract and client-side logic needed to implement a NestVault-compatible backup client, including the **accumulative backup mode** added in v2.8.

---

## Architecture Overview

```
Client                          Server (FastAPI + SQLite)
------                          -------------------------
scan files                      BackupID (label)
  └─ hash (SHA256)    ──►         └─ BackupVersion (version_key)
  └─ check/batch      ◄──              └─ VersionFile (path + sha256 + mtime)
  └─ upload (stream)  ──►         FileContent (sha256 → disk path, deduplicated)
  └─ absorb           ──►       [absorb: copy VersionFile refs from prev version]
```

Key invariant: **physical files are stored once per unique SHA256**. All versioning is pure DB references (`VersionFile` rows). `absorb` copies DB references only — no bytes are duplicated on disk.

---

## Authentication

All endpoints require the header:

```
X-API-Key: <your-api-key>
```

The server reads `BACKUP_API_KEY` from the environment. If unset, auth is disabled.

---

## Standard Backup Flow

A complete backup consists of these sequential API calls:

### 1. Ensure backup label exists

```
POST /backups
Content-Type: application/json

{
  "label": "my-photos",
  "client_name": "my-laptop",   // optional
  "prefix": "/Volumes/HD1"       // optional path prefix stored in metadata
}
```

Response: `{ "created": bool, "backup": BackupInfo }`

Idempotent — safe to call every run even if the label already exists.

---

### 2. Create a new version

```
POST /backups/{label}/versions
Content-Type: application/json

{
  "version_key": "2026-05-10T14:30:00"   // ISO datetime string used as key
}
```

Response: `{ "created": bool, "version": VersionInfo }`

`version_key` is the human-readable identifier for this snapshot. Use the current local datetime.

---

### 3. Fetch previous version cache (optional but important)

To avoid re-hashing unchanged files, fetch the file list from the last `done` version:

```
GET /backups/{label}/versions
```

Response: `list[VersionInfo]` ordered by `version_key` descending.

Pick the first entry with `status == "done"` → this is `prev_done_key`.

Then fetch its files:

```
GET /files?backup_label={label}&version_key={prev_done_key}
```

Response: `list[FileInfo]` with fields `original_path`, `sha256`, `size`, `mtime`.

Build a local map: `{ original_path → FileInfo }` for cache lookup during scanning.

> **Why:** If a file's `mtime` and `size` match the cache entry, skip SHA256 calculation entirely and call `register_file` directly with the cached `sha256`.

---

### 4. Check files (batch recommended)

For files that need hashing, check whether the server already has them:

```
POST /check/batch
Content-Type: application/json

{
  "backup_label": "my-photos",
  "version_key": "2026-05-10T14:30:00",
  "files": [
    { "original_path": "/Volumes/HD1/photo.jpg", "sha256": "abc123...", "size": 1048576, "mtime": 1715000000.0 },
    ...
  ]
}
```

Max 500 files per request. Response: `list[CheckBatchResultItem]` in the **same order** as input.

Each item:
```json
{
  "needs_upload": false,
  "content_exists": true,
  "reason": "Ja registrado nesta versao",
  "file_id": 42
}
```

Decision table:

| `needs_upload` | `content_exists` | Action |
|---|---|---|
| `false` | — | File already registered in this version. Skip. |
| `true` | `true` | Content exists on server. Call `register_file` (no upload). |
| `true` | `false` | Must upload the file content. |

Single-file alternative: `POST /check` with the same fields (non-batch).

---

### 5a. Register file (content already on server)

```
POST /upload
Headers:
  X-Backup-Label:   my-photos
  X-Version-Key:    2026-05-10T14:30:00
  X-Original-Path:  <base64url(utf-8 path)>
  X-Mtime:          1715000000.0
  X-Content-Sha256: abc123...64chars
```

No body. The server just creates the `VersionFile` DB row referencing the existing `FileContent`.

---

### 5b. Upload file (new content)

```
POST /upload
Headers:
  X-Backup-Label:   my-photos
  X-Version-Key:    2026-05-10T14:30:00
  X-Original-Path:  <base64url(utf-8 path)>
  X-Mtime:          1715000000.0
  Content-Type:     application/octet-stream
  Content-Length:   1048576

Body: raw binary file content (no multipart encoding)
```

Stream the file directly as the request body. The server hashes it on the fly and deduplicates.

> **Path encoding:** `base64.b64encode(path.encode("utf-8")).decode("ascii")`

---

### 6. Sync (optional, used for resumability)

```
POST /sync
Content-Type: application/json

{
  "backup_label": "my-photos",
  "version_key": "2026-05-10T14:30:00",
  "existing_paths": ["/Volumes/HD1/photo.jpg", ...]
}
```

Currently a no-op on the server (returns `{ "synced": true }`). Included for protocol completeness and future use.

---

### 7. Finish version

```
PATCH /backups/{label}/versions/{version_key}
Content-Type: application/json

{ "status": "done" }    // or "failed" if errors occurred
```

Marks the version complete. Triggers auto-cleanup in the background if disk is under 5% free.

---

### 8. Absorb (accumulative mode only)

After `finish_version` with `status == "done"`, if running in accumulative mode AND a previous `done` version exists:

```
POST /backups/{label}/versions/{version_key}/absorb
Content-Type: application/json

{
  "source_version_key": "2026-04-01T10:00:00"   // the prev_done_key fetched in step 3
}
```

Response:
```json
{
  "inherited": 3142,
  "skipped": 87
}
```

**What absorb does:**  
Copies `VersionFile` references (not physical files) from `source_version_key` into `version_key` for any `original_path` that does **not already exist** in the destination version.

**Why this matters for photo galleries:**  
When photos live on external HDs that aren't always connected, each backup session only sees a subset of the full collection. Absorb ensures the newest version accumulates all files ever seen, so a single restore always recovers the complete library — even for files not present in the current backup session.

**Modified file behavior:**  
If a path already exists in the new version (even with different content), absorb skips it. The new version's content wins — this is intentional. Only the latest version of a modified file is kept.

**Cleanup safety:**  
Because the new version is a superset of the old one after absorb, old versions can be deleted at any time without data loss. `cleanup --keep 1` is safe immediately after absorb.

---

## Server Check for Batch Support

Before using `/check/batch`, verify the server version supports it:

```
GET /health
```

Response:
```json
{ "status": "ok", "version": "2.8.0", "time": "2026-05-10T..." }
```

Batch is supported if `(major, minor) >= (2, 6)`. Fall back to single `/check` calls otherwise.

---

## Full Accumulative Backup Pseudocode

```python
def backup(directory, label, server, accumulate=False):
    version_key = now_iso()                              # e.g. "2026-05-10T14:30:00"

    ensure_backup(server, label)
    create_version(server, label, version_key)

    # Fetch previous version key + file cache in one pass
    prev_done_key, prev_cache = fetch_prev_cache(server, label)
    # prev_cache: { original_path → { sha256, size, mtime } }

    files = scan_directory(directory)

    fast_files = []   # cache hits: skip hashing
    to_hash    = []   # need SHA256 calculation

    for (local_path, original_path) in files:
        stat = local_path.stat()
        cached = prev_cache.get(original_path)
        if cached and cached["mtime"] == stat.mtime and cached["size"] == stat.size:
            fast_files.append((local_path, original_path, stat.mtime, cached["sha256"]))
        else:
            to_hash.append((local_path, original_path, stat.mtime, stat.size))

    # Hash pending files (parallelizable, CPU-bound)
    hashed = parallel_sha256(to_hash)

    # Check batch
    check_results = check_batch(server, label, version_key, hashed)

    # Process results
    for (original_path, result) in check_results:
        if not result["needs_upload"]:
            pass  # already registered
        elif result["content_exists"]:
            register_file(server, label, version_key, original_path, mtime, sha256)
        else:
            upload_file(server, label, version_key, original_path, local_path, mtime)

    # Fast-path: register cache hits without hashing
    for (local_path, original_path, mtime, sha256) in fast_files:
        register_file(server, label, version_key, original_path, mtime, sha256)

    sync(server, label, version_key, all_paths)

    status = "failed" if errors else "done"
    finish_version(server, label, version_key, status)

    # Accumulative: inherit files absent from this session
    if accumulate and status == "done" and prev_done_key:
        result = absorb(server, label, version_key, prev_done_key)
        # result: { "inherited": N, "skipped": M }
```

---

## API Reference Summary

### Backup management

| Method | Path | Description |
|--------|------|-------------|
| `POST` | `/backups` | Create or get backup label |
| `GET` | `/backups` | List all backup labels |
| `GET` | `/backups/{label}` | Get single backup info |
| `DELETE` | `/backups/{label}` | Delete label and all versions |

### Version management

| Method | Path | Description |
|--------|------|-------------|
| `POST` | `/backups/{label}/versions` | Create new version |
| `GET` | `/backups/{label}/versions` | List versions |
| `GET` | `/backups/{label}/versions/{key}` | Get version info |
| `PATCH` | `/backups/{label}/versions/{key}` | Finish version (set status) |
| `DELETE` | `/backups/{label}/versions/{key}` | Delete version |
| `POST` | `/backups/{label}/versions/{key}/absorb` | Inherit files from another version |

### File operations

| Method | Path | Description |
|--------|------|-------------|
| `POST` | `/check` | Check single file |
| `POST` | `/check/batch` | Check up to 500 files |
| `POST` | `/upload` | Upload or register file |
| `GET` | `/files` | List files in a version |
| `GET` | `/files/{id}/download` | Download file content |
| `POST` | `/sync` | Confirm sync (no-op, for protocol) |

### Maintenance

| Method | Path | Description |
|--------|------|-------------|
| `POST` | `/backups/{label}/cleanup` | Delete old versions |
| `POST` | `/maintenance/cleanup-orphans` | Remove unreferenced physical files |
| `GET` | `/health` | Server health + version |
| `GET` | `/storage/info` | Disk usage info |

---

## Key Schema Definitions

### `AbsorbRequest`
```json
{ "source_version_key": "string" }
```

### `AbsorbResponse`
```json
{ "inherited": 0, "skipped": 0 }
```

### `VersionInfo`
```json
{
  "id": 1,
  "version_key": "2026-05-10T14:30:00",
  "backup_label": "my-photos",
  "status": "done",
  "created_at": "...",
  "finished_at": "...",
  "duration_seconds": 42.3,
  "file_count": 3000,
  "total_size_bytes": 8589934592
}
```

### `FileInfo`
```json
{
  "id": 1,
  "original_path": "/Volumes/HD1/photo.jpg",
  "sha256": "abc123...64chars",
  "size": 1048576,
  "mtime": 1715000000.0,
  "created_at": "..."
}
```

### `CheckBatchResultItem`
```json
{
  "needs_upload": true,
  "content_exists": false,
  "reason": "Upload necessario",
  "file_id": null
}
```

---

## Accumulative Mode Rules

| Scenario | Behavior |
|----------|----------|
| File present in new session | Uploaded/registered normally (new content wins) |
| File absent from new session, present in prev version | Inherited via absorb (DB reference only) |
| File modified (same path, different content) | New version's content kept; old content skipped by absorb |
| File deleted on server (cleanup of old version) | Physical file preserved if any `VersionFile` row references its SHA256 |
| Multiple old versions, only latest absorbed | Always absorb from `prev_done_key` (the immediate predecessor) |

---

## Notes for Implementation

1. **Parallel hashing:** SHA256 is CPU-bound. Use process-level parallelism (not threads) to bypass the GIL for large file sets.

2. **Streaming uploads:** Send file content as raw binary in the request body — no multipart encoding. Set `Content-Type: application/octet-stream` and `Content-Length`.

3. **Path encoding:** All `original_path` values in upload headers must be Base64-encoded UTF-8 to safely handle non-ASCII characters and special characters in filenames.

4. **Session reuse:** Use a persistent HTTP session (keep-alive) to avoid TCP handshake overhead per request.

5. **Absorb is a post-processing step:** Call it only after `finish_version` succeeds with `status == "done"`. If the backup failed, do not absorb — the version is incomplete.

6. **prev_done_key is fetched once:** The same call to `GET /backups/{label}/versions` + `GET /files` that builds the mtime cache also provides `prev_done_key`. No second HTTP call needed.
