# osmo

Sync a local directory to/from an S3-compatible bucket (e.g. Cloudflare R2), with
per-file **merge** strategies so two machines can both write and nothing is lost.

osmo is content-agnostic — it just makes a directory and a bucket agree. That makes it a
good backing store for a **cache** (LLM responses, translations, build artifacts, …): a
fresh machine or CI warms from the bucket instead of recomputing, and pushes new entries
back at the end of a run.

```rust
let bucket = osmo::Bucket::new("my-cache");

// Warm the directory from the bucket (best-effort, once per process).
osmo::ensure_pulled(std::path::Path::new("./cache"), &bucket).await;

// ... do work that reads/writes ./cache ...

// Push new/changed files back.
let stats = osmo::flush(std::path::Path::new("./cache"), &bucket).await?;
println!("uploaded {} object(s)", stats.uploaded);
```

## How it works

Files are content-addressed by default, so the **set of paths** identifies the directory.
A commutative fingerprint (sum of per-file `xxh3` hashes) plus per-file content hashes are
stored in the bucket as a small `_osmo_manifest.json`; when the local fingerprint already
matches, pull/push skip the LIST/transfer entirely. Transfers run concurrently, writes are
atomic (temp-file + rename), and S3 requests retry with backoff on transient errors.

## Per-file strategies

Most files are immutable (a cache entry never changes), so the default works. For
**mutable** files, drop an `.osmo.json` at the directory root (it's synced too, so every
machine inherits it):

```json
{
  "files": [
    { "path": "*.jsonl", "strategy": "jsonl_merge", "key": "k" },
    { "path": "translations.json", "strategy": "json_merge" }
  ]
}
```

| strategy | for | reconciliation |
| --- | --- | --- |
| `path` (default) | immutable / content-addressed files | by path; transferred once |
| `content` | mutable blobs | content hash; last-writer-wins |
| `json_merge` | a mutable JSON object | union top-level keys (local wins ties) |
| `jsonl_merge` | append-only JSON Lines | union lines by `key` field (default `"k"`) |

`jsonl_merge` is the sweet spot for a **sharded key→value cache**: store entries as a few
big `NNN.jsonl` files instead of millions of tiny ones, and osmo unions them losslessly.

## Credentials

Read from the environment; the bucket name is passed in code:

- `R2_ACCOUNT_ID` (or `R2_ENDPOINT`)
- `R2_ACCESS_KEY_ID` (or `AWS_ACCESS_KEY_ID`)
- `R2_SECRET_ACCESS_KEY` (or `AWS_SECRET_ACCESS_KEY`)

## License

MIT
