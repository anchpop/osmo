//! # osmo
//!
//! Sync a local directory to/from an S3-compatible bucket (e.g. Cloudflare R2), with
//! per-file merge strategies so two machines can both write and nothing is lost.
//!
//! osmo doesn't care what's in the directory — it's a generic "make this directory and
//! this bucket agree" engine, well suited to backing a cache (LLM responses, translations,
//! …) so a fresh machine or CI can warm from the bucket instead of recomputing.
//!
//! - [`ensure_pulled`] — download anything missing/changed (best-effort, once per process
//!   per bucket+dir). Warms a cold directory.
//! - [`flush`] — upload local changes back to the bucket.
//!
//! A commutative fingerprint (the wrapping sum of per-file `xxh3` hashes) plus per-file
//! content hashes are stored in the bucket as a small `_osmo_manifest.json` object. When
//! the local fingerprint already matches the remote one, both pull and push skip the
//! expensive LIST/transfer.
//!
//! ## Per-file strategies
//!
//! By default a file is immutable/content-addressed (its *path* identifies it). Mutable
//! files opt into a different strategy via an `.osmo.json` config at the directory root
//! (synced to the bucket, so every machine inherits it):
//!
//! ```json
//! {
//!   "files": [
//!     { "path": "*.jsonl", "strategy": "jsonl_merge", "key": "k" },
//!     { "path": "translations.json", "strategy": "json_merge" }
//!   ]
//! }
//! ```
//!
//! - `path` (default): immutable; fingerprinted by path; transferred once.
//! - `content`: mutable; fingerprinted by content; last-writer-wins (remote wins on pull,
//!   local wins on push).
//! - `json_merge`: mutable JSON object; reconciled by unioning top-level keys.
//! - `jsonl_merge`: append-only JSON Lines; reconciled by unioning lines keyed by a field
//!   (`key`, default `"k"`). Ideal for a sharded key→value cache.
//!
//! Credentials come from the environment: `R2_ACCOUNT_ID` (or `R2_ENDPOINT`),
//! `R2_ACCESS_KEY_ID` (or `AWS_ACCESS_KEY_ID`), `R2_SECRET_ACCESS_KEY` (or
//! `AWS_SECRET_ACCESS_KEY`).

#![warn(missing_docs)]

use std::collections::{BTreeMap, HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, LazyLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use dashmap::DashMap;
use futures::stream::{StreamExt, TryStreamExt};
use hmac::{Hmac, Mac};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tokio::sync::{Mutex, OnceCell};
use xxhash_rust::xxh3::xxh3_64;

type HmacSha256 = Hmac<Sha256>;

/// A pooled HTTP client tuned for high concurrency against one host.
fn pooled_client() -> reqwest::Client {
    reqwest::Client::builder()
        .pool_max_idle_per_host(256)
        .pool_idle_timeout(Duration::from_secs(300))
        .build()
        .expect("failed to build HTTP client")
}

/// Max objects transferred concurrently during a pull/push.
const SYNC_CONCURRENCY: usize = 32;

/// Retry a fallible S3 operation a few times with backoff on transient errors.
async fn retry<T, F, Fut>(mut op: F) -> Result<T, Error>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<T, Error>>,
{
    const BACKOFF_MS: [u64; 3] = [100, 400, 1200];
    let mut attempt = 0;
    loop {
        match op().await {
            Ok(v) => return Ok(v),
            Err(e) if attempt < BACKOFF_MS.len() && is_retryable(&e) => {
                log::debug!("osmo: transient error (retrying): {e}");
                tokio::time::sleep(Duration::from_millis(BACKOFF_MS[attempt])).await;
                attempt += 1;
            }
            Err(e) => return Err(e),
        }
    }
}

/// Whether an error is worth retrying (network hiccups, throttling, 5xx).
fn is_retryable(e: &Error) -> bool {
    match e {
        Error::Http(_) => true,
        Error::BadStatus { status, .. } => *status == 429 || *status >= 500,
        _ => false,
    }
}

/// Write `bytes` to `path` atomically: write a sibling temp file, then rename over the
/// destination (atomic on the same filesystem). Prevents readers from ever seeing a
/// partially written cache file if the process is interrupted mid-write.
async fn atomic_write(path: &Path, bytes: &[u8]) -> Result<(), std::io::Error> {
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let parent = path.parent().unwrap_or_else(|| Path::new("."));
    tokio::fs::create_dir_all(parent).await?;
    let name = path
        .file_name()
        .map(|n| n.to_string_lossy().into_owned())
        .unwrap_or_default();
    let n = COUNTER.fetch_add(1, Ordering::Relaxed);
    let tmp = parent.join(format!(".{name}.tmp.{}.{n}", std::process::id()));
    if let Err(e) = tokio::fs::write(&tmp, bytes).await {
        let _ = tokio::fs::remove_file(&tmp).await;
        return Err(e);
    }
    tokio::fs::rename(&tmp, path).await
}

/// The bucket a directory is mirrored to.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Bucket {
    /// The bucket name.
    pub bucket: String,
    /// An optional key prefix within the bucket (default empty).
    pub prefix: String,
}

impl Bucket {
    /// A bucket with no key prefix.
    pub fn new(bucket: impl Into<String>) -> Self {
        Self {
            bucket: bucket.into(),
            prefix: String::new(),
        }
    }

    /// A bucket with a key prefix (objects are stored under `<prefix>/…`).
    pub fn with_prefix(bucket: impl Into<String>, prefix: impl Into<String>) -> Self {
        Self {
            bucket: bucket.into(),
            prefix: prefix.into().trim_matches('/').to_string(),
        }
    }
}

/// Outcome of an [`ensure_pulled`] or [`flush`] operation.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Stats {
    /// Number of objects downloaded from the bucket into the local cache.
    pub downloaded: usize,
    /// Number of local files uploaded to the bucket.
    pub uploaded: usize,
    /// True if the fingerprint matched and the LIST/transfer was skipped entirely.
    pub skipped: bool,
}

/// Errors that can occur while syncing the cache with a bucket.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Required credentials/configuration were not found in the environment.
    #[error("osmo: credentials not configured: {0}")]
    MissingCredentials(String),
    /// The HTTP request to the bucket failed.
    #[error("osmo: HTTP error: {0}")]
    Http(#[from] reqwest::Error),
    /// A filesystem error occurred while reading/writing the cache directory.
    #[error("osmo: IO error: {0}")]
    Io(#[from] std::io::Error),
    /// The bucket returned a non-success status code.
    #[error("osmo: request to {key} failed with status {status}: {body}")]
    BadStatus {
        /// The object key (or `?list` for a listing).
        key: String,
        /// The HTTP status code.
        status: u16,
        /// The (truncated) response body.
        body: String,
    },
}

// ===================================================================================
// Public entry points (deduped per process)
// ===================================================================================

type SyncKey = (String, PathBuf);

static PULL_ONCE: LazyLock<DashMap<SyncKey, Arc<OnceCell<()>>>> = LazyLock::new(DashMap::new);
static FLUSH_LOCK: LazyLock<DashMap<SyncKey, Arc<Mutex<()>>>> = LazyLock::new(DashMap::new);

fn sync_key(dir: &Path, bucket: &Bucket) -> SyncKey {
    let dir = std::fs::canonicalize(dir).unwrap_or_else(|_| dir.to_path_buf());
    (format!("{}/{}", bucket.bucket, bucket.prefix), dir)
}

/// Warm the local cache directory from the bucket. Runs at most once per process for a
/// given (bucket, dir); concurrent callers await the same operation. Best-effort: any
/// error is logged and swallowed so a sync failure never breaks request serving.
pub async fn ensure_pulled(dir: &Path, bucket: &Bucket) {
    let key = sync_key(dir, bucket);
    let cell = PULL_ONCE
        .entry(key)
        .or_insert_with(|| Arc::new(OnceCell::new()))
        .clone();

    let dir = dir.to_path_buf();
    let bucket = bucket.clone();
    cell.get_or_init(|| async move {
        match pull(&dir, &bucket).await {
            Ok(stats) if !stats.skipped => {
                log::info!(
                    "osmo: pulled {} object(s) from bucket {}",
                    stats.downloaded,
                    bucket.bucket
                );
            }
            Ok(_) => log::debug!("osmo: already in sync with bucket {}", bucket.bucket),
            Err(e) => log::warn!("osmo: pull from bucket {} failed: {e}", bucket.bucket),
        }
    })
    .await;
}

/// Push local cache files to the bucket. Serialized per (bucket, dir) so sibling clients
/// sharing a directory don't duplicate work; repeat calls are cheap no-ops once the
/// fingerprint matches.
pub async fn flush(dir: &Path, bucket: &Bucket) -> Result<Stats, Error> {
    let key = sync_key(dir, bucket);
    let lock = FLUSH_LOCK
        .entry(key)
        .or_insert_with(|| Arc::new(Mutex::new(())))
        .clone();
    let _guard = lock.lock().await;
    push(dir, bucket).await
}

// ===================================================================================
// Pull / push
// ===================================================================================

/// Config file at the directory root that assigns non-default sync strategies to files.
const SETTINGS_REL: &str = ".osmo.json";
/// Object holding the overall fingerprint plus per-file content hashes.
const MANIFEST_REL: &str = "_osmo_manifest.json";
/// Default field used to key lines in a `jsonl_merge` file.
const DEFAULT_JSONL_KEY: &str = "k";

fn is_control(rel: &str) -> bool {
    rel == SETTINGS_REL || rel == MANIFEST_REL
}

/// How a given file is fingerprinted and reconciled during sync.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Deserialize)]
#[serde(rename_all = "snake_case")]
enum Strategy {
    /// Default: file is immutable/content-addressed, so its *path* identifies it. Never
    /// re-transferred once present on both sides. (No content read needed.)
    #[default]
    Path,
    /// Mutable file: fingerprinted by whole-file content; last-writer-wins on transfer
    /// (remote wins on pull, local wins on push).
    Content,
    /// Mutable JSON object: fingerprinted by content, reconciled by *unioning* the
    /// top-level keys of the local and remote maps, so no entries are ever lost.
    JsonMerge,
    /// Append-only JSON Lines: fingerprinted by content, reconciled by *unioning* the
    /// lines keyed by a field (see [`FileRule::key`]).
    JsonlMerge,
}

impl Strategy {
    /// Strategies that read file contents (for hashing) and merge on transfer.
    fn is_content(self) -> bool {
        !matches!(self, Strategy::Path)
    }
}

/// Parsed `.osmo.json`.
#[derive(Debug, Default, Deserialize)]
struct SyncSettings {
    #[serde(default)]
    files: Vec<FileRule>,
}

#[derive(Debug, Clone, Deserialize)]
struct FileRule {
    /// Glob (supports `*` and `?`) matched against the relative path.
    path: String,
    #[serde(default)]
    strategy: Strategy,
    /// For `jsonl_merge`: the JSON field that identifies a line (default `"k"`).
    #[serde(default)]
    key: Option<String>,
}

impl SyncSettings {
    fn rule_for(&self, rel: &str) -> Option<&FileRule> {
        self.files.iter().find(|r| glob_match(&r.path, rel))
    }

    fn strategy_for(&self, rel: &str) -> Strategy {
        self.rule_for(rel).map(|r| r.strategy).unwrap_or_default()
    }

    /// The line-key field for a `jsonl_merge` file.
    fn jsonl_key_for(&self, rel: &str) -> String {
        self.rule_for(rel)
            .and_then(|r| r.key.clone())
            .unwrap_or_else(|| DEFAULT_JSONL_KEY.to_string())
    }
}

/// The bucket-side record: overall fingerprint (for the fast-path skip) plus the content
/// hash of every non-`path` file (so pull/push can compare without downloading them).
#[derive(Debug, Default, Serialize, Deserialize)]
struct Manifest {
    #[serde(default)]
    overall: u64,
    #[serde(default)]
    content: BTreeMap<String, u64>,
}

struct ScannedFile {
    rel: String,
    path: PathBuf,
    strategy: Strategy,
    /// `xxh3` of the file contents; `None` for `Strategy::Path` (not read).
    content_hash: Option<u64>,
}

struct LocalScan {
    files: Vec<ScannedFile>,
    /// Commutative fingerprint over all files (path hash, or path⊕content hash).
    overall: u64,
}

/// The per-file contribution to the overall fingerprint.
fn identity(rel: &str, content_hash: Option<u64>) -> u64 {
    match content_hash {
        Some(h) => xxh3_64(rel.as_bytes()) ^ h,
        None => xxh3_64(rel.as_bytes()),
    }
}

/// Walk the cache dir, classifying each file by strategy and hashing the contents of
/// non-`path` files.
async fn scan_local(dir: &Path, settings: &SyncSettings) -> Result<LocalScan, std::io::Error> {
    let mut files = Vec::new();
    let mut overall = 0u64;
    for (rel, path) in list_cache_files(dir).await? {
        let strategy = settings.strategy_for(&rel);
        let content_hash = if strategy.is_content() {
            Some(xxh3_64(&tokio::fs::read(&path).await?))
        } else {
            None
        };
        overall = overall.wrapping_add(identity(&rel, content_hash));
        files.push(ScannedFile {
            rel,
            path,
            strategy,
            content_hash,
        });
    }
    Ok(LocalScan { files, overall })
}

async fn pull(dir: &Path, bucket: &Bucket) -> Result<Stats, Error> {
    // Ensure the cache directory exists even if the bucket is empty or unreachable, so the
    // request path's "cache directory must exist" invariant holds after warming.
    tokio::fs::create_dir_all(dir).await?;

    let cfg = R2Config::from_env()?;
    let client = pooled_client();
    let prefix = &bucket.prefix;

    let settings = load_settings(dir, &client, &cfg, bucket).await;
    let scan = scan_local(dir, &settings).await?;
    let manifest = get_manifest(&client, &cfg, bucket).await;

    // Fast path: the bucket's recorded fingerprint matches local.
    if manifest.as_ref().map(|m| m.overall) == Some(scan.overall) {
        return Ok(Stats {
            skipped: true,
            ..Default::default()
        });
    }
    let remote_content = manifest.map(|m| m.content).unwrap_or_default();
    let local_by_rel: HashMap<&str, &ScannedFile> =
        scan.files.iter().map(|f| (f.rel.as_str(), f)).collect();

    let remote_keys = list_objects(&client, &cfg, &bucket.bucket, prefix).await?;

    // Download concurrently. Each task returns 1 if it transferred a file, else 0.
    let (settings, local_by_rel, remote_content, cfg, client) =
        (&settings, &local_by_rel, &remote_content, &cfg, &client);
    let downloaded = futures::stream::iter(remote_keys)
        .map(|full_key| async move {
            let Some(rel) = strip_prefix(&full_key, prefix) else {
                return Ok(0usize);
            };
            if is_control(rel) {
                return Ok(0);
            }
            let strategy = settings.strategy_for(rel);
            let local = local_by_rel.get(rel).copied();

            if strategy == Strategy::Path {
                if local.is_none() {
                    if let Some(bytes) = get_object(client, cfg, &bucket.bucket, &full_key).await? {
                        write_cache_file(dir, rel, &bytes).await?;
                        return Ok(1);
                    }
                }
                return Ok(0);
            }

            // content / json_merge: compare content hashes; only transfer on a difference.
            if local.is_some()
                && local.and_then(|f| f.content_hash) == remote_content.get(rel).copied()
            {
                return Ok(0);
            }
            let Some(remote_bytes) = get_object(client, cfg, &bucket.bucket, &full_key).await?
            else {
                return Ok(0);
            };
            let to_write = match local {
                // Mergeable strategy with a local copy: union local into remote.
                Some(f) if matches!(strategy, Strategy::JsonMerge | Strategy::JsonlMerge) => {
                    let local_bytes = tokio::fs::read(&f.path).await?;
                    merge(
                        strategy,
                        &remote_bytes,
                        &local_bytes,
                        &settings.jsonl_key_for(rel),
                    )
                    .unwrap_or_else(|| {
                        log::warn!("osmo: {rel} couldn't be merged; using remote copy");
                        remote_bytes
                    })
                }
                // Strategy::Content (remote wins on pull) or local missing: take remote as-is.
                _ => remote_bytes,
            };
            write_cache_file(dir, rel, &to_write).await?;
            Ok::<usize, Error>(1)
        })
        .buffer_unordered(SYNC_CONCURRENCY)
        .try_fold(0usize, |acc, n| async move { Ok(acc + n) })
        .await?;

    Ok(Stats {
        downloaded,
        ..Default::default()
    })
}

async fn push(dir: &Path, bucket: &Bucket) -> Result<Stats, Error> {
    tokio::fs::create_dir_all(dir).await?;

    let cfg = R2Config::from_env()?;
    let client = pooled_client();
    let prefix = &bucket.prefix;

    let settings = load_settings(dir, &client, &cfg, bucket).await;
    maybe_push_settings(dir, &client, &cfg, bucket).await?;

    let scan = scan_local(dir, &settings).await?;
    let manifest = get_manifest(&client, &cfg, bucket).await;
    if manifest.as_ref().map(|m| m.overall) == Some(scan.overall) {
        return Ok(Stats {
            skipped: true,
            ..Default::default()
        });
    }
    let remote_content = manifest.map(|m| m.content).unwrap_or_default();

    let remote_full = list_objects(&client, &cfg, &bucket.bucket, prefix).await?;
    let remote_rel: HashSet<String> = remote_full
        .iter()
        .filter_map(|k| strip_prefix(k, prefix))
        .filter(|r| !is_control(r))
        .map(String::from)
        .collect();

    // What each file's upload task reports back.
    struct Outcome {
        uploaded: bool,
        /// Content hash to record in the manifest (for non-`path` files).
        content: Option<(String, u64)>,
    }

    // Upload concurrently.
    let (remote_content_ref, remote_rel_ref, cfg_ref, client_ref, settings_ref) =
        (&remote_content, &remote_rel, &cfg, &client, &settings);
    let outcomes: Vec<Outcome> = futures::stream::iter(scan.files.iter())
        .map(|f| async move {
            let key = obj_key(prefix, &f.rel);
            match f.strategy {
                Strategy::Path => {
                    if remote_rel_ref.contains(&f.rel) {
                        return Ok(Outcome {
                            uploaded: false,
                            content: None,
                        });
                    }
                    let bytes = tokio::fs::read(&f.path).await?;
                    put_object(client_ref, cfg_ref, &bucket.bucket, &key, bytes).await?;
                    Ok(Outcome {
                        uploaded: true,
                        content: None,
                    })
                }
                Strategy::Content => {
                    let uploaded = if remote_content_ref.get(&f.rel).copied() != f.content_hash {
                        let bytes = tokio::fs::read(&f.path).await?;
                        put_object(client_ref, cfg_ref, &bucket.bucket, &key, bytes).await?;
                        true
                    } else {
                        false
                    };
                    Ok(Outcome {
                        uploaded,
                        content: f.content_hash.map(|h| (f.rel.clone(), h)),
                    })
                }
                Strategy::JsonMerge | Strategy::JsonlMerge => {
                    // Already in sync: just keep tracking its hash.
                    if remote_content_ref.get(&f.rel).copied() == f.content_hash {
                        return Ok(Outcome {
                            uploaded: false,
                            content: f.content_hash.map(|h| (f.rel.clone(), h)),
                        });
                    }
                    let local_bytes = tokio::fs::read(&f.path).await?;
                    let (final_bytes, write_back) = if remote_rel_ref.contains(&f.rel) {
                        match get_object(client_ref, cfg_ref, &bucket.bucket, &key).await? {
                            Some(remote_bytes) => {
                                let jsonl_key = settings_ref.jsonl_key_for(&f.rel);
                                match merge(f.strategy, &remote_bytes, &local_bytes, &jsonl_key) {
                                    // Union; write back locally so both sides converge.
                                    Some(m) => (m, true),
                                    None => {
                                        log::warn!(
                                            "osmo: {} couldn't be merged; uploading local copy",
                                            f.rel
                                        );
                                        (local_bytes, false)
                                    }
                                }
                            }
                            None => (local_bytes, false),
                        }
                    } else {
                        (local_bytes, false)
                    };
                    if write_back {
                        atomic_write(&f.path, &final_bytes).await?;
                    }
                    let hash = xxh3_64(&final_bytes);
                    put_object(client_ref, cfg_ref, &bucket.bucket, &key, final_bytes).await?;
                    Ok::<Outcome, Error>(Outcome {
                        uploaded: true,
                        content: Some((f.rel.clone(), hash)),
                    })
                }
            }
        })
        .buffer_unordered(SYNC_CONCURRENCY)
        .try_collect()
        .await?;

    let mut uploaded = 0;
    // Seed with remote content hashes so content files we don't touch stay tracked.
    let mut stored_content: BTreeMap<String, u64> = remote_content.clone();
    for o in outcomes {
        if o.uploaded {
            uploaded += 1;
        }
        if let Some((k, v)) = o.content {
            stored_content.insert(k, v);
        }
    }

    // Fingerprint of the resulting bucket state: every path-strategy object (union of
    // remote and local) plus every tracked content file.
    let mut path_union: HashSet<&str> = scan
        .files
        .iter()
        .filter(|f| f.strategy == Strategy::Path)
        .map(|f| f.rel.as_str())
        .collect();
    for r in &remote_rel {
        if settings.strategy_for(r) == Strategy::Path {
            path_union.insert(r.as_str());
        }
    }
    let mut overall = 0u64;
    for p in &path_union {
        overall = overall.wrapping_add(identity(p, None));
    }
    for (rel, h) in &stored_content {
        overall = overall.wrapping_add(identity(rel, Some(*h)));
    }
    put_manifest(
        &client,
        &cfg,
        bucket,
        &Manifest {
            overall,
            content: stored_content,
        },
    )
    .await?;

    Ok(Stats {
        uploaded,
        ..Default::default()
    })
}

/// Reconcile a mergeable file's remote and local copies into the union. Returns `None`
/// (caller falls back to last-writer-wins) only for `json_merge` when a side isn't a JSON
/// object; `jsonl_merge` always succeeds.
fn merge(strategy: Strategy, remote: &[u8], local: &[u8], jsonl_key: &str) -> Option<Vec<u8>> {
    match strategy {
        Strategy::JsonMerge => merge_json_maps(remote, local),
        Strategy::JsonlMerge => Some(merge_jsonl(remote, local, jsonl_key)),
        Strategy::Path | Strategy::Content => None,
    }
}

/// Union two JSON objects by top-level key (local wins on collision). Returns `None` if
/// either side is not a JSON object.
fn merge_json_maps(remote: &[u8], local: &[u8]) -> Option<Vec<u8>> {
    type Map = serde_json::Map<String, serde_json::Value>;
    let mut merged: Map = serde_json::from_slice(remote).ok()?;
    let local: Map = serde_json::from_slice(local).ok()?;
    for (k, v) in local {
        merged.insert(k, v);
    }
    serde_json::to_vec(&merged).ok()
}

/// Union two JSON Lines files, keyed by the `key_field` of each line (local wins on
/// collision). Lines that don't parse as a JSON object with that field are kept verbatim,
/// deduplicated by exact bytes. Output order: remote lines first, then new local lines.
fn merge_jsonl(remote: &[u8], local: &[u8], key_field: &str) -> Vec<u8> {
    // Identity of a line: its `key_field` value if present, else the whole line.
    fn line_key(line: &[u8], key_field: &str) -> Vec<u8> {
        serde_json::from_slice::<serde_json::Value>(line)
            .ok()
            .and_then(|v| v.get(key_field).and_then(|k| k.as_str()).map(String::from))
            .map(String::into_bytes)
            .unwrap_or_else(|| line.to_vec())
    }

    let mut order: Vec<Vec<u8>> = Vec::new();
    let mut by_key: HashMap<Vec<u8>, Vec<u8>> = HashMap::new();
    for line in remote
        .split(|&b| b == b'\n')
        .chain(local.split(|&b| b == b'\n'))
    {
        if line.is_empty() {
            continue;
        }
        let key = line_key(line, key_field);
        if by_key.insert(key.clone(), line.to_vec()).is_none() {
            order.push(key);
        }
    }

    let mut out = Vec::new();
    for key in order {
        out.extend_from_slice(&by_key[&key]);
        out.push(b'\n');
    }
    out
}

// ===================================================================================
// Settings / manifest objects
// ===================================================================================

/// Load sync settings, preferring a local `.tysm-sync.json`, then the bucket's copy
/// (cached locally for next time), else defaults. Best-effort: parse errors log and
/// fall back to defaults.
async fn load_settings(
    dir: &Path,
    client: &reqwest::Client,
    cfg: &R2Config,
    bucket: &Bucket,
) -> SyncSettings {
    let local_path = dir.join(SETTINGS_REL);
    if let Ok(bytes) = tokio::fs::read(&local_path).await {
        return parse_settings(&bytes);
    }
    let key = obj_key(&bucket.prefix, SETTINGS_REL);
    if let Ok(Some(bytes)) = get_object(client, cfg, &bucket.bucket, &key).await {
        let _ = tokio::fs::write(&local_path, &bytes).await;
        return parse_settings(&bytes);
    }
    SyncSettings::default()
}

fn parse_settings(bytes: &[u8]) -> SyncSettings {
    serde_json::from_slice(bytes).unwrap_or_else(|e| {
        log::warn!("osmo: ignoring invalid {SETTINGS_REL}: {e}");
        SyncSettings::default()
    })
}

/// Upload the local settings file to the bucket if it exists and differs from the remote.
async fn maybe_push_settings(
    dir: &Path,
    client: &reqwest::Client,
    cfg: &R2Config,
    bucket: &Bucket,
) -> Result<(), Error> {
    let Ok(local_bytes) = tokio::fs::read(dir.join(SETTINGS_REL)).await else {
        return Ok(());
    };
    let key = obj_key(&bucket.prefix, SETTINGS_REL);
    let remote = get_object(client, cfg, &bucket.bucket, &key).await?;
    if remote.as_deref() != Some(local_bytes.as_slice()) {
        put_object(client, cfg, &bucket.bucket, &key, local_bytes).await?;
    }
    Ok(())
}

async fn get_manifest(
    client: &reqwest::Client,
    cfg: &R2Config,
    bucket: &Bucket,
) -> Option<Manifest> {
    let key = obj_key(&bucket.prefix, MANIFEST_REL);
    match get_object(client, cfg, &bucket.bucket, &key).await {
        Ok(Some(bytes)) => serde_json::from_slice(&bytes).ok(),
        _ => None,
    }
}

async fn put_manifest(
    client: &reqwest::Client,
    cfg: &R2Config,
    bucket: &Bucket,
    manifest: &Manifest,
) -> Result<(), Error> {
    let key = obj_key(&bucket.prefix, MANIFEST_REL);
    let bytes = serde_json::to_vec(manifest).unwrap_or_default();
    put_object(client, cfg, &bucket.bucket, &key, bytes).await
}

/// Wildcard match supporting `*` (any run, including `/`) and `?` (one char).
fn glob_match(pattern: &str, text: &str) -> bool {
    let (p, t) = (pattern.as_bytes(), text.as_bytes());
    let (mut pi, mut ti) = (0usize, 0usize);
    let (mut star, mut mark) = (None, 0usize);
    while ti < t.len() {
        if pi < p.len() && (p[pi] == b'?' || p[pi] == t[ti]) {
            pi += 1;
            ti += 1;
        } else if pi < p.len() && p[pi] == b'*' {
            star = Some(pi);
            mark = ti;
            pi += 1;
        } else if let Some(s) = star {
            pi = s + 1;
            mark += 1;
            ti = mark;
        } else {
            return false;
        }
    }
    while pi < p.len() && p[pi] == b'*' {
        pi += 1;
    }
    pi == p.len()
}

// ===================================================================================
// Local cache directory helpers
// ===================================================================================

/// Write `bytes` to `<dir>/<rel>` atomically, creating parent directories as needed.
async fn write_cache_file(dir: &Path, rel: &str, bytes: &[u8]) -> Result<(), std::io::Error> {
    atomic_write(&dir.join(rel), bytes).await
}

/// Walk `dir`, returning `(relative-key, absolute-path)` for every file. The relative key
/// uses `/` separators (e.g. `042/<cache_key>`). Missing directory ⇒ empty list.
async fn list_cache_files(dir: &Path) -> Result<Vec<(String, PathBuf)>, std::io::Error> {
    let mut out = Vec::new();
    let mut stack = vec![dir.to_path_buf()];
    while let Some(d) = stack.pop() {
        let mut rd = match tokio::fs::read_dir(&d).await {
            Ok(rd) => rd,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => continue,
            Err(e) => return Err(e),
        };
        while let Some(entry) = rd.next_entry().await? {
            let path = entry.path();
            let ft = entry.file_type().await?;
            if ft.is_dir() {
                stack.push(path);
            } else if ft.is_file() {
                let rel = path
                    .strip_prefix(dir)
                    .unwrap_or(&path)
                    .components()
                    .map(|c| c.as_os_str().to_string_lossy())
                    .collect::<Vec<_>>()
                    .join("/");
                if is_control(&rel) {
                    continue;
                }
                out.push((rel, path));
            }
        }
    }
    Ok(out)
}

fn obj_key(prefix: &str, rel: &str) -> String {
    if prefix.is_empty() {
        rel.to_string()
    } else {
        format!("{prefix}/{rel}")
    }
}

/// Strip `<prefix>/` from a full object key, returning the relative key.
fn strip_prefix<'a>(full_key: &'a str, prefix: &str) -> Option<&'a str> {
    if prefix.is_empty() {
        Some(full_key)
    } else {
        full_key
            .strip_prefix(prefix)
            .and_then(|r| r.strip_prefix('/'))
    }
}

// ===================================================================================
// S3 configuration & requests (SigV4 over reqwest)
// ===================================================================================

struct R2Config {
    /// `https://<host>` with no trailing slash.
    endpoint_base: String,
    host: String,
    region: String,
    access_key: String,
    secret_key: String,
}

fn env_var(name: &str) -> Option<String> {
    std::env::var(name).ok().filter(|s| !s.is_empty())
}

impl R2Config {
    fn from_env() -> Result<Self, Error> {
        let endpoint = if let Some(e) = env_var("R2_ENDPOINT") {
            e
        } else {
            let acct = env_var("R2_ACCOUNT_ID").ok_or_else(|| {
                Error::MissingCredentials("set R2_ENDPOINT or R2_ACCOUNT_ID".to_string())
            })?;
            format!("https://{acct}.r2.cloudflarestorage.com")
        };
        let url = url::Url::parse(&endpoint).map_err(|e| {
            Error::MissingCredentials(format!("invalid R2 endpoint {endpoint:?}: {e}"))
        })?;
        let host = url
            .host_str()
            .ok_or_else(|| {
                Error::MissingCredentials(format!("R2 endpoint has no host: {endpoint:?}"))
            })?
            .to_string();
        let endpoint_base = format!("{}://{}", url.scheme(), host);

        let access_key = env_var("R2_ACCESS_KEY_ID")
            .or_else(|| env_var("AWS_ACCESS_KEY_ID"))
            .ok_or_else(|| {
                Error::MissingCredentials("set R2_ACCESS_KEY_ID (or AWS_ACCESS_KEY_ID)".to_string())
            })?;
        let secret_key = env_var("R2_SECRET_ACCESS_KEY")
            .or_else(|| env_var("AWS_SECRET_ACCESS_KEY"))
            .ok_or_else(|| {
                Error::MissingCredentials(
                    "set R2_SECRET_ACCESS_KEY (or AWS_SECRET_ACCESS_KEY)".to_string(),
                )
            })?;
        let region = env_var("R2_REGION").unwrap_or_else(|| "auto".to_string());

        Ok(Self {
            endpoint_base,
            host,
            region,
            access_key,
            secret_key,
        })
    }

    /// Sign and send a request, returning `(status, body)`.
    async fn send(
        &self,
        client: &reqwest::Client,
        method: &str,
        bucket: &str,
        key: &str,
        query: &[(&str, &str)],
        body: Vec<u8>,
    ) -> Result<(reqwest::StatusCode, Vec<u8>), Error> {
        let (date, datetime) = amz_dates(SystemTime::now());
        let payload_hash = sha256_hex(&body);

        let canonical_uri = if key.is_empty() {
            format!("/{}", uri_encode(bucket, true))
        } else {
            format!("/{}/{}", uri_encode(bucket, true), uri_encode(key, false))
        };

        let mut qp: Vec<(String, String)> = query
            .iter()
            .map(|(k, v)| (uri_encode(k, true), uri_encode(v, true)))
            .collect();
        qp.sort();
        let canonical_query = qp
            .iter()
            .map(|(k, v)| format!("{k}={v}"))
            .collect::<Vec<_>>()
            .join("&");

        let headers = [
            ("host".to_string(), self.host.clone()),
            ("x-amz-content-sha256".to_string(), payload_hash.clone()),
            ("x-amz-date".to_string(), datetime.clone()),
        ];
        let authorization = authorization_header(&SignParams {
            method,
            canonical_uri: &canonical_uri,
            canonical_query: &canonical_query,
            headers: &headers,
            payload_hash: &payload_hash,
            datetime: &datetime,
            date: &date,
            region: &self.region,
            service: "s3",
            access_key: &self.access_key,
            secret_key: &self.secret_key,
        });

        let mut url = format!("{}{}", self.endpoint_base, canonical_uri);
        if !canonical_query.is_empty() {
            url.push('?');
            url.push_str(&canonical_query);
        }

        let m = reqwest::Method::from_bytes(method.as_bytes()).expect("valid method");
        let mut rb = client
            .request(m, &url)
            .header("x-amz-content-sha256", &payload_hash)
            .header("x-amz-date", &datetime)
            .header("authorization", authorization);
        if !body.is_empty() {
            rb = rb.body(body);
        }
        let resp = rb.send().await?;
        let status = resp.status();
        let bytes = resp.bytes().await?.to_vec();
        Ok((status, bytes))
    }
}

async fn list_objects(
    client: &reqwest::Client,
    cfg: &R2Config,
    bucket: &str,
    prefix: &str,
) -> Result<Vec<String>, Error> {
    let mut keys = Vec::new();
    let mut continuation: Option<String> = None;
    let prefix_param = if prefix.is_empty() {
        String::new()
    } else {
        format!("{prefix}/")
    };

    loop {
        let mut query: Vec<(&str, &str)> = vec![("list-type", "2")];
        if !prefix_param.is_empty() {
            query.push(("prefix", &prefix_param));
        }
        if let Some(token) = &continuation {
            query.push(("continuation-token", token));
        }

        let body = retry(|| async {
            let (status, body) = cfg
                .send(client, "GET", bucket, "", &query, Vec::new())
                .await?;
            if status.is_success() {
                Ok(body)
            } else {
                Err(Error::BadStatus {
                    key: "?list".to_string(),
                    status: status.as_u16(),
                    body: truncate(&String::from_utf8_lossy(&body)),
                })
            }
        })
        .await?;
        let xml = String::from_utf8_lossy(&body);
        keys.extend(extract_tags(&xml, "Key"));

        match extract_tags(&xml, "NextContinuationToken")
            .into_iter()
            .next()
        {
            Some(token)
                if extract_tags(&xml, "IsTruncated")
                    .first()
                    .map(String::as_str)
                    == Some("true") =>
            {
                continuation = Some(token);
            }
            _ => break,
        }
    }
    Ok(keys)
}

async fn get_object(
    client: &reqwest::Client,
    cfg: &R2Config,
    bucket: &str,
    key: &str,
) -> Result<Option<Vec<u8>>, Error> {
    retry(|| async {
        let (status, body) = cfg
            .send(client, "GET", bucket, key, &[], Vec::new())
            .await?;
        if status.is_success() {
            Ok(Some(body))
        } else if status == reqwest::StatusCode::NOT_FOUND {
            Ok(None)
        } else {
            Err(Error::BadStatus {
                key: key.to_string(),
                status: status.as_u16(),
                body: truncate(&String::from_utf8_lossy(&body)),
            })
        }
    })
    .await
}

async fn put_object(
    client: &reqwest::Client,
    cfg: &R2Config,
    bucket: &str,
    key: &str,
    body: Vec<u8>,
) -> Result<(), Error> {
    retry(|| {
        let body = body.clone();
        async move {
            let (status, resp) = cfg.send(client, "PUT", bucket, key, &[], body).await?;
            if status.is_success() {
                Ok(())
            } else {
                Err(Error::BadStatus {
                    key: key.to_string(),
                    status: status.as_u16(),
                    body: truncate(&String::from_utf8_lossy(&resp)),
                })
            }
        }
    })
    .await
}

fn truncate(s: &str) -> String {
    s.chars().take(300).collect()
}

/// Extract the text content of every `<tag>...</tag>` occurrence, XML-unescaping the value.
fn extract_tags(xml: &str, tag: &str) -> Vec<String> {
    let open = format!("<{tag}>");
    let close = format!("</{tag}>");
    let mut out = Vec::new();
    let mut rest = xml;
    while let Some(start) = rest.find(&open) {
        let Some(after) = rest.get(start + open.len()..) else {
            break;
        };
        let Some(end) = after.find(&close) else {
            break;
        };
        if let Some(inner) = after.get(..end) {
            out.push(xml_unescape(inner));
        }
        rest = match after.get(end + close.len()..) {
            Some(r) => r,
            None => break,
        };
    }
    out
}

fn xml_unescape(s: &str) -> String {
    s.replace("&amp;", "&")
        .replace("&lt;", "<")
        .replace("&gt;", ">")
        .replace("&quot;", "\"")
        .replace("&#39;", "'")
}

// ===================================================================================
// SigV4 signing
// ===================================================================================

struct SignParams<'a> {
    method: &'a str,
    canonical_uri: &'a str,
    canonical_query: &'a str,
    headers: &'a [(String, String)],
    payload_hash: &'a str,
    datetime: &'a str,
    date: &'a str,
    region: &'a str,
    service: &'a str,
    access_key: &'a str,
    secret_key: &'a str,
}

/// Compute the `Authorization` header value for an AWS SigV4 (`s3`) request.
fn authorization_header(p: &SignParams) -> String {
    let mut headers = p.headers.to_vec();
    headers.sort_by(|a, b| a.0.cmp(&b.0));

    let canonical_headers: String = headers
        .iter()
        .map(|(k, v)| format!("{}:{}\n", k, v.trim()))
        .collect();
    let signed_headers = headers
        .iter()
        .map(|(k, _)| k.as_str())
        .collect::<Vec<_>>()
        .join(";");

    let canonical_request = format!(
        "{}\n{}\n{}\n{}\n{}\n{}",
        p.method,
        p.canonical_uri,
        p.canonical_query,
        canonical_headers,
        signed_headers,
        p.payload_hash
    );

    let scope = format!("{}/{}/{}/aws4_request", p.date, p.region, p.service);
    let string_to_sign = format!(
        "AWS4-HMAC-SHA256\n{}\n{}\n{}",
        p.datetime,
        scope,
        sha256_hex(canonical_request.as_bytes())
    );

    let k_date = hmac(
        format!("AWS4{}", p.secret_key).as_bytes(),
        p.date.as_bytes(),
    );
    let k_region = hmac(&k_date, p.region.as_bytes());
    let k_service = hmac(&k_region, p.service.as_bytes());
    let k_signing = hmac(&k_service, b"aws4_request");
    let signature = hex::encode(hmac(&k_signing, string_to_sign.as_bytes()));

    format!(
        "AWS4-HMAC-SHA256 Credential={}/{}, SignedHeaders={}, Signature={}",
        p.access_key, scope, signed_headers, signature
    )
}

fn sha256_hex(data: &[u8]) -> String {
    let mut h = Sha256::new();
    h.update(data);
    hex::encode(h.finalize())
}

fn hmac(key: &[u8], data: &[u8]) -> Vec<u8> {
    let mut mac = <HmacSha256 as Mac>::new_from_slice(key).expect("HMAC accepts any key length");
    mac.update(data);
    mac.finalize().into_bytes().to_vec()
}

/// AWS-style percent-encoding. `unreserved = A-Za-z0-9-._~`; everything else is encoded.
/// When `encode_slash` is false, `/` is left as-is (for object key paths).
fn uri_encode(s: &str, encode_slash: bool) -> String {
    let mut out = String::with_capacity(s.len());
    for &b in s.as_bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'.' | b'_' | b'~' => {
                out.push(b as char)
            }
            b'/' if !encode_slash => out.push('/'),
            _ => out.push_str(&format!("%{b:02X}")),
        }
    }
    out
}

/// Format a `SystemTime` as `(YYYYMMDD, YYYYMMDDTHHMMSSZ)` in UTC, without a date library.
fn amz_dates(t: SystemTime) -> (String, String) {
    let secs = t
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0);
    let days = secs.div_euclid(86400);
    let sod = secs.rem_euclid(86400);
    let (y, m, d) = civil_from_days(days);
    let (hh, mm, ss) = (sod / 3600, (sod % 3600) / 60, sod % 60);
    (
        format!("{y:04}{m:02}{d:02}"),
        format!("{y:04}{m:02}{d:02}T{hh:02}{mm:02}{ss:02}Z"),
    )
}

/// Convert a count of days since the Unix epoch to a `(year, month, day)` civil date.
/// Howard Hinnant's `civil_from_days` algorithm.
fn civil_from_days(z: i64) -> (i64, u32, u32) {
    let z = z + 719468;
    let era = if z >= 0 { z } else { z - 146096 } / 146097;
    let doe = (z - era * 146097) as u64; // [0, 146096]
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365; // [0, 399]
    let y = yoe as i64 + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100); // [0, 365]
    let mp = (5 * doy + 2) / 153; // [0, 11]
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32; // [1, 31]
    let m = if mp < 10 { mp + 3 } else { mp - 9 } as u32; // [1, 12]
    (y + i64::from(m <= 2), m, d)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The overall fingerprint is a commutative sum of per-file identities, so it is
    /// order-independent and changes when the set changes or a content hash changes.
    #[test]
    fn fingerprint_is_order_independent_and_sensitive() {
        let sum = |parts: &[u64]| parts.iter().fold(0u64, |a, p| a.wrapping_add(*p));

        let a = [
            identity("042/aaa", None),
            identity("100/bbb", None),
            identity("translate.json", Some(7)),
        ];
        let b = [
            identity("translate.json", Some(7)),
            identity("042/aaa", None),
            identity("100/bbb", None),
        ];
        assert_eq!(sum(&a), sum(&b), "order must not matter");

        // A changed content hash for the same path changes the fingerprint.
        assert_ne!(
            identity("translate.json", Some(7)),
            identity("translate.json", Some(8)),
        );
        // Adding a file changes the fingerprint.
        let c = [a[0], a[1], a[2], identity("123/ddd", None)];
        assert_ne!(sum(&a), sum(&c));
    }

    #[test]
    fn glob_match_rules() {
        assert!(glob_match(
            "google_translate/master_cache.json",
            "google_translate/master_cache.json"
        ));
        assert!(glob_match(
            "google_translate/*.json",
            "google_translate/master_cache.json"
        ));
        assert!(glob_match(
            "*/master_cache.json",
            "google_translate/master_cache.json"
        ));
        assert!(!glob_match(
            "google_translate/*.bin",
            "google_translate/master_cache.json"
        ));
        assert!(!glob_match(
            "wiktionary/*",
            "google_translate/master_cache.json"
        ));
    }

    #[test]
    fn strategy_lookup_and_default() {
        let settings: SyncSettings = serde_json::from_str(
            r#"{"files":[{"path":"google_translate/*.json","strategy":"json_merge"}]}"#,
        )
        .unwrap();
        assert_eq!(
            settings.strategy_for("google_translate/master_cache.json"),
            Strategy::JsonMerge
        );
        // Anything not matched is content-addressed (path) by default.
        assert_eq!(settings.strategy_for("042/abc"), Strategy::Path);
    }

    #[test]
    fn json_merge_unions_keys_local_wins() {
        let remote = br#"{"a":1,"b":2}"#;
        let local = br#"{"b":99,"c":3}"#;
        let merged = merge_json_maps(remote, local).unwrap();
        let m: serde_json::Map<String, serde_json::Value> =
            serde_json::from_slice(&merged).unwrap();
        assert_eq!(m["a"], 1);
        assert_eq!(m["b"], 99, "local value wins on collision");
        assert_eq!(m["c"], 3);
        // Non-objects are rejected (caller falls back to whole-file handling).
        assert!(merge_json_maps(b"[1,2]", b"{}").is_none());
    }

    #[test]
    fn jsonl_merge_unions_lines_by_key() {
        // Shared key "b" -> local wins; "a" only remote, "c" only local.
        let remote = b"{\"k\":\"a\",\"v\":1}\n{\"k\":\"b\",\"v\":2}\n";
        let local = b"{\"k\":\"b\",\"v\":99}\n{\"k\":\"c\",\"v\":3}\n";
        let merged = merge_jsonl(remote, local, "k");
        let lines: Vec<&str> = std::str::from_utf8(&merged)
            .unwrap()
            .lines()
            .filter(|l| !l.is_empty())
            .collect();
        assert_eq!(lines.len(), 3, "one line per distinct key: {lines:?}");
        assert!(lines.contains(&"{\"k\":\"a\",\"v\":1}"));
        assert!(
            lines.contains(&"{\"k\":\"b\",\"v\":99}"),
            "local wins on key collision: {lines:?}"
        );
        assert!(lines.contains(&"{\"k\":\"c\",\"v\":3}"));

        // Idempotent: merging a file with itself is a no-op set.
        let again = merge_jsonl(&merged, &merged, "k");
        assert_eq!(merge_jsonl(&again, b"", "k"), again);

        // jsonl key field is configurable per rule.
        let settings: SyncSettings = serde_json::from_str(
            r#"{"files":[{"path":"*.jsonl","strategy":"jsonl_merge","key":"id"}]}"#,
        )
        .unwrap();
        assert_eq!(settings.strategy_for("000.jsonl"), Strategy::JsonlMerge);
        assert_eq!(settings.jsonl_key_for("000.jsonl"), "id");
        // A path not matched by any rule falls back to the default key.
        assert_eq!(settings.jsonl_key_for("notes.txt"), "k");
    }

    #[test]
    fn civil_date_known_values() {
        // 2013-05-24 is day 15849 since the epoch.
        assert_eq!(civil_from_days(15849), (2013, 5, 24));
        // Epoch itself.
        assert_eq!(civil_from_days(0), (1970, 1, 1));
    }

    #[test]
    fn uri_encode_rules() {
        assert_eq!(uri_encode("a/b c", false), "a/b%20c");
        assert_eq!(uri_encode("a/b c", true), "a%2Fb%20c");
        assert_eq!(uri_encode("-._~AZ09", true), "-._~AZ09");
    }

    /// The official `aws-sig-v4-test-suite` `get-vanilla` vector. Locks down the
    /// canonical-request / string-to-sign / signing-key derivation without a network call.
    /// (Verified independently against the published expected signature.)
    #[test]
    fn sigv4_matches_official_get_vanilla_vector() {
        const EMPTY_SHA256: &str =
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";
        let headers = [
            ("host".to_string(), "example.amazonaws.com".to_string()),
            ("x-amz-date".to_string(), "20150830T123600Z".to_string()),
        ];
        let auth = authorization_header(&SignParams {
            method: "GET",
            canonical_uri: "/",
            canonical_query: "",
            headers: &headers,
            payload_hash: EMPTY_SHA256,
            datetime: "20150830T123600Z",
            date: "20150830",
            region: "us-east-1",
            service: "service",
            access_key: "AKIDEXAMPLE",
            secret_key: "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY",
        });
        assert_eq!(
            auth,
            "AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/20150830/us-east-1/service/aws4_request, \
             SignedHeaders=host;x-amz-date, \
             Signature=5fa00fa31553b73ebf1942676e86291e8372ff2a2260956d9b8aae1d763fbf31"
        );
    }
}
