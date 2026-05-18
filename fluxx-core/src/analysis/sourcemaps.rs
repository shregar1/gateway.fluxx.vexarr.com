//! Sourcemap store + on-ingest symbolication.
//!
//! Operators (or CI) `POST /v1/sourcemaps` with a multipart payload:
//! a release tag, a bundle URL (e.g. `https://app/static/main.abc.js`),
//! and either the `.map` body inline or a debug-id.
//!
//! At error-ingest time, when a JS stack frame is detected we look up
//! the matching sourcemap by `(release, bundle_url)` and rewrite the
//! frame's file/line/column to source coordinates. The original
//! minified frame is preserved in the `stack_raw` field for forensic
//! reference.
//!
//! Storage is a content-addressed directory tree (sha256-of-body
//! filename), so re-uploading the same map is a free dedup. The index
//! lives in memory backed by a JSON file under the same directory; on
//! restart the agent walks the file and reloads pointers without
//! re-reading the maps themselves until a lookup needs them.

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use sourcemap::SourceMap;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use tracing::{debug, warn};

use crate::analysis::error_tracking::ParsedStackTrace;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SourcemapMetadata {
    pub release: String,
    pub bundle_url: String,
    pub blob_sha256: String,
    pub uploaded_at_us: u64,
}

#[derive(Debug)]
pub struct SourcemapStore {
    /// Root directory for blobs + index.
    root: PathBuf,
    /// (release, bundle_url) → blob_sha256.
    index: RwLock<HashMap<(String, String), String>>,
    /// blob_sha256 → parsed map (lazy, populated on first lookup).
    cache: RwLock<HashMap<String, Arc<SourceMap>>>,
}

impl SourcemapStore {
    /// Open a store at `root`. Missing directories are created. Any
    /// failure to read an existing index is logged and treated as
    /// "start empty" — the operator can re-upload.
    pub fn open(root: impl Into<PathBuf>) -> std::io::Result<Arc<Self>> {
        let root = root.into();
        std::fs::create_dir_all(&root)?;
        std::fs::create_dir_all(root.join("blobs"))?;

        let mut index = HashMap::new();
        let idx_path = root.join("index.json");
        if idx_path.exists() {
            match std::fs::read_to_string(&idx_path) {
                Ok(s) => match serde_json::from_str::<Vec<SourcemapMetadata>>(&s) {
                    Ok(entries) => {
                        for e in entries {
                            index.insert((e.release, e.bundle_url), e.blob_sha256);
                        }
                    }
                    Err(e) => warn!(error = %e, "sourcemaps: index parse failed; starting empty"),
                },
                Err(e) => warn!(error = %e, "sourcemaps: index read failed; starting empty"),
            }
        }

        Ok(Arc::new(Self {
            root,
            index: RwLock::new(index),
            cache: RwLock::new(HashMap::new()),
        }))
    }

    /// Persist a sourcemap blob. Idempotent on content: re-uploading
    /// the same body is a no-op aside from index rewrites.
    pub fn put(
        &self,
        release: &str,
        bundle_url: &str,
        map_body: &[u8],
    ) -> std::io::Result<SourcemapMetadata> {
        let mut hasher = Sha256::new();
        hasher.update(map_body);
        let blob_sha256: String = hasher.finalize().iter().map(|b| format!("{:02x}", b)).collect();

        let blob_path = self.blob_path(&blob_sha256);
        if !blob_path.exists() {
            // Write atomically: tmp → rename.
            let tmp = blob_path.with_extension("tmp");
            std::fs::write(&tmp, map_body)?;
            std::fs::rename(&tmp, &blob_path)?;
        }

        let meta = SourcemapMetadata {
            release: release.to_string(),
            bundle_url: bundle_url.to_string(),
            blob_sha256: blob_sha256.clone(),
            uploaded_at_us: now_us(),
        };

        self.index.write().insert(
            (release.to_string(), bundle_url.to_string()),
            blob_sha256.clone(),
        );
        self.write_index()?;
        Ok(meta)
    }

    /// Look up a sourcemap and resolve the given `(file, line, col)` to
    /// source coordinates. Returns `None` if no map is registered for
    /// the release+url, the map can't be parsed, or the lookup misses.
    pub fn resolve_frame(
        &self,
        release: &str,
        bundle_url: &str,
        line: u32,
        col: u32,
    ) -> Option<ResolvedFrame> {
        let blob = self
            .index
            .read()
            .get(&(release.to_string(), bundle_url.to_string()))
            .cloned()?;
        let map = self.get_or_load(&blob)?;
        // sourcemap crate is 0-indexed; SDKs emit 1-indexed line/col.
        let l = line.saturating_sub(1);
        let c = col.saturating_sub(1);
        let token = map.lookup_token(l, c)?;
        let src_line_0 = token.get_src_line();
        let context = source_context(&map, token.get_src_id(), src_line_0);
        Some(ResolvedFrame {
            source: token.get_source().unwrap_or_default().to_string(),
            line: src_line_0 + 1,
            col: token.get_src_col() + 1,
            name: token.get_name().map(|s| s.to_string()),
            context,
        })
    }

    /// Symbolicate every frame in a `ParsedStackTrace` that matches a
    /// known bundle URL. Returns a *new* trace, the parallel context
    /// array (one snippet per frame; empty when none), and a touched
    /// flag for "did anything actually get rewritten" — the caller
    /// uses it to flip `frames_resolved`.
    pub fn symbolicate(
        &self,
        release: &str,
        trace: &ParsedStackTrace,
    ) -> (ParsedStackTrace, Vec<String>, bool) {
        let mut out = trace.clone();
        let mut context = Vec::with_capacity(out.frames.len());
        let mut touched = false;
        for frame in out.frames.iter_mut() {
            let Some(file) = frame.file.as_deref() else {
                context.push(String::new());
                continue;
            };
            let Some(line) = frame.line else {
                context.push(String::new());
                continue;
            };
            let col = frame.column.unwrap_or(0);
            if let Some(r) = self.resolve_frame(release, file, line, col) {
                frame.file = Some(r.source);
                frame.line = Some(r.line);
                frame.column = Some(r.col);
                if let Some(name) = r.name {
                    if frame.function.as_deref().map_or(true, str::is_empty) {
                        frame.function = Some(name);
                    }
                }
                context.push(r.context);
                touched = true;
            } else {
                context.push(String::new());
            }
        }
        (out, context, touched)
    }

    pub fn list(&self) -> Vec<SourcemapMetadata> {
        let idx = self.index.read();
        let mut out = Vec::with_capacity(idx.len());
        for ((release, bundle_url), blob) in idx.iter() {
            out.push(SourcemapMetadata {
                release: release.clone(),
                bundle_url: bundle_url.clone(),
                blob_sha256: blob.clone(),
                uploaded_at_us: 0, // not tracked per-entry in v1
            });
        }
        out.sort_by(|a, b| a.release.cmp(&b.release).then(a.bundle_url.cmp(&b.bundle_url)));
        out
    }

    fn get_or_load(&self, blob_sha256: &str) -> Option<Arc<SourceMap>> {
        if let Some(m) = self.cache.read().get(blob_sha256) {
            return Some(m.clone());
        }
        let path = self.blob_path(blob_sha256);
        let bytes = std::fs::read(&path).ok()?;
        let map = match SourceMap::from_reader(bytes.as_slice()) {
            Ok(m) => Arc::new(m),
            Err(e) => {
                warn!(error = %e, blob = %blob_sha256, "sourcemaps: parse failed");
                return None;
            }
        };
        self.cache.write().insert(blob_sha256.to_string(), map.clone());
        debug!(blob = %blob_sha256, "sourcemaps: loaded into cache");
        Some(map)
    }

    fn blob_path(&self, blob_sha256: &str) -> PathBuf {
        self.root.join("blobs").join(format!("{}.map", blob_sha256))
    }

    fn write_index(&self) -> std::io::Result<()> {
        let entries: Vec<SourcemapMetadata> = self
            .index
            .read()
            .iter()
            .map(|((release, bundle_url), blob_sha256)| SourcemapMetadata {
                release: release.clone(),
                bundle_url: bundle_url.clone(),
                blob_sha256: blob_sha256.clone(),
                uploaded_at_us: 0,
            })
            .collect();
        let body = serde_json::to_string_pretty(&entries)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;
        let path = self.root.join("index.json");
        let tmp = path.with_extension("tmp");
        std::fs::write(&tmp, body)?;
        std::fs::rename(&tmp, &path)?;
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct ResolvedFrame {
    pub source: String,
    pub line: u32,
    pub col: u32,
    pub name: Option<String>,
    /// Source-line context, ±2 lines around `line`, joined by `\n`.
    /// Empty when the sourcemap didn't ship `sourcesContent` for this
    /// source file. Convention: prefix each line with a 1-based line
    /// number and a `:` so the dashboard can split + render gutters
    /// without re-doing math.
    pub context: String,
}

/// Pull `±2` lines around `line_0` (0-indexed) from the sourcemap's
/// embedded sourcesContent. Returns an empty string when the source
/// isn't shipped inline — the dashboard then renders just the
/// resolved file:line header without a snippet.
///
/// Each returned line is prefixed with its 1-based line number and
/// a `:` so the dashboard can split + render a gutter without
/// reproducing the offset math here.
fn source_context(map: &SourceMap, src_id: u32, line_0: u32) -> String {
    let view = map.get_source_view(src_id);
    let Some(view) = view else { return String::new() };
    let start = line_0.saturating_sub(2);
    let end = line_0.saturating_add(2);
    let mut out = String::new();
    for l in start..=end {
        if let Some(content) = view.get_line(l) {
            // Lines from the view come without their trailing \n;
            // adding our own keeps the snippet a clean join.
            if !out.is_empty() {
                out.push('\n');
            }
            out.push_str(&format!("{}:{}", l + 1, content));
        }
    }
    out
}

fn now_us() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_micros() as u64
}

/// Convenience: rebuild the projection arrays used by the
/// `ErrorEventRow` from a symbolicated trace.
pub fn project_frames(trace: &ParsedStackTrace) -> Projected {
    let mut p = Projected::default();
    for f in &trace.frames {
        p.functions.push(f.function.clone().unwrap_or_default());
        p.files.push(f.file.clone().unwrap_or_default());
        p.lines.push(f.line.unwrap_or(0));
        p.cols.push(f.column.unwrap_or(0));
    }
    p
}

#[derive(Debug, Default)]
pub struct Projected {
    pub functions: Vec<String>,
    pub files: Vec<String>,
    pub lines: Vec<u32>,
    pub cols: Vec<u32>,
}

#[derive(Debug, Default)]
pub struct ProjectedWithContext {
    pub projected: Projected,
    pub context: Vec<String>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::analysis::error_tracking::StackFrame;

    fn tmp_dir() -> PathBuf {
        let p = std::env::temp_dir().join(format!("fluxx-smap-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&p).unwrap();
        p
    }

    fn fake_minified_map() -> Vec<u8> {
        // A minimal valid source map: maps line 1 col 0 of the
        // generated file to `src/app.js` line 10 col 5.
        // Constructed by hand so we don't pull a JS toolchain into the
        // test build.
        // Format reference: https://sourcemaps.info/spec.html
        let json = serde_json::json!({
            "version": 3,
            "file": "main.min.js",
            "sources": ["src/app.js"],
            "names": ["greet"],
            // Single mapping: gen(0,0) → src(0, 9, 4, "greet")
            // VLQ-encoded: "AASIA"
            //   A = 0, A = 0, S = 9, I = 4, A = 0
            // first segment: AAAAA expands to (0,0,0,0) → no name
            // we want a name index, so encode: AASIA (col=0, src=0, line=9, col=4, name=0)
            "mappings": "AASIA"
        });
        serde_json::to_vec(&json).unwrap()
    }

    #[test]
    fn put_is_content_addressed_and_idempotent() {
        let root = tmp_dir();
        let store = SourcemapStore::open(&root).unwrap();
        let body = fake_minified_map();
        let m1 = store.put("1.0.0", "https://x/main.min.js", &body).unwrap();
        let m2 = store.put("1.0.0", "https://x/main.min.js", &body).unwrap();
        assert_eq!(m1.blob_sha256, m2.blob_sha256);
        assert!(store.blob_path(&m1.blob_sha256).exists());
        std::fs::remove_dir_all(&root).ok();
    }

    #[test]
    fn list_returns_uploads_sorted() {
        let root = tmp_dir();
        let store = SourcemapStore::open(&root).unwrap();
        let body = fake_minified_map();
        store.put("1.0.0", "https://x/b.js", &body).unwrap();
        store.put("1.0.0", "https://x/a.js", &body).unwrap();
        store.put("0.9.0", "https://x/a.js", &body).unwrap();
        let l = store.list();
        assert_eq!(l.len(), 3);
        assert_eq!(l[0].release, "0.9.0");
        assert_eq!(l[1].bundle_url, "https://x/a.js");
        assert_eq!(l[2].bundle_url, "https://x/b.js");
        std::fs::remove_dir_all(&root).ok();
    }

    #[test]
    fn resolve_returns_none_for_unknown_release_or_url() {
        let root = tmp_dir();
        let store = SourcemapStore::open(&root).unwrap();
        store.put("1.0.0", "https://x/m.js", &fake_minified_map()).unwrap();
        assert!(store.resolve_frame("nope", "https://x/m.js", 1, 1).is_none());
        assert!(store.resolve_frame("1.0.0", "https://x/missing.js", 1, 1).is_none());
        std::fs::remove_dir_all(&root).ok();
    }

    #[test]
    fn symbolicate_marks_untouched_when_no_maps_apply() {
        let root = tmp_dir();
        let store = SourcemapStore::open(&root).unwrap();
        let trace = ParsedStackTrace {
            raw: "foo".into(),
            frames: vec![StackFrame {
                function: Some("f".into()),
                file: Some("https://nope.js".into()),
                line: Some(1),
                column: Some(1),
                context: None,
            }],
        };
        let (out, context, touched) = store.symbolicate("1.0.0", &trace);
        assert!(!touched);
        assert_eq!(out.frames[0].file.as_deref(), Some("https://nope.js"));
        // Context array always parallel to frames, even when none touched.
        assert_eq!(context.len(), out.frames.len());
        assert!(context[0].is_empty());
        std::fs::remove_dir_all(&root).ok();
    }

    #[test]
    fn symbolicate_returns_parallel_context_array() {
        let root = tmp_dir();
        let store = SourcemapStore::open(&root).unwrap();
        let trace = ParsedStackTrace {
            raw: "".into(),
            frames: vec![
                StackFrame { function: None, file: Some("a.js".into()), line: Some(1), column: Some(1), context: None },
                StackFrame { function: None, file: None, line: None, column: None, context: None },
                StackFrame { function: None, file: Some("b.js".into()), line: Some(1), column: None, context: None },
            ],
        };
        let (out, context, _) = store.symbolicate("1.0.0", &trace);
        assert_eq!(out.frames.len(), 3);
        assert_eq!(context.len(), 3);
        std::fs::remove_dir_all(&root).ok();
    }

    #[test]
    fn index_survives_reopen() {
        let root = tmp_dir();
        {
            let store = SourcemapStore::open(&root).unwrap();
            store.put("1.0.0", "https://x/m.js", &fake_minified_map()).unwrap();
        }
        let store = SourcemapStore::open(&root).unwrap();
        assert_eq!(store.list().len(), 1);
        std::fs::remove_dir_all(&root).ok();
    }
}
