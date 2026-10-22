//! Append-only audit journal (plan §3.6, M5).
//!
//! # Durability design
//!
//! Each journal is a directory of fsynced, append-only JSONL WAL segments
//! named `wal-{first_seq:010}.jsonl`, rotated every [`SEGMENT_ROTATION`]
//! entries: every `append` seals the hash chain, line-serializes the
//! record, then `sync_data()`s the segment before the sequence head
//! advances — a kill -9 or host crash may drop the in-flight record at
//! worst, never yield a torn or un-chained entry. Segments are replayed
//! in file-name order on reopen to restore sequence + chain-head
//! continuity.
//!
//! Compaction writes a real Apache Parquet snapshot (arrow 53 flat column
//! layout), mirroring the honba-me tick-lakehouse idiom.
//!
//! > **Plan deviation (§4 M5, as-written)**: the plan text names SQLx
//! > SQLite WAL storage. sqlx/sqlite are unavailable in this build
//! > environment's dependency cache and would couple the audit path to an
//! > external C library; the fsynced JSONL segment WAL delivers the same
//! > append-only, fsync-per-entry durability with a human-inspectable
//! > replay format. Documented here instead of silently substituting.
//!
//! # Zero-clock purity
//!
//! `append` stamps capture time from an injected `DateTime<Utc>` (AGENTS
//! §2.5) — the caller (adapter layer) owns the clock.

use std::fs::{File, OpenOptions};
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use arrow::array::{ArrayRef, StringArray, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use chrono::{DateTime, Utc};
use parquet::arrow::ArrowWriter;

use crate::entry::{AuditRecord, AuditSpec, SystemMeta};
use crate::hash_chain::{ChainLink, genesis, verify_chain};

/// WAL entries per segment before rotation (sealed segments keep replay
/// and chain verification memory-bounded).
pub const SEGMENT_ROTATION: usize = 10_000;

/// Journal failure modes.
#[derive(Debug, thiserror::Error)]
pub enum JournalError {
    /// Segment/compaction-backed I/O failure.
    #[error("journal I/O: {0}")]
    Io(#[from] std::io::Error),
    /// JSON line (de)serialization failure.
    #[error("journal serialization: {0}")]
    Serde(#[from] serde_json::Error),
    /// Parquet compaction failure.
    #[error("parquet compaction: {0}")]
    Parquet(String),
    /// Hash-chain or sequence-integrity verification failure (tamper).
    #[error("journal tamper evidence failed: {0}")]
    Tamper(String),
}

/// Journal head state under one lock: sequence, chain tip, open segment.
struct JournalState {
    seq: u64,
    prev_hash: String,
    segment: Option<SegmentWriter>,
}

struct SegmentWriter {
    file: BufWriter<File>,
    lines: usize,
}

/// Fsynced, hash-chained, append-only audit journal rooted at a directory.
pub struct AuditJournal {
    dir: PathBuf,
    meta: SystemMeta,
    rotation: usize,
    state: Mutex<JournalState>,
}

impl AuditJournal {
    /// Open (or create) a journal directory, replaying existing segments
    /// to restore sequence/chain-head continuity.
    ///
    /// # Errors
    /// Propagates I/O failures and refuses to open a journal whose lines
    /// no longer parse (corruption is fail-stop, per the tamper contract).
    pub fn open(dir: impl Into<PathBuf>, meta: SystemMeta) -> Result<Self, JournalError> {
        let dir = dir.into();
        std::fs::create_dir_all(&dir)?;
        let mut seq = 0_u64;
        let mut prev_hash = genesis();
        for segment in segment_paths(&dir)? {
            for line in BufReader::new(File::open(&segment)?).lines() {
                let line = line?;
                if line.trim().is_empty() {
                    continue;
                }
                let record: AuditRecord = serde_json::from_str(&line)?;
                seq = record.seq;
                prev_hash = record.hash;
            }
        }
        Ok(Self {
            dir,
            meta,
            rotation: SEGMENT_ROTATION,
            state: Mutex::new(JournalState {
                seq,
                prev_hash,
                segment: None,
            }),
        })
    }

    /// Override the default [`SEGMENT_ROTATION`] (small rotation keeps
    /// files inspectable; large minimizes rotate overhead).
    #[must_use]
    pub fn with_segment_rotation(mut self, entries: usize) -> Self {
        self.rotation = entries.max(1);
        self
    }

    /// Directory backing the journal.
    #[must_use]
    pub fn dir(&self) -> &Path {
        &self.dir
    }

    /// Current monotonic sequence head (0 for an empty journal).
    ///
    /// # Panics
    /// Only if the internal mutex was poisoned by another thread's panic.
    pub fn seq(&self) -> u64 {
        self.state.lock().unwrap().seq
    }

    /// Seal, fsync, and append one compliance event; returns the sealed
    /// record.
    ///
    /// # Errors
    /// I/O or serialization failures abort with the sequence head
    /// unchanged; the caller decides whether to retry the append.
    pub fn append(
        &self,
        captured_at: DateTime<Utc>,
        spec: AuditSpec,
    ) -> Result<AuditRecord, JournalError> {
        let mut state = self.state.lock().unwrap();
        let seq = state.seq + 1;
        let record = AuditRecord {
            seq,
            captured_at,
            client_order_id: spec.client_order_id,
            exchange_order_id: spec.exchange_order_id,
            algo_id: spec.algo_id,
            client_account_id: spec.client_account_id,
            client_pan: spec.client_pan,
            side: spec.side,
            product: spec.product,
            order_type: spec.order_type,
            quantity: spec.quantity,
            price: spec.price,
            event: spec.event,
            system: self.meta.clone(),
            raw_payload: spec.raw_payload,
            prev_hash: String::new(),
            hash: String::new(),
        }
        .finalize(state.prev_hash.clone());

        let sealed = serde_json::to_string(&record)?;
        let rotate = state
            .segment
            .as_ref()
            .is_none_or(|open_segment| open_segment.lines >= self.rotation);
        if rotate {
            let path = self.dir.join(segment_name(seq));
            let file = OpenOptions::new()
                .create_new(true)
                .append(true)
                .open(&path)?;
            state.segment = Some(SegmentWriter {
                file: BufWriter::new(file),
                lines: 0,
            });
        }
        {
            let segment = state
                .segment
                .as_mut()
                .expect("rotation guarantees a segment");
            let file = &mut segment.file;
            file.write_all(sealed.as_bytes())?;
            file.write_all(b"\n")?;
            file.flush()?;
            segment.file.get_ref().sync_data()?;
            segment.lines += 1;
        }
        state.seq = seq;
        state.prev_hash = record.hash.clone();
        Ok(record)
    }

    /// Replay every sealed record from disk, in segment order.
    ///
    /// # Errors
    /// I/O or corruption on segment replay.
    pub fn records(&self) -> Result<Vec<AuditRecord>, JournalError> {
        let mut out = Vec::new();
        for segment in segment_paths(&self.dir)? {
            for line in BufReader::new(File::open(&segment)?).lines() {
                let line = line?;
                if line.trim().is_empty() {
                    continue;
                }
                out.push(serde_json::from_str(&line)?);
            }
        }
        Ok(out)
    }

    /// Tamper-evidence audit: sequential numbering, chain digests, and the
    /// genesis anchor over the replayed records.
    ///
    /// # Errors
    /// [`JournalError::Tamper`] naming the offending sequence on any break.
    pub fn verify(&self) -> Result<(), JournalError> {
        let records = self.records()?;
        for (index, record) in records.iter().enumerate() {
            let expected_seq = index as u64 + 1;
            if record.seq != expected_seq {
                return Err(JournalError::Tamper(format!(
                    "sequence gap at position {expected_seq}: found seq {}",
                    record.seq
                )));
            }
        }
        let links: Vec<ChainLink> = records.iter().map(AuditRecord::chain_link).collect();
        verify_chain(&links).map_err(|break_| JournalError::Tamper(break_.to_string()))
    }

    /// Compact the whole journal into an Apache Parquet snapshot.
    ///
    /// `output = None` writes `audit-{head_seq:010}.parquet` beside the
    /// WAL segments.
    ///
    /// # Errors
    /// Empty journals, replay I/O, or Parquet encoding failures.
    pub fn compact_parquet(&self, output: Option<&Path>) -> Result<PathBuf, JournalError> {
        let records = self.records()?;
        let last_seq = records
            .last()
            .map(|record| record.seq)
            .ok_or_else(|| JournalError::Parquet("nothing to compact: journal is empty".into()))?;
        let target = output
            .map(Path::to_path_buf)
            .unwrap_or_else(|| self.dir.join(format!("audit-{last_seq:010}.parquet")));

        let captured_texts: Vec<String> =
            records.iter().map(AuditRecord::captured_at_text).collect();
        let client_id_texts: Vec<String> = records
            .iter()
            .map(AuditRecord::client_order_id_text)
            .collect();
        let columns: Vec<ArrayRef> = vec![
            Arc::new(UInt64Array::from(
                records
                    .iter()
                    .map(|record| Some(record.seq))
                    .collect::<Vec<_>>(),
            )),
            string_column(
                captured_texts
                    .iter()
                    .map(String::as_str)
                    .map(Some)
                    .collect(),
            ),
            string_column(
                client_id_texts
                    .iter()
                    .map(String::as_str)
                    .map(Some)
                    .collect(),
            ),
            string_column(
                records
                    .iter()
                    .map(|r| r.exchange_order_id.as_deref())
                    .collect(),
            ),
            string_column(records.iter().map(|r| Some(r.algo_id.as_str())).collect()),
            string_column(
                records
                    .iter()
                    .map(|r| Some(r.client_account_id.as_str()))
                    .collect(),
            ),
            string_column(
                records
                    .iter()
                    .map(|r| Some(r.client_pan.as_str()))
                    .collect(),
            ),
            string_column(records.iter().map(|r| Some(r.side.as_str())).collect()),
            string_column(records.iter().map(|r| Some(r.product.as_str())).collect()),
            string_column(
                records
                    .iter()
                    .map(|r| Some(r.order_type.as_str()))
                    .collect(),
            ),
            string_column(records.iter().map(|r| Some(r.quantity.as_str())).collect()),
            string_column(records.iter().map(|r| Some(r.price.as_str())).collect()),
            string_column(records.iter().map(|r| Some(r.event.as_label())).collect()),
            string_column(
                system_column(&records)
                    .iter()
                    .map(String::as_str)
                    .map(Some)
                    .collect(),
            ),
            string_column(
                records
                    .iter()
                    .map(|r| Some(r.raw_payload.as_str()))
                    .collect(),
            ),
            string_column(records.iter().map(|r| Some(r.prev_hash.as_str())).collect()),
            string_column(records.iter().map(|r| Some(r.hash.as_str())).collect()),
        ];

        let schema = Arc::new(parquet_schema());
        let batch = RecordBatch::try_new(schema.clone(), columns)
            .map_err(|error| JournalError::Parquet(error.to_string()))?;
        let file = File::create(&target)?;
        let mut writer = ArrowWriter::try_new(file, schema, None)
            .map_err(|error| JournalError::Parquet(error.to_string()))?;
        writer
            .write(&batch)
            .map_err(|error| JournalError::Parquet(error.to_string()))?;
        writer
            .close()
            .map_err(|error| JournalError::Parquet(error.to_string()))?;
        Ok(target)
    }
}

/// Nullable UTF-8 compaction column from pinned record fields.
fn string_column(values: Vec<Option<&str>>) -> ArrayRef {
    Arc::new(StringArray::from(values))
}

/// System identities serialized as JSON strings (one per record).
fn system_column(records: &[AuditRecord]) -> Vec<String> {
    records
        .iter()
        .map(|record| serde_json::to_string(&record.system).unwrap_or_else(|_| "{}".into()))
        .collect()
}

fn segment_name(first_seq: u64) -> String {
    format!("wal-{first_seq:010}.jsonl")
}

/// Sorted `wal-*.jsonl` segment paths (file-name order = journal order).
fn segment_paths(dir: &Path) -> Result<Vec<PathBuf>, JournalError> {
    let mut segments: Vec<PathBuf> = std::fs::read_dir(dir)?
        .flatten()
        .filter(|entry| {
            let name = entry.file_name();
            let name = name.to_string_lossy();
            name.starts_with("wal-") && name.ends_with(".jsonl")
        })
        .map(|entry| entry.path())
        .collect();
    segments.sort();
    Ok(segments)
}

/// The compaction column layout (plan §3.6 fields, one flat table).
fn parquet_schema() -> Schema {
    let utf8 = |name: &str| Field::new(name, DataType::Utf8, true);
    Schema::new(vec![
        Field::new("seq", DataType::UInt64, true),
        utf8("captured_at"),
        utf8("client_order_id"),
        utf8("exchange_order_id"),
        utf8("algo_id"),
        utf8("client_account_id"),
        utf8("client_pan"),
        utf8("side"),
        utf8("product"),
        utf8("order_type"),
        utf8("quantity"),
        utf8("price"),
        utf8("event"),
        utf8("system_meta"),
        utf8("raw_payload"),
        utf8("prev_hash"),
        utf8("hash"),
    ])
}
