//! M5 journal tests: fsynced appends, restart continuity, tamper evidence,
//! segment rotation, and Parquet compaction read-back.

use std::fs;
use std::path::PathBuf;

use arrow::array::StringArray;
use chrono::{DateTime, Duration as ChronoDuration, TimeZone, Utc};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use uuid::Uuid;

use honba_te_audit::{AuditEvent, AuditJournal, AuditSpec, JournalError, SystemMeta, genesis};

fn tmp_root() -> PathBuf {
    std::env::temp_dir().join(format!("honba-audit-test-{}", Uuid::new_v4().simple()))
}

fn spec(client_order_id: Uuid, payload: &str, exchange: Option<&str>) -> AuditSpec {
    AuditSpec {
        client_order_id,
        exchange_order_id: exchange.map(str::to_owned),
        algo_id: "ALGO-SEBI-0001".into(),
        client_account_id: "ACCT-1001".into(),
        client_pan: "AAAPA1234F".into(),
        side: "BUY".into(),
        product: "MIS".into(),
        order_type: "LIMIT".into(),
        quantity: "10".into(),
        price: "500.50".into(),
        event: AuditEvent::Placed,
        raw_payload: payload.into(),
    }
}

fn at(micros: i64) -> DateTime<Utc> {
    Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap() + ChronoDuration::microseconds(micros)
}

fn meta() -> SystemMeta {
    SystemMeta::new(
        Some("203.0.113.9".into()),
        Some("10.0.0.9".into()),
        Some("aa:bb:cc:dd:ee:ff".into()),
        Some(9090),
        Some("honba-host".into()),
    )
}

#[test]
fn append_seals_chain_and_reopen_continues() {
    let dir = tmp_root();
    let journal = AuditJournal::open(&dir, meta()).unwrap();
    assert_eq!(journal.seq(), 0, "fresh journal starts at genesis");

    let first_id = Uuid::new_v4();
    let first = journal
        .append(at(1_000), spec(first_id, "place-req-body", Some("EXO-100")))
        .unwrap();
    assert_eq!(first.seq, 1);
    assert_eq!(first.prev_hash, genesis(), "entry 1 anchors at genesis");
    assert_eq!(first.hash.len(), 64);
    assert_eq!(first.system.local_ip(), Some("10.0.0.9"));
    assert_eq!(first.system.port(), Some(9090));

    let second = journal
        .append(at(2_000), spec(Uuid::new_v4(), "modify-req-body", None))
        .unwrap();
    assert_eq!(second.seq, 2);
    assert_eq!(
        second.prev_hash, first.hash,
        "chain links to the sealed tip"
    );
    assert_eq!(journal.records().unwrap().len(), 2);

    // Reopen replays segments and the sequence/chain head persists.
    let reopened = AuditJournal::open(&dir, meta()).unwrap();
    assert_eq!(reopened.seq(), 2);
    let third = reopened
        .append(at(3_000), spec(Uuid::new_v4(), "cancel-req-body", None))
        .unwrap();
    assert_eq!(third.seq, 3, "reopened journal continues numbering");
    assert_eq!(
        third.prev_hash, second.hash,
        "reopened journal continues the chain"
    );
    reopened.verify().expect("reopened journal tamper-evident");
}

#[test]
fn tampered_segment_fails_verification() {
    let dir = tmp_root();
    let journal = AuditJournal::open(&dir, meta()).unwrap();
    journal
        .append(at(1), spec(Uuid::new_v4(), "place-req-body", None))
        .unwrap();
    journal
        .append(at(2), spec(Uuid::new_v4(), "second-body", None))
        .unwrap();

    // Rewrite the stored payload inside the segment: the seal breaks.
    let segments: Vec<PathBuf> = fs::read_dir(&dir)
        .unwrap()
        .flatten()
        .map(|entry| entry.path())
        .collect();
    assert!(!segments.is_empty(), "fixture must journal a segment");
    for segment in &segments {
        let text = fs::read_to_string(segment).unwrap();
        fs::write(segment, text.replace("place-req-body", "place-TAMPERED")).unwrap();
    }

    let forged = AuditJournal::open(&dir, meta()).unwrap();
    match forged.verify() {
        Err(JournalError::Tamper(message)) => {
            assert!(
                message.contains("sequence 1"),
                "first break reported: {message}"
            )
        }
        other => panic!("expected Tamper evidence, got {other:?}"),
    }
}

#[test]
fn segments_rotate_and_replay_in_order() {
    let dir = tmp_root();
    let journal = AuditJournal::open(&dir, meta())
        .unwrap()
        .with_segment_rotation(2);
    for tick in 0..5 {
        journal
            .append(at(tick as i64), spec(Uuid::new_v4(), "payload", None))
            .unwrap();
    }
    let segments: Vec<PathBuf> = fs::read_dir(&dir)
        .unwrap()
        .flatten()
        .filter(|entry| entry.file_name().to_string_lossy().starts_with("wal-"))
        .map(|entry| entry.path())
        .collect();
    assert_eq!(segments.len(), 3, "5 records at rotation 2 → 3 segments");
    assert_eq!(
        journal.records().unwrap().len(),
        5,
        "replay preserves order"
    );
    journal
        .verify()
        .expect("rotated journal verifies end-to-end");
}

#[test]
fn compaction_writes_a_parquet_snapshot() {
    let dir = tmp_root();
    let journal = AuditJournal::open(&dir, meta()).unwrap();
    let first_id = Uuid::new_v4();
    journal
        .append(at(10), spec(first_id, "place-req-body", Some("EXO-100")))
        .unwrap();
    journal
        .append(at(20), spec(Uuid::new_v4(), "modify-req-body", None))
        .unwrap();

    let snapshot = journal.compact_parquet(None).unwrap();
    assert_eq!(
        snapshot.file_name().unwrap().to_string_lossy(),
        "audit-0000000002.parquet",
        "default name is the head seq"
    );

    // Read the snapshot back through the plain arrow reader path.
    let file = fs::File::open(&snapshot).unwrap();
    let builder = ParquetRecordBatchReaderBuilder::try_new(file).unwrap();
    assert_eq!(builder.schema().fields().len(), 17, "flat §3.6 column set");
    let reader = builder.build().unwrap();
    let mut rows = 0;
    let mut first_client_id = String::new();
    for batch in reader {
        let batch = batch.unwrap();
        rows += batch.num_rows();
        let ids = batch
            .column(2)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        first_client_id = ids.value(0).to_owned();
    }
    assert_eq!(rows, 2);
    assert_eq!(first_client_id, first_id.to_string());

    // An explicit output path is honored.
    let custom = dir.join("custom-snapshot.parquet");
    let written = journal.compact_parquet(Some(&custom)).unwrap();
    assert_eq!(written, custom);
    assert!(custom.exists());
}

#[test]
fn compaction_refuses_an_empty_journal() {
    let dir = tmp_root();
    let journal = AuditJournal::open(&dir, meta()).unwrap();
    match journal.compact_parquet(None) {
        Err(JournalError::Parquet(message)) => {
            assert!(message.contains("nothing to compact"), "{message}")
        }
        other => panic!("expected Parquet(nothing to compact), got {other:?}"),
    }
}
