//! `honba-audit-export` CLI (plan §3.6, M5): verify the hash-chained
//! journal, emit the SEBI Annexure CSV (stdout), optionally replay JSONL,
//! and optionally compact a Parquet snapshot.

use std::path::PathBuf;

use clap::{Parser, ValueEnum};

use honba_te_audit::{AuditJournal, SystemMeta, to_annexure_csv};

/// Export media.
#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
enum Format {
    /// SEBI Annexure CSV (fixed column set).
    Csv,
    /// Raw JSONL replay of the sealed records.
    Jsonl,
}

#[derive(Debug, Parser)]
#[command(
    name = "honba-audit-export",
    version,
    about = "Verify the honba audit journal and export SEBI compliance artifacts"
)]
struct Cli {
    /// Directory holding the fsynced `wal-*.jsonl` audit segments.
    journal_dir: PathBuf,
    /// Also compact the journal to an Apache Parquet snapshot at OUT.
    #[arg(long, value_name = "OUT")]
    parquet: Option<PathBuf>,
    /// Export format (default: SEBI Annexure CSV).
    #[arg(long, value_enum, default_value_t = Format::Csv)]
    format: Format,
}

fn main() {
    let cli = Cli::parse();
    if let Err(error) = run(&cli) {
        eprintln!("honba-audit-export: {error}");
        std::process::exit(1);
    }
}

fn run(cli: &Cli) -> Result<(), String> {
    let journal = AuditJournal::open(&cli.journal_dir, SystemMeta::default())
        .map_err(|error| error.to_string())?;
    let records = journal.records().map_err(|error| error.to_string())?;
    journal
        .verify()
        .map_err(|error| format!("journal failed verification: {error}"))?;
    println!("verified: journal chain intact ({} records)", records.len());

    if let Some(target) = &cli.parquet {
        let written = journal
            .compact_parquet(Some(target))
            .map_err(|error| error.to_string())?;
        println!("compacted: {}", written.display());
    }

    match cli.format {
        Format::Csv => print!("{}", to_annexure_csv(&records)),
        Format::Jsonl => {
            for record in &records {
                let line = serde_json::to_string(record).map_err(|error| error.to_string())?;
                println!("{line}");
            }
        }
    }
    Ok(())
}
