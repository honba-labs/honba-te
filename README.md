# 本場・手 honba-te

> *The hand that executes.*

SEBI-compliant order placement for Indian equities. Every order carries a mandatory registered algo ID; the OTR router blocks runs that would breach per-scrip thresholds.

## Crates
- `honba-te-core` — Canonical `BrokerExecutor` trait v2, `ExecutionStream`, prioritized token-bucket rate limiter, `ProductType` taxonomy, exact-Decimal helpers
- `honba-te-auth` — Headless RFC 6238 TOTP login (Zerodha / Upstox / Angel One), credential stores (env / memory / OS keyring), 08:30 IST token refresh scheduler, CDSL eDIS TPIN batch pre-authorization
- `honba-te-{zerodha,upstox,angelone}` — Production broker adapters: full REST (place / modify / cancel / order book / positions / margins) plus order-stream listeners — Zerodha Axum postback webhook + Kite Ticker WebSocket, Upstox v2 feed WebSocket (302-redirect handshake), Angel One SmartStream (`tns.angelone.in`, four-header handshake + literal `ping` heartbeat). All wire numbers decode through exact Decimal-from-string paths (no `f64`); broker error codes map onto the canonical `BrokerError` taxonomy.
- `honba-te-router` — Resilient Smart Order Router v2: multi-broker registration with routing strategies (cost-optimized delivery→zero-brokerage / intraday→flat-fee, lowest-latency with optimistic discovery, pinned accounts), per-account health circuit breakers (rolling-RTT + 3-consecutive-failure trips, Closed→Open→HalfOpen probe recovery), SEBI OTR balancing across accounts (ceiling steering with cold-start arming), and transparent automated failover on transport/timeout faults (broker rejections are never re-routed — double-execution guard — and count as gateway-reached OTR actions). The legacy v1 `ExecRouter`/`SmartOrderRouter` (risk-engine-integrated flow) is preserved.
- `honba-te-slicer` — SEBI freeze-limit slicing engine: `FreezeLimitCatalog` (CSV/JSON master-data loaders + builtin index constants), `FreezeLimitSlicer` (parent → `⌈Q/Qfreeze⌉` synchronized children), native (Kite `iceberg` variety via te-core `IcebergOrderExecutor`) and synthetic iceberg runners, deterministic zero-float TWAP with bounded Gaussian jitter and limit-chase re-pricing
- `honba-te-audit` — Hash-chained regulatory journal: fsynced append-only JSONL segment WAL with SHA-256 chain `H_n = SHA-256(H_{n-1} | Timestamp | ClientOrdId | Payload)` (escape-framed, injective), all §3.6 mandatory compliance fields (µs UTC timestamps, client/exchange order IDs, SEBI algo ID, PAN/Demat, order parameters, system IP/MAC/port, raw wire payloads), Apache Parquet compaction (arrow 53), tamper-evidence verification, and the `honba-audit-export` CLI producing fixed-column SEBI Annexure CSV (RFC-4180) or JSONL replay.

  > **Deviation from plan §4 M5 (as written)**: the plan names SQLx SQLite WAL. sqlx/SQLite is unavailable in this workspace's offline dependency cache, so the WAL ships as fsynced JSONL segments — the same append-only, fsync-per-sealed-entry durability contract, human-inspectable at replay — while Parquet compaction is fully delivered.

## Testing
All crates keep tests in `tests/` parallel to `src/` (AGENTS.md mandate); run with `cargo test --workspace`. CI gates: `cargo clippy --all-targets -- -D warnings` and `cargo fmt --check`. Broker adapters are contract-tested against loopback axum mocks simulating 200/400/429/5xx, non-JSON gateway bodies, `-1` decimal sentinels, IST-suffix timestamps, and complete WebSocket frame sequences (heartbeat skip, cumulative-fill deltas, rejection / cancellation). The slicer carries the plan §5.1 property suite: no child order exceeds exchange freeze quantities across 10,000 randomized order sizes.

Work in progress per [`plan.md`](plan.md): **all six phases are complete** — Phase 1 (core traits & rate limiter), Phase 2 (headless auth), Phase 3 (multi-broker streaming adapters), Phase 4 (freeze-limit slicing & execution algos), Phase 5 (resilient SOR v2 & failover), Phase 6 (audit journal & SEBI exporter). Remaining pre-live work is integration/testing polish tracked in plan §5.

License: AGPL-3.0.
