# 本場・手 honba-te

> *The hand that executes.*

SEBI-compliant order placement for Indian equities. Every order carries a mandatory registered algo ID; the OTR router blocks runs that would breach per-scrip thresholds.

## Crates
- `honba-te-core` — Canonical `BrokerExecutor` trait v2, `ExecutionStream`, prioritized token-bucket rate limiter, `ProductType` taxonomy, exact-Decimal helpers
- `honba-te-auth` — Headless RFC 6238 TOTP login (Zerodha / Upstox / Angel One), credential stores (env / memory / OS keyring), 08:30 IST token refresh scheduler, CDSL eDIS TPIN batch pre-authorization
- `honba-te-{zerodha,upstox,angelone}` — Production broker adapters: full REST (place / modify / cancel / order book / positions / margins) plus order-stream listeners — Zerodha Axum postback webhook + Kite Ticker WebSocket, Upstox v2 feed WebSocket (302-redirect handshake), Angel One SmartStream (`tns.angelone.in`, four-header handshake + literal `ping` heartbeat). All wire numbers decode through exact Decimal-from-string paths (no `f64`); broker error codes map onto the canonical `BrokerError` taxonomy.
- `honba-te-router` — Resilient Smart Order Router v2: multi-broker registration with routing strategies (cost-optimized delivery→zero-brokerage / intraday→flat-fee, lowest-latency with optimistic discovery, pinned accounts), per-account health circuit breakers (rolling-RTT + 3-consecutive-failure trips, Closed→Open→HalfOpen probe recovery), SEBI OTR balancing across accounts (ceiling steering with cold-start arming), and transparent automated failover on transport/timeout faults (broker rejections are never re-routed — double-execution guard — and count as gateway-reached OTR actions). The legacy v1 `ExecRouter`/`SmartOrderRouter` (risk-engine-integrated flow) is preserved.
- `honba-te-slicer` — SEBI freeze-limit slicing engine: `FreezeLimitCatalog` (CSV/JSON master-data loaders + builtin index constants), `FreezeLimitSlicer` (parent → `⌈Q/Qfreeze⌉` synchronized children), native (Kite `iceberg` variety via te-core `IcebergOrderExecutor`) and synthetic iceberg runners, deterministic zero-float TWAP with bounded Gaussian jitter and limit-chase re-pricing

## Testing
All crates keep tests in `tests/` parallel to `src/` (AGENTS.md mandate); run with `cargo test --workspace`. CI gates: `cargo clippy --all-targets -- -D warnings` and `cargo fmt --check`. Broker adapters are contract-tested against loopback axum mocks simulating 200/400/429/5xx, non-JSON gateway bodies, `-1` decimal sentinels, IST-suffix timestamps, and complete WebSocket frame sequences (heartbeat skip, cumulative-fill deltas, rejection / cancellation). The slicer carries the plan §5.1 property suite: no child order exceeds exchange freeze quantities across 10,000 randomized order sizes.

Work in progress per [`plan.md`](plan.md): Phases 1–5 (core hardening, headless auth, multi-broker streaming adapters, freeze-limit slicing & execution algos, resilient SOR v2 & failover) are complete; the audit journal & SEBI exporter remain.

License: AGPL-3.0.
