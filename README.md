# 本場・手 honba-te

> *The hand that executes.*

SEBI-compliant order placement for Indian equities. Every order carries a mandatory registered algo ID; the OTR router blocks runs that would breach per-scrip thresholds.

## Crates
- `honba-te-core` — Canonical `BrokerExecutor` trait v2, `ExecutionStream`, prioritized token-bucket rate limiter, `ProductType` taxonomy, exact-Decimal helpers
- `honba-te-auth` — Headless RFC 6238 TOTP login (Zerodha / Upstox / Angel One), credential stores (env / memory / OS keyring), 08:30 IST token refresh scheduler, CDSL eDIS TPIN batch pre-authorization
- `honba-te-{zerodha,upstox,angelone}` — Broker adapters (exact Decimal wire payloads; Upstox uses decimal strings, no `f64`)
- `honba-te-router` — Smart order router with OTR counters

## Testing
All crates keep tests in `tests/` parallel to `src/` (AGENTS.md mandate); run with `cargo test --workspace`. CI gates: `cargo clippy --all-targets -- -D warnings` and `cargo fmt --check`.

Work in progress per [`plan.md`](plan.md): Phases 1–2 (core hardening + headless auth) are complete; multi-broker streaming adapters (`honba-te-zerodha`/`upstox`/`angelone` real-time feeds), freeze-limit slicing, SOR v2, and the audit journal remain.

License: AGPL-3.0.
