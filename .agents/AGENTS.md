# `honba-te` Agent Guidelines & Architecture Invariants

**Subsystem**: Hand (Order Routing & Execution)
**Language**: Rust
**Primary Responsibility**: Exchange execution routing, rate limiting, freeze slicing, TOTP headless auth, and SQLite WAL audit journal.

---

## 1. Role & Architectural Hints
* `honba-te` handles live execution and broker interaction:
  1. **Token-Bucket Rate Limiter**: Per-broker API rate limiting (e.g. Zerodha 10 req/s, Upstox 20 req/s) with queueing and prioritization.
  2. **Freeze Slicer**: Automatically slices large orders exceeding exchange quantity freeze limits (e.g., slicing 5,000 NIFTY into 1,800 + 1,800 + 1,400).
  3. **Smart Order Router (SOR)**: Routes orders to best execution venues (NSE vs. BSE).
  4. **SQLite WAL Audit Journal**: Microsecond-stamped append-only log of every outbound request, inbound ack, and exchange fill for regulatory compliance.
  5. **Headless TOTP Auth**: Automated daily broker session renewal.
* **Idea Inheritance**:
  - **StockSharp**: Algorithmic order slicers (TWAP, VWAP, Iceberg) and multi-broker routing.
  - **NautilusTrader**: Low-latency execution gateway and audit trails.

---

## 2. Coding Guidelines (DDD & TDD)
* **Interface-First**: Broker gateways implement `ExecutionGateway` trait; slicers implement `OrderSlicer` trait.
* **Zero Floats Policy**: Strict decimal arithmetic via `rust_decimal::Decimal`.
* **TDD & Parallel Test Directory Rule (MANDATORY)**:
  - All tests must live in `tests/` parallel to `src/`.
  - **STRICTLY FORBIDDEN**: In-file `#[test]` inside `src/`.

---

## 3. Development & CI Workflow
* **Linting & Formatting**:
  ```bash
  cargo fmt --check
  cargo clippy --all-targets -- -D warnings
  ```
* **Testing**:
  ```bash
  cargo test --all
  ```
