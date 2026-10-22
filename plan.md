# Master Plan: 本場・手 honba-te

> **The hand of the real market.**  
> Institutional-grade broker execution engine, multi-broker routing, automated session authentication, and SEBI regulatory audit system for Indian equities and derivatives (NSE/BSE/NFO/MCX). Bridges the deterministic domain heart (`honba-kokoro`) and synchronous risk gate (`honba-mon`) to live broker gateways (Zerodha Kite Connect v3, Upstox v2, Angel One SmartAPI 2.0, Fyers v3). Provides headless TOTP morning authentication, CDSL eDIS authorization, token-bucket rate limiting, real-time WebSocket/postback order streams, SEBI scrip freeze-quantity slicing, smart order routing (SOR), and an immutable append-only regulatory audit journal.

---

## 0. Executive Summary & Architectural Audit

### 0.1 Current State vs Target Architecture

`honba-te` was initially scaffolded with minimal REST adapters and a basic OTR counter. However, an architectural audit against Indian exchange microstructure and SEBI regulatory mandates reveals critical operational gaps, missing streaming interfaces, absent rate limiters, and complete lack of persistence:

| Area | Current State in `honba-te` | Target State (Master Architecture) | Severity |
| :--- | :--- | :--- | :--- |
| **Domain Types & Precision** | References deprecated `honba-kokoro-types`; Upstox/Angel adapters convert `rust_decimal::Decimal` into `f64` during payload serialization. | **100% unified on `honba-core`**: Strict `rust_decimal::Decimal` precision for prices and quantities across all wire payloads. Purge all references to `honba-kokoro-types`. Zero floating-point conversions. | **P0 (Critical)** |
| **Execution Event Streaming** | **Polling only**: Order updates require polling `get_order_status()`, which exhausts broker rate limits and introduces 1–2 second latency. | **Real-Time Streaming Feeds**: Dedicated `ExecutionStream` trait supporting Kite Connect Postback Webhooks (Axum HMAC SHA-256), Kite Ticker order updates, Upstox v2 WebSocket Order Feed, and Angel One SmartStream. | **P0 (Critical)** |
| **Broker Rate Limiting** | **None**: Orders are dispatched immediately via `reqwest`. Multiple concurrent strategies easily trip broker HTTP 429 (Too Many Requests) bans. | **Token-Bucket Rate Limiter**: Per-broker adaptive token bucket matching exact exchange limits (Zerodha 3 req/sec orders, Upstox 25 req/sec, Angel One 10 req/sec) with queue prioritization (cancels before new orders). | **P0 (Critical)** |
| **Session Lifecycle & Auth** | Manual token entry; requires daily morning browser login to retrieve access tokens. No automated TOTP or CDSL eDIS. | **Headless Auth (`honba-te-auth`)**: RFC 6238 TOTP automated login, OAuth2 refresh token rotation, secure keyring credential storage, and CDSL eDIS TPIN delivery authorization pipeline. | **P0 (Critical)** |
| **SEBI Freeze Limit Slicing** | Submits parent quantities as a single order; large orders breach NSE/BSE freeze limits (e.g., Nifty 1800, BankNifty 900) and get rejected. | **Execution Slicer (`honba-te-slicer`)**: Transparent auto-slicing for exchange freeze limits, native/synthetic Iceberg orders, and TWAP/VWAP execution algorithms with randomized jitter. | **P1 (High)** |
| **Smart Order Router (SOR)** | `SmartOrderRouter` is a basic placeholder selecting a primary or preferred broker with an in-memory OTR check. | **Dynamic SOR v2 (`honba-te-router`)**: Cost-optimized routing (delivery vs intraday/F&O), real-time broker health/latency circuit breakers, automated failover, and multi-account OTR balancing. | **P1 (High)** |
| **Regulatory Audit & Logging** | **No persistence**: Orders and responses are discarded from memory once returned; no audit trail. | **Immutable Audit Journal (`honba-te-audit`)**: Append-only SQLite WAL / Parquet journal storing UTC microsecond timestamps, SEBI algo ID, client ID, IP/MAC, raw broker wire payloads, and SHA-256 tamper-evident hash chains. | **P0 (Critical)** |
| **Indian Product Mapping** | Adapters hardcode product types (`"CNC"` in Zerodha, `"D"` in Upstox, `"DELIVERY"` in Angel One). | **Typed Product Taxonomy**: Canonical `ProductType` enum (`CncDelivery`, `MisIntraday`, `NrmlDerivative`, `MtfMargin`) mapped cleanly to broker-specific wire tokens. | **P1 (High)** |

---

## 1. Architectural Principles & System Integration

```text
               ┌────────────────────────────────────────────────────────────┐
               │              honba-kokoro (OMS / Heart)                    │
               │   Idempotent State Machine & Client Order ID Ledger        │
               └────────────────────────────┬───────────────────────────────┘
                                            │ Canonical OrderRequest (with algo_id)
                                            ▼
               ┌────────────────────────────────────────────────────────────┐
               │               honba-mon (Risk Gate / Door)                 │
               │   SEBI Algo ID, Scrip Price Bands, OTR Circuit Breakers    │
               └────────────────────────────┬───────────────────────────────┘
                                            │ Pre-Trade Approved Order
                                            ▼
┌───────────────────────────────────────────────────────────────────────────────────────────┐
│ 本場・手 honba-te Subsystem (Execution & Broker Gateway)                                    │
│                                                                                           │
│  ┌─────────────────────────┐      ┌─────────────────────────┐      ┌───────────────────┐  │
│  │     honba-te-slicer     │      │     honba-te-router     │      │   honba-te-core   │  │
│  │ Freeze Limit Slicing,   │─────▶│  Smart Order Router     │─────▶│ Token-Bucket Rate │  │
│  │ Iceberg & TWAP Engine   │      │ Cost, Health, OTR Split │      │  Limiter Queue    │  │
│  └─────────────────────────┘      └─────────────────────────┘      └─────────┬─────────┘  │
│                                                                              │            │
│  ┌───────────────────────────────────────────────────────────────────────────┼──────────┐ │
│  │ Multi-Broker Execution Adapters                                           │          │ │
│  │  ┌────────────────────┐   ┌────────────────────┐   ┌───────────────────┐  │          │ │
│  │  │  honba-te-zerodha  │   │   honba-te-upstox  │   │ honba-te-angelone │  │          │ │
│  │  │ Kite Connect v3    │   │ Upstox v2 REST     │   │ SmartAPI 2.0 REST │  │          │ │
│  │  │ Postback / Ticker  │   │ WebSocket Feed     │   │ SmartStream Feed  │  │          │ │
│  │  └─────────┬──────────┘   └─────────┬──────────┘   └─────────┬─────────┘  │          │ │
│  └────────────┼────────────────────────┼────────────────────────┼────────────┘          │ │
│               │                        │                        │                       │ │
│               ▼                        ▼                        ▼                       │ │
│  ┌───────────────────────────────────────────────────────────────────────────────────┐  │ │
│  │                           honba-te-audit (Journal)                                │  │ │
│  │ Append-only SQLite / Parquet: UTC µs, Algo ID, Raw Req/Resp, SHA-256 Hash Chains  │◀─┘ │
│  └───────────────────────────────────────────────────────────────────────────────────┘    │
└───────────────────────────────────────────┬───────────────────────────────────────────────┘
                                            │ HTTP / WebSocket Wire Protocols
                                            ▼
                  ┌───────────────────────────────────────────────────┐
                  │              Indian Broker Gateways               │
                  │   Zerodha (Kite)  |  Upstox v2  |  Angel SmartAPI │
                  └───────────────────────────────────────────────────┘
```

### Key Tenets
1. **Observation-Execution Decoupling**: Market data ingestion runs strictly in `honba-me`, while physical execution is orchestrated exclusively in `honba-te`. A strategy observes quotes on Zerodha or NSE Bhavcopy and dispatches orders to Upstox or Angel One with zero cross-crate leakage.
2. **Mandatory Synchronous Pre-Trade Risk Gate**: No order, modification, or cancellation ever reaches a broker network socket without prior synchronous validation and authorization by `honba-mon::RiskEngine` (verifying active SEBI Algo ID, scrip price bands, OTR thresholds, and kill-switches).
3. **Headless Session Lifecycle & Zero-Touch Morning Login**: Fully automated daily authentication at 08:30 IST via RFC 6238 TOTP computation, automated OAuth code exchange, secure credential caching, and automated CDSL eDIS TPIN delivery authorizations.
4. **Exchange Microstructure & Freeze Quantity Protection**: Built-in awareness of Indian market rules: mandatory exchange scrip freeze limits (automatic slicing of parent orders into permissible lot sizes), scrip tick sizes (₹0.05 / ₹0.01), and product type mapping (`CNC`, `MIS`, `NRML`).
5. **Token-Bucket Throttling & Priority Schedulers**: Every broker adapter is shielded by an adaptive token-bucket rate limiter that strictly adheres to broker API limits. Urgent safety actions (order cancellations, kill-switch market exits) jump the queue ahead of new passive limit orders.
6. **Regulatory Audit Trail & Non-Repudiation**: Append-only journaling of every order request, modification, cancellation, execution postback, and raw HTTP payload with microsecond UTC timestamps, algo ID, client ID, system IP, and cryptographic SHA-256 chaining conforming to SEBI inspection standards.

---

## 2. Crate Topology & Dependency Rules

The restructured `honba-te` workspace organizes into 8 cohesive, specialized crates:

```text
honba-te/
├── Cargo.toml
├── README.md
├── plan.md
└── crates/
    ├── honba-te-core/        # Canonical BrokerExecutor trait v2, ExecutionStream, RateLimiter, ProductType
    ├── honba-te-auth/        # Headless TOTP (RFC 6238), OAuth2 session refresh, CDSL eDIS authorization
    ├── honba-te-zerodha/     # Zerodha Kite Connect v3 REST client & Kite Postback webhook / Ticker stream
    ├── honba-te-upstox/      # Upstox v2 REST client (exact Decimal) & WebSocket Order Feed
    ├── honba-te-angelone/    # Angel One SmartAPI 2.0 REST client & SmartStream Order Feed
    ├── honba-te-slicer/      # SEBI scrip freeze limit slicing, Iceberg & TWAP/VWAP execution algorithms
    ├── honba-te-router/      # Smart Order Router (SOR v2): health circuit breaker, cost routing, OTR balancing
    └── honba-te-audit/       # Append-only regulatory journal (SQLite WAL / Parquet) & SEBI Annexure exporter
```

### Layering & Dependency Hierarchy
```text
Layer 3: [honba-te-router] ──▶ depends on [honba-te-slicer], [honba-te-core], [honba-te-audit]
             │
Layer 2: [honba-te-slicer], [honba-te-zerodha, upstox, angelone] ──▶ depend on [honba-te-core], [honba-te-auth]
             │
Layer 1: [honba-te-core], [honba-te-auth], [honba-te-audit]
             │
Layer 0: [honba-core], [honba-mon] ──▶ Canonical domain types, exact Decimal math, pre-trade risk engine
```

### Deprecation & Pruning Directive
- ❌ **`honba-kokoro-types`**: Immediately remove from workspace `Cargo.toml`. All domain types (`Price`, `Quantity`, `Money`, `OrderRequest`, `OrderStatus`) derive strictly from canonical `honba-core`.
- ❌ **Lossy Floating-Point Serialization**: Completely excise `.parse::<f64>()` in `honba-te-upstox` and `honba-te-angelone`. All wire serializers must format `rust_decimal::Decimal` with explicit scale or string representations.
- ❌ **Polling-Only Order Tracking**: Deprecate relying solely on `get_order_status()` loops; order lifecycles must be driven primarily by real-time push streams (`ExecutionStream`).

---

## 3. Deep-Dive Specification & Core Modules

### 3.1 `honba-te-core`: BrokerExecutor Trait v2, Streaming Feeds & Rate Limiter

- **`BrokerExecutor` Trait v2 Evolution**:
  ```rust
  #[async_trait]
  pub trait BrokerExecutor: Send + Sync {
      /// Broker identifier (e.g. "zerodha", "upstox", "angelone").
      fn name(&self) -> &str;

      /// Check current connection and session health.
      async fn health_check(&self) -> Result<BrokerHealth, BrokerError>;

      /// Place a new order with token-bucket rate limiting and priority.
      async fn place_order(&self, req: OrderRequest) -> Result<OrderAck, BrokerError>;

      /// Cancel an active order by broker order ID.
      async fn cancel_order(&self, order_id: &str) -> Result<(), BrokerError>;

      /// Modify price or quantity of an open order.
      async fn modify_order(
          &self,
          order_id: &str,
          new_price: Option<Price>,
          new_quantity: Option<Quantity>,
      ) -> Result<OrderAck, BrokerError>;

      /// Fetch full active order book snapshot from broker.
      async fn fetch_order_book(&self) -> Result<Vec<OrderSnapshot>, BrokerError>;

      /// Fetch current Demat and intraday positions.
      async fn fetch_positions(&self) -> Result<Vec<BrokerPosition>, BrokerError>;

      /// Fetch real-time available cash and margin balances.
      async fn fetch_margins(&self) -> Result<BrokerMargins, BrokerError>;
  }
  ```

- **`ExecutionStream` Push Trait**:
  ```rust
  /// Asynchronous real-time stream of order updates and trade fills pushed by broker.
  #[async_trait]
  pub trait ExecutionStream: Send + Sync {
      /// Subscribe to the broker's real-time order update channel.
      async fn next_event(&mut self) -> Result<BrokerExecutionEvent, BrokerError>;
  }

  #[derive(Debug, Clone)]
  pub enum BrokerExecutionEvent {
      OrderAck(OrderAck),
      Fill(FillReport),
      OrderCancelled { order_id: String, reason: Option<String> },
      OrderRejected { order_id: String, reason: String },
  }
  ```

- **Token-Bucket Rate Limiter with Queue Priority**:
  - Implements an adaptive token-bucket algorithm (`TokenBucketRateLimiter`).
  - Strict priority queues:
    1. **Priority 0 (Immediate)**: Emergency Kill-Switch cancellations and manual panic halts.
    2. **Priority 1 (High)**: Strategy stop-loss trigger adjustments and limit order cancellations.
    3. **Priority 2 (Normal)**: Passive order placements and modifications.
    4. **Priority 3 (Low)**: Background position and margin polling.

- **Indian Product Taxonomy (`ProductType`)**:
  ```rust
  #[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
  pub enum ProductType {
      /// Cash & Carry / Demat Delivery (Zerodha: CNC, Upstox: D, Angel: DELIVERY)
      CncDelivery,
      /// Margin Intraday Square-off (Zerodha: MIS, Upstox: I, Angel: INTRADAY)
      MisIntraday,
      /// Normal F&O Overnight Margin (Zerodha: NRML, Upstox: M, Angel: CARRYFORWARD)
      NrmlDerivative,
      /// Margin Trading Facility (Zerodha: MTF, Upstox: MTF)
      MtfMargin,
  }
  ```

---

### 3.2 `honba-te-auth`: Headless TOTP Automation & CDSL eDIS Authorization

- **Headless Morning Authentication Workflow (08:30 IST)**:
  - Automates daily broker token generation without manual browser redirects:
    1. Reads encrypted credentials from local OS keyring or environment variables (`API_KEY`, `API_SECRET`, `USER_ID`, `PASSWORD`, `TOTP_SEED`).
    2. Calculates current 6-digit TOTP code using RFC 6238 HMAC-SHA1.
    3. Authenticates with broker login endpoint (Zerodha headless session flow, Upstox OAuth2 authorize endpoint, Angel One `generateSession`).
    4. Obtains `access_token` and `refresh_token`, caching securely to disk with encrypted expiration metadata.
- **CDSL eDIS Demat Delivery Authorization Pipeline**:
  - For equity delivery sell orders (`CncDelivery`), SEBI mandates CDSL eDIS authentication.
  - Implements automated TPIN authorization:
    - Generates CDSL authorization form requests.
    - Manages morning batch pre-authorization for portfolio holdings so intraday sell orders do not block during high-speed market hours.

---

### 3.3 Production Multi-Broker Execution Adapters

#### A. `honba-te-zerodha` (Kite Connect v3 REST & Postback Stream)
- **Kite Connect REST API v3**:
  - Full implementation of `/orders/regular`, `/orders/iceberg`, `/orders/amo`, and `/portfolio/positions`.
  - Rate limited to 3 order requests/sec and 10 general requests/sec.
  - Native Kite iceberg order support (`iceberg_legs`, `iceberg_quantity`).
- **Postback Webhook Server & Kite Ticker Stream**:
  - Embedded lightweight Axum HTTP endpoint receiving Kite Connect Postback Webhooks.
  - HMAC SHA-256 signature validation against Kite API secret to prevent spoofed order updates.
  - Automatic fallback to Kite Ticker WebSocket binary order update packets (`order_update`).

#### B. `honba-te-upstox` (Upstox v2 REST & WebSocket Feed)
- **Upstox v2 API**:
  - Full JSON payload construction using stringified `rust_decimal::Decimal` (eliminating `f64` precision loss).
  - Accurate mapping of Upstox instrument keys (`NSE_EQ|INE...`, `NSE_FO|...`).
- **Upstox v2 WebSocket Order Feed**:
  - Connects to Upstox v2 Portfolio Stream (`wss://api.upstox.com/v2/feed/portfolio-stream-feed`).
  - Decodes real-time JSON order execution events into canonical `OrderAck` and `FillReport`.

#### C. `honba-te-angelone` (Angel One SmartAPI 2.0 REST & SmartStream)
- **SmartAPI 2.0**:
  - Complete header structure (`X-PrivateKey`, `X-SourceID`, `X-ClientLocalIP`, `X-ClientPublicIP`, `X-MACAddress`, JWT Bearer).
  - Explicit error code mapping (`AB1004` Invalid Token, `AB2001` Insufficient Funds, `AG8001` Scrip Inactive).
- **Angel One SmartStream Order Feed**:
  - Real-time WebSocket connection to SmartStream receiving order status transitions and trade executions.

---

### 3.4 `honba-te-slicer`: SEBI Freeze Limits & Execution Algorithms

- **SEBI Exchange Scrip Freeze Quantity Engine**:
  - Indian stock exchanges enforce strict scrip-level and contract-level freeze quantities:
    - **NIFTY Index Futures/Options**: 1,800 quantity (36 lots) max per order.
    - **BANKNIFTY**: 900 quantity (30 lots) max per order.
    - **FINNIFTY / MIDCPNIFTY**: 1,800 quantity max per order.
    - **Single Stock Equities / Options**: Varies by scrip liquidity (queried dynamically from Bhavcopy / Instrument Master).
  - `FreezeLimitSlicer`: Automatically inspects order quantity against instrument freeze limits. If $Q > Q_{\text{freeze}}$, transparently decomposes the parent `OrderRequest` into $N = \lceil Q / Q_{\text{freeze}} \rceil$ child orders with synchronized client IDs.
- **Execution Algorithms**:
  - **Iceberg Orders**: Divides large parent order into visible display slices ($Q_{\text{visible}}$) and hidden reserves, automatically submitting subsequent slices upon fill confirmation.
  - **Time-Weighted Average Price (TWAP)**: Evenly distributes child orders across a specified time horizon with randomized sub-second Gaussian noise to mask algorithm footprint.
  - **Volume Participation / VWAP**: Slices orders dynamically pegged to real-time market volume from `honba-me` depth feeds.

---

### 3.5 `honba-te-router`: Smart Order Router (SOR v2)

- **Cost-Optimized Routing Policy**:
  - Evaluates statutory Indian transaction charges and broker brokerage models:
    - Zero-brokerage accounts for equity delivery (`CncDelivery`).
    - Flat-fee or low-latency direct-access brokers for high-frequency intraday futures/options.
- **Dynamic Broker Health & Latency Circuit Breaker**:
  - Tracks rolling round-trip latency (RTT), HTTP 5xx errors, and WebSocket heartbeats for each registered broker.
  - If a broker's latency spikes above 500ms or 3 consecutive HTTP 5xx errors occur, the router trips a local circuit breaker and transparently routes new orders to healthy secondary brokers.
- **SEBI OTR Balancing across Multi-Account Deployments**:
  - Monitors real-time Order-to-Trade Ratio (OTR) across registered broker accounts.
  - Dynamically routes order modifications and new orders to accounts with ample OTR buffer, avoiding SEBI regulatory penalties on any single account.

---

### 3.6 `honba-te-audit`: High-Throughput Regulatory Journal & SEBI Exporter

- **SEBI Mandatory Compliance Fields**:
  - Every order lifecycle event is recorded with:
    1. Microsecond UTC timestamp (`DateTime<Utc>`).
    2. Client Order ID (`Uuid`) and Exchange Order ID.
    3. Registered SEBI Algo ID (`algo_id`).
    4. Client Account ID and PAN / Demat ID.
    5. Order parameters: side, quantity, price, order type, product type.
    6. System metadata: Public IP, Local IP, MAC Address, Port.
    7. Full raw HTTP/WebSocket request and response wire payloads.
- **Tamper-Evident SHA-256 Hash Chaining**:
  - Each log entry contains a cryptographic hash of its payload plus the hash of the preceding entry:
    $$H_n = \text{SHA-256}(H_{n-1} \parallel \text{Timestamp} \parallel \text{ClientOrdId} \parallel \text{Payload})$$
  - Guarantees non-repudiation and detects unauthorized tampering during regulatory inspections.
- **SEBI Regulatory Exporters**:
  - CLI and API exporters generating standardized SEBI Annexure CSV reports for algorithmic trading compliance audits.

---

## 4. Phase-by-Phase Implementation Roadmap

```text
2026-Q4 / 2027-Q1
W1   W2   W3   W4   W5   W6   W7   W8   W9   W10  W11  W12  W13
[===M0===]                                                      Phase 1: Core Trait & Rate Limiter
          [===M1===]                                            Phase 2: Headless TOTP & CDSL Auth
                    [======M2======]                            Phase 3: Multi-Broker Streaming Adapters
                                   [===M3===]                   Phase 4: Freeze Slicer & Algos
                                             [===M4===]         Phase 5: Resilient Smart Order Router
                                                       [===M5===] Phase 6: Regulatory Audit & SEBI Exporter
```

### Phase 1: Canonical Type Hardening, Token-Bucket Rate Limiter & Core Trait Evolution (M0) — Week 1–2
- [x] **Workspace Cleanup**: Excise all references to `honba-kokoro-types` in `Cargo.toml`. Standardize 100% on `honba-core`.
- [x] **Exact Decimal Precision**: Audit all order and fill payload structs. Replace all `.parse::<f64>()` with exact `rust_decimal::Decimal` formatting.
- [x] **Product Taxonomy**: Implement canonical `ProductType` (`CncDelivery`, `MisIntraday`, `NrmlDerivative`, `MtfMargin`).
- [x] **`BrokerExecutor` Trait v2**: Expand trait to include `fetch_positions`, `fetch_margins`, `fetch_order_book`, and `health_check`.
- [x] **`ExecutionStream` Trait**: Define asynchronous streaming interface for real-time order status updates and fills.
- [x] **Token-Bucket Rate Limiter**: Build prioritized `TokenBucketRateLimiter` with queue jumping for emergency cancel commands.

### Phase 2: Headless TOTP Authentication & CDSL eDIS Authorization (M1) — Week 3–4
- [x] **`honba-te-auth` Crate**: Scaffold new authentication crate with encrypted local keyring storage (`keyring-rs`).
- [x] **RFC 6238 TOTP Engine**: Implement zero-dependency TOTP calculation from Base32 secrets.
- [x] **Headless Session Login**: Build automated login state machines for Zerodha Kite, Upstox v2, and Angel One SmartAPI.
- [x] **Token Caching & Auto-Refresh**: Background task checking token expiry and triggering refresh before market open (08:30 IST).
- [x] **CDSL eDIS Pre-Authorization**: Implement CDSL TPIN batch pre-authorization workflow for delivery holdings.

### Phase 3: Production Multi-Broker REST Adapters & WebSocket Postback Listeners (M2) — Week 5–7
- [x] **Zerodha Kite Connect v3 REST**: Full implementation of order placement, modifications, cancellations, positions, and margins.
- [x] **Zerodha Axum Postback Listener**: Embedded HTTP webhook receiver with HMAC SHA-256 signature verification.
- [x] **Zerodha Kite Ticker Stream**: WebSocket order update parser extracting binary `order_update` payloads.
- [x] **Upstox v2 REST Client**: Full REST API implementation with exact Decimal serialization.
- [x] **Upstox v2 WebSocket Feed**: Portfolio and order execution stream decoding.
- [x] **Angel One SmartAPI 2.0 Client**: Full REST implementation with complete header metadata and error code taxonomy.
- [x] **Angel One SmartStream**: WebSocket order status listener.

### Phase 4: SEBI Scrip Freeze Limits & Execution Slicing Algos (M3) — Week 8–9
- [x] **`honba-te-slicer` Crate**: Scaffold execution slicing module.
- [x] **Scrip Freeze Limit Catalog**: Load instrument freeze quantities from NSE/BSE master data (e.g., NIFTY 1800, BANKNIFTY 900).
- [x] **Automatic Parent Order Slicing**: Transparently split orders exceeding freeze limits into synchronized child orders.
- [x] **Native & Synthetic Iceberg Engine**: Support broker-native icebergs (Zerodha) and synthetic slicing for Upstox/Angel.
- [x] **TWAP Execution Algorithm**: Time-sliced execution with randomized Gaussian intervals and limit-chase logic.

### Phase 5: Resilient Smart Order Router (SOR) & Dynamic Broker Failover (M4) — Week 10–11
- [x] **Dynamic SOR v2**: Refactor `honba-te-router` to support multi-broker registration and routing strategies.
- [x] **Broker Health Circuit Breakers**: Track rolling RTT latency, HTTP 5xx errors, and automatic trip/reset logic.
- [x] **Cost-Optimized Routing**: Policy routing delivery orders to zero-brokerage brokers and derivatives to flat-fee brokers.
- [x] **Multi-Account OTR Balancing**: Allocate order placements and modifications across broker accounts to prevent SEBI OTR threshold breaches.
- [x] **Automated Failover Engine**: If primary broker rejects with transport timeout or network error, seamlessly re-route to secondary broker.

### Phase 6: High-Throughput Audit Journal & SEBI Regulatory Exporter (M5) — Week 12–13
- [ ] **`honba-te-audit` Crate**: Scaffold regulatory journal module.
- [ ] **High-Performance Journaling**: Implement append-only SQLite WAL and Parquet event storage using `sqlx`.
- [ ] **Cryptographic Hash Chain**: Implement tamper-evident SHA-256 hashing for all recorded order actions.
- [ ] **Compliance Field Enrichment**: Automatically record system IP, MAC address, microsecond UTC timestamps, and raw wire payloads.
- [ ] **SEBI Regulatory Exporter**: Implement CLI tool generating standard SEBI Annexure CSV files for regulatory compliance audits.

---

## 5. Verification, Conformance & Benchmarks

### 5.1 Conformance & Integration Test Matrix
- **WireMock REST Testbeds**: Every broker adapter (`honba-te-zerodha`, `upstox`, `angelone`) must pass comprehensive integration tests against local mock servers simulating 200 OK, 400 Bad Request, 429 Too Many Requests, and 503 Service Unavailable scenarios.
- **WebSocket Fuzzing & Reconnect Testing**: Simulate abrupt network drops, corrupted payloads, and out-of-order packet arrival on postback and order update WebSocket streams.
- **SEBI Freeze Limit Verification**: Property-based tests verifying that no child order produced by `honba-te-slicer` exceeds exchange freeze quantities across 10,000 randomized order sizes.
- **Rate-Limiter Soak Testing**: Stress test `TokenBucketRateLimiter` with 1,000 concurrent order placement tasks; verify zero HTTP 429 errors and verify that priority kill-switch commands bypass standard limit queues in under 1ms.

### 5.2 Performance Benchmarks
- **Router Overhead**: Microsecond benchmark verifying Smart Order Router internal decision latency:
  $$\tau_{\text{SOR}} < 100\,\mu\text{s}$$
- **Pre-Trade Risk Gate Overhead**: Combined `honba-mon` check + rate limiter token acquisition:
  $$\tau_{\text{RiskGate}} < 150\,\mu\text{s}$$
- **Audit Logging Throughput**: Append-only SQLite WAL write throughput:
  $$\text{Throughput}_{\text{Audit}} \ge 25,000\,\text{records/second}$$
