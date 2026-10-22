//! Integration tests for `honba-te-core` (M0 Phase 1):
//! risk-gated execution, product taxonomy, exact decimal wire formatting,
//! execution stream channels, and the prioritized token-bucket rate limiter.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use rust_decimal::Decimal;
use tokio::sync::Mutex;

use honba_core::uuid::Uuid;
use honba_core::{Exchange, InstrumentId, OrderSide, OrderType, Price, Quantity};
use honba_mon::RiskEngine;
use honba_te_core::decimal::{format_fixed_exact, quantity_to_u32_exact};
use honba_te_core::{
    BrokerExecutionEvent, BrokerExecutor, BrokerRateCatalog, BrokerRateCatalog as Catalog,
    ChannelExecutionStream, ExecutionStream, MockBrokerExecutor, OrderAck, OrderRequest,
    OrderStatus, RateLimitedBroker, RateLimiterConfig, RequestPriority, RiskGatedExecutor,
    RiskRefusal, TokenBucketRateLimiter,
};

fn limit_req(symbol: &str, qty: u64, price: &str) -> OrderRequest {
    OrderRequest::new(
        InstrumentId::new(Exchange::Nse, symbol),
        OrderSide::Buy,
        OrderType::Limit(Price::from_rupees(price).unwrap()),
        Quantity::from_u64(qty),
        "algo-te-core-test",
    )
}

fn uuid_v4() -> Uuid {
    Uuid::new_v4()
}

// ---------------------------------------------------------------------------
// Risk-gated executor (moved from in-file unit tests)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn risk_gated_executor_blocks_rejected_orders() {
    let mock = MockBrokerExecutor::new(false);
    let risk_engine = Arc::new(Mutex::new(RiskEngine::default()));
    {
        let mut engine = risk_engine.lock().await;
        engine.register_algo("algo-te-core-test");
    }
    let gated = RiskGatedExecutor::new(mock, risk_engine.clone());

    let valid_req = limit_req("TCS", 10, "3000.00");

    // 1. Order passes
    let ack = gated.place_order(valid_req).await.unwrap();
    assert_eq!(ack.status, OrderStatus::New);

    // 2. Trip global halt
    {
        let mut engine = risk_engine.lock().await;
        engine.halt_global("Manual kill-switch", chrono::Utc::now());
    }

    // 3. New order immediately refused without dispatching to broker
    let err = gated
        .place_order(limit_req("TCS", 10, "3000.00"))
        .await
        .unwrap_err();
    match err {
        honba_te_core::BrokerError::RiskRefusal(inner) => {
            assert!(
                matches!(&*inner, RiskRefusal::GlobalHalted { .. }),
                "got {inner:?}"
            );
        }
        other => panic!("expected risk refusal, got {other:?}"),
    }
}

// ---------------------------------------------------------------------------
// Product taxonomy
// ---------------------------------------------------------------------------

#[test]
fn product_taxonomy_broker_tokens_match_plan() {
    use honba_te_core::ProductType;

    assert_eq!(ProductType::CncDelivery.zerodha(), "CNC");
    assert_eq!(ProductType::MisIntraday.zerodha(), "MIS");
    assert_eq!(ProductType::NrmlDerivative.zerodha(), "NRML");
    assert_eq!(ProductType::MtfMargin.zerodha(), "MTF");

    assert_eq!(ProductType::CncDelivery.upstox(), "D");
    assert_eq!(ProductType::MisIntraday.upstox(), "I");
    assert_eq!(ProductType::NrmlDerivative.upstox(), "M");
    assert_eq!(ProductType::MtfMargin.upstox(), "MTF");

    assert_eq!(ProductType::CncDelivery.angelone(), "DELIVERY");
    assert_eq!(ProductType::MisIntraday.angelone(), "INTRADAY");
    assert_eq!(ProductType::NrmlDerivative.angelone(), "CARRYFORWARD");
    assert_eq!(ProductType::MtfMargin.angelone(), "MTF");
}

#[test]
fn product_taxonomy_round_trip() {
    use honba_te_core::ProductType;
    for (token, expected) in [
        ("CNC", ProductType::CncDelivery),
        ("MIS", ProductType::MisIntraday),
        ("NRML", ProductType::NrmlDerivative),
        ("MTF", ProductType::MtfMargin),
    ] {
        assert_eq!(ProductType::from_zerodha(token), Some(expected));
    }
    for (token, expected) in [
        ("D", ProductType::CncDelivery),
        ("I", ProductType::MisIntraday),
        ("M", ProductType::NrmlDerivative),
        ("MTF", ProductType::MtfMargin),
    ] {
        assert_eq!(ProductType::from_upstox(token), Some(expected));
    }
    for (token, expected) in [
        ("DELIVERY", ProductType::CncDelivery),
        ("INTRADAY", ProductType::MisIntraday),
        ("CARRYFORWARD", ProductType::NrmlDerivative),
    ] {
        assert_eq!(ProductType::from_angelone(token), Some(expected));
    }
    assert_eq!(ProductType::from_zerodha("BOGUS"), None);
    // Broker-agnostic token lookup
    assert_eq!(
        ProductType::MisIntraday.broker_token("zerodha"),
        Some("MIS")
    );
    assert_eq!(ProductType::MisIntraday.broker_token("unknown"), None);
}

// ---------------------------------------------------------------------------
// Exact decimal wire formatting (zero-float mandate)
// ---------------------------------------------------------------------------

#[test]
fn format_fixed_exact_is_lossless() {
    let d = rust_decimal::Decimal::from_str_exact("2500.5").unwrap();
    assert_eq!(format_fixed_exact(d, 2).unwrap(), "2500.50");

    // Zero renders with the requested scale
    assert_eq!(format_fixed_exact(Decimal::ZERO, 2).unwrap(), "0.00");

    // Tick misaligned values must be rejected, never silently rounded
    let bad = rust_decimal::Decimal::from_str_exact("10.005").unwrap();
    assert!(format_fixed_exact(bad, 2).is_err());

    // More precision than requested but trailing zeros = still exact
    let d = rust_decimal::Decimal::from_str_exact("3800.000").unwrap();
    assert_eq!(format_fixed_exact(d, 2).unwrap(), "3800.00");
}

#[test]
fn quantity_to_u32_exact_rejects_fractional_and_overflow() {
    assert_eq!(
        quantity_to_u32_exact(Decimal::from(1800)).unwrap(),
        1_800u32
    );
    let frac = rust_decimal::Decimal::from_str_exact("10.5").unwrap();
    assert!(quantity_to_u32_exact(frac).is_err());
    let neg = -Decimal::from(5);
    assert!(quantity_to_u32_exact(neg).is_err());
    let big = Decimal::from(4_294_967_296u64); // u32::MAX + 1
    assert!(quantity_to_u32_exact(big).is_err());
}

// ---------------------------------------------------------------------------
// Execution stream channel transport
// ---------------------------------------------------------------------------

#[tokio::test]
async fn channel_execution_stream_round_trip() {
    use honba_core::FillReport;

    let (mut stream, sink) = ChannelExecutionStream::channel(8);
    let ack = OrderAck::new("ORD-1", uuid_v4(), OrderStatus::New, chrono::Utc::now());
    sink.send(BrokerExecutionEvent::OrderAck(ack.clone()))
        .await
        .unwrap();

    let fill = FillReport::new(
        "FILL-1",
        "ORD-1",
        ack.client_order_id,
        InstrumentId::new(Exchange::Nse, "TCS"),
        OrderSide::Buy,
        Price::from_rupees("3050.00").unwrap(),
        Quantity::from_u64(10),
        "algo-te-core-test",
        chrono::Utc::now(),
    );
    sink.try_send(BrokerExecutionEvent::Fill(fill.clone()))
        .unwrap();

    match stream.next_event().await.unwrap() {
        BrokerExecutionEvent::OrderAck(a) => assert_eq!(a.order_id, "ORD-1"),
        other => panic!("expected ack, got {other:?}"),
    }
    match stream.next_event().await.unwrap() {
        BrokerExecutionEvent::Fill(f) => assert_eq!(f.price.to_string(), "3050.00"),
        other => panic!("expected fill, got {other:?}"),
    }
}

#[tokio::test]
async fn stream_reports_transport_error_when_consumer_dropped() {
    let (stream, sink) = ChannelExecutionStream::channel(4);
    let ack = OrderAck::new("ORD-2", uuid_v4(), OrderStatus::New, chrono::Utc::now());
    sink.send(BrokerExecutionEvent::OrderAck(ack))
        .await
        .unwrap();
    drop(stream);
    let ack = OrderAck::new("ORD-3", uuid_v4(), OrderStatus::New, chrono::Utc::now());
    let err = sink
        .send(BrokerExecutionEvent::OrderAck(ack))
        .await
        .unwrap_err();
    assert!(err.to_string().contains("consumer dropped"));
}

// ---------------------------------------------------------------------------
// Token bucket rate limiter
// ---------------------------------------------------------------------------

fn cfg(capacity: u64, rate: u64, initial: u64) -> RateLimiterConfig {
    RateLimiterConfig {
        capacity_tokens: Decimal::from(capacity),
        refill_rate_per_sec: Decimal::from(rate),
        initial_tokens: Decimal::from(initial),
    }
}

#[test]
fn broker_rate_catalog_matches_plan_limits() {
    let z = Catalog::zerodha_orders();
    assert_eq!(z.refill_rate_per_sec, Decimal::from(3));
    assert_eq!(z.capacity_tokens, Decimal::from(3));
    assert_eq!(
        BrokerRateCatalog::zerodha_general().refill_rate_per_sec,
        Decimal::from(10)
    );
    assert_eq!(Catalog::upstox().refill_rate_per_sec, Decimal::from(25));
    assert_eq!(Catalog::angelone().refill_rate_per_sec, Decimal::from(10));
    assert_eq!(
        BrokerRateCatalog::for_broker("upstox").refill_rate_per_sec,
        Decimal::from(25)
    );
}

#[tokio::test]
async fn token_bucket_validates_config() {
    assert!(TokenBucketRateLimiter::new(cfg(0, 10, 10)).is_err());
    assert!(TokenBucketRateLimiter::new(cfg(10, 10, 11)).is_err());
    assert!(TokenBucketRateLimiter::new(cfg(10, 10, 10)).is_ok());
}

#[tokio::test]
async fn try_acquire_consumes_then_refills() {
    let limiter = TokenBucketRateLimiter::new(cfg(5, 100, 5)).unwrap();
    // Drain the bucket
    for _ in 0..5 {
        assert!(
            limiter
                .try_acquire(RequestPriority::Normal, Decimal::ONE)
                .await
                .unwrap()
        );
    }
    assert!(
        !limiter
            .try_acquire(RequestPriority::Normal, Decimal::ONE)
            .await
            .unwrap()
    );
    // Refill at 100/s → 100ms yields ~10 tokens
    tokio::time::sleep(Duration::from_millis(150)).await;
    assert!(
        limiter
            .try_acquire(RequestPriority::Normal, Decimal::ONE)
            .await
            .unwrap()
    );
}

#[tokio::test]
async fn immediate_priority_bypasses_queues_sub_millisecond() {
    // Setup: only 1 token available, capacity 3. Three lower-priority (P2)
    // waiters each needing 2 tokens park in the queue (1 < 2, strict bucket).
    // A fresh P0 kill-switch arrival needs only 1 token — the fast path must
    // serve it instantly, jumping the parked P2 queue.
    let limiter = Arc::new(TokenBucketRateLimiter::new(cfg(3, 3, 1)).unwrap());

    // Ensure the three P2 waiters are queued before P0 arrives.
    for _ in 0..3 {
        let l = limiter.clone();
        tokio::spawn(async move {
            l.acquire_for(
                RequestPriority::Normal,
                Decimal::from(2),
                Duration::from_secs(5),
            )
            .await
        });
    }

    // Wait until all three are parked (tokens can only creep to 1.09 < 2).
    tokio::time::sleep(Duration::from_millis(30)).await;
    assert_eq!(
        limiter.queued_counts().await.get(&2).copied().unwrap_or(0),
        3,
        "expected 3 parked Normal-priority waiters"
    );

    // Emergency cancellation bypasses the entire queue in < 1ms.
    let t0 = Instant::now();
    limiter
        .acquire_for(
            RequestPriority::Immediate,
            Decimal::ONE,
            Duration::from_secs(1),
        )
        .await
        .unwrap();
    assert!(
        t0.elapsed() < Duration::from_millis(1),
        "kill-switch acquire took {:?} (>1ms)?! queue bypass failed",
        t0.elapsed()
    );
}

#[tokio::test]
async fn immediate_priority_wins_next_token_over_low_priority() {
    // Refill fast, start empty: a queued P3 and a later P0 — the first token
    // must go to P0 (strict priority), not the older P3 waiter.
    let limiter = Arc::new(TokenBucketRateLimiter::new(cfg(1, 1000, 0)).unwrap());

    let p3 = {
        let l = limiter.clone();
        tokio::spawn(async move {
            l.acquire_for(RequestPriority::Low, Decimal::ONE, Duration::from_secs(5))
                .await
        })
    };
    tokio::time::sleep(Duration::from_millis(20)).await; // let P3 park

    let t0 = Instant::now();
    let p0 = {
        let l = limiter.clone();
        tokio::spawn(async move {
            let _ = l
                .acquire_for(
                    RequestPriority::Immediate,
                    Decimal::ONE,
                    Duration::from_secs(5),
                )
                .await;
            t0.elapsed()
        })
    };

    // P0 should complete first and quickly.
    let t = tokio::time::timeout(Duration::from_millis(250), p0)
        .await
        .expect("P0 did not win the next token")
        .unwrap();
    assert!(t < Duration::from_millis(100), "P0 grant took {t:?}");
    // P3 completes after (its grant happens on a later token cycle).
    tokio::time::timeout(Duration::from_secs(3), p3)
        .await
        .expect("P3 eventually granted within test window")
        .expect("P3 task joined successfully")
        .expect("P3 acquire succeeded");
}

#[tokio::test]
async fn acquire_times_out_when_no_refill() {
    let limiter = TokenBucketRateLimiter::new(cfg(1, 0, 0)).unwrap();
    let err = limiter
        .acquire_for(
            RequestPriority::Normal,
            Decimal::ONE,
            Duration::from_millis(50),
        )
        .await
        .unwrap_err();
    assert!(err.to_string().contains("rate limiter exhausted"));

    // Stray tickets must not linger.
    {
        let q = limiter.queued_counts().await;
        assert!(q.values().all(|c| *c == 0));
    }
    // The limiter handle is intentionally non-mutating for diagnostics.
    let _ = &limiter;
}

#[tokio::test]
async fn rate_limiter_soak_thermostat_honors_budget() {
    // "1,000 concurrent tasks" pattern, scaled to a fast session: 400 tasks
    // each of cost 1 with capacity 100 / rate 200 per second must never
    // overshoot the bucket budget, must all succeed, and must take at least
    // (400-100)/200 = 1.5s to drain — proving real throttling.
    let limiter = Arc::new(TokenBucketRateLimiter::new(cfg(100, 200, 100)).unwrap());
    let granted = Arc::new(AtomicUsize::new(0));
    let t0 = Instant::now();

    let mut handles = Vec::with_capacity(400);
    for _ in 0..400 {
        let l = limiter.clone();
        let g = granted.clone();
        handles.push(tokio::spawn(async move {
            if l.acquire_for(
                RequestPriority::Normal,
                Decimal::ONE,
                Duration::from_secs(5),
            )
            .await
            .is_ok()
            {
                g.fetch_add(1, Ordering::SeqCst);
            }
        }));
    }
    for h in handles {
        h.await.unwrap();
    }
    let elapsed = t0.elapsed();

    assert_eq!(
        granted.load(Ordering::SeqCst),
        400,
        "all acquires should succeed within budget"
    );
    assert!(
        elapsed >= Duration::from_millis(1200),
        "throttling failed: 400 tokens granted in {elapsed:?} with rate 200/s"
    );
    assert!(elapsed < Duration::from_secs(4));
}

#[tokio::test]
async fn adaptive_set_limits_clamps_balance() {
    let limiter = TokenBucketRateLimiter::new(cfg(10, 10, 10)).unwrap();
    limiter
        .set_limits(Decimal::from(3), Decimal::from(3))
        .await
        .unwrap();
    assert_eq!(limiter.available_tokens().await, Decimal::from(3));
    for _ in 0..3 {
        assert!(
            limiter
                .try_acquire(RequestPriority::Low, Decimal::ONE)
                .await
                .unwrap()
        );
    }
    assert!(
        !limiter
            .try_acquire(RequestPriority::Low, Decimal::ONE)
            .await
            .unwrap()
    );
}

#[tokio::test]
async fn same_priority_is_fifo() {
    // Exhaust the bucket, then queue two same-priority waiters: the first
    // queued must be granted before the second (no queue jumping within level).
    let limiter = Arc::new(TokenBucketRateLimiter::new(cfg(1, 500, 1)).unwrap());
    // Drain
    assert!(
        limiter
            .try_acquire(RequestPriority::Normal, Decimal::ONE)
            .await
            .unwrap()
    );

    let order = Arc::new(std::sync::Mutex::new(Vec::new()));

    let waiters = (0..2)
        .map(|i| {
            let l = limiter.clone();
            let order = order.clone();
            tokio::spawn(async move {
                l.acquire_for(
                    RequestPriority::Normal,
                    Decimal::ONE,
                    Duration::from_secs(5),
                )
                .await
                .unwrap();
                order.lock().unwrap().push(format!("w{i}:first"));
            })
        })
        .collect::<Vec<_>>();
    tokio::time::sleep(Duration::from_millis(30)).await;
    warn_queue(&limiter).await;
    for w in waiters {
        w.await.unwrap();
    }
    let order = order.lock().unwrap();
    assert_eq!(
        order[0], "w0:first",
        "FIFO violated within same priority: {order:?}"
    );
    assert_eq!(order[1], "w1:first");
}

async fn warn_queue(limiter: &TokenBucketRateLimiter) {
    let counts = limiter.queued_counts().await;
    let total: usize = counts.values().sum();
    assert!(total <= 2, "unexpected queue depth {counts:?}");
}

// ---------------------------------------------------------------------------
// RateLimitedBroker wrapper
// ---------------------------------------------------------------------------

/// Recording broker that logs the operation order it receives.
#[derive(Debug)]
struct RecordingBroker {
    log: Arc<Mutex<Vec<&'static str>>>,
    done_tx: Arc<tokio::sync::mpsc::UnboundedSender<&'static str>>,
}

impl RecordingBroker {
    fn new(done_tx: Arc<tokio::sync::mpsc::UnboundedSender<&'static str>>) -> Self {
        Self {
            log: Arc::new(Mutex::new(Vec::new())),
            done_tx,
        }
    }
}

#[async_trait::async_trait]
impl BrokerExecutor for RecordingBroker {
    fn name(&self) -> &str {
        "recording"
    }
    async fn place_order(&self, req: OrderRequest) -> Result<OrderAck, BrokerErrorAlias> {
        self.log.lock().await.push("place");
        self.done_tx.send("place").unwrap();
        Ok(OrderAck::new(
            req.instrument.symbol.clone(),
            req.client_order_id,
            OrderStatus::New,
            chrono::Utc::now(),
        ))
    }
    async fn cancel_order(&self, _order_id: &str) -> Result<(), BrokerErrorAlias> {
        self.log.lock().await.push("cancel");
        self.done_tx.send("cancel").unwrap();
        Ok(())
    }
    async fn get_order_status(&self, _order_id: &str) -> Result<OrderStatus, BrokerErrorAlias> {
        self.log.lock().await.push("status");
        self.done_tx.send("status").unwrap();
        Ok(OrderStatus::New)
    }
}

use honba_te_core::BrokerError as BrokerErrorAlias;

#[tokio::test]
async fn rate_limited_wrapper_prioritizes_cancellations() {
    // One token capacity, slow refill: schedule a place (Normal, queued) and a
    // kill-switch-style cancel; the cancel must reach the broker first even
    // though the place was enqueued earlier... NOTE: the wrapper maps
    // `cancel_order` to High (P1), so P1 must be granted before Normal.
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    let inner = RecordingBroker::new(Arc::new(tx));
    let broker = Arc::new(RateLimitedBroker::with_config(inner, cfg(1, 2, 1)).unwrap());

    // Drain the only token with a background fetch.
    let fetch = {
        let b = broker.clone();
        tokio::task::spawn(async move { b.get_order_status("X").await })
    };
    tokio::time::sleep(Duration::from_millis(10)).await;

    // Now competing ops while bucket is empty (rate 2/s → token every 500ms).
    let place = {
        let b = broker.clone();
        tokio::task::spawn(async move { b.place_order(limit_req("INFY", 10, "1600.00")).await })
    };
    tokio::time::sleep(Duration::from_millis(10)).await;

    let cancel = {
        let b = broker.clone();
        tokio::task::spawn(async move { b.cancel_order("ORD-X").await })
    };
    tokio::time::sleep(Duration::from_millis(10)).await;

    // Immediate kill-switch jumps everything, first token after drain.
    let now_verdict = {
        let b = broker.clone();
        tokio::task::spawn(async move { b.cancel_order_now("ORD-X").await })
    };

    // Expected token-consumption order:
    // 1. status (grabbed initial token), 2. immediate cancel, 3. high cancel, 4. place
    let mut got = Vec::new();
    for _ in 0..4 {
        got.push(
            tokio::time::timeout(Duration::from_secs(4), rx.recv())
                .await
                .expect("broker op never granted")
                .unwrap(),
        );
    }
    assert_eq!(
        got,
        vec!["status", "cancel", "cancel", "place"],
        "priority dispatch violated; account ops order = {got:?}"
    );
    let _ = (
        place.await.unwrap(),
        cancel.await.unwrap(),
        now_verdict.await.unwrap(),
        fetch.await.unwrap(),
    );
}
