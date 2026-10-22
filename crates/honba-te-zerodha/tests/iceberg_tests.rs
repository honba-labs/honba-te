//! M3 Zerodha native iceberg variety contract tests (Kite Connect v3
//! `POST /orders/iceberg` with iceberg_quantity / iceberg_legs).

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use axum::extract::State;
use axum::routing::post;
use axum::{Form, Json, Router};
use serde_json::json;

use honba_core::{Exchange, InstrumentId, OrderRequest, OrderSide, OrderType, Price, Quantity};
use honba_te_core::validate_native_legs;
use honba_te_core::{BrokerError, IcebergOrderExecutor, ProductType};
use honba_te_zerodha::ZerodhaExecutor;

type Logged = Arc<Mutex<Vec<(String, HashMap<String, String>)>>>;

async fn spawn_mock(app: Router) -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    format!("http://{addr}")
}

fn nifty_parent() -> OrderRequest {
    OrderRequest::new(
        InstrumentId::new(Exchange::Nse, "NIFTY"),
        OrderSide::Buy,
        OrderType::Limit(Price::from_rupees("24850.05").unwrap()),
        Quantity::from_u64(1000),
        "algo-m3",
    )
}

#[tokio::test]
async fn native_iceberg_posts_iceberg_variety_with_metered_legs() {
    let logged: Logged = Arc::new(Mutex::new(Vec::new()));
    let app = Router::new().route(
        "/orders/iceberg",
        post(
            move |State(log): State<Logged>, Form(form): Form<HashMap<String, String>>| async move {
                log.lock().unwrap().push(("ICEBERG".into(), form.clone()));
                Json(json!({"status":"success","data":{"order_id":"241209000197001"}}))
            },
        )
        .with_state(logged.clone()),
    );
    let base = spawn_mock(app).await;
    let executor = ZerodhaExecutor::with_base_url("key-m3", "tok-m3", &base)
        .with_product(ProductType::CncDelivery);

    let parent = nifty_parent();
    let ack = executor
        .place_iceberg_order(parent, Quantity::from_u64(250))
        .await
        .expect("native iceberg ack");
    assert_eq!(ack.order_id, "241209000197001");

    let entries = logged.lock().unwrap();
    let (route, form) = &entries[0];
    assert_eq!(route, "ICEBERG");
    assert_eq!(form["iceberg_quantity"], "250", "per-leg visible quantity");
    assert_eq!(form["iceberg_legs"], "4", "derived from quantity/display");
    assert_eq!(
        form["quantity"], "1000",
        "parent total rides the metered variety"
    );
    assert_eq!(form["order_type"], "LIMIT");
    assert_eq!(form["price"], "24850.05");
    assert_eq!(form["tag"], "algo-m3", "SEBI algo tag preserved");
}

#[tokio::test]
async fn native_iceberg_rejects_non_dividing_display() {
    // 1000 shares @ display 300 is not exact: gateway contract enforced
    // before any network call (base URL is never contacted).
    let executor = ZerodhaExecutor::with_base_url("k", "t", "http://unused.invalid");
    let err = executor
        .place_iceberg_order(nifty_parent(), Quantity::from_u64(300))
        .await
        .unwrap_err();
    match err {
        BrokerError::InvalidRequest(msg) => {
            assert!(msg.contains("divide"), "{msg}");
        }
        other => panic!("expected InvalidRequest, got {other:?}"),
    }
    // Leg-count cap: 2550/50 = 51 legs → beyond the documented 50.
    let err = executor
        .place_iceberg_order(
            OrderRequest {
                quantity: Quantity::from_u64(2550),
                ..nifty_parent()
            },
            Quantity::from_u64(50),
        )
        .await
        .unwrap_err();
    assert!(
        matches!(err, BrokerError::InvalidRequest(_)),
        "leg-count cap enforced: {err}"
    );
}

/// Contract parity shim — the te-core free function must agree with the
/// adapter's validation path.
#[test]
fn te_core_validation_matches_kite_rules() {
    assert_eq!(
        validate_native_legs(Quantity::from_u64(1000), Quantity::from_u64(250)).unwrap(),
        4
    );
    assert!(validate_native_legs(Quantity::from_u64(1000), Quantity::from_u64(1)).is_err());
}
