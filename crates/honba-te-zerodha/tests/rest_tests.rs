//! M2 Zerodha REST contract tests against a loopback axum mock.
//!
//! Verifies: exact decimal payload/decoding, modify/cancel routes, error
//! taxonomy (429/TokenException/InputException/NetworkException), order book
//! mapping incl. SL/SL-M and IST timestamps, positions with short side, and
//! margins aggregation.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use axum::extract::{Path, State};
use axum::response::IntoResponse;
use axum::routing::{delete, get, post, put};
use axum::{Form, Json, Router};
use rust_decimal::Decimal;
use serde_json::{Value, json};

use honba_core::{
    Exchange, InstrumentId, OrderRequest, OrderSide, OrderStatus, OrderType, Price, Quantity,
};
use honba_te_core::{BrokerError, BrokerExecutor, PositionSide, ProductType};
use honba_te_zerodha::ZerodhaExecutor;

type Logged = Arc<Mutex<Vec<(String, Value)>>>;

async fn spawn_mock(app: Router) -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    format!("http://{addr}")
}

#[tokio::test]
async fn place_order_posts_exact_form_and_maps_ack() {
    let logged: Logged = Arc::new(Mutex::new(Vec::new()));
    let app = Router::new()
        .route(
            "/orders/regular",
            post(
                |State(log): State<Logged>, Form(form): Form<HashMap<String, String>>| async move {
                    log.lock().unwrap().push(("PLACE".into(), json!(form)));
                    Json(json!({"status":"success","data":{"order_id":"220119000332528"}}))
                },
            ),
        )
        .with_state(logged.clone());
    let base = spawn_mock(app).await;
    let executor = ZerodhaExecutor::with_base_url("key-1", "tok-1", &base)
        .with_product(ProductType::MisIntraday);

    let req = OrderRequest::new(
        InstrumentId::new(Exchange::Nse, "RELIANCE"),
        OrderSide::Buy,
        OrderType::Limit(Price::from_rupees("2500.5").unwrap()),
        Quantity::from_u64(10),
        "algo-m2",
    );
    let ack = executor.place_order(req).await.expect("ack");
    assert_eq!(ack.order_id, "220119000332528");
    assert_eq!(ack.status, OrderStatus::New);

    let first = { logged.lock().unwrap()[0].1.clone() };
    let form = first.as_object().unwrap();
    assert_eq!(form["price"], "2500.50");
    assert_eq!(form["quantity"], "10");
    assert_eq!(form["product"], "MIS");
    assert_eq!(form["tag"], "algo-m2");
    assert_eq!(form["transaction_type"], "BUY");
}

#[tokio::test]
async fn modify_and_cancel_hit_verb_route() {
    let logged: Logged = Arc::new(Mutex::new(Vec::new()));
    let modify_app = Router::new()
        .route(
            "/orders/regular/:order_id",
            put(
                |State(log): State<Logged>,
                 Path(order_id): Path<String>,
                 Form(form): Form<HashMap<String, String>>| async move {
                    log.lock()
                        .unwrap()
                        .push(("PUT".into(), json!({ "order_id": order_id, "form": form })));
                    Json(json!({"status":"success","data":{"order_id": order_id}}))
                },
            ),
        )
        .with_state(logged.clone());
    let mock_base = spawn_mock(modify_app).await;
    let executor = ZerodhaExecutor::with_base_url("k", "t", &mock_base);

    let req_price = Price::from_rupees("1990.55").unwrap();
    let req_qty = Quantity::from_u64(3);
    let ack = executor
        .modify_order("201230000456789", Some(req_price), Some(req_qty))
        .await
        .expect("modify ack");
    assert_eq!(ack.order_id, "201230000456789");
    let first = { logged.lock().unwrap()[0].1.clone() };
    let obj = first.as_object().unwrap();
    assert_eq!(obj["order_id"], "201230000456789");
    assert_eq!(obj["form"]["price"], "1990.55");
    assert_eq!(obj["form"]["quantity"], "3");

    let cancel_app = Router::new().route(
        "/orders/regular/:order_id",
        delete(|Path(order_id): Path<String>| async move {
            Json(json!({"status":"success","data":{"order_id": order_id}}))
        }),
    );
    let base2 = spawn_mock(cancel_app).await;
    let cancel_executor = ZerodhaExecutor::with_base_url("k", "t", base2);
    cancel_executor
        .cancel_order("220119000332528")
        .await
        .expect("cancel ok");
}

#[tokio::test]
async fn order_book_maps_statuses_types_and_ist_time() {
    let app = Router::new().route(
        "/orders",
        get(|| async {
            Json(json!({
                "status": "success",
                "data": [
                    {
                        "order_id": "220119000332528",
                        "exchange_order_id": "1000000001482421",
                        "exchange": "NSE",
                        "tradingsymbol": "SBIN",
                        "transaction_type": "SELL",
                        "status": "COMPLETE",
                        "product": "CNC",
                        "order_type": "LIMIT",
                        "quantity": 2,
                        "filled_quantity": 2,
                        "price": 470.25,
                        "order_timestamp": "2022-03-03 09:24:25"
                    },
                    {
                        "order_id": "220119000332529",
                        "exchange": "NSE",
                        "tradingsymbol": "INFY",
                        "transaction_type": "BUY",
                        "status": "OPEN",
                        "product": "MIS",
                        "order_type": "SL",
                        "quantity": "10",
                        "filled_quantity": 0,
                        "price": "1550.00",
                        "trigger_price": "1545.50",
                        "order_timestamp": "2022-03-03T09:30:00"
                    },
                    {
                        "order_id": "220119000332530",
                        "exchange": "MCX",
                        "tradingsymbol": "GOLD",
                        "transaction_type": "BUY",
                        "status": "REJECTED",
                        "product": "NRML",
                        "order_type": "SL-M",
                        "quantity": 1,
                        "trigger_price": 51200
                    }
                ]
            }))
        })
        .with_state(()),
    );
    let base = spawn_mock(app).await;
    let executor = ZerodhaExecutor::with_base_url("k", "t", base);
    let book = executor.fetch_order_book().await.expect("book");

    assert_eq!(book.len(), 3);
    let complete = &book[0];
    assert_eq!(complete.status, OrderStatus::Filled);
    assert_eq!(complete.side, OrderSide::Sell);
    assert_eq!(complete.quantity.0, Decimal::from(2));
    assert_eq!(complete.price.unwrap().0.to_string(), "470.25");
    assert_eq!(
        complete.timestamp.format("%Y-%m-%dT%H:%M:%S").to_string(),
        "2022-03-03T03:54:25",
        "IST -> UTC -5:30"
    );

    let sl = &book[1];
    match sl.order_type {
        OrderType::StopLimit { trigger, limit } => {
            assert_eq!(trigger.0.to_string(), "1545.50");
            assert_eq!(limit.0.to_string(), "1550.00");
        }
        other => panic!("expected SL leg, got {other:?}"),
    }
    assert_eq!(sl.status, OrderStatus::New);

    let slm = &book[2];
    assert!(matches!(slm.order_type, OrderType::StopLoss(_)));
    assert_eq!(slm.instrument.exchange, Exchange::Mcx);
}

#[tokio::test]
async fn positions_map_short_side_and_pnl() {
    let app = Router::new().route(
        "/portfolio/positions",
        get(|| async {
            Json(json!({
                "status": "success",
                "data": {
                    "net": [
                        {
                            "tradingsymbol": "SBIN",
                            "exchange": "NSE",
                            "product": "MIS",
                            "quantity": -2,
                            "average_price": 470.25,
                            "pnl": -10.50,
                            "last_price": 465
                        },
                        {
                            "tradingsymbol": "INE040A01034",
                            "exchange": "NSE",
                            "product": "CNC",
                            "quantity": 0,
                            "average_price": 0
                        }
                    ],
                    "day": []
                }
            }))
        })
        .with_state(()),
    );
    let base = spawn_mock(app).await;
    let executor = ZerodhaExecutor::with_base_url("k", "t", base);
    let positions = executor.fetch_positions().await.expect("positions");
    assert_eq!(positions.len(), 2);
    assert_eq!(positions[0].side, PositionSide::Short);
    assert_eq!(positions[0].quantity.0, Decimal::from(2));
    assert_eq!(positions[0].average_price.0.to_string(), "470.25");
    let pnl = positions[0].realized_pnl.unwrap().amount;
    assert_eq!(
        pnl * Decimal::from(100),
        Decimal::from(-1050),
        "exactly -10.50"
    );
    assert_eq!(positions[1].side, PositionSide::Flat);
}

#[tokio::test]
async fn margins_sum_utilised_buckets() {
    let app = Router::new().route(
        "/user/margins",
        get(|| async {
            Json(json!({
                "status": "success",
                "data": {
                    "equity": {
                        "available": {"cash": 2340.4, "collateral": 15000, "live_balance": 26000},
                        "utilised": {"debits": 500, "margins": 2250.75}
                    },
                    "commodity": {
                        "available": {"cash": 1000},
                        "utilised": {"debits": 0}
                    }
                }
            }))
        })
        .with_state(()),
    );
    let base = spawn_mock(app).await;
    let executor = ZerodhaExecutor::with_base_url("k", "t", base);
    let margins = executor.fetch_margins().await.expect("margins");
    assert_eq!(
        margins.available_cash.amount * Decimal::from(100),
        Decimal::from(234_040),
        "exactly 2340.40"
    );
    assert_eq!(
        margins.used_margin.amount * Decimal::from(100),
        Decimal::from(275_075),
        "exactly 2750.75 (debits + margins)"
    );
    assert_eq!(margins.collateral.amount, Decimal::from(15_000));
}

#[tokio::test]
async fn kite_error_taxonomy_maps_status_and_type() {
    // (probe id, HTTP status, error_type, expected class)
    let cases: Vec<(&str, u16, &str, &'static str)> = vec![
        ("r429", 429, "TooManyRequests", "RateLimited"),
        ("r401", 401, "TokenException", "Auth"),
        ("r403", 403, "TokenException", "Auth"),
        ("r400", 400, "InputException", "InvalidRequest"),
        ("r504", 504, "NetworkException", "Transport"),
        ("r500", 500, "OrderException", "Rejected"),
    ];

    let mut app = Router::new();
    for (probe, status_code, error_type, _) in cases.clone() {
        let path = format!("/orders/{probe}");
        let payload = json!({
            "status": "error",
            "message": "mock failure",
            "error_type": error_type,
            "data": null
        });
        app =
            app.route(
                &path,
                get(move || {
                    let payload = payload.clone();
                    async move {
                        (status_code_payload(status_code).unwrap(), Json(payload)).into_response()
                    }
                }),
            );
    }
    let base = spawn_mock(app).await;
    let executor = ZerodhaExecutor::with_base_url("k", "t", base);
    for (probe, status, error_type, expected) in cases {
        let err = match executor.get_order_status(probe).await {
            Err(err) => err,
            Ok(status) => panic!("expected error for {probe}, got status {status:?}"),
        };
        assert_eq!(
            classify(&err),
            expected,
            "status={status} et={error_type} err={err}"
        );
    }
}

fn status_code_payload(code: u16) -> Option<axum::http::StatusCode> {
    axum::http::StatusCode::from_u16(code).ok()
}

fn classify(err: &BrokerError) -> &'static str {
    match err {
        BrokerError::Auth(_) => "Auth",
        BrokerError::Rejected(_) => "Rejected",
        BrokerError::Transport(_) => "Transport",
        BrokerError::InvalidRequest(_) => "InvalidRequest",
        BrokerError::RateLimited(_) => "RateLimited",
        _ => "Other",
    }
}

#[tokio::test]
async fn non_json_gateway_body_is_transport_error() {
    let app = Router::new().route(
        "/user/profile",
        get(|| async {
            (
                axum::http::StatusCode::BAD_GATEWAY,
                "<html>upgrading</html>",
            )
        }),
    );
    let base = spawn_mock(app).await;
    let executor = ZerodhaExecutor::with_base_url("k", "t", base);
    let health = executor.health_check().await;
    match health {
        Err(BrokerError::Transport(msg)) => assert!(msg.contains("non-JSON")),
        other => panic!("expected transport error, got {other:?}"),
    }
}

// serde_json::Value must flow through helpers above (e.g. postback JSON body).
#[allow(dead_code)]
fn _assert_value_crate(v: Value) -> Value {
    v
}
