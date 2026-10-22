//! M2 Upstox REST contract tests against a loopback axum mock.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use axum::extract::{Path, Query, State};
use axum::routing::{delete, get, post, put};
use axum::{Json, Router};
use rust_decimal::Decimal;
use serde_json::{Value, json};

use honba_core::{
    Exchange, InstrumentId, OrderRequest, OrderSide, OrderStatus, OrderType, Price, Quantity,
};
use honba_te_core::{BrokerError, BrokerExecutor, PositionSide, ProductType};
use honba_te_upstox::{UpstoxExecutor, map_upstox_error};

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
async fn place_order_posts_exact_json_payload() {
    let logged: Logged = Arc::new(Mutex::new(Vec::new()));
    let app = Router::new()
        .route(
            "/order/place",
            post(
                |State(log): State<Logged>, Json(body): Json<Value>| async move {
                    log.lock().unwrap().push(("PLACE".into(), body.clone()));
                    Json(json!({"status": "success", "data": {"order_id": "240212000000081"}}))
                },
            ),
        )
        .with_state(logged.clone());
    let base = spawn_mock(app).await;
    let executor =
        UpstoxExecutor::with_base_url("k", "tok-1", &base).with_product(ProductType::MisIntraday);

    let req = OrderRequest::new(
        InstrumentId {
            exchange: Exchange::Nse,
            symbol: "SBIN".into(),
            token: Some("NSE_EQ|INE062A00720".into()),
            isin: None,
        },
        OrderSide::Buy,
        OrderType::StopLimit {
            trigger: Price::from_rupees("1545.5").unwrap(),
            limit: Price::from_rupees("1550.00").unwrap(),
        },
        Quantity::from_u64(10),
        "algo-m2",
    );
    let ack = executor.place_order(req).await.expect("ack");
    assert_eq!(ack.order_id, "240212000000081");
    assert_eq!(ack.status, OrderStatus::New);

    let body = { logged.lock().unwrap()[0].1.clone() };
    let obj = body.as_object().unwrap();
    assert_eq!(obj["instrument_token"], "NSE_EQ|INE062A00720");
    assert_eq!(obj["order_type"], "SL");
    assert_eq!(obj["price"], "1550.00");
    assert_eq!(obj["trigger_price"], "1545.50");
    assert_eq!(obj["quantity"], 10);
    assert_eq!(obj["product"], "I");
    assert_eq!(obj["tag"], "algo-m2");
    assert_eq!(obj["transaction_type"], "BUY");
}

#[tokio::test]
async fn modify_reads_details_then_patches() {
    let logged: Logged = Arc::new(Mutex::new(Vec::new()));
    let log_get = logged.clone();
    let log_put = logged.clone();
    let app = Router::new()
        .route(
            "/order/details/:order_id",
            get(move |Path(order_id): Path<String>| {
                let log = log_get.clone();
                async move {
                    log.lock().unwrap().push(("GET".into(), json!(order_id)));
                    Json(json!({
                        "status": "success",
                        "data": [{
                            "order_id": order_id,
                            "quantity": 10,
                            "product": "D",
                            "order_type": "LIMIT",
                            "transaction_type": "BUY",
                            "price": "1550.00",
                            "status": "open",
                            "instrument_token": "NSE_EQ|INE062A00720",
                            "tradingsymbol": "SBIN-EQ"
                        }]
                    }))
                }
            }),
        )
        .route(
            "/order/modify",
            put(
                move |State(log): State<Logged>, Json(body): Json<Value>| async move {
                    log.lock().unwrap().push(("PUT".into(), body.clone()));
                    Json(json!({"status": "success", "data": {"order_id": body["order_id"]}}))
                },
            )
            .with_state(log_put.clone()),
        )
        .with_state(logged.clone());
    let base = spawn_mock(app).await;
    let executor = UpstoxExecutor::with_base_url("k", "t", &base);

    let ack = executor
        .modify_order(
            "240212000000081",
            Some(Price::from_rupees("1580.25").unwrap()),
            None,
        )
        .await
        .expect("modify ack");
    assert_eq!(ack.order_id, "240212000000081");

    let entries = logged.lock().unwrap();
    assert_eq!(entries[0].0, "GET", "details first");
    assert_eq!(entries[1].0, "PUT", "then modify");
    let obj = entries[1].1.as_object().unwrap();
    assert_eq!(obj["order_id"], "240212000000081");
    assert_eq!(obj["price"], "1580.25");
    assert_eq!(obj["quantity"], 10, "inherit current quantity");
    assert_eq!(obj["instrument_token"], "NSE_EQ|INE062A00720");
    assert_eq!(obj["transaction_type"], "BUY");
}

#[tokio::test]
async fn cancel_hits_query_route() {
    let app = Router::new().route(
        "/order/cancel",
        delete(|Query(q): Query<HashMap<String, String>>| async move {
            Json(json!({"status": "success", "data": {"order_id": q["order_id"]}}))
        }),
    );
    let base = spawn_mock(app).await;
    let executor = UpstoxExecutor::with_base_url("k", "t", base);
    executor
        .cancel_order("240212000000081")
        .await
        .expect("cancel ok");
}

#[tokio::test]
async fn order_book_maps_lowercase_statuses_and_sls() {
    let app = Router::new().route(
        "/order/book",
        get(|| async {
            Json(json!({
                "status": "success",
                "data": [
                    {
                        "order_id": "240212000000081",
                        "exchange_order_id": "151102000542877",
                        "exchange": "NSE",
                        "tradingsymbol": "SBIN-EQ",
                        "instrument_token": "NSE_EQ|INE062A00720",
                        "transaction_type": "SELL",
                        "product": "D",
                        "order_type": "LIMIT",
                        "status": "complete",
                        "quantity": 2,
                        "filled_quantity": 2,
                        "price": "470.25",
                        "exchange_timestamp": "2024-02-21 14:40:02"
                    },
                    {
                        "order_id": "240212000000082",
                        "exchange": "MCX",
                        "instrument_token": "MCX_FO|FM00GOLD",
                        "transaction_type": "BUY",
                        "product": "MTF",
                        "order_type": "SL-M",
                        "status": "open",
                        "quantity": "1",
                        "filled_quantity": 0,
                        "trigger_price": "51200"
                    },
                    {
                        "order_id": "240212000000083",
                        "exchange": "BSE",
                        "instrument_token": "BSE_EQ|INE002A00714",
                        "transaction_type": "BUY",
                        "product": "I",
                        "order_type": "MARKET",
                        "status": "rejected",
                        "status_message": "insufficient margin",
                        "quantity": 5
                    }
                ]
            }))
        })
        .with_state(()),
    );
    let base = spawn_mock(app).await;
    let executor = UpstoxExecutor::with_base_url("k", "t", base);
    let book = executor.fetch_order_book().await.expect("book");
    assert_eq!(book.len(), 3);

    assert_eq!(book[0].status, OrderStatus::Filled);
    assert_eq!(book[0].side, OrderSide::Sell);
    assert_eq!(book[0].instrument.exchange, Exchange::Nse);
    assert_eq!(book[0].instrument.symbol, "SBIN");
    assert_eq!(book[0].price.unwrap().0.to_string(), "470.25");
    assert_eq!(
        book[0].timestamp.format("%Y-%m-%dT%H:%M:%S").to_string(),
        "2024-02-21T09:10:02"
    );
    assert_eq!(book[0].product, Some(ProductType::CncDelivery));

    let slm = &book[1];
    assert!(matches!(slm.order_type, OrderType::StopLoss(_)));
    assert_eq!(slm.instrument.exchange, Exchange::Mcx);
    assert_eq!(slm.product, Some(ProductType::MtfMargin));
    assert_eq!(slm.trigger_price.unwrap().0.to_string(), "51200");

    let rejected = &book[2];
    assert_eq!(rejected.status, OrderStatus::Rejected);
    assert_eq!(rejected.instrument.exchange, Exchange::Bse);
}

#[tokio::test]
async fn positions_map_short_side_and_value_scale() {
    let app = Router::new().route(
        "/portfolio/short-term-positions",
        get(|| async {
            Json(json!({
                "status": "success",
                "data": [
                    {
                        "quantity": -2,
                        "product": "D",
                        "net_price": 470.25,
                        "isin": "INE062A00720",
                        "instrument_token": "NSE_EQ|INE062A00720",
                        "tradingsymbol": "SBIN-EQ",
                        "pnl": -10.5,
                        "last_price": 465.5
                    },
                    {
                        "quantity": 1,
                        "product": "I",
                        "net_price": 89.85,
                        "isin": "INE062F02083",
                        "instrument_token": "NSE_EQ|INE062F02083",
                        "tradingsymbol": "BUGG-EQ"
                    }
                ]
            }))
        })
        .with_state(()),
    );
    let base = spawn_mock(app).await;
    let executor = UpstoxExecutor::with_base_url("k", "t", base);
    let positions = executor.fetch_positions().await.expect("positions");

    assert_eq!(positions[0].side, PositionSide::Short);
    assert_eq!(positions[0].quantity.0, Decimal::from(2));
    assert_eq!(positions[0].average_price.0.to_string(), "470.25");
    let pnl = positions[0].realized_pnl.unwrap().amount;
    assert_eq!(pnl * Decimal::from(100), Decimal::from(-1050));
    assert_eq!(positions[0].last_price.unwrap().0.to_string(), "465.5");
    assert_eq!(positions[1].side, PositionSide::Long);
    assert_eq!(positions[1].average_price.0.to_string(), "89.85");
}

#[tokio::test]
async fn margins_sum_cross_segment_utilised() {
    let app = Router::new().route(
        "/user/funds-and-margin",
        get(|| async {
            Json(json!({
                "status": "success",
                "data": {
                    "equity_cash": 2340.4,
                    "commodity_cash": 1000.0,
                    "equity_margin_utilised": {"debits": 500},
                    "commodity_margin_utilised": {"debits": 2250.75},
                    "collateral": 15000
                }
            }))
        })
        .with_state(()),
    );
    let base = spawn_mock(app).await;
    let executor = UpstoxExecutor::with_base_url("k", "t", base);
    let margins = executor.fetch_margins().await.expect("margins");
    assert_eq!(
        margins.available_cash.amount * Decimal::from(100),
        Decimal::from(334_040),
        "equity + commodity cash exactly"
    );
    assert_eq!(
        margins.used_margin.amount * Decimal::from(100),
        Decimal::from(275_075)
    );
    assert_eq!(margins.collateral.amount, Decimal::from(15_000));
}

#[test]
fn error_taxonomy_maps_upstox_envelopes() {
    let cases: Vec<(u16, Value, &'static str)> = vec![
        (
            429,
            json!({"errorCode": "UPI1001", "errorMessage": "rate limit exceeded", "errorType": "RateLimitException"}),
            "RateLimited",
        ),
        (
            401,
            json!({"errorCode": "UDC1002", "errorMessage": "Access token expired", "errorType": "AuthenticationException"}),
            "Auth",
        ),
        (
            400,
            json!({"errorCode": "UDI1001", "errorMessage": "invalid quantity", "errorType": "InvalidArgumentException"}),
            "InvalidRequest",
        ),
        (
            500,
            json!({"errorCode": "UPT1001", "errorMessage": "no funds available", "errorType": "UpstoxException"}),
            "Rejected",
        ),
    ];
    for (status, error, expected) in cases {
        let err = map_upstox_error(status, &[error]);
        assert_eq!(classify(&err), expected, "{err}");
    }
    // Empty error list defaults to Rejected:
    assert_eq!(classify(&map_upstox_error(500, &[])), "Rejected");
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
    let executor = UpstoxExecutor::with_base_url("k", "t", base);
    let health = executor.health_check().await;
    match health {
        Err(BrokerError::Transport(msg)) => assert!(msg.contains("non-JSON")),
        other => panic!("expected transport error, got {other:?}"),
    }
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
