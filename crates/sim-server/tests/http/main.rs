use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use hl_wire::{AssetId, PriceTicks};
use http_body_util::BodyExt;
use serde_json::{Value, json};
use sim_core::{
    ApplyResult, CancelStatus, CommandResult, Engine, PlacementDisposition, PlacementStatus,
};
use sim_server::http::{
    HttpConfig, MarketObservation, MarketSnapshot, MarketView, MarketViewError, router_with_config,
};
use sim_server::{
    PendingRuntimeReply, RuntimeError, RuntimeEvent, RuntimeHandle, RuntimeLimits, RuntimePort,
    RuntimeReply, RuntimeRequest, bounded_runtime,
};
use std::{
    num::NonZeroUsize,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio::sync::broadcast;
use tower::ServiceExt;

const ALICE: &str = "0x1111111111111111111111111111111111111111";
const BOB: &str = "0x2222222222222222222222222222222222222222";
const SIG: &str = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";

#[derive(Clone)]
struct StaticMarket(MarketSnapshot);
impl MarketView for StaticMarket {
    fn snapshot(&self) -> Result<MarketSnapshot, MarketViewError> {
        Ok(self.0.clone())
    }
}

#[derive(Clone)]
struct CountingMarket {
    calls: Arc<AtomicUsize>,
    snapshot: MarketSnapshot,
}

impl MarketView for CountingMarket {
    fn snapshot(&self) -> Result<MarketSnapshot, MarketViewError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Ok(self.snapshot.clone())
    }
}

struct OverloadedPort;
impl RuntimePort for OverloadedPort {
    fn try_request(
        &self,
        _: RuntimeRequest,
    ) -> Result<sim_server::PendingRuntimeReply, RuntimeError> {
        Err(RuntimeError::Overloaded)
    }
    fn subscribe_events(&self) -> broadcast::Receiver<RuntimeEvent> {
        let (tx, rx) = broadcast::channel(1);
        drop(tx);
        rx
    }
}

struct FakeRuntimePort(RuntimeHandle);

impl RuntimePort for FakeRuntimePort {
    fn try_request(&self, request: RuntimeRequest) -> Result<PendingRuntimeReply, RuntimeError> {
        self.0.try_request(request)
    }

    fn subscribe_events(&self) -> broadcast::Receiver<RuntimeEvent> {
        self.0.subscribe_events()
    }
}

fn fake_runtime_reply(reply: RuntimeReply) -> Arc<dyn RuntimePort> {
    let limits = RuntimeLimits::new(NonZeroUsize::MIN, NonZeroUsize::MIN);
    let (port, mut owner, _) = bounded_runtime(limits);
    tokio::spawn(async move {
        let envelope = owner.recv().await.expect("HTTP request reached fake runtime");
        assert!(matches!(envelope.request, RuntimeRequest::Apply(_)));
        envelope.respond(Ok(reply)).expect("HTTP handler receives fake reply");
    });
    Arc::new(FakeRuntimePort(port))
}

fn market(stale: Option<AssetId>) -> Arc<dyn MarketView> {
    Arc::new(StaticMarket(MarketSnapshot {
        captured_at: 1_000,
        observations: vec![
            MarketObservation {
                asset: AssetId::BTC,
                oracle_price: PriceTicks::new(600_001).unwrap(),
                observed_at: 1_000,
                is_stale: stale == Some(AssetId::BTC),
            },
            MarketObservation {
                asset: AssetId::ETH,
                oracle_price: PriceTicks::new(300_025).unwrap(),
                observed_at: 1_000,
                is_stale: stale == Some(AssetId::ETH),
            },
            MarketObservation {
                asset: AssetId::SOL,
                oracle_price: PriceTicks::new(15_025).unwrap(),
                observed_at: 1_000,
                is_stale: stale == Some(AssetId::SOL),
            },
        ],
    }))
}

fn runtime() -> Arc<dyn RuntimePort> {
    let limits = RuntimeLimits::new(NonZeroUsize::new(32).unwrap(), NonZeroUsize::new(8).unwrap());
    let (port, mut owner, _) = bounded_runtime(limits);
    tokio::spawn(async move {
        let mut engine = Engine::new(7);
        while let Some(envelope) = owner.recv().await {
            let reply = match &envelope.request {
                RuntimeRequest::Apply(command) => {
                    RuntimeReply::Applied(engine.apply(command.clone()))
                }
                RuntimeRequest::EngineSnapshot => RuntimeReply::EngineSnapshot(engine.snapshot()),
                RuntimeRequest::Book(asset) => RuntimeReply::Book(engine.book_snapshot(*asset)),
                RuntimeRequest::Account(user) => {
                    RuntimeReply::Account(engine.account_snapshot(user))
                }
                RuntimeRequest::Order(id) => RuntimeReply::Order(engine.order(*id)),
                RuntimeRequest::EventsAfter(sequence) => {
                    RuntimeReply::Events(engine.events_after(*sequence))
                }
                RuntimeRequest::ObserveOracle(_) => {
                    panic!("HTTP adapter did not submit an oracle observation")
                }
                RuntimeRequest::OracleHealth => {
                    panic!("HTTP adapter did not request oracle health")
                }
                RuntimeRequest::Shutdown => RuntimeReply::Shutdown,
            };
            let _ = envelope.respond(Ok(reply));
        }
    });
    Arc::new(port)
}

fn app(
    runtime: Arc<dyn RuntimePort>,
    market: Arc<dyn MarketView>,
    max_batch: usize,
) -> axum::Router {
    app_with_config(
        runtime,
        market,
        HttpConfig {
            max_batch,
            max_body_bytes: 16 * 1024,
            max_observation_age_ms: 60_000,
            runtime_reply_timeout: Duration::from_secs(1),
        },
    )
}

fn app_with_config(
    runtime: Arc<dyn RuntimePort>,
    market: Arc<dyn MarketView>,
    config: HttpConfig,
) -> axum::Router {
    router_with_config(runtime, market, config)
}

async fn post_raw(
    app: axum::Router,
    path: &str,
    body: impl Into<Body>,
    content_type: Option<&str>,
    user: Option<&str>,
) -> (StatusCode, Value) {
    let mut request = Request::post(path);
    if let Some(content_type) = content_type {
        request = request.header("content-type", content_type);
    }
    if let Some(user) = user {
        request = request.header("x-sim-user", user);
    }
    let response = app.oneshot(request.body(body.into()).unwrap()).await.unwrap();
    let status = response.status();
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    (status, serde_json::from_slice(&bytes).unwrap())
}

async fn post(
    app: axum::Router,
    path: &str,
    body: Value,
    user: Option<&str>,
) -> (StatusCode, Value) {
    post_raw(app, path, body.to_string(), Some("application/json"), user).await
}

fn exchange(action: Value) -> Value {
    json!({"action": action, "nonce": 900, "signature": {"r": SIG, "s": SIG, "v": 27}, "vaultAddress": null})
}

fn order(asset: u8, buy: bool, price: &str, size: &str, tif: &str) -> Value {
    json!({"a": asset, "b": buy, "p": price, "s": size, "r": false, "t": {"limit": {"tif": tif}}})
}

#[tokio::test]
async fn public_info_variants_are_fixed_order_and_canonical() {
    let app = app(runtime(), market(None), 8);
    let (_, meta) = post(app.clone(), "/info", json!({"type":"meta"}), None).await;
    assert_eq!(
        meta,
        json!({"universe":[{"name":"BTC","szDecimals":5},{"name":"ETH","szDecimals":4},{"name":"SOL","szDecimals":2}]})
    );

    let (_, mids) = post(app.clone(), "/info", json!({"type":"allMids"}), None).await;
    assert_eq!(mids, json!({"BTC":"60000.1","ETH":"3000.25","SOL":"15.025"}));

    let (_, contexts) = post(app.clone(), "/info", json!({"type":"metaAndAssetCtxs"}), None).await;
    assert_eq!(contexts[0], meta);
    assert_eq!(
        contexts[1][0],
        json!({"oraclePx":"60000.1","midPx":"60000.1","observedAt":1000,"isStale":false})
    );

    let (_, book) = post(app, "/info", json!({"type":"l2Book","coin":"BTC"}), None).await;
    assert_eq!(book, json!({"coin":"BTC","time":0,"levels":[[],[]]}));
}

#[tokio::test]
async fn user_info_uses_header_identity_not_payload_identity() {
    let app = app(runtime(), market(None), 8);
    let action = exchange(
        json!({"type":"order","orders":[order(0,true,"60000.1","0.00001","Gtc")],"grouping":"na"}),
    );
    assert_eq!(post(app.clone(), "/exchange", action, Some(ALICE)).await.0, StatusCode::OK);

    let (_, alice) =
        post(app.clone(), "/info", json!({"type":"openOrders","user":BOB}), Some(ALICE)).await;
    let (_, bob) =
        post(app.clone(), "/info", json!({"type":"openOrders","user":ALICE}), Some(BOB)).await;
    assert_eq!(alice.as_array().unwrap().len(), 1);
    assert_eq!(bob, json!([]));

    let (_, state) =
        post(app, "/info", json!({"type":"clearinghouseState","user":BOB}), Some(ALICE)).await;
    assert_eq!(state["user"], ALICE);
}

#[tokio::test]
async fn missing_or_non_normalized_identity_is_unauthorized() {
    let app = app(runtime(), market(None), 8);
    for user in [None, Some("0xAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"), Some("0x1234")] {
        let (status, body) =
            post(app.clone(), "/info", json!({"type":"openOrders","user":ALICE}), user).await;
        assert_eq!(status, StatusCode::UNAUTHORIZED);
        assert_eq!(body["error"]["category"], "unauthorized_sim_user");
    }
}

#[tokio::test]
async fn exchange_preserves_ordered_partial_statuses_and_supports_cancel() {
    let app = app(runtime(), market(None), 8);
    let action = exchange(json!({"type":"order","orders":[
        order(0,false,"60000.1","0.00002","Gtc"),
        order(0,true,"60000.1","0.00001","Alo"),
        order(1,true,"3000.25","0.0001","Ioc")
    ],"grouping":"na"}));
    let (status, body) = post(app.clone(), "/exchange", action, Some(ALICE)).await;
    assert_eq!(status, StatusCode::OK);
    let statuses = body["response"]["data"]["statuses"].as_array().unwrap();
    assert!(statuses[0].get("resting").is_some());
    assert_eq!(statuses[1]["error"]["category"], "domain_reject");
    assert_eq!(statuses[2]["error"]["category"], "domain_reject");

    let oid = statuses[0]["resting"]["oid"].as_u64().unwrap();
    let cancel = exchange(json!({"type":"cancel","cancels":[{"a":0,"o":oid},{"a":0,"o":9999}]}));
    let (_, cancelled) = post(app, "/exchange", cancel, Some(ALICE)).await;
    assert_eq!(cancelled["response"]["data"]["statuses"][0], "success");
    assert_eq!(cancelled["response"]["data"]["statuses"][1]["error"]["category"], "domain_reject");
}

#[tokio::test]
async fn batch_limit_vault_reduce_only_and_unknown_semantics_fail_closed() {
    let app = app(runtime(), market(None), 1);
    let too_many = exchange(
        json!({"type":"order","orders":[order(0,true,"1","0.00001","Gtc"),order(0,true,"2","0.00001","Gtc")],"grouping":"na"}),
    );
    assert_eq!(
        post(app.clone(), "/exchange", too_many, Some(ALICE)).await.1["error"]["category"],
        "invalid_request"
    );

    let mut vault = exchange(json!({"type":"cancel","cancels":[]}));
    vault["vaultAddress"] = json!(BOB);
    assert_eq!(
        post(app.clone(), "/exchange", vault, Some(ALICE)).await.1["error"]["category"],
        "unsupported"
    );

    let mut reduce = order(0, true, "1", "0.00001", "Gtc");
    reduce["r"] = json!(true);
    let reduce = exchange(json!({"type":"order","orders":[reduce],"grouping":"na"}));
    assert_eq!(
        post(app.clone(), "/exchange", reduce, Some(ALICE)).await.1["error"]["category"],
        "unsupported"
    );

    let trigger = exchange(json!({"type":"trigger","orders":[]}));
    assert_eq!(
        post(app, "/exchange", trigger, Some(ALICE)).await.1["error"]["category"],
        "unsupported"
    );
}

#[tokio::test]
async fn malformed_json_and_signature_are_invalid_request() {
    let app = app(runtime(), market(None), 8);
    let response = app
        .clone()
        .oneshot(
            Request::post("/info")
                .header("content-type", "application/json")
                .body(Body::from("{"))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let (_, body) = post(app, "/exchange", json!({"action":{"type":"cancel","cancels":[]},"nonce":1,"signature":{"r":"bad","s":SIG,"v":27},"vaultAddress":null}), Some(ALICE)).await;
    assert_eq!(body["error"]["category"], "invalid_request");
}

#[tokio::test]
async fn exact_shape_unknown_assets_are_unsupported_and_bad_shapes_stay_invalid() {
    let app = app(runtime(), market(None), 8);

    for coin in ["DOGE", "", "btc"] {
        let (status, body) =
            post(app.clone(), "/info", json!({"type":"l2Book","coin":coin}), None).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(body["error"]["category"], "unsupported", "coin={coin}, body={body}");
    }
    for request in [
        json!({"type":"l2Book"}),
        json!({"type":"l2Book","coin":3}),
        json!({"type":"l2Book","coin":"DOGE","extra":true}),
    ] {
        let (_, body) = post(app.clone(), "/info", request, None).await;
        assert_eq!(body["error"]["category"], "invalid_request", "body={body}");
    }

    for asset in [3_u8, 255] {
        for action in [
            json!({"type":"order","orders":[order(asset,true,"1","0.00001","Gtc")],"grouping":"na"}),
            json!({"type":"cancel","cancels":[{"a":asset,"o":1}]}),
        ] {
            let (_, body) = post(app.clone(), "/exchange", exchange(action), Some(ALICE)).await;
            assert_eq!(body["error"]["category"], "unsupported", "body={body}");
        }
    }

    let malformed_actions = [
        json!({"type":"cancel","cancels":[{"o":1}]}),
        json!({"type":"cancel","cancels":[{"a":"3","o":1}]}),
        json!({"type":"cancel","cancels":[{"a":3.0,"o":1}]}),
        json!({"type":"cancel","cancels":[{"a":256,"o":1}]}),
        json!({"type":"cancel","cancels":[{"a":-1,"o":1}]}),
        json!({"type":"cancel","cancels":[{"a":3,"o":1,"extra":true}]}),
        json!({"type":"order","orders":[{"a":3}],"grouping":"na"}),
        json!({"type":"order","orders":[order(3,true,"1.0","0.00001","Gtc")],"grouping":"na"}),
        json!({"type":"order","orders":[order(3,true,"1","0.00001","Gtc")],"grouping":"na","extra":true}),
    ];
    for action in malformed_actions {
        let (_, body) = post(app.clone(), "/exchange", exchange(action), Some(ALICE)).await;
        assert_eq!(body["error"]["category"], "invalid_request", "body={body}");
    }
}

#[tokio::test]
async fn json_media_type_and_body_failures_have_stable_http_errors() {
    let app = app_with_config(
        runtime(),
        market(None),
        HttpConfig {
            max_batch: 8,
            max_body_bytes: 32,
            max_observation_age_ms: 60_000,
            runtime_reply_timeout: Duration::from_secs(1),
        },
    );
    let valid_body = r#"{"type":"meta"}"#;

    for content_type in [None, Some("text/plain"), Some("application/json; charset")] {
        let (status, body) = post_raw(app.clone(), "/info", valid_body, content_type, None).await;
        assert_eq!(status, StatusCode::UNSUPPORTED_MEDIA_TYPE);
        assert_eq!(body["error"]["category"], "invalid_request");
    }

    for content_type in ["application/json", "APPLICATION/JSON", "application/json; charset=utf-8"]
    {
        let (status, body) =
            post_raw(app.clone(), "/info", valid_body, Some(content_type), None).await;
        assert_eq!(status, StatusCode::OK, "content-type={content_type}, body={body}");
        assert!(body["universe"].is_array());
    }

    let (status, body) = post_raw(app.clone(), "/info", "{", Some("application/json"), None).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(body["error"]["category"], "invalid_request");

    let oversized = format!(r#"{{"type":"meta","padding":"{}"}}"#, "x".repeat(32));
    let (status, body) = post_raw(app, "/info", oversized, Some("application/json"), None).await;
    assert_eq!(status, StatusCode::PAYLOAD_TOO_LARGE);
    assert_eq!(body["error"]["category"], "invalid_request");
}

#[tokio::test]
async fn stale_oracle_blocks_placement_but_not_cancel() {
    let app = app(runtime(), market(Some(AssetId::BTC)), 8);
    let action = exchange(
        json!({"type":"order","orders":[order(0,true,"60000.1","0.00001","Gtc")],"grouping":"na"}),
    );
    let (status, body) = post(app.clone(), "/exchange", action, Some(ALICE)).await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(body["error"]["category"], "oracle_stale");
    let cancel = exchange(json!({"type":"cancel","cancels":[{"a":0,"o":1}]}));
    assert_ne!(
        post(app, "/exchange", cancel, Some(ALICE)).await.1["error"]["category"],
        "oracle_stale"
    );
}

#[tokio::test]
async fn runtime_stale_race_names_asset_without_blocking_cancel_or_query() {
    let limits = RuntimeLimits::new(NonZeroUsize::new(4).unwrap(), NonZeroUsize::MIN);
    let (port, mut owner, _) = bounded_runtime(limits);
    tokio::spawn(async move {
        let mut engine = Engine::new(7);
        while let Some(envelope) = owner.recv().await {
            let reply = match &envelope.request {
                RuntimeRequest::Apply(sim_core::Command::PlaceBatch { .. }) => {
                    Err(RuntimeError::OracleStale(AssetId::ETH))
                }
                RuntimeRequest::Apply(command @ sim_core::Command::CancelBatch { .. }) => {
                    Ok(RuntimeReply::Applied(engine.apply(command.clone())))
                }
                RuntimeRequest::Book(asset) => Ok(RuntimeReply::Book(engine.book_snapshot(*asset))),
                RuntimeRequest::EngineSnapshot => {
                    Ok(RuntimeReply::EngineSnapshot(engine.snapshot()))
                }
                RuntimeRequest::Account(user) => {
                    Ok(RuntimeReply::Account(engine.account_snapshot(user)))
                }
                RuntimeRequest::Order(id) => Ok(RuntimeReply::Order(engine.order(*id))),
                RuntimeRequest::EventsAfter(sequence) => {
                    Ok(RuntimeReply::Events(engine.events_after(*sequence)))
                }
                RuntimeRequest::ObserveOracle(_) => {
                    panic!("HTTP adapter did not submit an oracle observation")
                }
                RuntimeRequest::OracleHealth => {
                    panic!("HTTP adapter did not request oracle health")
                }
                RuntimeRequest::Shutdown => Ok(RuntimeReply::Shutdown),
            };
            envelope.respond(reply).expect("HTTP handler receives fake reply");
        }
    });
    let app = app(Arc::new(FakeRuntimePort(port)), market(None), 8);

    let placement = exchange(
        json!({"type":"order","orders":[order(0,true,"60000.1","0.00001","Gtc")],"grouping":"na"}),
    );
    let (status, body) = post(app.clone(), "/exchange", placement, Some(ALICE)).await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(body["error"]["category"], "oracle_stale");
    assert_eq!(body["error"]["message"], "oracle observation is stale for ETH");

    let cancel = exchange(json!({"type":"cancel","cancels":[{"a":0,"o":1}]}));
    let (status, body) = post(app.clone(), "/exchange", cancel, Some(ALICE)).await;
    assert_eq!(status, StatusCode::OK, "body={body}");
    assert_ne!(body["response"]["data"]["statuses"][0]["error"]["category"], "oracle_stale");

    let (status, body) = post(app, "/info", json!({"type":"l2Book","coin":"BTC"}), None).await;
    assert_eq!(status, StatusCode::OK, "body={body}");
    assert_eq!(body["coin"], "BTC");
}

#[tokio::test]
async fn runtime_overload_has_stable_category() {
    let app = app(Arc::new(OverloadedPort), market(None), 8);
    let (status, body) = post(app, "/info", json!({"type":"l2Book","coin":"BTC"}), None).await;
    assert_eq!(status, StatusCode::TOO_MANY_REQUESTS);
    assert_eq!(body["error"]["category"], "overloaded");
}

#[tokio::test]
async fn explicit_expiry_and_empty_batches_fail_closed() {
    let app = app(runtime(), market(None), 8);

    for expiry in [json!(null), json!(1_234)] {
        let mut request = exchange(json!({"type":"cancel","cancels":[{"a":0,"o":1}]}));
        request["expiresAfter"] = expiry;
        let (status, body) = post(app.clone(), "/exchange", request, Some(ALICE)).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(body["error"]["category"], "unsupported");
    }

    for action in
        [json!({"type":"order","orders":[],"grouping":"na"}), json!({"type":"cancel","cancels":[]})]
    {
        let (status, body) = post(app.clone(), "/exchange", exchange(action), Some(ALICE)).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(body["error"]["category"], "invalid_request");
    }
}

#[tokio::test]
async fn placement_derives_freshness_from_snapshot_timestamps() {
    for (observed_at, provider_stale, expected_status) in [
        (40_000, false, StatusCode::OK),
        (39_999, false, StatusCode::SERVICE_UNAVAILABLE),
        (100_001, false, StatusCode::SERVICE_UNAVAILABLE),
        (100_000, true, StatusCode::SERVICE_UNAVAILABLE),
    ] {
        let snapshot = MarketSnapshot {
            captured_at: 100_000,
            observations: AssetId::ALL
                .into_iter()
                .map(|asset| MarketObservation {
                    asset,
                    oracle_price: PriceTicks::new(1).unwrap(),
                    observed_at: if asset == AssetId::BTC { observed_at } else { 100_000 },
                    is_stale: asset == AssetId::BTC && provider_stale,
                })
                .collect(),
        };
        let app = app(runtime(), Arc::new(StaticMarket(snapshot)), 8);
        let request = exchange(json!({
            "type":"order",
            "orders":[order(0,true,"1","0.00001","Gtc")],
            "grouping":"na"
        }));
        let (status, body) = post(app, "/exchange", request, Some(ALICE)).await;
        assert_eq!(status, expected_status, "observed_at={observed_at}, body={body}");
        if expected_status == StatusCode::SERVICE_UNAVAILABLE {
            assert_eq!(body["error"]["category"], "oracle_stale");
        }
    }
}

#[tokio::test]
async fn cancel_does_not_read_market_snapshot() {
    let calls = Arc::new(AtomicUsize::new(0));
    let counting_market = CountingMarket {
        calls: Arc::clone(&calls),
        snapshot: MarketSnapshot { captured_at: 0, observations: Vec::new() },
    };
    let app = app(runtime(), Arc::new(counting_market), 8);
    let cancel = exchange(json!({"type":"cancel","cancels":[{"a":0,"o":1}]}));
    let (status, _) = post(app, "/exchange", cancel, Some(ALICE)).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(calls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn accepted_runtime_request_without_reply_times_out_stably() {
    let limits = RuntimeLimits::new(NonZeroUsize::MIN, NonZeroUsize::MIN);
    let (port, mut owner, _) = bounded_runtime(limits);
    tokio::spawn(async move {
        let _envelope = owner.recv().await.expect("request was accepted");
        tokio::time::sleep(Duration::from_secs(1)).await;
    });
    let config = HttpConfig {
        max_batch: 8,
        max_body_bytes: 16 * 1024,
        max_observation_age_ms: 60_000,
        runtime_reply_timeout: Duration::from_millis(10),
    };
    let app = app_with_config(Arc::new(port), market(None), config);

    let (status, body) = post(app, "/info", json!({"type":"l2Book","coin":"BTC"}), None).await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(body["error"]["category"], "internal");
    assert_eq!(body["error"]["message"], "runtime is unavailable");
}

fn applied(result: CommandResult) -> RuntimeReply {
    RuntimeReply::Applied(ApplyResult { result, events: Vec::new() })
}

fn resting_status(order_id: u64) -> PlacementStatus {
    PlacementStatus::Accepted {
        order_id,
        filled_lots: 0,
        remaining_lots: 1,
        disposition: PlacementDisposition::Resting,
    }
}

async fn assert_invalid_apply_reply(runtime: Arc<dyn RuntimePort>, action: Value) {
    let (status, body) =
        post(app(runtime, market(None), 8), "/exchange", exchange(action), Some(ALICE)).await;
    assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR, "body={body}");
    assert_eq!(body["error"]["category"], "internal");
    assert_eq!(body["error"]["message"], "runtime returned an invalid apply result");
}

#[tokio::test]
async fn order_rejects_wrong_inner_result_and_status_cardinality() {
    let action = || {
        json!({"type":"order","orders":[
            order(0,true,"60000.1","0.00001","Gtc"),
            order(1,true,"3000.25","0.0001","Gtc")
        ],"grouping":"na"})
    };
    let invalid_replies = [
        applied(CommandResult::Cancellation {
            statuses: vec![
                CancelStatus::Cancelled { order_id: 1, cancelled_lots: 1 },
                CancelStatus::Cancelled { order_id: 2, cancelled_lots: 1 },
            ],
        }),
        applied(CommandResult::Placement { statuses: vec![resting_status(1)] }),
        applied(CommandResult::Placement {
            statuses: vec![resting_status(1), resting_status(2), resting_status(3)],
        }),
    ];

    for reply in invalid_replies {
        assert_invalid_apply_reply(fake_runtime_reply(reply), action()).await;
    }
}

#[tokio::test]
async fn cancel_rejects_wrong_inner_result_and_status_cardinality() {
    let action = || json!({"type":"cancel","cancels":[{"a":0,"o":1},{"a":1,"o":2}]});
    let invalid_replies = [
        applied(CommandResult::Placement { statuses: vec![resting_status(1), resting_status(2)] }),
        applied(CommandResult::Cancellation {
            statuses: vec![CancelStatus::Cancelled { order_id: 1, cancelled_lots: 1 }],
        }),
        applied(CommandResult::Cancellation {
            statuses: vec![
                CancelStatus::Cancelled { order_id: 1, cancelled_lots: 1 },
                CancelStatus::Cancelled { order_id: 2, cancelled_lots: 1 },
                CancelStatus::Cancelled { order_id: 3, cancelled_lots: 1 },
            ],
        }),
    ];

    for reply in invalid_replies {
        assert_invalid_apply_reply(fake_runtime_reply(reply), action()).await;
    }
}
