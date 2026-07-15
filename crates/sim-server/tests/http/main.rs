use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use hl_wire::{AssetId, PriceTicks};
use http_body_util::BodyExt;
use serde_json::{Value, json};
use sim_core::Engine;
use sim_server::http::{
    HttpConfig, MarketObservation, MarketSnapshot, MarketView, MarketViewError, router_with_config,
};
use sim_server::{
    RuntimeError, RuntimeEvent, RuntimeLimits, RuntimePort, RuntimeReply, RuntimeRequest,
    bounded_runtime,
};
use std::{num::NonZeroUsize, sync::Arc};
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

fn market(stale: Option<AssetId>) -> Arc<dyn MarketView> {
    Arc::new(StaticMarket(MarketSnapshot {
        observations: vec![
            MarketObservation {
                asset: AssetId::BTC,
                oracle_price: PriceTicks::new(600_001).unwrap(),
                observed_at: 100,
                is_stale: stale == Some(AssetId::BTC),
            },
            MarketObservation {
                asset: AssetId::ETH,
                oracle_price: PriceTicks::new(300_025).unwrap(),
                observed_at: 101,
                is_stale: stale == Some(AssetId::ETH),
            },
            MarketObservation {
                asset: AssetId::SOL,
                oracle_price: PriceTicks::new(15_025).unwrap(),
                observed_at: 102,
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
    router_with_config(runtime, market, HttpConfig { max_batch, max_body_bytes: 16 * 1024 })
}

async fn post(
    app: axum::Router,
    path: &str,
    body: Value,
    user: Option<&str>,
) -> (StatusCode, Value) {
    let mut request = Request::post(path).header("content-type", "application/json");
    if let Some(user) = user {
        request = request.header("x-sim-user", user);
    }
    let response = app.oneshot(request.body(Body::from(body.to_string())).unwrap()).await.unwrap();
    let status = response.status();
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    (status, serde_json::from_slice(&bytes).unwrap())
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
        json!({"oraclePx":"60000.1","midPx":"60000.1","observedAt":100,"isStale":false})
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
async fn runtime_overload_has_stable_category() {
    let app = app(Arc::new(OverloadedPort), market(None), 8);
    let (status, body) = post(app, "/info", json!({"type":"l2Book","coin":"BTC"}), None).await;
    assert_eq!(status, StatusCode::TOO_MANY_REQUESTS);
    assert_eq!(body["error"]["category"], "overloaded");
}
