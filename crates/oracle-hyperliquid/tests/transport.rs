use futures_util::{SinkExt, StreamExt};
use hl_wire::{AssetId, DecimalScale};
use oracle_hyperliquid::{
    Clock, LiveConfig, LiveTransportError, NoJitter, OracleIngestEvent, OracleOrchestrator,
    OracleTransport, OrchestratorExit, PriceScales, ReconnectBackoff, TokioHyperliquidTransport,
    TokioSleeper,
};
use serde_json::{Value, json};
use std::{
    sync::{
        Arc,
        atomic::{AtomicU64, AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio::net::{TcpListener, TcpStream};
use tokio_tungstenite::{MaybeTlsStream, accept_async, tungstenite::Message};

#[derive(Clone)]
struct RecordingClock {
    now_ms: Arc<AtomicU64>,
    reads: Arc<AtomicUsize>,
}

impl Clock for RecordingClock {
    fn now_ms(&self) -> u64 {
        self.reads.fetch_add(1, Ordering::SeqCst);
        self.now_ms.load(Ordering::SeqCst)
    }
}

fn scales() -> PriceScales {
    let scale = DecimalScale::new(2).expect("valid scale");
    PriceScales::new(scale, scale, scale)
}

#[test]
fn default_mainnet_config_compiles_with_rustls_and_webpki_roots() {
    fn recognizes_rustls_stream(stream: &MaybeTlsStream<TcpStream>) -> bool {
        matches!(stream, MaybeTlsStream::Rustls(_))
    }

    let _compile_time_rustls_feature_check: fn(&MaybeTlsStream<TcpStream>) -> bool =
        recognizes_rustls_stream;
    assert!(LiveConfig::default().ws_url.starts_with("wss://"));
    assert!(include_str!("../../../Cargo.toml").contains(
        "tokio-tungstenite = { version = \"0.29\", default-features = false, features = [\"connect\", \"rustls-tls-webpki-roots\"] }"
    ));
}

#[tokio::test]
async fn live_transport_subscribes_reads_heartbeats_isolates_bad_data_and_disconnects_cleanly() {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind fake WS");
    let address = listener.local_addr().expect("fake WS address");
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.expect("accept client");
        let mut ws = accept_async(stream).await.expect("accept websocket");

        let mut subscriptions = Vec::new();
        for _ in 0..3 {
            let Message::Text(text) =
                ws.next().await.expect("subscription frame").expect("frame ok")
            else {
                panic!("subscription must be text");
            };
            subscriptions.push(serde_json::from_str::<Value>(&text).expect("subscription JSON"));
        }
        assert_eq!(
            subscriptions,
            [
                json!({"method":"subscribe","subscription":{"type":"activeAssetCtx","coin":"BTC"}}),
                json!({"method":"subscribe","subscription":{"type":"activeAssetCtx","coin":"ETH"}}),
                json!({"method":"subscribe","subscription":{"type":"activeAssetCtx","coin":"SOL"}}),
            ]
        );

        for payload in [
            "not-json".to_owned(),
            json!({"channel":"activeSpotAssetCtx","data":{"coin":"BTC","ctx":{"oraclePx":"1"}}})
                .to_string(),
            json!({"channel":"activeAssetCtx","data":{"coin":"DOGE","ctx":{"oraclePx":"2"}}})
                .to_string(),
            json!({"channel":"activeAssetCtx","data":{"coin":"ETH","ctx":{"oraclePx":"not-a-price"}}})
                .to_string(),
            json!({"channel":"activeAssetCtx","data":{"coin":"ETH","ctx":{"oraclePx":"0"}}})
                .to_string(),
            json!({"channel":"activeAssetCtx","data":{"coin":"ETH","ctx":{"oraclePx":"1.001"}}})
                .to_string(),
        ] {
            ws.send(Message::Text(payload.into())).await.expect("send isolated bad payload");
        }
        ws.send(Message::Ping(vec![1, 2, 3].into())).await.expect("send transport ping");

        let mut saw_transport_pong = false;
        let mut saw_json_heartbeat = false;
        while !saw_transport_pong || !saw_json_heartbeat {
            match ws.next().await.expect("client control frame").expect("control frame ok") {
                Message::Pong(payload) if payload.as_ref() == [1, 2, 3] => {
                    saw_transport_pong = true;
                }
                Message::Text(text)
                    if serde_json::from_str::<Value>(&text).expect("heartbeat JSON")
                        == json!({"method":"ping"}) =>
                {
                    saw_json_heartbeat = true;
                    ws.send(Message::Text(json!({"channel":"pong"}).to_string().into()))
                        .await
                        .expect("send JSON pong");
                }
                other => panic!("unexpected client frame: {other:?}"),
            }
        }

        ws.send(Message::Text(
            json!({
                "channel":"activeAssetCtx",
                "data":{"coin":"BTC","ctx":{"oraclePx":"60123.40"}}
            })
            .to_string()
            .into(),
        ))
        .await
        .expect("send valid update");
        ws.close(None).await.expect("clean close");
    });

    let now_ms = Arc::new(AtomicU64::new(77_777));
    let reads = Arc::new(AtomicUsize::new(0));
    let clock = RecordingClock { now_ms: Arc::clone(&now_ms), reads: Arc::clone(&reads) };
    let config = LiveConfig {
        ws_url: format!("ws://{address}"),
        info_url: "http://127.0.0.1:1/info".to_owned(),
        connect_timeout: Duration::from_secs(1),
        write_timeout: Duration::from_secs(1),
        read_timeout: Duration::from_secs(1),
        send_timeout: Duration::from_secs(1),
        heartbeat_interval: Duration::from_millis(20),
    };
    let mut transport = TokioHyperliquidTransport::new(config, clock);

    transport.connect().await.expect("connect and fixed subscriptions");
    assert_eq!(reads.load(Ordering::SeqCst), 0, "clock is not sampled before a payload");
    let observation = transport
        .next_active_asset_ctx(9, scales())
        .await
        .expect("valid update survives malformed messages and control traffic");
    assert_eq!(observation.asset, AssetId::BTC);
    assert_eq!(observation.price.value(), 6_012_340);
    assert_eq!(observation.observed_at_ms, 77_777);
    assert_eq!(observation.upstream_sequence, 9);
    assert_eq!(reads.load(Ordering::SeqCst), 1);

    let disconnect = transport
        .next_active_asset_ctx(10, scales())
        .await
        .expect_err("clean close is a reconnect signal");
    assert!(matches!(disconnect, LiveTransportError::Disconnected));
    assert!(disconnect.is_reconnectable());
    server.await.expect("fake WS server completed");
}

#[tokio::test]
async fn malformed_eth_then_valid_btc_stays_on_ws_without_unavailable_or_fallback() {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind fake WS");
    let address = listener.local_addr().expect("fake WS address");
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.expect("accept client");
        let mut ws = accept_async(stream).await.expect("accept websocket");
        for _ in 0..3 {
            ws.next().await.expect("subscription frame").expect("frame ok");
        }
        ws.send(Message::Text(
            json!({
                "channel":"activeAssetCtx",
                "data":{"coin":"ETH","ctx":{"oraclePx":"malformed"}}
            })
            .to_string()
            .into(),
        ))
        .await
        .expect("send malformed ETH");
        ws.send(Message::Text(
            json!({
                "channel":"activeAssetCtx",
                "data":{"coin":"BTC","ctx":{"oraclePx":"60123.40"}}
            })
            .to_string()
            .into(),
        ))
        .await
        .expect("send valid BTC");
        while ws.next().await.is_some() {}
    });

    let receive_reads = Arc::new(AtomicUsize::new(0));
    let receive_clock = RecordingClock {
        now_ms: Arc::new(AtomicU64::new(88_888)),
        reads: Arc::clone(&receive_reads),
    };
    let unavailable_reads = Arc::new(AtomicUsize::new(0));
    let unavailable_clock = RecordingClock {
        now_ms: Arc::new(AtomicU64::new(99_999)),
        reads: Arc::clone(&unavailable_reads),
    };
    let config = LiveConfig {
        ws_url: format!("ws://{address}"),
        info_url: "http://127.0.0.1:1/info".to_owned(),
        connect_timeout: Duration::from_secs(1),
        write_timeout: Duration::from_secs(1),
        read_timeout: Duration::from_secs(1),
        send_timeout: Duration::from_secs(1),
        heartbeat_interval: Duration::from_millis(500),
    };
    let factory = move || TokioHyperliquidTransport::new(config.clone(), receive_clock.clone());
    let orchestrator = OracleOrchestrator::new(
        factory,
        TokioSleeper,
        NoJitter,
        unavailable_clock,
        ReconnectBackoff::new(Duration::from_secs(1), Duration::from_secs(1)),
        scales(),
    );
    let (output_tx, mut output_rx) = tokio::sync::mpsc::channel(8);
    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    let task = tokio::spawn(orchestrator.run(output_tx, shutdown_rx));

    let event = tokio::time::timeout(Duration::from_secs(1), output_rx.recv())
        .await
        .expect("orchestrator produces a WS observation")
        .expect("output remains open");
    let OracleIngestEvent::Observation(observation) = event else {
        panic!("malformed ETH must not emit unavailable: {event:?}");
    };
    assert_eq!(observation.asset, AssetId::BTC);
    assert_eq!(observation.price.value(), 6_012_340);
    assert_eq!(observation.observed_at_ms, 88_888);
    assert_eq!(receive_reads.load(Ordering::SeqCst), 1);
    assert_eq!(unavailable_reads.load(Ordering::SeqCst), 0, "fallback path was not entered");

    shutdown_tx.send(true).expect("orchestrator watches shutdown");
    assert_eq!(task.await.expect("orchestrator task"), OrchestratorExit::Shutdown);
    assert!(output_rx.try_recv().is_err(), "no unavailable or fallback event was emitted");
    server.await.expect("fake WS server completed");
}
