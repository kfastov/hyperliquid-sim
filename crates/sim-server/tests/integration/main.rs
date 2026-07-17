#![cfg(unix)]

use std::{
    io::{Read, Write},
    net::{SocketAddr, TcpListener, TcpStream},
    process::{Child, Command, Output, Stdio},
    thread,
    time::{Duration, Instant},
};

use futures_util::{SinkExt, StreamExt};
use serde_json::{Value, json};
use tokio_tungstenite::{
    connect_async,
    tungstenite::{ClientRequestBuilder, Message},
};

const PROCESS_BOUND: Duration = Duration::from_secs(5);
const ALICE: &str = "0x1111111111111111111111111111111111111111";
const BOB: &str = "0x2222222222222222222222222222222222222222";
const CHARLIE: &str = "0x3333333333333333333333333333333333333333";
const SIG: &str = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";

struct ChildGuard(Child);

impl Drop for ChildGuard {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn unused_loopback_addr() -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").expect("reserve ephemeral address");
    let address = listener.local_addr().expect("local address");
    drop(listener);
    address
}

fn server_command(address: SocketAddr) -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_sim-server"));
    command
        .env_clear()
        .env("SIM_BIND_ADDR", address.to_string())
        .env("SIM_ORACLE_MODE", "offline")
        .env("SIM_ACTOR_INTERVAL_MS", "10")
        .env("SIM_REPLY_TIMEOUT_MS", "500")
        .env("SIM_SHUTDOWN_TIMEOUT_MS", "1000");
    command
}

fn complete_http_response_len(response: &[u8]) -> Option<usize> {
    let header_end = response.windows(4).position(|window| window == b"\r\n\r\n")? + 4;
    let head = std::str::from_utf8(&response[..header_end]).ok()?;
    let content_length = head.lines().find_map(|line| {
        let (name, value) = line.split_once(':')?;
        name.eq_ignore_ascii_case("content-length")
            .then(|| value.trim().parse::<usize>().ok())
            .flatten()
    })?;
    header_end.checked_add(content_length)
}

fn read_http_response(stream: &mut TcpStream) -> std::io::Result<String> {
    let mut response = Vec::new();
    let mut buffer = [0_u8; 4096];
    loop {
        if let Some(expected_len) = complete_http_response_len(&response)
            && response.len() >= expected_len
        {
            response.truncate(expected_len);
            break;
        }
        match stream.read(&mut buffer) {
            Ok(0) => break,
            Ok(read) => response.extend_from_slice(&buffer[..read]),
            Err(error)
                if error.kind() == std::io::ErrorKind::ConnectionReset
                    && complete_http_response_len(&response)
                        .is_some_and(|expected_len| response.len() >= expected_len) =>
            {
                break;
            }
            Err(error) => return Err(error),
        }
    }
    let expected_len = complete_http_response_len(&response).ok_or_else(|| {
        std::io::Error::new(
            std::io::ErrorKind::UnexpectedEof,
            "HTTP response ended before complete headers and Content-Length body",
        )
    })?;
    if response.len() < expected_len {
        return Err(std::io::Error::new(
            std::io::ErrorKind::UnexpectedEof,
            format!("HTTP response ended at {} of {expected_len} bytes", response.len()),
        ));
    }
    String::from_utf8(response)
        .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidData, error.utf8_error()))
}

fn wait_for_response(address: SocketAddr, path: &str, expected_status: &str) -> String {
    let deadline = Instant::now() + PROCESS_BOUND;
    loop {
        match TcpStream::connect_timeout(&address, Duration::from_millis(100)) {
            Ok(mut stream) => {
                write!(
                    stream,
                    "GET {path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n"
                )
                .expect("write HTTP request");
                match read_http_response(&mut stream) {
                    Ok(response) if response.starts_with(expected_status) => return response,
                    Ok(response) => {
                        assert!(Instant::now() < deadline, "unexpected response: {response}");
                    }
                    Err(error) => {
                        assert!(
                            Instant::now() < deadline,
                            "HTTP listener did not return a complete response: {error}"
                        );
                    }
                }
                thread::sleep(Duration::from_millis(20));
            }
            Err(_) if Instant::now() < deadline => thread::sleep(Duration::from_millis(20)),
            Err(error) => panic!("HTTP listener did not start: {error}"),
        }
    }
}

fn request(
    address: SocketAddr,
    method: &str,
    path: &str,
    headers: &[(&str, &str)],
    body: &str,
) -> (u16, String) {
    let mut stream = TcpStream::connect_timeout(&address, Duration::from_millis(500))
        .expect("connect to server");
    stream.set_read_timeout(Some(PROCESS_BOUND)).expect("set read timeout");
    stream.set_write_timeout(Some(PROCESS_BOUND)).expect("set write timeout");
    write!(
        stream,
        "{method} {path} HTTP/1.1\r\nHost: localhost\r\nContent-Length: {}\r\nConnection: close\r\n",
        body.len()
    )
    .expect("write request line");
    for (name, value) in headers {
        write!(stream, "{name}: {value}\r\n").expect("write request header");
    }
    write!(stream, "\r\n{body}").expect("write request body");

    let response = read_http_response(&mut stream).expect("read complete HTTP response");
    let (head, body) = response.split_once("\r\n\r\n").expect("complete HTTP response");
    let status = head
        .lines()
        .next()
        .and_then(|line| line.split_whitespace().nth(1))
        .and_then(|status| status.parse().ok())
        .expect("numeric HTTP status");
    (status, body.to_owned())
}

fn post_json(address: SocketAddr, path: &str, user: Option<&str>, body: &str) -> (u16, Value) {
    let mut headers = vec![("Content-Type", "application/json")];
    if let Some(user) = user {
        headers.push(("X-Sim-User", user));
    }
    let (status, body) = request(address, "POST", path, &headers, body);
    (status, serde_json::from_str(&body).expect("JSON response"))
}

fn wait_for_exit(child: &mut Child) -> std::process::ExitStatus {
    let deadline = Instant::now() + PROCESS_BOUND;
    loop {
        if let Some(status) = child.try_wait().expect("poll child") {
            return status;
        }
        assert!(Instant::now() < deadline, "server did not exit within bound");
        thread::sleep(Duration::from_millis(20));
    }
}

fn run_bind_failure(address: SocketAddr) -> Output {
    server_command(address).output().expect("run bind-failure process")
}

async fn receive_ws_json<S>(socket: &mut tokio_tungstenite::WebSocketStream<S>) -> Value
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    loop {
        let message = tokio::time::timeout(PROCESS_BOUND, socket.next())
            .await
            .expect("WebSocket response exceeded process bound")
            .expect("WebSocket closed before response")
            .expect("valid WebSocket frame");
        if message.is_text() {
            return serde_json::from_str(message.to_text().expect("WebSocket text frame"))
                .expect("WebSocket JSON response");
        }
    }
}

async fn subscribe_and_snapshot<S>(
    socket: &mut tokio_tungstenite::WebSocketStream<S>,
    subscription: Value,
    channel: &str,
) -> Value
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    socket
        .send(Message::text(
            json!({"method":"subscribe","subscription":subscription.clone()}).to_string(),
        ))
        .await
        .expect("send subscription");
    assert_eq!(
        receive_ws_json(socket).await,
        json!({
            "channel":"subscriptionResponse",
            "data":{"method":"subscribe","subscription":subscription}
        })
    );
    let snapshot = receive_ws_json(socket).await;
    assert_eq!(snapshot["channel"], channel);
    snapshot
}

async fn subscribe_without_snapshot<S>(
    socket: &mut tokio_tungstenite::WebSocketStream<S>,
    subscription: Value,
) where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    socket
        .send(Message::text(
            json!({"method":"subscribe","subscription":subscription.clone()}).to_string(),
        ))
        .await
        .expect("send subscription");
    assert_eq!(
        receive_ws_json(socket).await,
        json!({
            "channel":"subscriptionResponse",
            "data":{"method":"subscribe","subscription":subscription}
        })
    );
}

async fn assert_no_ws_message<S>(socket: &mut tokio_tungstenite::WebSocketStream<S>)
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    assert!(
        tokio::time::timeout(Duration::from_millis(150), socket.next()).await.is_err(),
        "unrelated subscriber received a leaked event"
    );
}

#[test]
fn health_sigterm_zero_exit_and_port_reuse() {
    let address = unused_loopback_addr();
    let child = server_command(address)
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn server");
    let mut child = ChildGuard(child);

    let response = wait_for_response(address, "/healthz", "HTTP/1.1 200 OK\r\n");
    assert!(response.ends_with("\r\n\r\n{\"status\":\"alive\"}"), "{response}");
    let response = wait_for_response(address, "/readyz", "HTTP/1.1 200 OK\r\n");
    assert!(response.ends_with("\r\n\r\n{\"status\":\"ready\"}"), "{response}");

    let signal = Command::new("kill")
        .args(["-TERM", &child.0.id().to_string()])
        .status()
        .expect("send SIGTERM");
    assert!(signal.success());
    let status = wait_for_exit(&mut child.0);
    assert_eq!(status.code(), Some(0));

    let rebound = TcpListener::bind(address).expect("listener port is released after shutdown");
    drop(rebound);
    assert!(TcpStream::connect_timeout(&address, Duration::from_millis(100)).is_err());
}

#[test]
fn occupied_bind_fails_without_starting_a_service() {
    let occupied = TcpListener::bind("127.0.0.1:0").expect("occupy listener");
    let address = occupied.local_addr().expect("occupied address");
    let output = run_bind_failure(address);

    assert_eq!(output.status.code(), Some(1));
    let stderr = String::from_utf8(output.stderr).expect("utf8 stderr");
    assert!(stderr.contains("sim-server listener bind failed:"), "{stderr}");
    assert!(TcpStream::connect_timeout(&address, Duration::from_millis(100)).is_ok());
}

#[tokio::test(flavor = "multi_thread")]
async fn offline_websocket_process_exposes_truthful_initial_profile_and_bounded_shutdown() {
    let address = unused_loopback_addr();
    let child = server_command(address)
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn server");
    let mut child = ChildGuard(child);
    wait_for_response(address, "/readyz", "HTTP/1.1 200 OK\r\n");
    let url = format!("ws://{address}/ws");

    let (mut public, upgrade) = connect_async(&url).await.expect("public WebSocket upgrade");
    assert_eq!(upgrade.status(), 101);
    let mids = subscribe_and_snapshot(&mut public, json!({"type":"allMids"}), "allMids").await;
    assert_eq!(mids["data"], json!({"BTC":"100000","ETH":"3000","SOL":"150"}));
    assert!(mids["sequence"].as_u64().is_some());

    let book =
        subscribe_and_snapshot(&mut public, json!({"type":"l2Book","coin":"BTC"}), "l2Book").await;
    assert_eq!(book["data"], json!({"coin":"BTC","time":0,"levels":[[],[]]}));

    public.send(Message::text(r#"{"method":"ping"}"#)).await.expect("send JSON ping");
    assert_eq!(receive_ws_json(&mut public).await, json!({"channel":"pong"}));
    public.send(Message::Ping(vec![1, 2, 3].into())).await.expect("send frame ping");
    let pong = tokio::time::timeout(PROCESS_BOUND, public.next())
        .await
        .expect("frame pong bound")
        .expect("socket remains open")
        .expect("valid pong");
    assert_eq!(pong, Message::Pong(vec![1, 2, 3].into()));

    public
        .send(Message::text(
            r#"{"method":"subscribe","subscription":{"type":"candles","coin":"BTC"}}"#,
        ))
        .await
        .expect("send unsupported subscription");
    assert_eq!(receive_ws_json(&mut public).await["data"]["category"], "unsupported");
    public
        .send(Message::text(format!(
            r#"{{"method":"subscribe","subscription":{{"type":"orderUpdates","user":"{ALICE}"}}}}"#
        )))
        .await
        .expect("send unauthenticated private subscription");
    assert_eq!(receive_ws_json(&mut public).await["data"]["category"], "unauthorized_sim_user");

    let non_normalized = ClientRequestBuilder::new(url.parse().expect("WebSocket URI"))
        .with_header("X-Sim-User", ALICE.to_ascii_uppercase());
    let (mut malformed_identity, _) =
        connect_async(non_normalized).await.expect("upgrade with untrusted header");
    malformed_identity
        .send(Message::text(format!(
            r#"{{"method":"subscribe","subscription":{{"type":"orderUpdates","user":"{ALICE}"}}}}"#
        )))
        .await
        .expect("send private subscription");
    assert_eq!(
        receive_ws_json(&mut malformed_identity).await["data"]["category"],
        "unauthorized_sim_user"
    );
    malformed_identity.close(None).await.expect("close malformed identity socket");

    let authenticated = ClientRequestBuilder::new(url.parse().expect("WebSocket URI"))
        .with_header("X-Sim-User", ALICE);
    let (mut private, _) = connect_async(authenticated).await.expect("authenticated upgrade");
    let updates = subscribe_and_snapshot(
        &mut private,
        json!({"type":"orderUpdates","user":ALICE}),
        "orderUpdates",
    )
    .await;
    assert_eq!(updates["data"], json!([]));

    let signal = Command::new("kill")
        .args(["-TERM", &child.0.id().to_string()])
        .status()
        .expect("send SIGTERM");
    assert!(signal.success());
    let closed = tokio::time::timeout(PROCESS_BOUND, async {
        loop {
            match private.next().await {
                Some(Ok(message)) if message.is_close() => return true,
                None | Some(Err(_)) => return true,
                Some(Ok(_)) => {}
            }
        }
    })
    .await
    .expect("WebSocket closes within shutdown bound");
    assert!(closed);
    assert_eq!(wait_for_exit(&mut child.0).code(), Some(0));
    let rebound = TcpListener::bind(address).expect("listener released after WebSocket shutdown");
    drop(rebound);
}

#[tokio::test(flavor = "multi_thread")]
async fn offline_process_projects_private_fills_trades_and_sequenced_books_without_leaks() {
    let address = unused_loopback_addr();
    let child = server_command(address)
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn server");
    let mut child = ChildGuard(child);
    wait_for_response(address, "/readyz", "HTTP/1.1 200 OK\r\n");
    let url = format!("ws://{address}/ws");

    async fn private_socket(
        url: &str,
        user: &'static str,
    ) -> tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>
    {
        let request = ClientRequestBuilder::new(url.parse().expect("WebSocket URI"))
            .with_header("X-Sim-User", user);
        let (mut socket, _) = connect_async(request).await.expect("authenticated upgrade");
        let snapshot = subscribe_and_snapshot(
            &mut socket,
            json!({"type":"orderUpdates","user":user}),
            "orderUpdates",
        )
        .await;
        assert_eq!(snapshot["data"], json!([]));
        socket
    }

    let mut alice = private_socket(&url, ALICE).await;
    let mut bob = private_socket(&url, BOB).await;
    let mut unrelated = private_socket(&url, CHARLIE).await;
    let (mut public, _) = connect_async(&url).await.expect("public upgrade");
    let initial_book =
        subscribe_and_snapshot(&mut public, json!({"type":"l2Book","coin":"BTC"}), "l2Book").await;
    subscribe_without_snapshot(&mut public, json!({"type":"trades","coin":"BTC"})).await;
    let initial_sequence = initial_book["sequence"].as_u64().expect("book sequence");

    let maker_order = format!(
        r#"{{"action":{{"type":"order","orders":[{{"a":0,"b":false,"p":"100000","s":"0.00002","r":false,"t":{{"limit":{{"tif":"Gtc"}}}}}}],"grouping":"na"}},"nonce":1000,"signature":{{"r":"{SIG}","s":"{SIG}","v":27}},"vaultAddress":null}}"#
    );
    let (status, maker_reply) = post_json(address, "/exchange", Some(ALICE), &maker_order);
    assert_eq!(status, 200);
    let maker_id = maker_reply["response"]["data"]["statuses"][0]["resting"]["oid"]
        .as_u64()
        .expect("maker rests");
    let accepted = receive_ws_json(&mut alice).await;
    let rested = receive_ws_json(&mut alice).await;
    assert_eq!(accepted["data"][0]["status"], "open");
    assert_eq!(accepted["data"][0]["order"]["oid"], maker_id);
    assert_eq!(rested["data"][0]["status"], "open");
    assert!(accepted["sequence"].as_u64().unwrap() > initial_sequence);
    let rested_book = receive_ws_json(&mut public).await;
    assert_eq!(rested_book["channel"], "l2Book");
    assert_eq!(rested_book["data"]["levels"][1][0]["sz"], "0.00002");
    assert_no_ws_message(&mut bob).await;
    assert_no_ws_message(&mut unrelated).await;

    let taker_order = format!(
        r#"{{"action":{{"type":"order","orders":[{{"a":0,"b":true,"p":"100001","s":"0.00001","r":false,"t":{{"limit":{{"tif":"Gtc"}}}}}}],"grouping":"na"}},"nonce":1001,"signature":{{"r":"{SIG}","s":"{SIG}","v":27}},"vaultAddress":null}}"#
    );
    let (status, taker_reply) = post_json(address, "/exchange", Some(BOB), &taker_order);
    assert_eq!(status, 200);
    assert!(taker_reply["response"]["data"]["statuses"][0]["filled"].is_object());

    let alice_fill = receive_ws_json(&mut alice).await;
    let alice_final = receive_ws_json(&mut alice).await;
    assert_eq!(alice_fill["data"][0]["status"], "fill");
    assert_eq!(alice_fill["data"][0]["order"]["oid"], maker_id);
    assert_eq!(alice_fill["data"][0]["fill"]["px"], "100000");
    assert_eq!(alice_fill["data"][0]["fill"]["sz"], "0.00001");
    assert_eq!(alice_final["data"][0]["status"], "open");

    let bob_accepted = receive_ws_json(&mut bob).await;
    let bob_fill = receive_ws_json(&mut bob).await;
    let bob_final = receive_ws_json(&mut bob).await;
    assert_eq!(bob_accepted["data"][0]["status"], "open");
    assert_eq!(bob_fill["data"][0]["status"], "fill");
    assert_eq!(bob_fill["data"][0]["fill"]["px"], "100000");
    assert_eq!(bob_final["data"][0]["status"], "filled");
    assert_eq!(alice_fill["sequence"], bob_fill["sequence"]);

    let fill_sequence = alice_fill["sequence"].as_u64().expect("fill sequence");
    let mut saw_trade = false;
    let mut saw_fill_book = false;
    for _ in 0..4 {
        let message = receive_ws_json(&mut public).await;
        if message["channel"] == "trades" {
            assert_eq!(message["sequence"], fill_sequence);
            assert_eq!(message["data"][0]["px"], "100000");
            assert_eq!(message["data"][0]["sz"], "0.00001");
            assert_eq!(message["data"][0]["side"], "B");
            saw_trade = true;
        }
        if message["channel"] == "l2Book" && message["sequence"] == fill_sequence {
            assert_eq!(message["data"]["levels"][1][0]["sz"], "0.00001");
            saw_fill_book = true;
        }
    }
    assert!(saw_trade, "fill must reach trades subscriber");
    assert!(saw_fill_book, "fill sequence must reach l2 subscriber");
    assert_no_ws_message(&mut unrelated).await;

    let cancel = format!(
        r#"{{"action":{{"type":"cancel","cancels":[{{"a":0,"o":{maker_id}}}]}},"nonce":1002,"signature":{{"r":"{SIG}","s":"{SIG}","v":27}},"vaultAddress":null}}"#
    );
    let (status, cancelled) = post_json(address, "/exchange", Some(ALICE), &cancel);
    assert_eq!(status, 200);
    assert_eq!(cancelled["response"]["data"]["statuses"], json!(["success"]));
    let alice_cancel = receive_ws_json(&mut alice).await;
    assert_eq!(alice_cancel["data"][0]["status"], "canceled");
    assert_eq!(alice_cancel["data"][0]["order"]["oid"], maker_id);
    assert_no_ws_message(&mut bob).await;
    assert_no_ws_message(&mut unrelated).await;

    let signal = Command::new("kill")
        .args(["-TERM", &child.0.id().to_string()])
        .status()
        .expect("send SIGTERM");
    assert!(signal.success());
    assert_eq!(wait_for_exit(&mut child.0).code(), Some(0));
}

#[test]
fn offline_http_surface_is_local_bounded_and_stateful() {
    let address = unused_loopback_addr();
    let upstream = TcpListener::bind("127.0.0.1:0").expect("bind upstream tripwire");
    upstream.set_nonblocking(true).expect("make upstream tripwire nonblocking");
    let upstream_address = upstream.local_addr().expect("upstream tripwire address");

    let child = server_command(address)
        .env("SIM_ORACLE_INFO_URL", format!("http://{upstream_address}/info"))
        .env("SIM_ORACLE_WSS_URL", format!("ws://{upstream_address}/ws"))
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn server");
    let mut child = ChildGuard(child);
    wait_for_response(address, "/readyz", "HTTP/1.1 200 OK\r\n");

    let (status, meta) = post_json(address, "/info", None, r#"{"type":"meta"}"#);
    assert_eq!(status, 200);
    assert_eq!(
        meta,
        json!({"universe":[{"name":"BTC","szDecimals":5},{"name":"ETH","szDecimals":4},{"name":"SOL","szDecimals":2}]})
    );
    let (status, mids) = post_json(address, "/info", None, r#"{"type":"allMids"}"#);
    assert_eq!(status, 200);
    assert_eq!(mids, json!({"BTC":"100000","ETH":"3000","SOL":"150"}));
    let (status, book) = post_json(address, "/info", None, r#"{"type":"l2Book","coin":"BTC"}"#);
    assert_eq!(status, 200);
    assert_eq!(book, json!({"coin":"BTC","time":0,"levels":[[],[]]}));

    let open_orders = format!(r#"{{"type":"openOrders","user":"{BOB}"}}"#);
    let (status, orders) = post_json(address, "/info", Some(ALICE), &open_orders);
    assert_eq!(status, 200);
    assert_eq!(orders, json!([]));

    let order = format!(
        r#"{{"action":{{"type":"order","orders":[{{"a":0,"b":true,"p":"99999.9","s":"0.00001","r":false,"t":{{"limit":{{"tif":"Gtc"}}}}}}],"grouping":"na"}},"nonce":900,"signature":{{"r":"{SIG}","s":"{SIG}","v":27}},"vaultAddress":null}}"#
    );
    let (status, ordered) = post_json(address, "/exchange", Some(ALICE), &order);
    assert_eq!(status, 200);
    assert_eq!(ordered["status"], "ok");
    let statuses = ordered["response"]["data"]["statuses"].as_array().expect("ordered statuses");
    assert_eq!(statuses.len(), 1);
    let order_id = statuses[0]["resting"]["oid"].as_u64().expect("resting order id");

    let (status, orders) = post_json(address, "/info", Some(ALICE), &open_orders);
    assert_eq!(status, 200);
    assert_eq!(
        orders,
        json!([{
            "coin":"BTC", "limitPx":"99999.9", "oid":order_id, "side":"B",
            "sz":"0.00001", "timestamp":1, "origSz":"0.00001", "cloid":null
        }])
    );

    let cancel = format!(
        r#"{{"action":{{"type":"cancel","cancels":[{{"a":0,"o":{order_id}}}]}},"nonce":901,"signature":{{"r":"{SIG}","s":"{SIG}","v":27}},"vaultAddress":null}}"#
    );
    let (status, cancelled) = post_json(address, "/exchange", Some(ALICE), &cancel);
    assert_eq!(status, 200);
    assert_eq!(cancelled["response"]["data"]["statuses"], json!(["success"]));
    assert_eq!(post_json(address, "/info", Some(ALICE), &open_orders), (200, json!([])));

    for content_type in [None, Some("text/plain")] {
        let headers = content_type.map_or_else(Vec::new, |value| vec![("Content-Type", value)]);
        let (status, body) = request(address, "POST", "/info", &headers, r#"{"type":"meta"}"#);
        assert_eq!(status, 415);
        assert_eq!(
            serde_json::from_str::<Value>(&body).expect("415 JSON"),
            json!({"error":{"category":"invalid_request","message":"Content-Type must be application/json"}})
        );
    }
    let (status, body) = post_json(address, "/info", Some("0x1234"), &open_orders);
    assert_eq!(status, 401);
    assert_eq!(
        body,
        json!({"error":{"category":"unauthorized_sim_user","message":"a normalized X-Sim-User header is required"}})
    );

    assert!(
        wait_for_response(address, "/healthz", "HTTP/1.1 200 OK\r\n")
            .ends_with("\r\n\r\n{\"status\":\"alive\"}")
    );
    assert!(
        wait_for_response(address, "/readyz", "HTTP/1.1 200 OK\r\n")
            .ends_with("\r\n\r\n{\"status\":\"ready\"}")
    );
    assert!(
        matches!(upstream.accept(), Err(error) if error.kind() == std::io::ErrorKind::WouldBlock),
        "offline mode must not expose any upstream request surface"
    );

    let signal = Command::new("kill")
        .args(["-TERM", &child.0.id().to_string()])
        .status()
        .expect("send SIGTERM");
    assert!(signal.success());
    assert_eq!(wait_for_exit(&mut child.0).code(), Some(0));
}
