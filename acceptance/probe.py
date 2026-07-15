#!/usr/bin/env python3
"""Stdlib-only external Probe-A for transport and initial snapshots.

This deliberately stops before order placement, trades, cancellation, and stale
oracle behavior.  Its HTTP and RFC 6455 helpers are reusable by later probes.
"""

from __future__ import annotations

import argparse
import base64
import hashlib
import json
import os
import socket
import ssl
import struct
import sys
import time
from dataclasses import dataclass
from typing import Any, Optional
from urllib import error, parse, request

DEFAULT_USER = "0x1111111111111111111111111111111111111111"
DEFAULT_TIMEOUT = 5.0
MAX_TIMEOUT = 30.0
MAX_HTTP_BODY = 1 << 20
MAX_WS_PAYLOAD = 1 << 20
MAX_HANDSHAKE = 16 << 10
WS_GUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"


class ProbeFailure(RuntimeError):
    """Expected probe failure with a concise external diagnostic."""


class WebSocketEOF(ProbeFailure):
    """Peer closed the transport while a frame was being read."""


@dataclass(frozen=True)
class HttpResponse:
    status: int
    body: Any


def decode_http_json(status: int, raw: bytes, operation: str = "HTTP response") -> HttpResponse:
    """Decode a bounded JSON response, preserving bodies for every status."""
    if len(raw) > MAX_HTTP_BODY:
        raise ProbeFailure(f"{operation} body exceeds {MAX_HTTP_BODY} bytes")
    try:
        body = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ProbeFailure(f"{operation} returned invalid JSON (HTTP {status})") from exc
    return HttpResponse(status=status, body=body)


class HttpClient:
    """Small JSON HTTP client that captures JSON error bodies from non-2xx."""

    def __init__(self, base_url: str, timeout: float):
        parsed = parse.urlsplit(base_url)
        if (
            parsed.scheme not in ("http", "https")
            or not parsed.hostname
            or parsed.username
            or parsed.password
            or parsed.query
            or parsed.fragment
        ):
            raise ProbeFailure("--base-url must be an absolute http(s) URL without credentials, query, or fragment")
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout

    def _request(self, method: str, path: str, payload: Any = None, user: Optional[str] = None) -> HttpResponse:
        if not path.startswith("/"):
            raise ProbeFailure("HTTP request path must start with /")
        headers = {"Accept": "application/json"}
        data = None
        if payload is not None:
            data = json.dumps(payload, separators=(",", ":")).encode("utf-8")
            headers["Content-Type"] = "application/json"
        if user is not None:
            _validate_header_value(user, "user")
            headers["X-Sim-User"] = user
        outgoing = request.Request(self.base_url + path, data=data, headers=headers, method=method)
        try:
            with request.urlopen(outgoing, timeout=self.timeout) as response:
                status = response.status
                raw = response.read(MAX_HTTP_BODY + 1)
        except error.HTTPError as exc:
            status = exc.code
            raw = exc.read(MAX_HTTP_BODY + 1)
        except (error.URLError, TimeoutError, OSError) as exc:
            raise ProbeFailure(f"{method} {path} failed: {exc}") from exc
        return decode_http_json(status, raw, f"{method} {path}")

    def get(self, path: str) -> HttpResponse:
        return self._request("GET", path)

    def post_json(self, path: str, payload: Any, user: Optional[str] = None) -> HttpResponse:
        return self._request("POST", path, payload, user)


def websocket_accept(key: str) -> str:
    digest = hashlib.sha1((key + WS_GUID).encode("ascii")).digest()
    return base64.b64encode(digest).decode("ascii")


def parse_upgrade_response(raw: bytes, key: str) -> dict[str, str]:
    """Validate an RFC 6455 server Upgrade response and return its headers."""
    try:
        text = raw.decode("iso-8859-1")
    except UnicodeDecodeError as exc:  # defensive; ISO-8859-1 maps every byte
        raise ProbeFailure("WebSocket upgrade headers are invalid") from exc
    lines = text.split("\r\n")
    if not lines or lines[0] != "HTTP/1.1 101 Switching Protocols":
        raise ProbeFailure(f"WebSocket upgrade expected HTTP 101, got {lines[0] if lines else 'empty response'}")
    headers: dict[str, str] = {}
    for line in lines[1:]:
        if not line or ":" not in line:
            raise ProbeFailure("WebSocket upgrade returned a malformed header")
        name, value = line.split(":", 1)
        lowered = name.strip().lower()
        if not lowered or lowered in headers:
            raise ProbeFailure("WebSocket upgrade returned duplicate or empty header name")
        headers[lowered] = value.strip()
    upgrade_tokens = {item.strip().lower() for item in headers.get("upgrade", "").split(",")}
    connection_tokens = {item.strip().lower() for item in headers.get("connection", "").split(",")}
    if "websocket" not in upgrade_tokens or "upgrade" not in connection_tokens:
        raise ProbeFailure("WebSocket upgrade headers do not confirm Upgrade")
    if headers.get("sec-websocket-accept") != websocket_accept(key):
        raise ProbeFailure("WebSocket upgrade returned invalid Sec-WebSocket-Accept")
    return headers


def encode_client_frame(payload: bytes, opcode: int, mask_key: Optional[bytes] = None) -> bytes:
    """Encode one masked, final client text/ping/close frame."""
    if opcode not in (0x1, 0x9, 0xA, 0x8):
        raise ProbeFailure(f"unsupported client opcode {opcode}")
    if len(payload) > MAX_WS_PAYLOAD:
        raise ProbeFailure("WebSocket client payload exceeds configured bound")
    if opcode >= 0x8 and len(payload) > 125:
        raise ProbeFailure("WebSocket control payload exceeds 125 bytes")
    key = os.urandom(4) if mask_key is None else mask_key
    if len(key) != 4:
        raise ValueError("mask_key must be exactly four bytes")
    length = len(payload)
    if length < 126:
        header = struct.pack("!BB", 0x80 | opcode, 0x80 | length)
    elif length <= 0xFFFF:
        header = struct.pack("!BBH", 0x80 | opcode, 0x80 | 126, length)
    else:
        header = struct.pack("!BBQ", 0x80 | opcode, 0x80 | 127, length)
    masked = bytes(value ^ key[index % 4] for index, value in enumerate(payload))
    return header + key + masked


def _recv_exact(sock: Any, count: int) -> bytes:
    result = bytearray()
    while len(result) < count:
        chunk = sock.recv(count - len(result))
        if not chunk:
            raise WebSocketEOF("WebSocket peer closed during frame read")
        result.extend(chunk)
    return bytes(result)


def read_server_frame(sock: Any) -> tuple[int, bytes]:
    """Parse one bounded, unmasked, unfragmented server frame."""
    first, second = _recv_exact(sock, 2)
    fin = bool(first & 0x80)
    opcode = first & 0x0F
    if first & 0x70:
        raise ProbeFailure("WebSocket server frame uses unsupported RSV bits")
    if not fin:
        raise ProbeFailure("WebSocket fragmentation is unsupported")
    if opcode not in (0x1, 0x8, 0x9, 0xA):
        raise ProbeFailure(f"WebSocket server frame has invalid opcode {opcode}")
    if second & 0x80:
        raise ProbeFailure("WebSocket server frame must not be masked")
    length = second & 0x7F
    if length == 126:
        length = struct.unpack("!H", _recv_exact(sock, 2))[0]
        if length < 126:
            raise ProbeFailure("WebSocket frame uses non-minimal length encoding")
    elif length == 127:
        length = struct.unpack("!Q", _recv_exact(sock, 8))[0]
        if length < 65536 or length & (1 << 63):
            raise ProbeFailure("WebSocket frame uses invalid 64-bit length")
    if length > MAX_WS_PAYLOAD:
        raise ProbeFailure("WebSocket server payload exceeds configured bound")
    if opcode >= 0x8 and length > 125:
        raise ProbeFailure("WebSocket control frame exceeds 125 bytes")
    payload = _recv_exact(sock, length)
    if opcode == 0x8:
        if len(payload) == 1:
            raise ProbeFailure("WebSocket close frame has invalid payload length")
        if len(payload) >= 2:
            try:
                payload[2:].decode("utf-8")
            except UnicodeDecodeError as exc:
                raise ProbeFailure("WebSocket close reason is invalid UTF-8") from exc
    return opcode, payload


def _validate_header_value(value: str, label: str) -> None:
    if not value or "\r" in value or "\n" in value:
        raise ProbeFailure(f"{label} contains an invalid header value")


class WebSocketClient:
    """Minimal RFC 6455 ws/wss client for Probe-A and later probes."""

    def __init__(self, ws_url: str, timeout: float, user: Optional[str] = None):
        parsed = parse.urlsplit(ws_url)
        if (
            parsed.scheme not in ("ws", "wss")
            or not parsed.hostname
            or parsed.username
            or parsed.password
            or parsed.fragment
        ):
            raise ProbeFailure("--ws-url must be an absolute ws(s) URL without credentials or fragment")
        try:
            port = parsed.port or (443 if parsed.scheme == "wss" else 80)
        except ValueError as exc:
            raise ProbeFailure("--ws-url contains an invalid port") from exc
        raw = socket.create_connection((parsed.hostname, port), timeout=timeout)
        try:
            if parsed.scheme == "wss":
                raw = ssl.create_default_context().wrap_socket(raw, server_hostname=parsed.hostname)
            raw.settimeout(timeout)
            self.sock = raw
            self.timeout = timeout
            self.close_sent = False
            self.close_received = False
            key = base64.b64encode(os.urandom(16)).decode("ascii")
            path = parse.urlunsplit(("", "", parsed.path or "/", parsed.query, ""))
            default_port = 443 if parsed.scheme == "wss" else 80
            host = f"[{parsed.hostname}]" if ":" in parsed.hostname else parsed.hostname
            if port != default_port:
                host = f"{host}:{port}"
            lines = [
                f"GET {path} HTTP/1.1",
                f"Host: {host}",
                "Upgrade: websocket",
                "Connection: Upgrade",
                f"Sec-WebSocket-Key: {key}",
                "Sec-WebSocket-Version: 13",
            ]
            if user is not None:
                _validate_header_value(user, "user")
                lines.append(f"X-Sim-User: {user}")
            raw.sendall(("\r\n".join(lines) + "\r\n\r\n").encode("ascii"))
            parse_upgrade_response(self._read_upgrade(), key)
        except Exception:
            raw.close()
            raise

    def _read_upgrade(self) -> bytes:
        data = bytearray()
        while not data.endswith(b"\r\n\r\n"):
            if len(data) >= MAX_HANDSHAKE:
                raise ProbeFailure("WebSocket upgrade exceeds configured header bound")
            data.extend(_recv_exact(self.sock, 1))
        return bytes(data[:-4])

    def send_text_json(self, value: Any) -> None:
        payload = json.dumps(value, separators=(",", ":")).encode("utf-8")
        self.sock.sendall(encode_client_frame(payload, 0x1))

    def send_ping(self, payload: bytes) -> None:
        self.sock.sendall(encode_client_frame(payload, 0x9))

    def receive(self) -> tuple[int, bytes]:
        while True:
            try:
                opcode, payload = read_server_frame(self.sock)
            except socket.timeout as exc:
                raise ProbeFailure("WebSocket receive timed out") from exc
            if opcode == 0x9:
                self.sock.sendall(encode_client_frame(payload, 0xA))
                continue
            if opcode == 0x8:
                self.close_received = True
            return opcode, payload

    def receive_json(self) -> Any:
        opcode, payload = self.receive()
        if opcode != 0x1:
            raise ProbeFailure(f"expected WebSocket text frame, got opcode {opcode}")
        try:
            return json.loads(payload.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise ProbeFailure("WebSocket text frame contains invalid JSON") from exc

    def expect_pong(self, expected: bytes) -> None:
        opcode, payload = self.receive()
        if opcode != 0xA or payload != expected:
            raise ProbeFailure(f"expected matching WebSocket pong, got opcode={opcode} payload={payload!r}")

    def close_cleanly(self) -> None:
        if not self.close_sent:
            self.sock.sendall(encode_client_frame(struct.pack("!H", 1000), 0x8))
            self.close_sent = True
        if not self.close_received:
            for _ in range(16):
                try:
                    opcode, payload = self.receive()
                except WebSocketEOF:
                    # Some compliant server stacks complete their side of the
                    # closing handshake and immediately close the transport,
                    # so the echoed close frame is not observable to this
                    # minimal client.  EOF is clean only after our close frame.
                    break
                if opcode == 0xA:  # a previously queued pong may precede the close reply
                    continue
                if opcode != 0x8:
                    raise ProbeFailure(f"expected WebSocket close reply, got opcode {opcode}")
                if len(payload) >= 2 and struct.unpack("!H", payload[:2])[0] != 1000:
                    raise ProbeFailure("WebSocket peer did not close normally")
                break
            else:
                raise ProbeFailure("WebSocket peer did not send a close reply within frame bound")
        self.sock.close()

    def abort(self) -> None:
        self.sock.close()


def require(condition: bool, message: str) -> None:
    if not condition:
        raise ProbeFailure(message)


def require_ok(response: HttpResponse, operation: str) -> Any:
    if not 200 <= response.status < 300:
        raise ProbeFailure(f"{operation} returned HTTP {response.status}: {response.body!r}")
    return response.body


def subscribe(ws: WebSocketClient, subscription: dict[str, Any], channel: str) -> Any:
    ws.send_text_json({"method": "subscribe", "subscription": subscription})
    ack = ws.receive_json()
    expected = {
        "channel": "subscriptionResponse",
        "data": {"method": "subscribe", "subscription": subscription},
    }
    require(ack == expected, f"{channel} acknowledgement mismatch: {ack!r}")
    snapshot = ws.receive_json()
    require(isinstance(snapshot, dict) and snapshot.get("channel") == channel, f"missing {channel} snapshot")
    require(type(snapshot.get("sequence")) is int, f"{channel} snapshot lacks integer sequence")
    return snapshot


def run_probe(base_url: str, ws_url: str, timeout: float, user: str) -> dict[str, Any]:
    started = time.monotonic()
    http = HttpClient(base_url, timeout)
    health = require_ok(http.get("/healthz"), "healthz")
    ready = require_ok(http.get("/readyz"), "readyz")
    require(health == {"status": "alive"}, f"unexpected healthz body: {health!r}")
    require(ready == {"status": "ready"}, f"unexpected readyz body: {ready!r}")

    meta = require_ok(http.post_json("/info", {"type": "meta"}), "meta")
    require(isinstance(meta, dict) and isinstance(meta.get("universe"), list), "meta lacks universe")
    assets = [item.get("name") for item in meta["universe"] if isinstance(item, dict)]
    require(assets == ["BTC", "ETH", "SOL"], f"unexpected fixed universe: {assets!r}")
    mids = require_ok(http.post_json("/info", {"type": "allMids"}), "allMids")
    require(isinstance(mids, dict) and set(mids) == set(assets), "allMids does not cover fixed universe")
    book = require_ok(http.post_json("/info", {"type": "l2Book", "coin": "BTC"}), "l2Book")
    require(
        isinstance(book, dict)
        and book.get("coin") == "BTC"
        and isinstance(book.get("time"), int)
        and isinstance(book.get("levels"), list)
        and len(book["levels"]) == 2,
        f"invalid l2Book snapshot: {book!r}",
    )

    ws = WebSocketClient(ws_url, timeout, user)
    try:
        mids_ws = subscribe(ws, {"type": "allMids"}, "allMids")
        require(isinstance(mids_ws.get("data"), dict) and set(mids_ws["data"]) == set(assets), "invalid WS allMids snapshot")
        book_ws = subscribe(ws, {"type": "l2Book", "coin": "BTC"}, "l2Book")
        book_data = book_ws.get("data")
        require(
            isinstance(book_data, dict)
            and book_data.get("coin") == "BTC"
            and isinstance(book_data.get("time"), int)
            and isinstance(book_data.get("levels"), list)
            and len(book_data["levels"]) == 2,
            "invalid WS l2Book snapshot",
        )
        orders_ws = subscribe(ws, {"type": "orderUpdates", "user": user}, "orderUpdates")
        require(isinstance(orders_ws.get("data"), list), "invalid WS orderUpdates initial snapshot")
        ping_payload = b"probe-a"
        ws.send_ping(ping_payload)
        ws.expect_pong(ping_payload)
        ws.close_cleanly()
    except Exception:
        ws.abort()
        raise

    return {
        "result": "PASS",
        "probe": "A",
        "profile": "sim-header-v1",
        "checks": [
            "healthz",
            "readyz",
            "http_meta",
            "http_allMids",
            "http_l2Book",
            "ws_allMids",
            "ws_l2Book",
            "ws_orderUpdates",
            "ws_ping_pong",
            "ws_clean_close",
        ],
        "elapsed_ms": int((time.monotonic() - started) * 1000),
    }


def bounded_timeout(value: str) -> float:
    try:
        timeout = float(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("timeout must be numeric") from exc
    if not 0.1 <= timeout <= MAX_TIMEOUT:
        raise argparse.ArgumentTypeError(f"timeout must be between 0.1 and {MAX_TIMEOUT:g} seconds")
    return timeout


def main(argv: Optional[list[str]] = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base-url", required=True, help="HTTP(S) service origin")
    parser.add_argument("--ws-url", required=True, help="WS(S) service endpoint")
    parser.add_argument("--timeout", type=bounded_timeout, default=DEFAULT_TIMEOUT, help="per-I/O timeout in seconds (0.1-30)")
    parser.add_argument("--user", default=DEFAULT_USER, help="normalized synthetic user for private initial snapshot")
    args = parser.parse_args(argv)
    try:
        _validate_header_value(args.user, "user")
        summary = run_probe(args.base_url, args.ws_url, args.timeout, args.user)
    except Exception as exc:
        print(
            json.dumps(
                {"result": "FAIL", "probe": "A", "error": type(exc).__name__, "message": str(exc)},
                separators=(",", ":"),
                sort_keys=True,
            ),
            file=sys.stderr,
        )
        return 1
    print(json.dumps(summary, separators=(",", ":"), sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
