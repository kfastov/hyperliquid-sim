#!/usr/bin/env python3
"""Stdlib-only external Probe-A/B1 plus explicit trade/private flows.

The default (or explicit ``--state-flow``) preserves Probe-B1 placement/cancel.
``--trade-flow`` runs one public maker/taker BTC match. ``--private-flow``
proves private maker/taker order-update correspondence plus an unrelated-user
negative isolation window. ``--basic-only`` runs only the Probe-A
transport/snapshot path.
"""

from __future__ import annotations

import argparse
import base64
import hashlib
import json
import math
import os
import re
import socket
import ssl
import struct
import sys
import time
from dataclasses import dataclass
from typing import Any, Callable, Optional
from urllib import error, parse, request

DEFAULT_USER = "0x1111111111111111111111111111111111111111"
DEFAULT_TAKER = "0x2222222222222222222222222222222222222222"
DEFAULT_UNRELATED = "0x3333333333333333333333333333333333333333"
SIGNATURE_COMPONENT = "0x" + "a" * 64
DEFAULT_TIMEOUT = 5.0
MAX_TIMEOUT = 30.0
MAX_HTTP_BODY = 1 << 20
MAX_WS_PAYLOAD = 1 << 20
MAX_HANDSHAKE = 16 << 10
MAX_FILTERED_MESSAGES = 16
DUPLICATE_WINDOW = 0.25
WS_GUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"
CANONICAL_DECIMAL = re.compile(r"^(?:0|[1-9][0-9]*)(?:\.[0-9]*[1-9])?$")
NORMALIZED_USER = re.compile(r"^0x[0-9a-f]{40}$")


class ProbeFailure(RuntimeError):
    """Expected probe failure with a concise external diagnostic."""


class WebSocketEOF(ProbeFailure):
    """Peer closed the transport while a frame was being read."""


class WebSocketTimeout(ProbeFailure):
    """A bounded WebSocket poll found no complete frame."""


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
                raise WebSocketTimeout("WebSocket receive timed out") from exc
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

    def poll_json(self, timeout: float) -> Any:
        previous = self.sock.gettimeout()
        self.sock.settimeout(timeout)
        try:
            return self.receive_json()
        except WebSocketTimeout:
            return None
        finally:
            self.sock.settimeout(previous)

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


def subscribe_ack_only(ws: WebSocketClient, subscription: dict[str, Any], channel: str) -> None:
    ws.send_text_json({"method": "subscribe", "subscription": subscription})
    ack = ws.receive_json()
    expected = {
        "channel": "subscriptionResponse",
        "data": {"method": "subscribe", "subscription": subscription},
    }
    require(ack == expected, f"{channel} acknowledgement mismatch: {ack!r}")


def require_canonical(value: Any, expected: str, label: str) -> None:
    require(
        type(value) is str and CANONICAL_DECIMAL.fullmatch(value) is not None,
        f"{label} is not a canonical decimal string: {value!r}",
    )
    require(value == expected, f"{label} expected {expected!r}, got {value!r}")


def exchange_envelope(action: dict[str, Any], nonce: int) -> dict[str, Any]:
    return {
        "action": action,
        "nonce": nonce,
        "signature": {"r": SIGNATURE_COMPONENT, "s": SIGNATURE_COMPONENT, "v": 27},
        "vaultAddress": None,
    }


def ordered_status(body: Any, response_type: str, operation: str) -> Any:
    require(isinstance(body, dict) and set(body) == {"status", "response"}, f"{operation} response shape mismatch")
    require(body["status"] == "ok", f"{operation} response status is not ok")
    response = body["response"]
    require(isinstance(response, dict) and set(response) == {"type", "data"}, f"{operation} inner response shape mismatch")
    require(response["type"] == response_type, f"{operation} response type mismatch")
    data = response["data"]
    require(isinstance(data, dict) and set(data) == {"statuses"}, f"{operation} data shape mismatch")
    statuses = data["statuses"]
    require(isinstance(statuses, list) and len(statuses) == 1, f"{operation} expected exactly one ordered status")
    return statuses[0]


def validate_owned_update(message: Any, oid: int, status: str, previous_sequence: int) -> int:
    require(isinstance(message, dict) and set(message) == {"channel", "sequence", "data"}, "order update envelope shape mismatch")
    require(message["channel"] == "orderUpdates", "expected orderUpdates channel")
    sequence = message["sequence"]
    require(type(sequence) is int and sequence > previous_sequence, "order update sequence is not monotonic")
    require(isinstance(message["data"], list) and len(message["data"]) == 1, "order update cardinality mismatch")
    update = message["data"][0]
    require(
        isinstance(update, dict) and set(update) == {"order", "status", "statusTimestamp"},
        f"order update shape mismatch: {update!r}",
    )
    require(update["status"] == status and type(update["statusTimestamp"]) is int, "order update status/timestamp mismatch")
    order = update["order"]
    require(
        isinstance(order, dict)
        and set(order) == {"coin", "side", "limitPx", "sz", "oid", "timestamp", "origSz", "cloid"},
        f"owned order shape mismatch: {order!r}",
    )
    require(order["coin"] == "BTC" and order["side"] == "B" and order["oid"] == oid, "owned order identity mismatch")
    require(type(order["timestamp"]) is int and order["cloid"] is None, "owned order timestamp/cloid mismatch")
    require_canonical(order["limitPx"], "99999.9", "owned order limitPx")
    require_canonical(order["sz"], "0.00001", "owned order sz")
    require_canonical(order["origSz"], "0.00001", "owned order origSz")
    return sequence


def receive_owned_update(ws: WebSocketClient, oid: int, status: str, previous_sequence: int) -> int:
    for _ in range(MAX_FILTERED_MESSAGES):
        message = ws.receive_json()
        if not isinstance(message, dict) or message.get("channel") != "orderUpdates":
            continue
        data = message.get("data")
        if not isinstance(data, list) or len(data) != 1 or not isinstance(data[0], dict):
            continue
        order = data[0].get("order")
        if isinstance(order, dict) and order.get("oid") == oid and data[0].get("status") == status:
            return validate_owned_update(message, oid, status, previous_sequence)
    raise ProbeFailure(f"did not receive owned {status} update within {MAX_FILTERED_MESSAGES} filtered reads")


def validate_open_order(order: Any, oid: int) -> None:
    require(
        isinstance(order, dict)
        and set(order) == {"coin", "limitPx", "oid", "side", "sz", "timestamp", "origSz", "cloid"},
        f"open order shape mismatch: {order!r}",
    )
    require(order["coin"] == "BTC" and order["side"] == "B" and order["oid"] == oid, "open order identity mismatch")
    require(type(order["timestamp"]) is int and order["cloid"] is None, "open order timestamp/cloid mismatch")
    require_canonical(order["limitPx"], "99999.9", "open order limitPx")
    require_canonical(order["sz"], "0.00001", "open order sz")
    require_canonical(order["origSz"], "0.00001", "open order origSz")


def run_stale_cancel_flow(
    http: Any,
    wait_seconds: float,
    user: str,
    sleeper: Callable[[float], None] = time.sleep,
    clock: Callable[[], float] = time.monotonic,
) -> dict[str, int]:
    """Prove BTC placement stales while cancellation and queries stay available.

    The HTTP boundary may be a client exposing ``post_json`` or an equivalent
    injected callable. The sleeper and monotonic clock are injected so the
    complete greater-than-60-second transcript is deterministic in unit tests.
    This helper intentionally has no CLI wiring yet.
    """
    require(
        type(user) is str and NORMALIZED_USER.fullmatch(user) is not None,
        "stale flow requires a normalized lowercase user",
    )
    require(
        type(wait_seconds) in (int, float)
        and math.isfinite(wait_seconds)
        and wait_seconds > 60.0,
        "stale flow wait must be finite and strictly greater than 60 seconds",
    )
    post_json = http.post_json if hasattr(http, "post_json") else http
    require(callable(post_json), "stale flow HTTP boundary must be a client or post_json callable")
    require(callable(sleeper) and callable(clock), "stale flow sleeper and clock must be callable")

    order_action = {
        "type": "order",
        "orders": [{
            "a": 0, "b": True, "p": "99999.9", "s": "0.00001", "r": False,
            "t": {"limit": {"tif": "Gtc"}},
        }],
        "grouping": "na",
    }
    placed_response = post_json("/exchange", exchange_envelope(order_action, 4000), user)
    require(
        isinstance(placed_response, HttpResponse),
        "fresh BTC GTC placement returned malformed HTTP response",
    )
    placed = require_ok(placed_response, "fresh BTC GTC placement")
    placement_status = ordered_status(placed, "order", "fresh BTC GTC placement")
    require(
        isinstance(placement_status, dict) and set(placement_status) == {"resting"},
        "fresh BTC GTC placement did not return exact resting status",
    )
    resting = placement_status["resting"]
    require(
        isinstance(resting, dict)
        and set(resting) == {"oid"}
        and type(resting["oid"]) is int,
        "fresh BTC GTC resting oid shape mismatch",
    )
    preserved_oid = resting["oid"]

    started_raw = clock()
    require(type(started_raw) in (int, float), "stale flow clock returned invalid time")
    started = float(started_raw)
    require(math.isfinite(started), "stale flow clock returned invalid time")
    sleeper(wait_seconds)
    ended_raw = clock()
    require(type(ended_raw) in (int, float), "stale flow clock returned invalid time")
    ended = float(ended_raw)
    require(math.isfinite(ended), "stale flow clock returned invalid time")
    require(ended - started >= wait_seconds, "stale flow wait ended before configured duration elapsed")

    rejected = post_json("/exchange", exchange_envelope(order_action, 4001), user)
    require(isinstance(rejected, HttpResponse), "stale BTC placement returned malformed HTTP response")
    require(
        rejected.status == 503,
        f"stale BTC placement expected HTTP 503, got HTTP {rejected.status}",
    )
    require(
        isinstance(rejected.body, dict) and set(rejected.body) == {"error"},
        "stale BTC placement error envelope shape mismatch",
    )
    detail = rejected.body["error"]
    require(
        isinstance(detail, dict) and set(detail) == {"category", "message"},
        "stale BTC placement error detail shape mismatch",
    )
    require(detail["category"] == "oracle_stale", "stale BTC placement error category mismatch")
    require(
        detail["message"] == "oracle observation is stale for BTC",
        "stale placement error does not attribute BTC in the accepted envelope",
    )

    cancel_action = {"type": "cancel", "cancels": [{"a": 0, "o": preserved_oid}]}
    canceled_response = post_json("/exchange", exchange_envelope(cancel_action, 4002), user)
    require(
        isinstance(canceled_response, HttpResponse),
        "BTC cancel during staleness returned malformed HTTP response",
    )
    canceled = require_ok(canceled_response, "BTC cancel during staleness")
    require(
        ordered_status(canceled, "cancel", "BTC cancel during staleness") == "success",
        "BTC cancel during staleness status mismatch",
    )

    remaining_response = post_json("/info", {"type": "openOrders", "user": user}, user)
    require(
        isinstance(remaining_response, HttpResponse),
        "openOrders after stale cancel returned malformed HTTP response",
    )
    remaining = require_ok(remaining_response, "openOrders after stale cancel")
    require(isinstance(remaining, list), "openOrders after stale cancel is not a list")
    require(
        all(isinstance(order, dict) and type(order.get("oid")) is int for order in remaining),
        "openOrders after stale cancel contains malformed order",
    )
    require(
        all(order["oid"] != preserved_oid for order in remaining),
        "preserved oid remains in openOrders after stale cancel",
    )
    return {"placements": 1, "stale_rejections": 1, "cancels": 1}


def run_state_flow(
    http: HttpClient,
    ws_url: str,
    timeout: float,
    user: str,
    websocket_factory: Any = WebSocketClient,
) -> dict[str, int]:
    ws = websocket_factory(ws_url, timeout, user)
    try:
        initial = subscribe(ws, {"type": "orderUpdates", "user": user}, "orderUpdates")
        require(initial["data"] == [], "state flow requires an empty initial orderUpdates snapshot")
        initial_sequence = initial["sequence"]

        order_action = {
            "type": "order",
            "orders": [{"a": 0, "b": True, "p": "99999.9", "s": "0.00001", "r": False, "t": {"limit": {"tif": "Gtc"}}}],
            "grouping": "na",
        }
        placed = require_ok(http.post_json("/exchange", exchange_envelope(order_action, 1000), user), "BTC GTC placement")
        status = ordered_status(placed, "order", "BTC GTC placement")
        require(isinstance(status, dict) and list(status) == ["resting"], "BTC GTC placement did not return resting status")
        resting = status["resting"]
        require(isinstance(resting, dict) and list(resting) == ["oid"] and type(resting["oid"]) is int, "resting status oid shape mismatch")
        oid = resting["oid"]
        placement_sequence = receive_owned_update(ws, oid, "open", initial_sequence)

        open_orders = require_ok(http.post_json("/info", {"type": "openOrders", "user": user}, user), "openOrders after placement")
        require(isinstance(open_orders, list) and len(open_orders) == 1, "openOrders after placement expected exactly one order")
        validate_open_order(open_orders[0], oid)

        cancel_action = {"type": "cancel", "cancels": [{"a": 0, "o": oid}]}
        canceled = require_ok(http.post_json("/exchange", exchange_envelope(cancel_action, 1001), user), "BTC cancel")
        require(ordered_status(canceled, "cancel", "BTC cancel") == "success", "BTC cancel status mismatch")
        receive_owned_update(ws, oid, "canceled", placement_sequence)

        remaining = require_ok(http.post_json("/info", {"type": "openOrders", "user": user}, user), "openOrders after cancel")
        require(remaining == [], "canceled order remains in openOrders")
        ws.close_cleanly()
        return {"placements": 1, "cancels": 1}
    except Exception:
        ws.abort()
        raise


def _trade_payload(message: Any, previous_sequence: int) -> tuple[int, list[Any]] | None:
    if not isinstance(message, dict) or message.get("channel") != "trades":
        return None
    require(set(message) == {"channel", "sequence", "data"}, "trades envelope shape mismatch")
    sequence = message["sequence"]
    require(
        type(sequence) is int and sequence > 0 and sequence > previous_sequence,
        "trades sequence is not nonzero and monotonic",
    )
    data = message["data"]
    require(isinstance(data, list), "trades data must be a list")
    return sequence, data


def _validate_matching_trade(message: Any, previous_sequence: int) -> tuple[int, int] | None:
    payload = _trade_payload(message, previous_sequence)
    if payload is None:
        return None
    sequence, data = payload
    btc = [item for item in data if isinstance(item, dict) and item.get("coin") == "BTC"]
    if not btc:
        return sequence, 0
    require(len(data) == 1 and len(btc) == 1, "matching BTC trade event cardinality mismatch")
    trade = btc[0]
    require(
        set(trade) == {"coin", "side", "px", "sz", "time", "tid"},
        f"matching BTC trade shape mismatch: {trade!r}",
    )
    require(trade["coin"] == "BTC", "matching trade asset mismatch")
    require(trade["side"] == "B", "matching trade side mismatch")
    require(type(trade["time"]) is int, "matching trade time mismatch")
    require(type(trade["tid"]) is int and trade["tid"] > 0, "matching trade id mismatch")
    require_canonical(trade["px"], "100000", "maker price")
    require_canonical(trade["sz"], "0.00001", "trade quantity")
    return sequence, trade["tid"]


def _receive_matching_trade(ws: WebSocketClient) -> tuple[int, int]:
    previous_sequence = 0
    for _ in range(MAX_FILTERED_MESSAGES):
        result = _validate_matching_trade(ws.receive_json(), previous_sequence)
        if result is None:
            continue
        previous_sequence, tid = result
        if tid:
            return previous_sequence, tid
    raise ProbeFailure(f"missing matching BTC trade within {MAX_FILTERED_MESSAGES} filtered reads")


def _require_no_duplicate_trade(
    ws: WebSocketClient,
    sequence: int,
    tid: int,
    timeout: float,
) -> None:
    deadline = time.monotonic() + min(DUPLICATE_WINDOW, timeout)
    for _ in range(MAX_FILTERED_MESSAGES):
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            return
        message = ws.poll_json(remaining)
        if message is None:
            return
        result = _validate_matching_trade(message, sequence)
        if result is None:
            continue
        sequence, next_tid = result
        if next_tid == tid:
            raise ProbeFailure("duplicate matching BTC trade")
    raise ProbeFailure("duplicate check exceeded bounded filtered-message allowance")


def run_trade_flow(
    http: HttpClient,
    ws_url: str,
    timeout: float,
    maker: str,
    taker: str,
    websocket_factory: Any = WebSocketClient,
) -> dict[str, Any]:
    require(maker != taker, "maker and taker must be distinct")
    require(
        NORMALIZED_USER.fullmatch(maker) is not None and NORMALIZED_USER.fullmatch(taker) is not None,
        "trade users must be normalized lowercase addresses",
    )
    ws = websocket_factory(ws_url, timeout, None)
    try:
        subscribe_ack_only(ws, {"type": "trades", "coin": "BTC"}, "trades")

        maker_action = {
            "type": "order",
            "orders": [{
                "a": 0, "b": False, "p": "100000", "s": "0.00001", "r": False,
                "t": {"limit": {"tif": "Gtc"}},
            }],
            "grouping": "na",
        }
        placed = require_ok(
            http.post_json("/exchange", exchange_envelope(maker_action, 2000), maker),
            "maker BTC GTC placement",
        )
        maker_status = ordered_status(placed, "order", "maker BTC GTC placement")
        require(isinstance(maker_status, dict) and set(maker_status) == {"resting"}, "maker did not rest")
        resting = maker_status["resting"]
        require(
            isinstance(resting, dict) and set(resting) == {"oid"} and type(resting["oid"]) is int,
            "maker resting oid shape mismatch",
        )
        maker_oid = resting["oid"]

        taker_action = {
            "type": "order",
            "orders": [{
                "a": 0, "b": True, "p": "100000", "s": "0.00001", "r": False,
                "t": {"limit": {"tif": "Ioc"}},
            }],
            "grouping": "na",
        }
        crossed = require_ok(
            http.post_json("/exchange", exchange_envelope(taker_action, 2001), taker),
            "taker BTC IOC crossing placement",
        )
        taker_status = ordered_status(crossed, "order", "taker BTC IOC crossing placement")
        require(isinstance(taker_status, dict) and set(taker_status) == {"filled"}, "taker did not fill")
        filled = taker_status["filled"]
        require(
            isinstance(filled, dict) and set(filled) == {"totalSz", "avgPx", "oid"},
            "taker filled status shape mismatch",
        )
        require(type(filled["oid"]) is int and filled["oid"] != maker_oid, "taker oid mismatch")
        require_canonical(filled["totalSz"], "0.00001", "HTTP filled quantity")
        require_canonical(filled["avgPx"], "100000", "HTTP maker price")

        sequence, tid = _receive_matching_trade(ws)
        _require_no_duplicate_trade(ws, sequence, tid, timeout)

        open_orders = require_ok(
            http.post_json("/info", {"type": "openOrders", "user": maker}, maker),
            "maker openOrders after fill",
        )
        require(isinstance(open_orders, list), "maker openOrders response is not a list")
        require(
            all(not isinstance(order, dict) or order.get("oid") != maker_oid for order in open_orders),
            "filled maker oid remains in openOrders",
        )
        ws.close_cleanly()
        return {"trades": 1, "maker_price": True}
    except Exception:
        ws.abort()
        raise


def _private_role(
    message: Any,
    expected_user: str,
    expected_oid: int,
    expected_side: str,
    previous_sequence: int,
) -> tuple[int, str] | None:
    serialized = json.dumps(message, separators=(",", ":"), sort_keys=True)
    if isinstance(message, dict) and message.get("channel") == "error":
        raise ProbeFailure(f"channel:error on private socket for {expected_user}: {message!r}")
    if not isinstance(message, dict) or message.get("channel") != "orderUpdates":
        return None
    payload_users = set(re.findall(r"0x[0-9a-f]{40}", serialized))
    require(
        not payload_users or payload_users == {expected_user},
        f"private orderUpdates payload contains wrong user on {expected_user} socket",
    )
    require(set(message) == {"channel", "sequence", "data"}, "private orderUpdates envelope shape mismatch")
    sequence = message["sequence"]
    require(
        type(sequence) is int and sequence > 0 and sequence > previous_sequence,
        f"private orderUpdates sequence is nonpositive or regressed for {expected_user}",
    )
    data = message["data"]
    require(isinstance(data, list) and len(data) == 1, "private orderUpdates cardinality mismatch")
    update = data[0]
    require(isinstance(update, dict), "private orderUpdates item is not an object")
    status = update.get("status")
    role = "placement" if status == "open" else status
    require(role in {"placement", "fill", "filled"}, f"unexpected private orderUpdates status {status!r}")
    expected_keys = {"order", "status", "statusTimestamp"} | ({"fill"} if role == "fill" else set())
    require(set(update) == expected_keys, f"private orderUpdates item shape mismatch: {update!r}")
    require(type(update["statusTimestamp"]) is int, "private orderUpdates statusTimestamp mismatch")
    order = update["order"]
    require(
        isinstance(order, dict)
        and set(order) == {"coin", "side", "limitPx", "sz", "oid", "timestamp", "origSz", "cloid"},
        f"private owned order shape mismatch: {order!r}",
    )
    require(
        order["oid"] == expected_oid,
        f"private orderUpdates wrong oid on {expected_user} socket: expected {expected_oid}, got {order.get('oid')!r}",
    )
    require(order["coin"] == "BTC" and order["side"] == expected_side, "private order identity/role mismatch")
    require(type(order["timestamp"]) is int and order["cloid"] is None, "private order timestamp/cloid mismatch")
    require_canonical(order["limitPx"], "100000", "private order limitPx")
    require_canonical(order["origSz"], "0.00001", "private order origSz")
    require_canonical(order["sz"], "0.00001" if role == "placement" else "0", "private order remaining sz")
    if role == "fill":
        fill = update["fill"]
        require(
            isinstance(fill, dict) and set(fill) == {"tid", "side", "px", "sz"},
            f"private fill shape mismatch: {fill!r}",
        )
        require(type(fill["tid"]) is int and fill["tid"] > 0, "private fill tid mismatch")
        require(fill["side"] == expected_side, "private fill side mismatch")
        require_canonical(fill["px"], "100000", "private fill price")
        require_canonical(fill["sz"], "0.00001", "private fill quantity")
    return sequence, role


def _collect_private_roles(
    ws: WebSocketClient,
    timeout: float,
    user: str,
    oid: int,
    side: str,
    initial_sequence: int,
) -> tuple[int, set[str]]:
    deadline = time.monotonic() + timeout
    previous_sequence = initial_sequence
    roles: set[str] = set()
    for _ in range(MAX_FILTERED_MESSAGES):
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            break
        message = ws.poll_json(remaining)
        if message is None:
            break
        result = _private_role(message, user, oid, side, previous_sequence)
        if result is None:
            continue
        previous_sequence, role = result
        if role == "filled" and role in roles:
            raise ProbeFailure(f"duplicate terminal event for {user}")
        if role == "fill" and role in roles:
            raise ProbeFailure(f"duplicate fill event for {user}")
        roles.add(role)
        if roles == {"placement", "fill", "filled"}:
            return previous_sequence, roles
    missing = sorted({"placement", "fill", "filled"} - roles)
    raise ProbeFailure(f"missing private roles for {user}: {','.join(missing)}")


def _audit_private_duplicates(
    sockets: list[tuple[WebSocketClient, str, int, str, int]],
    timeout: float,
) -> None:
    deadline = time.monotonic() + min(DUPLICATE_WINDOW, timeout)
    reads = 0
    while time.monotonic() < deadline and reads < MAX_FILTERED_MESSAGES:
        for ws, user, oid, side, sequence in sockets:
            remaining = deadline - time.monotonic()
            if remaining <= 0 or reads >= MAX_FILTERED_MESSAGES:
                break
            message = ws.poll_json(min(0.02, remaining))
            reads += 1
            if message is None:
                continue
            result = _private_role(message, user, oid, side, sequence)
            if result is None:
                continue
            _next_sequence, role = result
            if role == "filled":
                raise ProbeFailure(f"duplicate terminal event for {user}")
            raise ProbeFailure(f"duplicate private {role} event for {user}")


def run_private_flow(
    http: HttpClient,
    ws_url: str,
    timeout: float,
    maker: str,
    taker: str,
    websocket_factory: Any = WebSocketClient,
    unrelated: str = DEFAULT_UNRELATED,
) -> dict[str, int]:
    private_users = (maker, taker)
    users = (*private_users, unrelated)
    require(len(set(users)) == 3, "private and unrelated users must be distinct")
    require(
        all(NORMALIZED_USER.fullmatch(user) is not None for user in users),
        "private and unrelated users must be normalized lowercase addresses",
    )
    sockets: dict[str, WebSocketClient] = {}
    initial_sequences: dict[str, int] = {}
    try:
        # Both private subscriptions exist before either state-changing HTTP call.
        for user in users:
            ws = websocket_factory(ws_url, timeout, user)
            sockets[user] = ws
            initial = subscribe(ws, {"type": "orderUpdates", "user": user}, "orderUpdates")
            require(
                set(initial) == {"channel", "sequence", "data"}
                and type(initial["sequence"]) is int
                and initial["sequence"] >= 0
                and initial["data"] == [],
                f"private flow initial orderUpdates mismatch for {user}: {initial!r}",
            )
            initial_sequences[user] = initial["sequence"]

        maker_action = {
            "type": "order",
            "orders": [{
                "a": 0, "b": False, "p": "100000", "s": "0.00001", "r": False,
                "t": {"limit": {"tif": "Gtc"}},
            }],
            "grouping": "na",
        }
        placed = require_ok(
            http.post_json("/exchange", exchange_envelope(maker_action, 3000), maker),
            "private maker BTC GTC placement",
        )
        maker_status = ordered_status(placed, "order", "private maker BTC GTC placement")
        require(isinstance(maker_status, dict) and set(maker_status) == {"resting"}, "private maker did not rest")
        resting = maker_status["resting"]
        require(
            isinstance(resting, dict) and set(resting) == {"oid"} and type(resting["oid"]) is int,
            "private maker resting oid shape mismatch",
        )
        maker_oid = resting["oid"]

        taker_action = {
            "type": "order",
            "orders": [{
                "a": 0, "b": True, "p": "100000", "s": "0.00001", "r": False,
                "t": {"limit": {"tif": "Ioc"}},
            }],
            "grouping": "na",
        }
        crossed = require_ok(
            http.post_json("/exchange", exchange_envelope(taker_action, 3001), taker),
            "private taker BTC IOC crossing placement",
        )
        taker_status = ordered_status(crossed, "order", "private taker BTC IOC crossing placement")
        require(isinstance(taker_status, dict) and set(taker_status) == {"filled"}, "private taker did not fill")
        filled = taker_status["filled"]
        require(
            isinstance(filled, dict) and set(filled) == {"totalSz", "avgPx", "oid"},
            "private taker filled status shape mismatch",
        )
        taker_oid = filled["oid"]
        require(type(taker_oid) is int and taker_oid != maker_oid, "private taker oid mismatch")
        require_canonical(filled["totalSz"], "0.00001", "private HTTP filled quantity")
        require_canonical(filled["avgPx"], "100000", "private HTTP maker price")

        maker_sequence, _maker_roles = _collect_private_roles(
            sockets[maker], timeout, maker, maker_oid, "A", initial_sequences[maker]
        )
        taker_sequence, _taker_roles = _collect_private_roles(
            sockets[taker], timeout, taker, taker_oid, "B", initial_sequences[taker]
        )
        _audit_private_duplicates(
            [
                (sockets[maker], maker, maker_oid, "A", maker_sequence),
                (sockets[taker], taker, taker_oid, "B", taker_sequence),
            ],
            timeout,
        )
        unrelated_leaks = observe_unrelated_private_stream(
            sockets[unrelated],
            unrelated,
            deadline=time.monotonic() + min(DUPLICATE_WINDOW, timeout),
        )
        for ws in sockets.values():
            ws.close_cleanly()
        return {"private_users": len(private_users), "unrelated_leaks": unrelated_leaks}
    except Exception:
        for ws in sockets.values():
            ws.abort()
        raise


def observe_unrelated_private_stream(
    source: Any,
    unrelated_user: str,
    deadline: float,
    clock: Any = time.monotonic,
) -> int:
    """Observe an already-subscribed private stream until a clean timeout.

    ``source`` may be a callable with ``poll_json(timeout)`` semantics or an
    object exposing that method. Only the exact application-level pong is
    ignorable; wire ping/pong is consumed below this projection boundary.
    """
    require(
        type(unrelated_user) is str and NORMALIZED_USER.fullmatch(unrelated_user) is not None,
        "isolation requires a normalized unrelated user",
    )
    receive = source.poll_json if hasattr(source, "poll_json") else source
    require(callable(receive), "isolation source must be a receive callable or poll_json socket")

    for _ in range(MAX_FILTERED_MESSAGES):
        remaining = deadline - clock()
        if remaining <= 0:
            return 0
        try:
            message = receive(remaining)
        except WebSocketTimeout:
            return 0
        except WebSocketEOF as exc:
            raise ProbeFailure("unexpected close during unrelated isolation window") from exc
        if message is None:
            return 0
        if not isinstance(message, dict):
            raise ProbeFailure(f"unrelated stream malformed envelope: {message!r}")

        channel = message.get("channel")
        if channel == "error":
            raise ProbeFailure(f"channel:error on unrelated stream: {message!r}")
        if channel == "orderUpdates":
            raise ProbeFailure(f"unrelated orderUpdates event leak: {message!r}")

        serialized = json.dumps(message, separators=(",", ":"), sort_keys=True)
        payload_users = set(re.findall(r"0x[0-9a-f]{40}", serialized))
        require(
            not payload_users or payload_users == {unrelated_user},
            f"unrelated stream contains wrong identity: {message!r}",
        )
        if message == {"channel": "pong"}:
            continue
        raise ProbeFailure(f"unrelated stream unexpected envelope: {message!r}")

    raise ProbeFailure("unrelated isolation window exceeded bounded control-message allowance")


def run_basic_probe(base_url: str, ws_url: str, timeout: float, user: str) -> dict[str, Any]:
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


def run_probe(
    base_url: str,
    ws_url: str,
    timeout: float,
    user: str,
    basic_only: bool = False,
    trade_flow: bool = False,
    taker: str = DEFAULT_TAKER,
    private_flow: bool = False,
) -> dict[str, Any]:
    started = time.monotonic()
    summary = run_basic_probe(base_url, ws_url, timeout, user)
    if basic_only:
        return summary
    if private_flow:
        summary["probe"] = "B2b2"
        summary.update(run_private_flow(HttpClient(base_url, timeout), ws_url, timeout, user, taker))
        summary["checks"].extend([
            "three_private_subscriptions_preopened",
            "exact_private_ack_and_initial_snapshot",
            "maker_private_placement_fill_terminal",
            "taker_private_placement_fill_terminal",
            "private_user_oid_correspondence",
            "private_sequence_positive_and_monotonic",
            "no_duplicate_private_terminal",
            "unrelated_private_isolation_window",
        ])
    elif trade_flow:
        summary["probe"] = "B2a"
        summary.update(run_trade_flow(HttpClient(base_url, timeout), ws_url, timeout, user, taker))
        summary["checks"].extend([
            "maker_resting_status",
            "exact_order_status_cardinality",
            "taker_crossing_fill",
            "matching_btc_trade",
            "maker_execution_price",
            "canonical_trade_quantity",
            "nonzero_monotonic_trade_sequence",
            "no_duplicate_matching_trade",
            "filled_maker_oid_absent_from_open_orders",
        ])
    else:
        summary["probe"] = "B1"
        summary["counts"] = run_state_flow(HttpClient(base_url, timeout), ws_url, timeout, user)
        summary["checks"].extend([
            "ordered_placement_response",
            "placement_update",
            "open_orders_contains_oid",
            "canonical_decimals",
            "monotonic_event_sequence",
            "ordered_cancel_response",
            "cancel_update",
            "open_orders_empty_after_cancel",
        ])
    summary["elapsed_ms"] = int((time.monotonic() - started) * 1000)
    return summary


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
    parser.add_argument("--user", default=DEFAULT_USER, help="normalized synthetic user (maker for trade/private flows)")
    parser.add_argument("--taker", default=DEFAULT_TAKER, help="normalized synthetic taker for trade/private flows")
    flow = parser.add_mutually_exclusive_group()
    flow.add_argument("--trade-flow", action="store_true", help="run the narrow Probe-B2a maker/taker trades path")
    flow.add_argument("--private-flow", action="store_true", help="run Probe-B2b2 private correspondence and unrelated isolation")
    flow.add_argument("--state-flow", action="store_true", help="explicitly run the default one-user placement/cancel flow")
    flow.add_argument("--basic-only", action="store_true", help="run Probe-A transport/snapshots without changing state")
    args = parser.parse_args(argv)
    try:
        for label, user in (("user", args.user), ("taker", args.taker)):
            _validate_header_value(user, label)
            require(NORMALIZED_USER.fullmatch(user) is not None, f"{label} must be a normalized lowercase 0x address")
        summary = run_probe(
            args.base_url,
            args.ws_url,
            args.timeout,
            args.user,
            args.basic_only,
            args.trade_flow,
            args.taker,
            args.private_flow,
        )
    except Exception as exc:
        failed_probe = "A" if args.basic_only else ("B2b2" if args.private_flow else ("B2a" if args.trade_flow else "B1"))
        print(
            json.dumps(
                {"result": "FAIL", "probe": failed_probe, "error": type(exc).__name__, "message": str(exc)},
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
