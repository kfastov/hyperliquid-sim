"""Unit and fast real-process tests for external Probe-A/B1.

Run after building the current binary:
  cargo build --bin sim-server
  python3 -m unittest acceptance/test_probe.py -v
"""

from __future__ import annotations

import io
import json
import os
import signal
import socket
import struct
import subprocess
import sys
import time
import unittest
from pathlib import Path
from unittest import mock
from urllib import error

ACCEPTANCE = Path(__file__).resolve().parent
ROOT = ACCEPTANCE.parent
UNRELATED = "0x3333333333333333333333333333333333333333"
sys.path.insert(0, str(ACCEPTANCE))

import probe  # noqa: E402


class BufferSocket:
    def __init__(self, data: bytes):
        self.data = bytearray(data)

    def recv(self, count: int) -> bytes:
        result = bytes(self.data[:count])
        del self.data[:count]
        return result


def server_frame(opcode: int, payload: bytes = b"", *, fin: bool = True, masked: bool = False) -> bytes:
    first = (0x80 if fin else 0) | opcode
    mask = 0x80 if masked else 0
    if len(payload) < 126:
        return bytes((first, mask | len(payload))) + (b"\0\0\0\0" if masked else b"") + payload
    if len(payload) <= 0xFFFF:
        return bytes((first, mask | 126)) + struct.pack("!H", len(payload)) + payload
    return bytes((first, mask | 127)) + struct.pack("!Q", len(payload)) + payload


class FramingTests(unittest.TestCase):
    def test_client_text_ping_and_close_frames_are_masked(self):
        key = b"\x01\x02\x03\x04"
        for opcode, payload in ((0x1, b"hello"), (0x9, b"ping"), (0x8, struct.pack("!H", 1000))):
            with self.subTest(opcode=opcode):
                frame = probe.encode_client_frame(payload, opcode, key)
                self.assertEqual(frame[0], 0x80 | opcode)
                self.assertTrue(frame[1] & 0x80)
                self.assertEqual(frame[2:6], key)
                decoded = bytes(value ^ key[index % 4] for index, value in enumerate(frame[6:]))
                self.assertEqual(decoded, payload)

    def test_client_extended_lengths_are_encoded_and_masked(self):
        key = b"mask"
        for size, marker, header_size in ((126, 126, 4), (65536, 127, 10)):
            with self.subTest(size=size):
                payload = b"x" * size
                frame = probe.encode_client_frame(payload, 0x1, key)
                self.assertEqual(frame[1] & 0x7F, marker)
                encoded = struct.unpack("!H" if marker == 126 else "!Q", frame[2:header_size])[0]
                self.assertEqual(encoded, size)
                self.assertEqual(frame[header_size : header_size + 4], key)

    def test_server_text_pong_and_close_are_parsed(self):
        cases = ((0x1, b"{}"), (0xA, b"probe-a"), (0x8, struct.pack("!H", 1000)))
        for opcode, payload in cases:
            with self.subTest(opcode=opcode):
                self.assertEqual(probe.read_server_frame(BufferSocket(server_frame(opcode, payload))), (opcode, payload))

    def test_server_extended_payload_is_bounded_and_parsed(self):
        payload = b"x" * 65536
        self.assertEqual(probe.read_server_frame(BufferSocket(server_frame(0x1, payload))), (0x1, payload))
        oversized_header = b"\x81\x7f" + struct.pack("!Q", probe.MAX_WS_PAYLOAD + 1)
        with self.assertRaisesRegex(probe.ProbeFailure, "exceeds configured bound"):
            probe.read_server_frame(BufferSocket(oversized_header))

    def test_server_rejects_fragmentation_masking_and_invalid_opcodes(self):
        bad = (
            (server_frame(0x1, b"x", fin=False), "fragmentation"),
            (server_frame(0x1, b"", masked=True), "must not be masked"),
            (server_frame(0x2, b"binary"), "invalid opcode"),
            (b"\x88\x01x", "invalid payload length"),
        )
        for frame, message in bad:
            with self.subTest(message=message), self.assertRaisesRegex(probe.ProbeFailure, message):
                probe.read_server_frame(BufferSocket(frame))

    def test_non_minimal_length_is_rejected(self):
        with self.assertRaisesRegex(probe.ProbeFailure, "non-minimal"):
            probe.read_server_frame(BufferSocket(b"\x81\x7e\x00\x01x"))


class HandshakeTests(unittest.TestCase):
    def test_valid_upgrade_accept_is_verified(self):
        key = "dGhlIHNhbXBsZSBub25jZQ=="
        raw = (
            "HTTP/1.1 101 Switching Protocols\r\n"
            "Upgrade: websocket\r\n"
            "Connection: keep-alive, Upgrade\r\n"
            f"Sec-WebSocket-Accept: {probe.websocket_accept(key)}"
        ).encode("ascii")
        headers = probe.parse_upgrade_response(raw, key)
        self.assertEqual(headers["sec-websocket-accept"], "s3pPLMBiTxaQ9kYGzzhZRbK+xOo=")

    def test_wrong_accept_and_missing_upgrade_are_rejected(self):
        key = "test-key"
        wrong = b"HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: wrong"
        with self.assertRaisesRegex(probe.ProbeFailure, "Sec-WebSocket-Accept"):
            probe.parse_upgrade_response(wrong, key)
        missing = f"HTTP/1.1 101 Switching Protocols\r\nSec-WebSocket-Accept: {probe.websocket_accept(key)}".encode()
        with self.assertRaisesRegex(probe.ProbeFailure, "do not confirm"):
            probe.parse_upgrade_response(missing, key)


class HttpTests(unittest.TestCase):
    def test_json_decoder_preserves_non_2xx_body(self):
        body = {"error": {"category": "invalid_request", "message": "bad input"}}
        response = probe.decode_http_json(422, json.dumps(body).encode(), "POST /info")
        self.assertEqual(response, probe.HttpResponse(422, body))

    def test_json_decoder_rejects_invalid_and_oversized_body(self):
        with self.assertRaisesRegex(probe.ProbeFailure, "invalid JSON"):
            probe.decode_http_json(500, b"not json")
        with self.assertRaisesRegex(probe.ProbeFailure, "exceeds"):
            probe.decode_http_json(200, b" " * (probe.MAX_HTTP_BODY + 1))

    def test_http_client_get_and_json_post_without_network(self):
        good = mock.MagicMock()
        good.__enter__.return_value = good
        good.status = 200
        good.read.return_value = b'{"status":"alive"}'
        failure = error.HTTPError(
            "http://example.test/info",
            503,
            "unavailable",
            {},
            io.BytesIO(b'{"error":{"category":"oracle_unavailable"}}'),
        )
        with mock.patch.object(probe.request, "urlopen", side_effect=[good, failure]) as opened:
            client = probe.HttpClient("http://example.test", 1.0)
            self.assertEqual(client.get("/healthz").body, {"status": "alive"})
            response = client.post_json("/info", {"type": "allMids"}, probe.DEFAULT_USER)
        self.assertEqual(response.status, 503)
        self.assertEqual(response.body["error"]["category"], "oracle_unavailable")
        post_request = opened.call_args_list[1].args[0]
        self.assertEqual(post_request.get_method(), "POST")
        self.assertEqual(json.loads(post_request.data), {"type": "allMids"})
        self.assertEqual(post_request.get_header("Content-type"), "application/json")
        self.assertEqual(post_request.get_header("X-sim-user"), probe.DEFAULT_USER)

    def test_timeout_argument_is_bounded(self):
        self.assertEqual(probe.bounded_timeout("0.1"), 0.1)
        self.assertEqual(probe.bounded_timeout("30"), 30.0)
        for value in ("0", "30.1", "nan", "inf"):
            with self.subTest(value=value), self.assertRaises(Exception):
                probe.bounded_timeout(value)


def subscription_ack(subscription):
    return {
        "channel": "subscriptionResponse",
        "data": {"method": "subscribe", "subscription": subscription},
    }


def order_update(sequence, oid, status, price="99999.9", size="0.00001", side="B", fill=None):
    result = {
        "channel": "orderUpdates",
        "sequence": sequence,
        "data": [{
            "order": {
                "coin": "BTC", "side": side, "limitPx": price, "sz": size,
                "oid": oid, "timestamp": sequence, "origSz": "0.00001", "cloid": None,
            },
            "status": status,
            "statusTimestamp": sequence,
        }],
    }
    if fill is not None:
        result["data"][0]["fill"] = fill
    return result


class ScriptedWebSocket:
    def __init__(self, setup, events, polled=None):
        self.setup = list(setup)
        self.events = list(events)
        self.polled = list(polled or [])
        self.sent = []
        self.closed = False

    def send_text_json(self, value):
        self.sent.append(value)

    def receive_json(self):
        return self.setup.pop(0) if self.setup else self.events.pop(0)

    def poll_json(self, _timeout):
        if self.events:
            return self.events.pop(0)
        return self.polled.pop(0) if self.polled else None

    def close_cleanly(self):
        self.closed = True

    def abort(self):
        self.closed = True


class ScriptedReceive:
    def __init__(self, outcomes):
        self.outcomes = list(outcomes)
        self.timeouts = []

    def __call__(self, timeout):
        self.timeouts.append(timeout)
        outcome = self.outcomes.pop(0)
        if isinstance(outcome, BaseException):
            raise outcome
        return outcome


class ScriptedHttp:
    def __init__(self, bodies):
        self.bodies = list(bodies)
        self.calls = []

    def post_json(self, path, payload, user=None):
        self.calls.append((path, payload, user))
        return probe.HttpResponse(200, self.bodies.pop(0))


def exchange_response(kind, status):
    return {"status": "ok", "response": {"type": kind, "data": {"statuses": [status]}}}


def open_order(oid, price="99999.9", size="0.00001"):
    return {
        "coin": "BTC", "limitPx": price, "oid": oid, "side": "B", "sz": size,
        "timestamp": 1, "origSz": size, "cloid": None,
    }


class StateFlowTests(unittest.TestCase):
    def dependencies(self):
        subscription = {"type": "orderUpdates", "user": probe.DEFAULT_USER}
        ws = ScriptedWebSocket(
            [
                subscription_ack(subscription),
                {"channel": "orderUpdates", "sequence": 0, "data": []},
            ],
            [
                {"channel": "allMids", "sequence": 1, "data": {"BTC": "100000"}},
                order_update(1, 7, "open"),
                order_update(2, 7, "open"),
                order_update(3, 7, "canceled"),
            ],
        )
        http = ScriptedHttp([
            exchange_response("order", {"resting": {"oid": 7}}),
            [open_order(7)],
            exchange_response("cancel", "success"),
            [],
        ])
        return http, ws

    def test_mocked_one_user_place_cancel_flow_filters_messages(self):
        http, ws = self.dependencies()
        counts = probe.run_state_flow(
            http, "ws://mock/ws", 0.1, probe.DEFAULT_USER,
            lambda _url, _timeout, _user: ws,
        )
        self.assertEqual(counts, {"placements": 1, "cancels": 1})
        self.assertTrue(ws.closed)
        self.assertEqual([call[0] for call in http.calls], ["/exchange", "/info", "/exchange", "/info"])
        self.assertTrue(all(call[2] == probe.DEFAULT_USER for call in http.calls))

    def test_mocked_flow_rejects_noncanonical_decimal(self):
        http, ws = self.dependencies()
        http.bodies[1][0]["limitPx"] = "99999.90"
        with self.assertRaisesRegex(probe.ProbeFailure, "canonical"):
            probe.run_state_flow(
                http, "ws://mock/ws", 0.1, probe.DEFAULT_USER,
                lambda _url, _timeout, _user: ws,
            )

    def test_mocked_flow_rejects_nonmonotonic_cancel_sequence(self):
        http, ws = self.dependencies()
        ws.events[-1]["sequence"] = 1
        with self.assertRaisesRegex(probe.ProbeFailure, "monotonic"):
            probe.run_state_flow(
                http, "ws://mock/ws", 0.1, probe.DEFAULT_USER,
                lambda _url, _timeout, _user: ws,
            )


def trade_event(sequence=4, price="100000", size="0.00001", tid=9):
    return {
        "channel": "trades",
        "sequence": sequence,
        "data": [{
            "coin": "BTC", "side": "B", "px": price, "sz": size,
            "time": 2001, "tid": tid,
        }],
    }


class TradeFlowTests(unittest.TestCase):
    def dependencies(self, polled=None):
        subscription = {"type": "trades", "coin": "BTC"}
        ws = ScriptedWebSocket(
            [subscription_ack(subscription)],
            [
                {"channel": "allMids", "sequence": 2, "data": {"BTC": "100000"}},
                trade_event(),
            ],
            polled,
        )
        http = ScriptedHttp([
            exchange_response("order", {"resting": {"oid": 7}}),
            exchange_response("order", {"filled": {"totalSz": "0.00001", "avgPx": "100000", "oid": 8}}),
            [],
        ])
        return http, ws

    def run_flow(self, http, ws):
        return probe.run_trade_flow(
            http, "ws://mock/ws", 0.1, probe.DEFAULT_USER, probe.DEFAULT_TAKER,
            lambda _url, _timeout, user: ws if user is None else self.fail("trade socket must be public"),
        )

    def test_scripted_trade_flow_uses_one_ack_only_subscription_and_exact_trade(self):
        http, ws = self.dependencies()
        result = self.run_flow(http, ws)
        self.assertEqual(result, {"trades": 1, "maker_price": True})
        self.assertTrue(ws.closed)
        self.assertEqual(ws.sent, [{"method": "subscribe", "subscription": {"type": "trades", "coin": "BTC"}}])
        self.assertEqual([call[0] for call in http.calls], ["/exchange", "/exchange", "/info"])
        self.assertEqual(http.calls[0][2], probe.DEFAULT_USER)
        self.assertEqual(http.calls[1][2], probe.DEFAULT_TAKER)
        self.assertEqual(http.calls[2][1], {"type": "openOrders", "user": probe.DEFAULT_USER})

    def test_scripted_trade_flow_rejects_status_cardinality(self):
        http, ws = self.dependencies()
        http.bodies[0]["response"]["data"]["statuses"].append({"resting": {"oid": 8}})
        with self.assertRaisesRegex(probe.ProbeFailure, "exactly one ordered status"):
            self.run_flow(http, ws)

    def test_scripted_trade_flow_rejects_wrong_price_duplicate_and_unconsumed_maker(self):
        cases = (
            ("wrong_price", "maker price"),
            ("duplicate", "duplicate matching BTC trade"),
            ("maker_open", "filled maker oid remains"),
        )
        for change, message in cases:
            with self.subTest(change=change):
                http, ws = self.dependencies([trade_event(sequence=5)] if change == "duplicate" else None)
                if change == "wrong_price":
                    ws.events[-1]["data"][0]["px"] = "100000.1"
                elif change == "maker_open":
                    http.bodies[-1] = [{"oid": 7}]
                with self.assertRaisesRegex(probe.ProbeFailure, message):
                    self.run_flow(http, ws)


def private_fill(sequence, oid, side):
    return order_update(
        sequence, oid, "fill", price="100000", size="0", side=side,
        fill={"tid": 9, "side": side, "px": "100000", "sz": "0.00001"},
    )


class PrivateFlowTests(unittest.TestCase):
    def dependencies(self):
        users = (probe.DEFAULT_USER, probe.DEFAULT_TAKER, probe.DEFAULT_UNRELATED)
        sockets = {}
        for user in users:
            subscription = {"type": "orderUpdates", "user": user}
            sockets[user] = ScriptedWebSocket(
                [subscription_ack(subscription), {"channel": "orderUpdates", "sequence": 0, "data": []}],
                [],
            )
        # Harmless public-channel messages and a repeated placement prove that
        # collection is bounded and semantic rather than adjacency-based.
        sockets[probe.DEFAULT_USER].events.extend([
            {"channel": "allMids", "sequence": 1, "data": {"BTC": "100000"}},
            order_update(1, 7, "open", price="100000", side="A"),
            order_update(2, 7, "open", price="100000", side="A"),
            private_fill(4, 7, "A"),
            order_update(5, 7, "filled", price="100000", size="0", side="A"),
        ])
        sockets[probe.DEFAULT_TAKER].events.extend([
            {"channel": "l2Book", "sequence": 2, "data": {"coin": "BTC"}},
            order_update(3, 8, "open", price="100000"),
            private_fill(4, 8, "B"),
            order_update(6, 8, "filled", price="100000", size="0"),
        ])
        http = ScriptedHttp([
            exchange_response("order", {"resting": {"oid": 7}}),
            exchange_response("order", {"filled": {"totalSz": "0.00001", "avgPx": "100000", "oid": 8}}),
        ])
        return http, sockets

    def run_flow(self, http, sockets):
        return probe.run_private_flow(
            http, "ws://mock/ws", 0.1, probe.DEFAULT_USER, probe.DEFAULT_TAKER,
            lambda _url, _timeout, user: sockets[user],
        )

    def test_scripted_interleaving_proves_private_roles(self):
        http, sockets = self.dependencies()
        result = self.run_flow(http, sockets)
        self.assertEqual(result, {"private_users": 2, "unrelated_leaks": 0})
        self.assertTrue(all(socket.closed for socket in sockets.values()))
        self.assertEqual([call[2] for call in http.calls], [probe.DEFAULT_USER, probe.DEFAULT_TAKER])
        for user, socket in sockets.items():
            self.assertEqual(socket.sent, [{
                "method": "subscribe", "subscription": {"type": "orderUpdates", "user": user},
            }])

    def test_scripted_private_flow_rejects_unrelated_leak_and_closes_every_socket(self):
        http, sockets = self.dependencies()
        sockets[probe.DEFAULT_UNRELATED].events.append(
            order_update(7, 99, "open", price="100000")
        )
        with self.assertRaisesRegex(probe.ProbeFailure, "orderUpdates.*leak"):
            self.run_flow(http, sockets)
        self.assertTrue(all(socket.closed for socket in sockets.values()))

    def test_scripted_private_flow_rejects_wrong_user_on_either_socket(self):
        for socket_user, other_user in (
            (probe.DEFAULT_USER, probe.DEFAULT_TAKER),
            (probe.DEFAULT_TAKER, probe.DEFAULT_USER),
        ):
            with self.subTest(socket_user=socket_user):
                http, sockets = self.dependencies()
                sockets[socket_user].events[1]["user"] = other_user
                with self.assertRaisesRegex(probe.ProbeFailure, "wrong user"):
                    self.run_flow(http, sockets)

    def test_scripted_private_flow_rejects_error_wrong_oid_missing_role_duplicate_terminal_and_sequence(self):
        cases = (
            ("channel_error", "channel:error"),
            ("wrong_oid", "wrong oid"),
            ("missing_role", "missing.*fill"),
            ("duplicate_terminal", "duplicate terminal"),
            ("regressed_sequence", "regressed"),
            ("nonpositive_sequence", "nonpositive"),
        )
        for change, message in cases:
            with self.subTest(change=change):
                http, sockets = self.dependencies()
                maker = sockets[probe.DEFAULT_USER]
                if change == "channel_error":
                    maker.events[0] = {"channel": "error", "data": {"category": "internal"}}
                elif change == "wrong_oid":
                    maker.events[1]["data"][0]["order"]["oid"] = 8
                elif change == "missing_role":
                    maker.events = [event for event in maker.events if not (
                        event.get("channel") == "orderUpdates" and event["data"][0]["status"] == "fill"
                    )]
                elif change == "duplicate_terminal":
                    maker.polled.append(order_update(7, 7, "filled", price="100000", size="0", side="A"))
                elif change == "regressed_sequence":
                    maker.events[-1]["sequence"] = 3
                else:
                    maker.events[1]["sequence"] = 0
                with self.assertRaisesRegex(probe.ProbeFailure, message):
                    self.run_flow(http, sockets)


class UnrelatedPrivateStreamTests(unittest.TestCase):
    def observe(self, outcomes, unrelated=UNRELATED):
        receive = ScriptedReceive(outcomes)
        result = probe.observe_unrelated_private_stream(
            receive,
            unrelated,
            deadline=10.25,
            clock=lambda: 10.0,
        )
        return result, receive

    def test_clean_timeout_passes_with_strict_remaining_window(self):
        result, receive = self.observe([None])
        self.assertEqual(result, 0)
        self.assertEqual(receive.timeouts, [0.25])

    def test_exact_application_pong_is_allowed_before_clean_timeout(self):
        socket = ScriptedWebSocket([], [{"channel": "pong"}])
        result = probe.observe_unrelated_private_stream(
            socket,
            UNRELATED,
            deadline=10.25,
            clock=lambda: 10.0,
        )
        self.assertEqual(result, 0)

    def test_order_updates_payload_is_always_a_leak(self):
        with self.assertRaisesRegex(probe.ProbeFailure, "orderUpdates.*leak"):
            self.observe([order_update(7, 99, "open", price="100000")])

    def test_channel_and_projection_error_envelopes_fail(self):
        for message in (
            {"channel": "error", "data": {"category": "lagged", "message": "resubscribe"}},
            {"channel": "error", "data": {"category": "internal", "message": "event projection failed"}},
        ):
            with self.subTest(message=message), self.assertRaisesRegex(probe.ProbeFailure, "channel:error"):
                self.observe([message])

    def test_malformed_envelope_identity_and_unallowlisted_data_fail(self):
        cases = (
            (["not", "an", "envelope"], UNRELATED, "malformed envelope"),
            ({"channel": "pong", "user": probe.DEFAULT_USER}, UNRELATED, "identity|unexpected envelope"),
            ({"channel": "trades", "sequence": 9, "data": []}, UNRELATED, "unexpected envelope"),
            ({"channel": "pong"}, "not-a-user", "normalized unrelated user"),
        )
        for message, unrelated, diagnostic in cases:
            with self.subTest(message=message), self.assertRaisesRegex(probe.ProbeFailure, diagnostic):
                self.observe([message], unrelated)

    def test_unexpected_close_fails(self):
        close = probe.WebSocketEOF("WebSocket peer closed during frame read")
        with self.assertRaisesRegex(probe.ProbeFailure, "unexpected close"):
            self.observe([close])


class RealOfflineProcessTest(unittest.TestCase):
    def test_current_offline_binary_passes_default_b1_trade_and_private_flows(self):
        binary = Path(os.environ.get("SIM_SERVER_BIN", ROOT / "target" / "debug" / "sim-server"))
        self.assertTrue(binary.is_file(), f"current binary missing; run cargo build --bin sim-server: {binary}")
        reservation = socket.socket()
        reservation.bind(("127.0.0.1", 0))
        port = reservation.getsockname()[1]
        reservation.close()
        env = os.environ.copy()
        env.update(
            {
                "SIM_BIND_ADDR": f"127.0.0.1:{port}",
                "SIM_ORACLE_MODE": "offline",
                "SIM_ACTORS_ENABLED": "false",
                "SIM_REPLY_TIMEOUT_MS": "1000",
                "SIM_SHUTDOWN_TIMEOUT_MS": "2000",
            }
        )
        server = subprocess.Popen(
            [str(binary)], cwd=ROOT, env=env, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, text=True
        )
        stderr = ""
        try:
            deadline = time.monotonic() + 5
            while time.monotonic() < deadline:
                try:
                    with socket.create_connection(("127.0.0.1", port), timeout=0.1):
                        break
                except OSError:
                    if server.poll() is not None:
                        self.fail(f"server exited during startup: {server.stderr.read()}")
                    time.sleep(0.02)
            else:
                self.fail("server listener did not start within 5 seconds")
            completed = subprocess.run(
                [
                    sys.executable,
                    str(ACCEPTANCE / "probe.py"),
                    "--base-url",
                    f"http://127.0.0.1:{port}",
                    "--ws-url",
                    f"ws://127.0.0.1:{port}/ws",
                    "--timeout",
                    "2",
                ],
                cwd=ROOT,
                text=True,
                capture_output=True,
                timeout=15,
                check=False,
            )
            self.assertEqual(completed.returncode, 0, completed.stderr)
            lines = completed.stdout.splitlines()
            self.assertEqual(len(lines), 1, completed.stdout)
            summary = json.loads(lines[0])
            self.assertEqual(summary["result"], "PASS")
            self.assertEqual(summary["probe"], "B1")
            self.assertEqual(summary["counts"], {"placements": 1, "cancels": 1})
            self.assertIn("placement_update", summary["checks"])
            self.assertIn("cancel_update", summary["checks"])

            traded = subprocess.run(
                [
                    sys.executable,
                    str(ACCEPTANCE / "probe.py"),
                    "--base-url",
                    f"http://127.0.0.1:{port}",
                    "--ws-url",
                    f"ws://127.0.0.1:{port}/ws",
                    "--timeout",
                    "2",
                    "--trade-flow",
                ],
                cwd=ROOT,
                text=True,
                capture_output=True,
                timeout=15,
                check=False,
            )
            self.assertEqual(traded.returncode, 0, traded.stderr)
            trade_lines = traded.stdout.splitlines()
            self.assertEqual(len(trade_lines), 1, traded.stdout)
            trade_summary = json.loads(trade_lines[0])
            self.assertEqual(trade_summary["result"], "PASS")
            self.assertEqual(trade_summary["probe"], "B2a")
            self.assertEqual(trade_summary["trades"], 1)
            self.assertIs(trade_summary["maker_price"], True)

            private = subprocess.run(
                [
                    sys.executable,
                    str(ACCEPTANCE / "probe.py"),
                    "--base-url",
                    f"http://127.0.0.1:{port}",
                    "--ws-url",
                    f"ws://127.0.0.1:{port}/ws",
                    "--timeout",
                    "2",
                    "--private-flow",
                ],
                cwd=ROOT,
                text=True,
                capture_output=True,
                timeout=5,
                check=False,
            )
            self.assertEqual(private.returncode, 0, private.stderr)
            private_lines = private.stdout.splitlines()
            self.assertEqual(len(private_lines), 1, private.stdout)
            private_summary = json.loads(private_lines[0])
            elapsed_ms = private_summary.pop("elapsed_ms")
            self.assertLess(elapsed_ms, 2_000)
            self.assertEqual(private_summary, {
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
                    "three_private_subscriptions_preopened",
                    "exact_private_ack_and_initial_snapshot",
                    "maker_private_placement_fill_terminal",
                    "taker_private_placement_fill_terminal",
                    "private_user_oid_correspondence",
                    "private_sequence_positive_and_monotonic",
                    "no_duplicate_private_terminal",
                    "unrelated_private_isolation_window",
                ],
                "private_users": 2,
                "probe": "B2b2",
                "profile": "sim-header-v1",
                "result": "PASS",
                "unrelated_leaks": 0,
            })
        finally:
            if server.poll() is None:
                server.send_signal(signal.SIGTERM)
            try:
                _stdout, stderr = server.communicate(timeout=5)
            except subprocess.TimeoutExpired:
                server.kill()
                _stdout, stderr = server.communicate(timeout=5)
                self.fail(f"server did not honor SIGTERM: {stderr}")
        self.assertEqual(server.returncode, 0, stderr)


if __name__ == "__main__":
    unittest.main()
