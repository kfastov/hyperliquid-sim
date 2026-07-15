"""Unit and fast real-process tests for external Probe-A.

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


class RealOfflineProcessTest(unittest.TestCase):
    def test_current_offline_binary_passes_probe_a(self):
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
            self.assertEqual(summary["probe"], "A")
            self.assertNotIn("matching", summary)
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
