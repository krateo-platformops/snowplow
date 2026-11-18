"""rehf1's post-run check (2026-11-18): 5 of 1445 renderer-map lines were "failed reads" with "the CDP connection
closed", all in the drive's last 10 s, none naming a session, ask or class. They were not reads: one per Chrome
(5 sessions), each the watcher's own websocket closing when its Chrome closed at session end, which renderers.js
wrote as a renderer_map line with pid null. The close is now the watcher's event (kind renderer_cdp, "close"); a
read running at the close is the only read it fails, on its own line; an ask still queued is never read.

k6/renderers.js runs under the k6 image's own k6 (Docker), against a scripted CDP server (a stdlib websocket
server, below) standing in for Chrome: it closes its connection with no read running, and in the middle of a read
with a second ask queued.
"""

from __future__ import annotations

import base64
import hashlib
import json
import shutil
import socket
import struct
import subprocess
import threading

import pytest

from tests.test_k6_830 import BENCH_DIR, needs_docker

_GUID = b"258EAFA5-E914-47DA-95CA-C5AB0DC85B11"


class _FakeChrome:
    """One CDP websocket endpoint: answers the calls renderers.js makes, then closes as `close_at` says: "idle" once
    the page has its binding (no ask), "mid-read" when the first read starts its trace (two asks queued before)."""

    def __init__(self, close_at: str):
        self.close_at = close_at
        self.srv = socket.socket()
        self.srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.srv.bind(("127.0.0.1", 0))
        self.srv.listen(1)
        self.port = self.srv.getsockname()[1]
        self.seen: list[str] = []
        self.t = threading.Thread(target=self._serve, daemon=True)
        self.t.start()

    def _serve(self):
        c, _ = self.srv.accept()
        c.settimeout(60)
        req = b""
        while b"\r\n\r\n" not in req:
            req += c.recv(4096)
        key = next(l.split(b":", 1)[1].strip() for l in req.split(b"\r\n") if l.lower().startswith(b"sec-websocket-key"))
        acc = base64.b64encode(hashlib.sha1(key + _GUID).digest())
        c.sendall(b"HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n"
                  b"Sec-WebSocket-Accept: " + acc + b"\r\n\r\n")
        try:
            while True:
                m = self._recv(c)
                if m is None:
                    return
                self.seen.append(m["method"])
                if self._answer(c, m):
                    c.sendall(b"\x88\x00")          # close frame
                    c.close()
                    return
        finally:
            self.srv.close()

    def _recv(self, c) -> dict | None:
        def exact(n):
            b = b""
            while len(b) < n:
                x = c.recv(n - len(b))
                if not x:
                    raise EOFError
                b += x
            return b
        try:
            h = exact(2)
            op, ln = h[0] & 0x0F, h[1] & 0x7F
            if ln == 126:
                ln = struct.unpack(">H", exact(2))[0]
            elif ln == 127:
                ln = struct.unpack(">Q", exact(8))[0]
            mask = exact(4) if h[1] & 0x80 else b"\0\0\0\0"
            data = bytes(b ^ mask[i % 4] for i, b in enumerate(exact(ln)))
        except (EOFError, OSError):
            return None
        if op == 8:
            return None
        return json.loads(data)

    def _send(self, c, o: dict):
        p = json.dumps(o).encode()
        h = b"\x81" + (bytes([len(p)]) if len(p) < 126 else b"\x7e" + struct.pack(">H", len(p)))
        c.sendall(h + p)

    def _answer(self, c, m: dict) -> bool:
        """Reply to m; True: close the connection now."""
        meth, sid = m["method"], m.get("sessionId")
        if meth == "Target.setDiscoverTargets":
            self._send(c, {"id": m["id"], "result": {}})
            self._send(c, {"method": "Target.targetCreated", "params": {"targetInfo": {"type": "page", "targetId": "T1"}}})
        elif meth == "Target.attachToTarget":
            self._send(c, {"id": m["id"], "result": {"sessionId": "S1"}})
        elif meth == "Runtime.enable":
            self._send(c, {"id": m["id"], "result": {}, "sessionId": sid})
        elif meth == "Runtime.addBinding":
            self._send(c, {"id": m["id"], "result": {}, "sessionId": sid})
            if self.close_at == "idle":
                return True
            for ask in ("bench-read:login:1", "bench-read:after:2"):
                self._send(c, {"method": "Runtime.bindingCalled", "sessionId": "S1",
                               "params": {"name": "__benchRead", "payload": ask}})
        elif meth == "Runtime.evaluate":
            self._send(c, {"id": m["id"], "sessionId": sid, "result": {"result": {
                "type": "string", "value": json.dumps({"session": "s1", "class": "0"})}}})
        elif meth == "Tracing.start":
            return True                              # Chrome closes in the middle of the read
        else:
            self._send(c, {"id": m["id"], "result": {}, "sessionId": sid})
        return False


def _run_watcher(tmp_path, chrome: _FakeChrome) -> list[dict]:
    from bench import k6_job
    k6dir, out = tmp_path / "k6", tmp_path / "out"
    k6dir.mkdir()
    out.mkdir(mode=0o777)
    out.chmod(0o777)
    shutil.copy(BENCH_DIR / "k6" / "renderers.js", k6dir / "renderers.js")
    targets = json.dumps([{"udd": "/tmp/k6browser-data-s1", "url": f"ws://127.0.0.1:{chrome.port}/devtools/browser/x"}])
    p = subprocess.run(["docker", "run", "--rm", "--network", "host", "-v", f"{k6dir}:/opt/k6:ro", "-v", f"{out}:/out",
                        "-e", f"CDP_TARGETS={targets}", "-e", "READ_MS=3000", "--entrypoint", "k6",
                        k6_job.K6_BROWSER_IMAGE, "run", "--quiet", "--log-format", "raw",
                        "--console-output", "/out/renderer-map.jsonl", "/opt/k6/renderers.js"],
                       capture_output=True, text=True, timeout=180)
    assert p.returncode == 0, p.stderr[-3000:]
    chrome.t.join(timeout=10)
    return [json.loads(x) for x in (out / "renderer-map.jsonl").read_text().splitlines() if x.strip()]


@needs_docker
def test_a_chrome_closing_with_no_read_running_leaves_no_failed_read(tmp_path):
    """The session ended and its Chrome closed the connection; no read was running: no renderer_map line at all,
    and one close event saying so (mutation: emitting the close as a failed read, rehf1's lines, turns this RED)."""
    rows = _run_watcher(tmp_path, _FakeChrome("idle"))
    assert [r for r in rows if r.get("kind") == "renderer_map"] == [], rows
    closes = [r for r in rows if r.get("kind") == "renderer_cdp" and r.get("event") == "close"]
    assert len(closes) == 1 and closes[0]["read_in_flight"] is False and closes[0]["asks_unread"] == 0, rows


@needs_docker
def test_a_chrome_closing_mid_read_fails_that_read_only_and_reads_nothing_after(tmp_path):
    """Two asks queued; the connection closes while the first read runs (its trace started): that read is the one
    failed line, with its own ask and session; the second ask is never read, and nothing is sent after the close
    (mutations: reading on after the close; the close as a line of its own)."""
    chrome = _FakeChrome("mid-read")
    rows = _run_watcher(tmp_path, chrome)
    maps = [r for r in rows if r.get("kind") == "renderer_map"]
    assert len(maps) == 1, rows
    m = maps[0]
    assert (m["pid"], m["ask"], m["session"], m["class"]) == (None, "login", "s1", "0"), m
    assert m["why"] == "the CDP connection closed during the read", m
    closes = [r for r in rows if r.get("kind") == "renderer_cdp" and r.get("event") == "close"]
    assert len(closes) == 1 and closes[0]["read_in_flight"] is True and closes[0]["asks_unread"] == 1, rows
    assert chrome.seen[-1] == "Tracing.start", chrome.seen                 # nothing reached Chrome after it
