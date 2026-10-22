"""A stand-in snowplow for navigate.js's scheduling test: /first answers after
FIRST_S seconds (a slow first load, as in run 11's storm), /route after
ROUTE_S. Every request is logged to LOG as one JSON line with the caller's
bearer and its start / end times, so the test can see, per user, whether a
route click began before that user's first load had finished.

    python3 k6_nav_fixture.py <port> <first_s> <route_s> <log>"""
import http.server
import json
import sys
import threading
import time

PORT, FIRST_S, ROUTE_S, LOG = int(sys.argv[1]), float(sys.argv[2]), float(sys.argv[3]), sys.argv[4]
lock = threading.Lock()


class H(http.server.BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def do_GET(self):
        t0 = time.time()
        time.sleep(FIRST_S if self.path.startswith("/first") else ROUTE_S)
        body = b"{}"
        self.send_response(200)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)
        with lock, open(LOG, "a") as fh:
            fh.write(json.dumps({"auth": self.headers.get("Authorization", ""), "path": self.path,
                                 "t0": t0, "t1": time.time()}) + "\n")

    def log_message(self, *a):
        pass


srv = http.server.ThreadingHTTPServer(("0.0.0.0", PORT), H)
srv.daemon_threads = True
srv.serve_forever()
