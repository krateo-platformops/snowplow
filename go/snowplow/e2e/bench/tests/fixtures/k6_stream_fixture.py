"""Ports for the browser wrapper's forward wait (#834 review): 8080 answers
GET / with a 200 text/event-stream that never ends (keepalives every 0.5 s);
8081 accepts connections and never answers."""
import http.server
import socket
import threading
import time


class Stream(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream")
        self.end_headers()
        try:
            while True:
                self.wfile.write(b": keepalive\n\n")
                self.wfile.flush()
                time.sleep(0.5)
        except OSError:
            pass

    def log_message(self, *a):
        pass


def silent():
    s = socket.socket()
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    s.bind(("0.0.0.0", 8081))
    s.listen(64)
    held = []
    while True:
        held.append(s.accept()[0])


threading.Thread(target=silent, daemon=True).start()
srv = http.server.ThreadingHTTPServer(("0.0.0.0", 8080), Stream)
srv.daemon_threads = True
srv.serve_forever()
