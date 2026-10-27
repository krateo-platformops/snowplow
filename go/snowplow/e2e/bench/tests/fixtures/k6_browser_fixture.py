"""A tiny SPA with the portal's observable contract, for running k6/browser.js
in real Chrome under test (tests/test_k6_830.py, #831 gate):

  /login       the antd form ids (#basic_username, #basic_password); Enter
               "signs in" after SIGNIN_MS (authn's x509 login), then the app
               navigates to /dashboard (history.pushState, like Login.tsx:92)
  a route      renders, after SKELETON_DELAY_MS, `[data-widget-renderer]`
               skeletons (WidgetLoading) that resolve PAGE_MS later
  /never       its skeleton never resolves (a page timeout)
  /err         resolves to a `data-testid="widget-error"` end state
  sidebar      `li.ant-menu-item` with `data-menu-id="rc-menu-<path>"`
  live updates POST /refreshes (fetch) and EventSource /notifications
  /call        one GET /call that answers, one to a refused port (#919)

Usage: k6_browser_fixture.py PORT SIGNIN_MS SKELETON_DELAY_MS PAGE_MS"""
import http.server
import sys
import time

PORT, SIGNIN_MS, SKEL_MS, PAGE_MS = (int(x) for x in sys.argv[1:5])

PAGE = """<!doctype html><html><head><meta charset="utf-8"><title>fixture</title></head>
<body><ul id="menu"></ul><div id="content"></div><script>
const ROUTES = ['/dashboard', '/compositions', '/never', '/err'];
const SIGNIN = @SIGNIN@, SKEL = @SKEL@, PAGE = @PAGE@;
const content = document.getElementById('content');
function menu () {
  const ul = document.getElementById('menu');
  ul.innerHTML = '';
  for (const r of ROUTES) {
    const li = document.createElement('li');
    li.className = 'ant-menu-item';
    li.setAttribute('data-menu-id', 'rc-menu-' + r);
    li.textContent = r;
    li.onclick = () => { history.pushState({}, '', r); render(r); };
    ul.appendChild(li);
  }
}
let gen = 0;
function render (path) {
  const g = ++gen;
  content.innerHTML = '';
  if (path === '/login') {
    content.innerHTML = '<form><input id="basic_username"><input id="basic_password" type="password"></form>';
    document.getElementById('basic_password').addEventListener('keydown', (e) => {
      if (e.key !== 'Enter') return;
      e.preventDefault();
      setTimeout(() => { history.pushState({}, '', '/dashboard'); menu(); render('/dashboard'); live(); }, SIGNIN);
    });
    return;
  }
  setTimeout(() => {
    if (g !== gen) return;
    content.innerHTML = '<div data-widget-renderer>loading</div><div data-widget-renderer>loading</div>';
    if (path === '/never') return;
    setTimeout(() => {
      if (g !== gen) return;
      content.innerHTML = path === '/err'
        ? '<div data-testid="widget-error">error</div><div>ok</div>' : '<div>ok</div><div>ok</div>';
    }, PAGE);
  }, SKEL);
}
function live () {
  fetch('/refreshes', { method: 'POST', body: '[]' }).catch(() => {});
  new EventSource('/notifications');
  // #919: a /call that answers, and one refused at network level (nothing
  // listens on port 9 in the browser's container), each with user extras
  fetch('/call?resource=tables&name=t1&namespace=ns1&extras=FIXTURE-SECRET').catch(() => {});
  fetch('http://127.0.0.1:9/call?resource=boom&name=b1&namespace=ns1&extras=FIXTURE-SECRET').catch(() => {});
}
window.addEventListener('popstate', () => render(location.pathname));
if (location.pathname !== '/login') { menu(); live(); }
render(location.pathname);
</script></body></html>""".replace("@SIGNIN@", str(SIGNIN_MS)).replace(
    "@SKEL@", str(SKEL_MS)).replace("@PAGE@", str(PAGE_MS))


class H(http.server.BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def _send(self, body: bytes, ctype: str):
        self.send_response(200)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _stream(self):
        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream")
        self.end_headers()
        self.close_connection = True
        try:
            while True:
                self.wfile.write(b": keepalive\n\n")
                self.wfile.flush()
                time.sleep(1)
        except OSError:
            pass

    def do_GET(self):
        if self.path.startswith("/notifications"):
            return self._stream()
        return self._send(PAGE.encode(), "text/html")

    def do_POST(self):
        self.rfile.read(int(self.headers.get("Content-Length") or 0))
        if self.path.startswith("/refreshes"):
            return self._stream()
        return self._send(b"{}", "application/json")

    def log_message(self, *a):
        pass


srv = http.server.ThreadingHTTPServer(("0.0.0.0", PORT), H)
srv.daemon_threads = True
srv.serve_forever()
