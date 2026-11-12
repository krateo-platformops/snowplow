"""A tiny SPA with the portal's observable contract, for running k6/browser.js
in real Chrome under test (tests/test_k6_830.py, #831 gate):

  /login       DOM=1.8.40 (the default): frontend 1.8.40's page, frontend#659's
               contract: an LDAPConfig's form FIRST (form#login_ldap_corp, the
               same "Username" / "Password" labels, its own "LDAP sign in"
               submit button, which signs nobody in), then basic's
               form#login_basic (#login_basic_username, #login_basic_password)
               and its "Sign in" submit button. DOM=1.8.36: the same page as
               1.8.36 .. 1.8.39 render it ("Sign In", "LDAP Sign In").
               DOM=two-submits: 1.8.40's with a second submit button in
               form#login_basic (no release has one). DOM=old: the page before
               #659 (the antd form ids #basic_username, #basic_password;
               Enter). Either "signs in" after
               SIGNIN_MS (authn's x509 login), then the app navigates to
               /dashboard (history.pushState, like Login.tsx:92)
  /__seen      every GET path served so far (a test's proof of what was reached)
  a route      renders, after SKELETON_DELAY_MS, `[data-widget-renderer]`
               skeletons (WidgetLoading) that resolve PAGE_MS later
  /never       its skeleton never resolves (a page timeout)
  /err         resolves to a `data-testid="widget-error"` end state
  sidebar      `li.ant-menu-item` with `data-menu-id="rc-menu-<path>"`
  live updates POST /refreshes (fetch) and EventSource /notifications
  /call        one GET /call that answers, one to a refused port (#919)
  sign-in      stores K_user {accessToken} in localStorage, as the SPA's Login
               does (utils/getAccessToken.ts reads it back)
  /debug/vars  (#935) Bearer-only (401 without): snowplow's two learned-class
               counters; a class is "learned" the first time its token reads

Usage: k6_browser_fixture.py PORT SIGNIN_MS SKELETON_DELAY_MS PAGE_MS [1.8.40|1.8.36|two-submits|old]"""
import http.server
import json
import sys
import threading
import time

PORT, SIGNIN_MS, SKEL_MS, PAGE_MS = (int(x) for x in sys.argv[1:5])
DOM = sys.argv[5] if len(sys.argv) > 5 else "1.8.40"
if DOM not in ("1.8.40", "1.8.36", "two-submits", "old"):
    sys.exit(f"DOM must be 1.8.40, 1.8.36, two-submits or old, got {DOM!r}")

PAGE = """<!doctype html><html><head><meta charset="utf-8"><title>fixture</title></head>
<body><ul id="menu"></ul><div id="content"></div><script>
const ROUTES = ['/dashboard', '/compositions', '/never', '/err'];
const SIGNIN = @SIGNIN@, SKEL = @SKEL@, PAGE = @PAGE@, DOM = '@DOM@';
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
    const signIn = (who) => setTimeout(() => {
      localStorage.setItem('K_user', JSON.stringify({ accessToken: 'fixture-token-' + who }));
      history.pushState({}, '', '/dashboard'); menu(); render('/dashboard'); live();
    }, SIGNIN);
    if (DOM === 'old') {
      content.innerHTML = '<form><input id="basic_username"><input id="basic_password" type="password"></form>';
      document.getElementById('basic_password').addEventListener('keydown', (e) => {
        if (e.key !== 'Enter') return;
        e.preventDefault();
        signIn(document.getElementById('basic_username').value);
      });
      return;
    }
    // antd: the form's name is its id and every field id's prefix; the button's label in a span.
    // The labels as LoginForm.tsx renders them: 1.8.36 .. 1.8.39 "Sign In" / "LDAP Sign In",
    // 1.8.40 "Sign in" / "LDAP sign in"
    const v36 = DOM === '1.8.36';
    const submit = (text) => '<button type="submit" class="ant-btn"><span>' + text + '</span></button>';
    const form = (name, button, extra) => '<section><form id="' + name + '">' +
      '<label for="' + name + '_username">Username</label><input id="' + name + '_username" autocomplete="username">' +
      '<label for="' + name + '_password">Password</label>' +
      '<input id="' + name + '_password" type="password" autocomplete="current-password">' +
      submit(button) + (extra || '') + '</form></section>';
    content.innerHTML = form('login_ldap_corp', v36 ? 'LDAP Sign In' : 'LDAP sign in') +
      form('login_basic', v36 ? 'Sign In' : 'Sign in', DOM === 'two-submits' ? submit('Sign in again') : '') +
      '<div id="login-error"></div>';
    document.getElementById('login_ldap_corp').addEventListener('submit', (e) => {
      e.preventDefault();
      document.getElementById('login-error').textContent = 'LDAP: invalid credentials';
    });
    document.getElementById('login_basic').addEventListener('submit', (e) => {
      e.preventDefault();
      signIn(document.getElementById('login_basic_username').value);
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
  // listens on port 8099 in the browser's container; not one of Chrome's
  // blocked 'unsafe' ports, which fail before connecting), with user extras
  fetch('/call?resource=tables&name=t1&namespace=ns1&extras=FIXTURE-SECRET').catch(() => {});
  fetch('http://127.0.0.1:8099/call?resource=boom&name=b1&namespace=ns1&extras=FIXTURE-SECRET').catch(() => {});
}
window.addEventListener('popstate', () => render(location.pathname));
if (location.pathname !== '/login') { menu(); live(); }
render(location.pathname);
</script></body></html>""".replace("@SIGNIN@", str(SIGNIN_MS)).replace(
    "@SKEL@", str(SKEL_MS)).replace("@PAGE@", str(PAGE_MS)).replace("@DOM@", DOM)


LEARNED: set = set()
#: every GET path served, in order (GET /__seen answers them, itself excluded)
SEEN: list = []
SEEN_LOCK = threading.Lock()
LEARNED_LOCK = threading.Lock()


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

    def _vars(self):
        auth = self.headers.get("Authorization") or ""
        if not auth.startswith("Bearer ") or len(auth) <= len("Bearer "):
            self.send_response(401)
            self.send_header("Content-Length", "0")
            self.end_headers()
            return
        with LEARNED_LOCK:
            LEARNED.add(auth)
            n = len(LEARNED)
        body = json.dumps({"snowplow_learned_classes_registered": n, "snowplow_learned_classes_seeded": n}).encode()
        self._send(body, "application/json")

    def do_GET(self):
        if self.path == "/__seen":
            with SEEN_LOCK:
                return self._send(json.dumps(SEEN).encode(), "application/json")
        with SEEN_LOCK:
            SEEN.append(self.path)
        if self.path.startswith("/debug/vars"):
            return self._vars()
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
