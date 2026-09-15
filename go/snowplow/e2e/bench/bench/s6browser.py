"""S6 browser half — the half that proves the BROWSER converged, not just the counters.

Companion to `accept1126.py` (the counter half). That module owns the expvar snapshots and
the stage machinery; this one drives a real Chromium against the real gateway and signals
stage boundaries through the hook protocol it defines.

WHY THIS EXISTS AT ALL. The 1.12.5 acceptance reported "new children in 13s" and the number
came from a MANUAL /call the tester issued. An effect was observed and read as evidence of a
mechanism nobody had watched. So the claim this half makes is narrower and harder: a browser
that nobody touched re-fetched, because a frame arrived.

THE TWO CHANNELS, AND WHY BOTH. Channel A is the injected fetch wrapper: it reads the
`/refreshes` SSE body in-page and records frames whose data equals the armed key. Channel B
is the response listener: it counts `/call`s for the child widget that THIS SCRIPT did not
initiate. `crosscheck_channels` in the counter half fails the stage on any disagreement and
never reconciles it — a frame with no refetch is an SPA defect; a refetch with no frame is
the test measuring itself.

THE SILENT NO-OP THIS IS SHAPED TO PREVENT. A widget that never armed produces: no frame, no
refetch, no error — a clean pass proving nothing. Two guards live here, both asserted before
the delete, and either failing aborts rather than proceeding to a meaningless delete:
  * `X-Snowplow-Refresh-Key` must be present on the child's `/call` (snowplow stamps it on
    all non-hit paths INCLUDING declines, so its presence is necessary, not sufficient);
  * the `?sub=` the SPA actually sent must contain the child's coordinates, decoded rather
    than assumed.
A third question — does an L1 entry actually EXIST behind the armed key — cannot be answered
from the browser: no cache-status header is stamped on `/call`, only Refresh-Key and
Refresh-Class. It belongs to the counter half's store_total/hit_total bracket, which is the
server's own accounting and the project's counters-not-timing rule. An earlier version of
this file claimed it by reading an `x-snowplow-cache` header that does not exist, so the
check degraded silently to one that is true for exactly the declined widget it was meant to
reject.

OWNERSHIP, AND THE SHARED PAGE ROOT. The child is per-run and name-prefixed. The page root is
`page-s6-probe` — fixed, not per-run, because the `/s6-probe` route shipped in portal 1.8.23
resolves `flexes/page-<slug>` by convention, so a per-run name would resolve to nothing. That
makes the root SHARED, and a prefix cannot tell two concurrent runs apart: both match it, and
apply-then-delete would let whichever finishes first remove the page the other is measuring
on. So the root also carries a `krateo.io/s6-run` label, create refuses a root belonging to a
different run, and teardown removes only a root carrying this run's id.
"""

from __future__ import annotations

import base64
import json
import os
import subprocess
import sys
import time
from pathlib import Path

from bench import accept1126, browser as bench_browser

#: The page root name is fixed by the nav entry shipped in portal 1.8.23
#: (`{ order: 970, path: /s6-probe, page: s6-probe }` → `flexes/page-s6-probe`).
#: The facts doc §10.5 sketched `page-s6-<runid>`, which assumed a per-run route; a single
#: static route cannot resolve a per-run page name. Concurrent runs would collide on this
#: name, which is correct for a bench acceptance and is what the ownership guard is for.
PAGE_ROOT = "page-s6-probe"
PROBE_PATH = "/s6-probe"
NS = accept1126.NS

#: How long to wait for the frame + induced refetch after the delete. The server paces
#: eviction publication under a per-subscriber token bucket, so this is deliberately longer
#: than a human would guess from a local test.
CONVERGE_TIMEOUT_S = int(os.environ.get("S6_CONVERGE_TIMEOUT", "120"))


def _kubectl(*args: str, stdin: str | None = None) -> str:
    """kubectl with an EXPLICIT context, always. A bench that inherits the ambient context
    will one day run its delete against whatever cluster was last used."""
    cmd = ["kubectl", "--context", accept1126.EXPECTED_CONTEXT, *args]
    proc = subprocess.run(cmd, input=stdin, capture_output=True, text=True)
    if proc.returncode != 0:
        raise accept1126.PreflightFailed(
            f"{' '.join(cmd)} failed: {proc.stderr.strip()[:300]}")
    return proc.stdout


def child_name(run_id: str) -> str:
    """The disposable widget under test. Prefix is what `assert_owned` enforces."""
    return f"{accept1126.OWNED_PREFIX}{run_id[:12]}"


def _manifests(run_id: str) -> str:
    """The minimal CR pair from facts §10.5.

    The child is a `Paragraph` with no apiRef, no resourcesRefs and no keyExtras: nothing
    RBAC-sensitive, no stages, no external touch — so none of the four decline branches in
    widgets.go can fire and the body is genuinely cached. That matters more than it looks:
    snowplow stamps the refresh-class header on declines too, so a declined widget arms a
    key with no cache entry behind it and no eviction can ever fire for it.

    The page root carries the inline GET ref, which makes the ROOT RBAC-sensitive and the
    CHILD widgetContent-eligible. The child is therefore the object to delete.
    """
    child = child_name(run_id)
    return json.dumps({"apiVersion": "v1", "kind": "List", "items": [
        {"apiVersion": "widgets.templates.krateo.io/v1beta1", "kind": "Paragraph",
         "metadata": {"name": child, "namespace": NS,
                      "labels": {"krateo.io/purpose": "s6-acceptance",
                                 "krateo.io/s6-run": run_id[:12]}},
         "spec": {"widgetData": {"text": f"s6 probe {run_id[:12]}"}}},
        {"apiVersion": "widgets.templates.krateo.io/v1beta1", "kind": "Flex",
         "metadata": {"name": PAGE_ROOT, "namespace": NS,
                      "labels": {"krateo.io/purpose": "s6-acceptance",
                                 "krateo.io/s6-run": run_id[:12]}},
         "spec": {
             "resourcesRefs": {"items": [
                 {"id": "s6-probe",
                  "apiVersion": "widgets.templates.krateo.io/v1beta1",
                  "resource": "paragraphs", "name": child,
                  "namespace": NS, "verb": "GET"}]},
             # `allowedResources` is REQUIRED by the Flex CRD alongside `items` — the §10.5
             # sketch omitted it and run 1 was rejected at apply. It names the child's RESOURCE
             # plural, matching the live access-detail-title-stack Flex on 057, which has a
             # paragraph child and carries exactly this.
             "widgetData": {"allowedResources": ["paragraphs"],
                            "items": [{"resourceRefId": "s6-probe"}]}}},
    ]})


# ─── The in-page frame recorder (channel A) ─────────────────────────────────

#: Injected via add_init_script so it is installed BEFORE any application code runs — the
#: SPA opens its /refreshes stream during first render, and a wrapper added afterwards would
#: miss the stream it is meant to observe.
#:
#: It wraps fetch and tees the SSE body. It does NOT parse the protocol beyond splitting
#: frames, because the counter half's assertion is about a specific key arriving, not about
#: frame semantics — and a clever parser here is a second implementation to keep in sync.
_FRAME_RECORDER = r"""
(() => {
  window.__s6 = { frames: [], subs: [], streamOpens: 0 };
  const origFetch = window.fetch;
  window.fetch = function (input, init) {
    const url = typeof input === 'string' ? input : (input && input.url) || '';
    if (url.includes('/refreshes')) {
      window.__s6.streamOpens += 1;
      try {
        const q = url.split('?sub=')[1];
        if (q) window.__s6.subs.push(decodeURIComponent(q.split('&')[0]));
      } catch (e) { /* a malformed sub is itself worth seeing as absent */ }
      return origFetch.apply(this, arguments).then((res) => {
        try {
          const [a, b] = res.body.tee();
          (async () => {
            const rd = b.getReader();
            const dec = new TextDecoder();
            let buf = '';
            for (;;) {
              const { done, value } = await rd.read();
              if (done) break;
              buf += dec.decode(value, { stream: true });
              let i;
              while ((i = buf.indexOf('\n\n')) !== -1) {
                const block = buf.slice(0, i);
                buf = buf.slice(i + 2);
                let ev = null, data = null;
                for (const line of block.split('\n')) {
                  if (line.startsWith('event:')) ev = line.slice(6).trim();
                  else if (line.startsWith('data:')) data = line.slice(5).trim();
                }
                if (ev === 'refresh' && data) {
                  window.__s6.frames.push({ key: data, at: Date.now() });
                }
              }
            }
          })();
          return new Response(a, { headers: res.headers, status: res.status });
        } catch (e) { return res; }
      });
    }
    return origFetch.apply(this, arguments);
  };
})();
"""


class S6Browser:
    """Drives the browser half and writes the hooks the counter half waits on."""

    def __init__(self, run_dir: Path, portal_base: str):
        self.run_dir = Path(run_dir)
        self.portal = portal_base.rstrip("/")
        self.run_id = accept1126.read_run_id(self.run_dir)
        self.child = child_name(self.run_id)
        self.armed_key: str | None = None
        #: Calls this script caused. Anything outside these windows is "uninitiated" —
        #: which is the whole claim: the browser refetched without being told to.
        self._initiating = True
        self._child_calls: list[dict] = []
        #: The exact /call URL the SPA issued for the child, replayed for the second lookup so
        #: it is the same request under the same identity — and therefore the same store key.
        self._child_call_url: str | None = None

    # ─── hooks ──────────────────────────────────────────────────────────────

    def _hook(self, name: str, data: dict) -> None:
        accept1126.write_hook(self.run_dir, name, self.run_id, data)
        print(f"    s6browser: hook {name} <- {json.dumps(data)[:160]}")

    # ─── stages ─────────────────────────────────────────────────────────────

    def _root_run_label(self) -> str | None:
        """The run id stamped on an existing page root, or None if there is no root."""
        out = _kubectl("get", "flex", PAGE_ROOT, "-n", NS, "--ignore-not-found",
                       "-o", r"jsonpath={.metadata.labels.krateo\.io/s6-run}")
        return out.strip() or None

    def create(self) -> None:
        accept1126.assert_owned("paragraphs", self.child)
        accept1126.assert_owned("flexes", PAGE_ROOT)
        # (b) The root is shared by construction, so the prefix alone cannot detect a collision:
        # a concurrent run matches it too. Refuse to overwrite a root another run is measuring
        # on — `apply` would silently repoint it at that run's child, and the first teardown to
        # fire would delete the page out from under the other.
        owner = self._root_run_label()
        mine = self.run_id[:12]
        if owner and owner != mine:
            raise accept1126.NotOwned(
                f"{PAGE_ROOT} already exists and belongs to run {owner!r}, not {mine!r}. A "
                f"concurrent S6 run is using it. This is expected — the root name is pinned by "
                f"the nav entry and cannot be per-run — so wait for that run to finish rather "
                f"than overwriting the page it is measuring on.")
        _kubectl("apply", "-f", "-", stdin=_manifests(self.run_id))
        self._hook("created", {"child": self.child, "page_root": PAGE_ROOT,
                               "namespace": NS})

    def render_and_prove(self, page) -> None:
        """Navigate, confirm the widget genuinely ARMED, and prove its body is cached.

        Order matters. Arming without a cache entry is the silent no-op, so the hit proof
        must come before the delete — a delete whose entry never existed evicts nothing and
        the absence of a frame afterwards would be indistinguishable from a product bug.
        """
        page.on("response", self._on_response)
        # NOT networkidle. This page's whole purpose is to hold a /refreshes SSE stream open,
        # and an SSE response is an in-flight request that never completes — so Playwright's
        # idle condition is unreachable BY CONSTRUCTION here, and run 3 sat in page.goto for
        # the full 120s timeout. (browser_login gets away with networkidle because the login
        # page arms no widgets and opens no stream.) Wait for the document, then for the
        # evidence that actually matters: the child's own /call landing.
        page.goto(f"{self.portal}{PROBE_PATH}", wait_until="domcontentloaded", timeout=60000)
        self._await_child_call(page, "the first render")

        first = [c for c in self._child_calls]
        if not first:
            raise accept1126.AssertionsFailed(
                f"no /call for {self.child} after rendering {PROBE_PATH} — the widget never "
                f"loaded, so nothing armed. Check the route exists (acceptance.s6Probe) and "
                f"that the page root is named {PAGE_ROOT}.")
        key = first[0].get("refresh_key")
        if not key:
            raise accept1126.AssertionsFailed(
                f"the child's /call carried no X-Snowplow-Refresh-Key, so the SPA had "
                f"nothing to arm with. Without a key there is no subscription and no frame "
                f"can ever match.")
        self.armed_key = key

        subs = page.evaluate("() => window.__s6 ? window.__s6.subs : []")
        if not subs:
            raise accept1126.AssertionsFailed(
                "the SPA opened no /refreshes stream (no ?sub= observed), so nothing is "
                "subscribed and the delete would prove nothing.")
        decoded = _decode_subs(subs)
        if not any(c.get("name") == self.child for c in decoded):
            raise accept1126.AssertionsFailed(
                f"{self.child} is NOT in the subscription ({len(decoded)} coords armed). "
                f"It rendered but did not arm — exactly the silent no-op this stage exists "
                f"to catch.")
        self._hook("rendered", {"armed_key": key, "refresh_key": key,
                                "armed_coords": len(decoded),
                                "stream_opens": page.evaluate(
                                    "() => window.__s6.streamOpens")})

        # Re-request the child so the counter half has a second lookup to bracket. The claim
        # that an L1 entry EXISTS is deliberately NOT made here.
        #
        # BB1: an earlier version read `x-snowplow-cache` / `x-cache` to prove a hit. Neither
        # header exists — snowplow stamps only Refresh-Key and Refresh-Class on /call — so the
        # check always fell through to `refresh_key == key`, which is deterministic from the
        # inputs and is stamped on DECLINES too. It was therefore true for exactly the declined
        # widget the guard existed to reject: a tautology wearing the shape of a proof.
        #
        # There is no cache-status header to read, so the browser cannot answer this question at
        # all. The counter half owns it: its store_total/hit_total bracket around this window is
        # real evidence from the server's own accounting, which is also the project's
        # counters-not-timing rule. This half reports the key so that bracket can be attributed.
        # IN-PAGE REFETCH, not page.reload(). §12 asks for "the browser's second /call", and a
        # reload is a far heavier instrument: run 6's reload re-stored the entire shell
        # (store_total Δ=18) and muddied the global brackets the counter half reads. It also
        # tears down the SPA and re-arms from scratch. Re-issuing the widget's own /call from
        # inside the live page keeps the subscription and the shell exactly as they are, and is
        # the thing the stage actually names.
        before = len(self._child_calls)
        page.evaluate("""async (url) => {
          const u = JSON.parse(localStorage.getItem('K_user') || '{}');
          await fetch(url, { headers: { Authorization: `Bearer ${u.accessToken}` } });
        }""", self._child_call_url or "")
        self._await_child_call(page, "the second lookup", since=before)
        later = self._child_calls[before:]
        if not later:
            raise accept1126.AssertionsFailed(
                f"the reload issued no second /call for {self.child}, so the counter half has "
                f"no second lookup to bracket and prove-hit cannot be evaluated server-side.")
        # No `l1_hit` field, deliberately. The browser cannot know whether its /call was served
        # from L1: snowplow stamps no cache-status header, and Refresh-Key is present on hits
        # and declines alike. The counter half answers it per-key instead, by looking the armed
        # key up in the store itself (/debug/apistage?key_hash=…) around both calls. So this
        # half's job is to REPORT the key and to make the second lookup happen — not to claim
        # anything about the result.
        self._hook("hit_proved", {"armed_key": key, "refresh_key": key,
                                  "calls_seen": len(self._child_calls),
                                  "second_lookup_calls": len(later),
                                  "proof_owner": "counter-half:/debug/apistage?key_hash + "
                                                 "store_total/hit_total"})

    def _await_ack(self, page, name: str, timeout_s: int = 120) -> dict:
        """Block until the counter half announces `name` for THIS run.

        The run id is checked, not just the file's presence: --from-stage reuses a run dir, and
        a stale announcement from a previous run would release the delete against evidence that
        belongs to someone else.
        """
        path = self.run_dir / "announce" / f"{name}.json"
        deadline = time.time() + timeout_s
        while time.time() < deadline:
            if path.exists():
                try:
                    payload = json.loads(path.read_text())
                except Exception:
                    payload = {}
                if payload.get("run_id") == self.run_id:
                    print(f"    s6browser: ack {name} <- {json.dumps(payload.get('data', {}))[:120]}")
                    return payload
            page.wait_for_timeout(500)
        raise accept1126.AssertionsFailed(
            f"no {name!r} announcement from the counter half within {timeout_s}s. It either "
            f"failed its cache proof or is not running; either way the delete must NOT proceed, "
            f"because an unverified entry makes the eviction evidence meaningless.")

    def _await_child_call(self, page, what: str, since: int = 0,
                          timeout_s: int = 60) -> None:
        """Wait until the child's own /call has landed, rather than for the network to fall
        idle — which it never does while the refresh stream is open."""
        deadline = time.time() + timeout_s
        while time.time() < deadline:
            if len(self._child_calls) > since:
                page.wait_for_timeout(1500)   # let the arm + any follow-up settle
                return
            page.wait_for_timeout(500)
        raise accept1126.AssertionsFailed(
            f"no /call for {self.child} within {timeout_s}s during {what}. The widget did not "
            f"load, so nothing armed. Check that the /s6-probe route is live "
            f"(acceptance.s6Probe on the portal component) and that the page root is named "
            f"{PAGE_ROOT}.")

    def delete_and_observe(self, page) -> dict:
        """Delete the child, then watch BOTH channels without touching the browser."""
        # From here nothing this script does may cause a /call. Anything observed is the
        # browser acting on its own, which is the entire claim.
        # WAIT FOR THE COUNTER HALF. Run 6 deleted 0.6s after hit_proved, before the counter
        # half had finished its second inspector lookup — so that lookup saw count 0, which was
        # OUR OWN eviction, and the cache proof failed on the success signal. The two halves
        # cannot be ordered by hooks alone: the browser had no way to know the other side had
        # looked. It blocks on the ack now, and fails loudly rather than deleting blind.
        self._await_ack(page, "hit_verified")

        self._initiating = False
        frames_before = page.evaluate("() => window.__s6.frames.length")
        calls_before = len(self._child_calls)

        # (a) Re-assert at the point of use. create() and teardown() both check, but the delete
        # is the irreversible one and the name has travelled through the object since then.
        accept1126.assert_owned("paragraphs", self.child)
        _kubectl("delete", "paragraph", self.child, "-n", NS, "--ignore-not-found")
        self._hook("deleted", {"child": self.child, "armed_key": self.armed_key,
                               "frames_before": frames_before,
                               "calls_before": calls_before})

        deadline = time.time() + CONVERGE_TIMEOUT_S
        frames_for_key = 0
        while time.time() < deadline:
            frames = page.evaluate("() => window.__s6.frames")
            frames_for_key = sum(1 for f in frames[frames_before:]
                                 if f.get("key") == self.armed_key)
            uninitiated = len(self._child_calls) - calls_before
            if frames_for_key and uninitiated:
                break
            page.wait_for_timeout(1000)

        uninitiated = len(self._child_calls) - calls_before
        result = {"frames_for_armed_key": frames_for_key,
                  "uninitiated_calls": uninitiated,
                  "armed_key": self.armed_key,
                  "waited_s": CONVERGE_TIMEOUT_S if not (frames_for_key and uninitiated)
                  else None}
        self._hook("asserted", result)
        return result

    def teardown(self) -> None:
        """Delete only what THIS run created.

        The child is per-run, so the name prefix settles it. The root is not: it is shared by
        construction, so removing it on the prefix alone would take a concurrent run's page with
        it. (b) — the root goes only when it still carries this run's label; a root belonging to
        someone else is left exactly where it is, and said so out loud.
        """
        try:
            accept1126.assert_owned("paragraphs", self.child)
            _kubectl("delete", "paragraph", self.child, "-n", NS, "--ignore-not-found")
        except accept1126.NotOwned as exc:
            print(f"    s6browser: REFUSING to delete paragraph/{self.child}: {exc}")
        try:
            accept1126.assert_owned("flexes", PAGE_ROOT)
            owner = self._root_run_label()
            if owner and owner != self.run_id[:12]:
                print(f"    s6browser: leaving {PAGE_ROOT} — it belongs to run {owner!r}, "
                      f"not {self.run_id[:12]!r}")
            else:
                _kubectl("delete", "flex", PAGE_ROOT, "-n", NS, "--ignore-not-found")
        except accept1126.NotOwned as exc:
            print(f"    s6browser: REFUSING to delete flex/{PAGE_ROOT}: {exc}")

    # ─── network capture (channel B) ────────────────────────────────────────

    def _on_response(self, response) -> None:
        url = response.url
        if "/call" not in url or self.child not in url:
            return
        headers = {}
        try:
            headers = response.headers
        except Exception:
            pass
        if self._child_call_url is None:
            self._child_call_url = url
        self._child_calls.append({
            "at": time.time(),
            "initiated_by_harness": self._initiating,
            "status": response.status,
            "refresh_key": headers.get("x-snowplow-refresh-key"),
            "refresh_class": headers.get("x-snowplow-refresh-class"),
            # No cache-status field: snowplow stamps only Refresh-Key and Refresh-Class on
            # /call. Recording a header that does not exist is what let the removed hit-proof
            # look like it was checking something, so the field goes rather than lingering as
            # a permanently-None value for someone to build on again.
        })


def _decode_subs(subs: list[str]) -> list[dict]:
    """Decode the base64url `?sub=` payloads the SPA sent. Unpadded, per refreshSse.ts."""
    out: list[dict] = []
    for s in subs:
        try:
            pad = "=" * (-len(s) % 4)
            out.extend(json.loads(base64.urlsafe_b64decode(s + pad)))
        except Exception:
            continue
    return out


def run_browser_half(run_dir: Path, portal_base: str) -> int:
    """Entry point. Returns a process exit code; never raises past this boundary."""
    from playwright.sync_api import sync_playwright

    user, password = accept1126._creds()
    s6 = S6Browser(run_dir, portal_base)
    print(f"    s6browser: run_id={s6.run_id} child={s6.child} page={PAGE_ROOT}")

    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=True)
        # BB2 — make_browser_context defaults ignore_https_errors=True (browser.py:1253), which
        # is wrong for the context that types the harness password. The portal serves a real
        # Let's Encrypt certificate, so there is nothing to tolerate and a MITM must fail the run.
        ctx = bench_browser.make_browser_context(browser, ignore_https_errors=False)
        ctx.add_init_script(_FRAME_RECORDER)
        page = ctx.new_page()
        try:
            if not bench_browser.browser_login(page, user, password):
                raise accept1126.PreflightFailed(
                    "browser login failed — the harness identity could not sign in through "
                    "the UI. Check HARNESS_USER/HARNESS_PASSWORD and that the s6-harness "
                    "User CR still exists.")
            s6.create()
            s6.render_and_prove(page)
            result = s6.delete_and_observe(page)
            ok = result["frames_for_armed_key"] == 1 and result["uninitiated_calls"] == 1
            print(f"    s6browser: {'OK' if ok else 'MISMATCH'} {json.dumps(result)}")
            return 0 if ok else 1
        finally:
            s6.teardown()
            ctx.close()
            browser.close()


def add_parsers(sub) -> None:
    p = sub.add_parser("accept1126-browser",
                       help="S6 browser half: create, render, prove-hit, delete, observe")
    p.add_argument("--run-dir", required=True)
    p.add_argument("--portal-base", default=accept1126.DEFAULT_PORTAL_BASE)
    p.set_defaults(func=lambda a: run_browser_half(Path(a.run_dir), a.portal_base))
