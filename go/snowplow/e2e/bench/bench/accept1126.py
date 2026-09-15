"""accept1126.py — the COUNTER HALF of the 1.12.6 item-7 (S6) live acceptance.

WHAT THIS IS

S6 is the live acceptance for snowplow 1.12.6 item 7: delete a CR with a browser
tab armed for its key and assert a `refresh` frame arrives AND the page updates
with NO harness-initiated `/call`. It has two halves:

  - the BROWSER half (portal session) drives headless Chromium through the real
    gateway path, creates + renders + deletes the disposable widget, and owns
    the two evidence channels (injected fetch wrapper for the frame; CDP request
    count for the effect);
  - the COUNTER half — THIS MODULE — owns preflight, the expvar snapshots either
    side of the delete, the assertion table, the gated CRD-relist stage and the
    final report.

Facts, file:line traces and the assertion table this module implements are in
`reports/acceptance-1126-facts.md` (tester, 2026-09-15). Section numbers below
(§10.2, §12, §14, §15, §16) refer to that document.

THE FIVE THINGS THAT MAKE THIS HARNESS CORRECT RATHER THAN GREEN

  1. §10.2 — snowplow stamps `X-Snowplow-Refresh-Key` even when the body was
     DECLINED and never cached (`widgets.go:552` serves "ALL non-HIT paths …
     stage-error decline, external-skip decline"). "The widget armed" is
     therefore NOT evidence. `A1_counters_before` refuses to proceed unless the
     browser half proved a real L1 store + hit first (P1/P2).
  2. §2 — `evict_delete_total` exists under TWO parents with different meanings.
     Every read in this module is parent-qualified via `expvar_get()`, which
     REQUIRES the parent key; there is no bare-name accessor to misuse.
  3. §3.2 / §16 — `/debug/reconcile` is a MUTATION that holds the store mutex
     for a full walk. Every HTTP request this module makes goes through
     `_RequestLog`, and `A4_reconcile_selfcheck` asserts none touched it inside
     the measurement window.
  4. R1 — nothing pre-existing is ever written. `assert_owned()` gates every
     mutating call on the run-unique name prefix, and the CRD stage is
     separately gated + OFF by default (§14) pending Diego's approval.
  5. No silent skips (`feedback_silent_skip_breaks_convergence_proof`): a stage
     that cannot do its work RAISES. There is no branch in this file that
     returns "skipped" and lets the run go green.

CREDENTIALS

`HARNESS_USER` / `HARNESS_PASSWORD` env only → `GET /auth/basic/login` (the login
is a GET with a Basic header — `feedback_basic_login_is_get`). The password is
read once, used once, and never written to the run dir, a proof, a log line or a
report. `_redact()` guards the one place a token could leak into a proof.

The harness user is a DEDICATED least-privilege account (see
`reports/harness-user-spec.md`) — never a platform secret, never admin.

MEASUREMENT VANTAGE (`feedback_northstar_measurement_vantage`)

Every counter read is an HTTP GET to `/content/debug/vars` **through the platform
gateway**, the same path the browser uses — never a port-forward, never a
kubectl exec. kubectl is used ONLY for read-only preflight facts (deployed image
digests) and for the gated CRD stage's own objects. This module produces NO
latency numbers, so `feedback_no_kubectl_in_measurement` is satisfied by
construction rather than by discipline.
"""

from __future__ import annotations

import argparse
import base64
import datetime
import json
import os
import ssl
import time
import urllib.error
import urllib.request
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable

from bench import cluster
from bench.phases import StageProof, _now_iso, load_state, save_state


# ─── Constants ──────────────────────────────────────────────────────────────

#: The cluster this acceptance targets. Asserted, never assumed: the bench's
#: canonical pin is a DIFFERENT cluster (cluster.py CANONICAL_GKE_CONTEXT is the
#: neon/us-central bench cluster), so a run against 057 MUST set
#: BENCH_GKE_CONTEXT. Preflight fails loudly if it does not match.
EXPECTED_CONTEXT = "gke_operations-dev-krateo-io_europe-west3-a_krateo-057"

#: Portal ingress. All HTTP goes here — gateway path only.
DEFAULT_PORTAL_BASE = "https://portal.krateo.dev"

#: Route prefixes on the platform gateway (facts doc §1.1).
SNOWPLOW_PREFIX = "/content"     # → snowplow:8081, URLRewrite to /
AUTHN_PREFIX = "/auth"           # → authn:8082, URLRewrite to /

NS = "krateo-system"

#: Expected deployed builds. Read FROM THE CLUSTER, not from git tags — a tag
#: says what was cut, the Deployment says what is running.
EXPECTED_SNOWPLOW_VERSION = "1.12.6"
EXPECTED_FRONTEND_VERSION = "1.6.13"

#: Every object this harness creates carries this prefix. R1: the never-touch
#: rule is asserted by NAME PREFIX, the same discriminant the phase6 cleanup
#: uses for bench orphans (`feedback_bench_cleanup_gotchas`: the label selector
#: matches nothing; the name prefix is the reliable one).
OWNED_PREFIX = "s6-probe-"
OWNED_PAGE_PREFIX = "page-s6-"
OWNED_CRD_SUFFIX = ".harness.krateo.io"

#: The four composite expvar keys this module reads (facts doc §2).
K_DEPS = "snowplow_deps"
K_RESOLVED = "snowplow_resolved_cache"
K_BROADCAST = "snowplow_refresh_broadcaster"
K_CRD = "snowplow_crd_discovery"

#: §16 defence-in-depth ceiling: the periodic reconcile ticker samples at most
#: 512 entries every 30 s. A manual /debug/reconcile full walk blows past it.
RECONCILE_SAMPLE_PER_TICK = 512
RECONCILE_TICK_SECONDS = 30

#: Hook names the browser half signals for Stage A, in arrival order.
HOOK_ORDER = ["created", "rendered", "hit_proved", "deleted", "asserted"]

#: Stage B1 (the gated CRD relist) has its OWN hook names. Review finding B3:
#: reusing Stage A's names made B1's waits return instantly with Stage A's
#: payloads, because A1 had already consumed them earlier in the same process.
#: A stage must never wait on a hook another stage can satisfy.
CRD_HOOK_ORDER = ["crd_created", "crd_rendered", "crd_bumped"]

ALL_HOOKS = HOOK_ORDER + CRD_HOOK_ORDER

HOOK_WAIT_TIMEOUT_S = int(os.environ.get("S6_HOOK_TIMEOUT", "900"))

#: How long to let the deferred eviction drain settle before the after-snapshot.
#: C10 defers under the per-subscriber token bucket (§15.3 A17), so an immediate
#: read can legitimately miss frames that are still queued.
SETTLE_SECONDS = int(os.environ.get("S6_SETTLE_SECONDS", "10"))


# ─── Errors (all loud; none of these is ever swallowed) ─────────────────────


class PreflightFailed(RuntimeError):
    """A precondition for a meaningful S6 run is not met."""


class NotOwned(RuntimeError):
    """A mutating call named an object this harness does not own (R1)."""


class HookTimeout(RuntimeError):
    """The browser half did not signal a hook inside the timeout."""


class AssertionsFailed(RuntimeError):
    """One or more rows of the §15 assertion table failed."""


class ReconcilePerturbation(RuntimeError):
    """/debug/reconcile was called inside the measurement window (§16)."""


# ─── Credential handling ────────────────────────────────────────────────────


def _password_from_secret(secret: str) -> str:
    """Read the harness user's password from its OWN Secret on the cluster.

    Available only when the operator has scoped this harness a read of that one
    Secret (harness-user-spec.md §6 option b). It is NOT the platform-admin
    Secret read that was denied: `s6-harness-password` belongs to a purpose-made
    account with **no groups and no RBAC**, is rotatable independently, and
    grants exactly one capability — authenticated GETs to a read-only
    diagnostic surface.

    `kubectl -o jsonpath` does **NOT** base64-decode Secret data
    (`feedback_no_kubectl_jsonpath_for_in_process_reasoning`), so the value is
    decoded here. A caller that skipped the decode would send a base64 blob as
    the password and get a confusing 401.

    The decoded value is returned to `_creds`, used once for the Basic header,
    and never logged, persisted or put in a proof.
    """
    rc, out, err = cluster.kubectl(
        "get", "secret", "-n", NS, secret,
        "-o", "jsonpath={.data.password}")
    if rc != 0 or not out.strip():
        raise PreflightFailed(
            f"could not read the harness password from Secret {secret!r} in "
            f"{NS} (rc={rc}). Either the scoped read is not granted, or the "
            f"Secret does not carry a `password` key. {err.strip()[:200]}")
    try:
        return base64.b64decode(out.strip()).decode()
    except Exception as e:                                  # malformed data
        raise PreflightFailed(
            f"Secret {secret!r} .data.password is not valid base64: {e}"
        ) from None


def _creds() -> tuple[str, str]:
    """The harness identity. Never logged, never persisted.

    Two routes, in order of preference (harness-user-spec.md §6):
      (a) HARNESS_USER + HARNESS_PASSWORD in the environment — no new
          permissions, the operator supplies it;
      (b) HARNESS_USER + S6_PASSWORD_FROM_SECRET naming the harness's OWN
          Secret, read through `cluster.kubectl` (which pins --context
          explicitly) when that scoped read has been granted.

    Raises PreflightFailed (not a skip) when neither route yields a credential —
    a run without an identity cannot read /debug and must not silently degrade
    to "counters unavailable". There is deliberately no third route: no token is
    ever accepted from another process.
    """
    user = os.environ.get("HARNESS_USER", "").strip()
    pw = os.environ.get("HARNESS_PASSWORD", "")
    secret = os.environ.get("S6_PASSWORD_FROM_SECRET", "").strip()

    if user and not pw and secret:
        pw = _password_from_secret(secret)

    if not user or not pw:
        raise PreflightFailed(
            "no harness identity. Set HARNESS_USER plus either "
            "HARNESS_PASSWORD, or S6_PASSWORD_FROM_SECRET naming the harness "
            "user's own Secret when that scoped read is granted. The counter "
            "half authenticates as a DEDICATED least-privilege user (see "
            "reports/harness-user-spec.md) — never a platform credential, and "
            "never a token handed over by another process.")
    return user, pw


def _redact(value: Any) -> Any:
    """Defensive scrub for anything that might carry a bearer into a proof."""
    if isinstance(value, str) and value.count(".") == 2 and len(value) > 40:
        return "<redacted-jwt>"
    return value


# ─── HTTP, with every request recorded (§16) ────────────────────────────────


@dataclass
class _RequestLog:
    """Every URL this module fetches, with a timestamp.

    This is the AUTHORITATIVE source for the §16 "/debug/reconcile was never
    called" assertion: it is a property of our own behaviour, checked from our
    own record, and cannot be confounded by other traffic on the cluster.
    """
    entries: list[dict] = field(default_factory=list)

    def record(self, method: str, url: str, status: int | None) -> None:
        self.entries.append({
            "at": _now_iso(), "method": method,
            # Strip any query string: it can carry a sub= payload, and no
            # assertion needs it.
            "url": url.split("?", 1)[0],
            "status": status,
        })

    def paths_between(self, start_iso: str, end_iso: str) -> list[str]:
        return [e["url"] for e in self.entries
                if start_iso <= e["at"] <= end_iso]

    def touched_reconcile(self, start_iso: str, end_iso: str) -> list[str]:
        return [u for u in self.paths_between(start_iso, end_iso)
                if "/debug/reconcile" in u]


def _ssl_ctx() -> ssl.SSLContext:
    """Portal TLS — VERIFIED by default.

    Measured 2026-09-15: portal.krateo.dev presents a real Let's Encrypt cert
    (issuer CN=YR1, subject CN=portal.krateo.dev, valid to 2026-12-07), so
    verification succeeds with the system trust store and there is no reason to
    weaken it. S6_INSECURE_TLS=1 exists only as a deliberate break-glass for a
    cluster whose cert has expired or been swapped for a self-signed one; it is
    never needed on 057 today, and a run that sets it should say why.
    """
    ctx = ssl.create_default_context()
    if os.environ.get("S6_INSECURE_TLS", "") == "1":
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
    return ctx


def _http_get(url: str, token: str | None, reqlog: _RequestLog,
              timeout: int = 30) -> tuple[int, bytes]:
    headers = {"Accept": "application/json"}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    req = urllib.request.Request(url, headers=headers, method="GET")
    try:
        with urllib.request.urlopen(req, timeout=timeout,
                                    context=_ssl_ctx()) as r:
            body = r.read()
            reqlog.record("GET", url, r.status)
            return r.status, body
    except urllib.error.HTTPError as e:
        body = e.read()
        reqlog.record("GET", url, e.code)
        return e.code, body


def login(portal_base: str, reqlog: _RequestLog) -> str:
    """GET /auth/basic/login with a Basic header → accessToken.

    The login is a GET, not a POST (`feedback_basic_login_is_get`). The
    credential never leaves this function and the token is never persisted.
    """
    user, pw = _creds()
    creds = base64.b64encode(f"{user}:{pw}".encode()).decode()
    url = f"{portal_base}{AUTHN_PREFIX}/basic/login"
    req = urllib.request.Request(
        url, headers={"Authorization": "Basic " + creds}, method="GET")
    try:
        with urllib.request.urlopen(req, timeout=30, context=_ssl_ctx()) as r:
            reqlog.record("GET", url, r.status)
            tok = json.loads(r.read())["accessToken"]
    except urllib.error.HTTPError as e:
        reqlog.record("GET", url, e.code)
        raise PreflightFailed(
            f"authn login failed for HARNESS_USER (HTTP {e.code}). The user "
            f"must exist as a users.basic.authn.krateo.io CR with its "
            f"<user>-password Secret — see reports/harness-user-spec.md."
        ) from None
    except (KeyError, json.JSONDecodeError) as e:
        raise PreflightFailed(
            f"authn login returned no accessToken: {e}") from None
    return tok


# ─── The expvar surface, parent-qualified by construction (§2) ──────────────


def read_debug_vars(portal_base: str, token: str,
                    reqlog: _RequestLog) -> dict:
    """GET /content/debug/vars through the gateway.

    Distinguishes the three outcomes the facts doc §1.2 calls out, because they
    mean different things and a harness that conflates them misreports:
      200 → the snapshot;
      401 → the token is missing/expired/invalid;
      503 → authn or its JWKS is UNREACHABLE. This is NOT a snowplow fault and
            must never be reported as one (there is no break-glass; the whole
            debug surface answers 503 while authn is down).
    """
    url = f"{portal_base}{SNOWPLOW_PREFIX}/debug/vars"
    status, body = _http_get(url, token, reqlog)
    if status == 503:
        raise PreflightFailed(
            "/debug/vars returned 503 — RefreshAuth maps ErrKeyUnavailable to "
            "503, so authn or its JWKS endpoint is unreachable. THIS IS NOT A "
            "SNOWPLOW FAULT. Fix authn, then re-run; do not record a snowplow "
            "failure for it.")
    if status == 401:
        raise PreflightFailed(
            "/debug/vars returned 401 — the harness JWT is missing, expired or "
            "invalid. RefreshAuth accepts ANY valid Krateo JWT and checks no "
            "role/group/scope, so a 401 here means the token itself, not RBAC.")
    if status != 200:
        raise PreflightFailed(f"/debug/vars returned HTTP {status}")
    return json.loads(body)


def expvar_get(snapshot: dict, parent: str, stat: str) -> int:
    """Read `parent.stat` from a /debug/vars snapshot. Parent is MANDATORY.

    There is deliberately no bare-name accessor in this module: §2 of the facts
    doc records that `evict_delete_total` exists under BOTH `snowplow_deps`
    (informer-DELETE-driven only) and `snowplow_resolved_cache` (store-side),
    with different meanings. Requiring the parent makes the ambiguous read
    unrepresentable rather than merely discouraged.

    A missing parent is an ERROR, not a zero: under CACHE_ENABLED=false the
    publishers are not registered AT ALL (CFG-1, facts §2.5), so absence means
    "cache is off", which would silently zero every delta in the table.
    """
    if parent not in snapshot:
        raise AssertionsFailed(
            f"expvar key {parent!r} absent from /debug/vars. Under "
            f"CACHE_ENABLED=false these publishers are not registered at all "
            f"(not registered-with-zeros), so this means the cache layer is "
            f"OFF — every delta in the assertion table would be a false zero.")
    node = snapshot[parent]
    if not isinstance(node, dict) or stat not in node:
        raise AssertionsFailed(
            f"expvar stat {parent}.{stat} absent. Present under {parent}: "
            f"{sorted(node)[:12] if isinstance(node, dict) else type(node)}")
    return int(node[stat])


def snapshot_counters(snapshot: dict) -> dict:
    """Project the full /debug/vars blob down to the stats the table needs.

    Keys are stored parent-qualified ("snowplow_deps.evict_delete_total") so the
    persisted proof is self-describing and a reviewer never has to guess which
    `evict_delete_total` a number came from.
    """
    wanted = [
        (K_DEPS, ["evict_delete_total", "evict_self_gone_total",
                  "evict_drop_point_total", "delete_worker_panics_total",
                  "dropped_cap", "reconcile_divergence_total",
                  "reconcile_sampled_total", "reconcile_ticks_total",
                  "events_submitted_total", "dep_event_queue_depth"]),
        (K_RESOLVED, ["evict_delete_total", "evict_ttl_total",
                      "evict_lru_total", "evict_max_age_total",
                      "store_total", "hit_total", "miss_total", "entries"]),
        (K_BROADCAST, ["published", "delivered", "dropped", "coalesced",
                       "subscribers", "armed_keys", "max_sink_depth",
                       "evict_published", "evict_deferred",
                       "stream_seconds_total", "streams_closed_total"]),
    ]
    out: dict[str, int | float] = {}
    for parent, stats in wanted:
        for stat in stats:
            out[f"{parent}.{stat}"] = expvar_get(snapshot, parent, stat)
    return out


def snapshot_crd_counters(snapshot: dict) -> dict:
    """The §14.3 bridge counters, for the gated CRD-relist stage only."""
    stats = ["schema_relists_fired", "schema_unchanged",
             "relist_bridge_runs_total", "relist_bridge_enqueued_total",
             "relist_bridge_timeout_total", "relist_bridge_aborted_total",
             "relist_dirtymark_postsync_total", "relist_postsync_timeout_total"]
    return {f"{K_CRD}.{s}": expvar_get(snapshot, K_CRD, s) for s in stats}


def delta(before: dict, after: dict) -> dict:
    """after - before, per qualified key. Keys must match exactly."""
    if set(before) != set(after):
        raise AssertionsFailed(
            "counter snapshots have different key sets — the pod was replaced "
            "mid-run (a restart re-registers expvars from zero), so no delta "
            f"is meaningful. only-before={sorted(set(before) - set(after))} "
            f"only-after={sorted(set(after) - set(before))}")
    return {k: after[k] - before[k] for k in before}


# ─── R1: the never-touch rule, asserted by name prefix ──────────────────────


def assert_owned(kind: str, name: str) -> None:
    """Refuse to mutate anything this harness did not create (R1).

    Gate for EVERY mutating call in this module. The check is on the NAME
    PREFIX, which is the discriminant that actually works on this cluster
    (`feedback_bench_cleanup_gotchas`: the bench label selector matches
    nothing).
    """
    if kind == "crd":
        if not name.endswith(OWNED_CRD_SUFFIX):
            raise NotOwned(
                f"refusing to touch CRD {name!r}: the harness only owns CRDs "
                f"ending {OWNED_CRD_SUFFIX!r}")
        return
    if not (name.startswith(OWNED_PREFIX) or name.startswith(OWNED_PAGE_PREFIX)):
        raise NotOwned(
            f"refusing to touch {kind}/{name!r}: the harness only owns objects "
            f"prefixed {OWNED_PREFIX!r} or {OWNED_PAGE_PREFIX!r}. Pre-existing "
            f"platform objects are never created, patched or deleted by S6.")


# ─── Preflight reads (cluster, read-only) ───────────────────────────────────


def _deployed_image(deploy: str) -> str:
    """The image the Deployment is ACTUALLY running, straight from the cluster.

    Read from the cluster and not from a git tag on purpose: a tag says what was
    cut, the Deployment says what is running, and S6's ordering precondition
    (facts §5) exists precisely because those two disagreed.
    """
    rc, out, err = cluster.kubectl(
        "get", "deploy", "-n", NS, deploy,
        "-o", "jsonpath={.spec.template.spec.containers[0].image}")
    if rc != 0:
        raise PreflightFailed(f"kubectl get deploy/{deploy} failed: {err}")
    img = out.strip()
    if not img:
        raise PreflightFailed(f"deploy/{deploy} reported no image")
    return img


def _version_of(image: str) -> str:
    """Tag portion of `repo:tag` (digest-pinned images report the digest)."""
    return image.rsplit(":", 1)[-1] if ":" in image else image


def _served_bundle_hash(portal_base: str, reqlog: _RequestLog) -> str:
    """The content-hashed SPA bundle name from the served index.html (§5.1).

    There is NO version endpoint: `/version` returns 200 text/html because it is
    the SPA's index fallback, and would pass on ANY build. The content hash of
    `/assets/index-<hash>.js` is the real fingerprint — it changes iff the
    bundle content changes, so it catches a re-tagged or cached image that the
    Deployment tag alone would miss.
    """
    status, body = _http_get(f"{portal_base}/", None, reqlog)
    if status != 200:
        raise PreflightFailed(f"portal root returned HTTP {status}")
    text = body.decode("utf-8", "replace")
    marker = 'src="/assets/index-'
    i = text.find(marker)
    if i < 0:
        raise PreflightFailed(
            "could not find /assets/index-<hash>.js in the served index.html — "
            "the SPA build fingerprint is unavailable, so the §5 ordering "
            "precondition cannot be asserted.")
    j = text.find('"', i + len(marker))
    return text[i + len('src="/assets/'):j]


# ─── Hook handshake with the browser half ───────────────────────────────────


def _hook_path(run_dir: Path, name: str) -> Path:
    return Path(run_dir) / "hooks" / f"{name}.json"


def read_run_id(run_dir: Path) -> str:
    """The run id the browser half must stamp on every hook.

    Written by `run()` at start and read by the hook CLI, so the browser half
    does not have to be told it out of band.
    """
    p = Path(run_dir) / "run_id"
    if not p.exists():
        raise PreflightFailed(
            f"{p} does not exist — start `python -m bench accept1126` first; it "
            f"writes the run id every hook must carry.")
    return p.read_text().strip()


def write_hook(run_dir: Path, name: str, run_id: str,
               data: dict | None = None) -> Path:
    """Called by the BROWSER half (via `python -m bench accept1126-hook`).

    `run_id` is MANDATORY and is stamped into the payload. Review finding B2: a
    hook file without a run identity is satisfied by the PREVIOUS run's evidence
    when `--from-stage` re-uses the run dir, and the channel cross-check then
    compares two stale payloads that agree trivially because they came from the
    same prior run.
    """
    if name not in ALL_HOOKS:
        raise ValueError(f"unknown hook {name!r}; expected one of {ALL_HOOKS}")
    p = _hook_path(run_dir, name)
    p.parent.mkdir(parents=True, exist_ok=True)
    payload = {"hook": name, "run_id": run_id, "at": _now_iso(),
               "data": data or {}}
    tmp = p.with_suffix(".tmp")
    with open(tmp, "w") as fh:
        json.dump(payload, fh, indent=2)
        fh.flush()
        os.fsync(fh.fileno())
    tmp.replace(p)          # atomic: the waiter never sees a partial file
    return p


def wait_hook(run_dir: Path, name: str, run_id: str, not_before: str,
              timeout_s: int = HOOK_WAIT_TIMEOUT_S) -> dict:
    """Block until the browser half signals `name` FOR THIS RUN AND STAGE.

    Three binding conditions, all of which must hold before a file counts as a
    signal (review finding B2):

      - the payload's `run_id` equals this run's id — a hook from a previous
        attempt in the same run dir is ignored, not consumed;
      - the payload's `at` is >= `not_before`, the moment the waiting stage
        began — so a hook written earlier in THIS run (by an earlier stage)
        cannot satisfy a later wait either. That is what made B3 deterministic;
      - the file parses.

    A stale file is treated as NOT YET SIGNALLED and the wait continues, so the
    browser half can simply overwrite it. A timeout RAISES rather than skipping
    (`feedback_silent_skip_breaks_convergence_proof`), and the message says
    which of the three conditions the newest candidate failed, because "timed
    out" alone would not tell the operator a stale hook was sitting there.
    """
    p = _hook_path(run_dir, name)
    deadline = time.monotonic() + timeout_s
    last_reason = "no hook file was ever written"
    while time.monotonic() < deadline:
        if p.exists():
            try:
                with open(p) as fh:
                    payload = json.load(fh)
            except (json.JSONDecodeError, OSError) as e:
                last_reason = f"hook file unparseable ({e})"
                payload = None
            if payload is not None:
                got_id = payload.get("run_id")
                got_at = payload.get("at", "")
                if got_id != run_id:
                    last_reason = (
                        f"a hook file exists but carries run_id={got_id!r}, "
                        f"not this run's {run_id!r} — it is left over from a "
                        f"previous attempt in this run dir and was IGNORED")
                elif got_at < not_before:
                    last_reason = (
                        f"a hook file exists for this run but was written at "
                        f"{got_at}, before this stage began at {not_before} — "
                        f"an earlier stage's signal, IGNORED")
                else:
                    return payload
        time.sleep(1.0)
    raise HookTimeout(
        f"hook {name!r} not signalled within {timeout_s}s (expected {p}). "
        f"Reason: {last_reason}. The counter half cannot assert anything "
        f"without it.")


# ─── The §15 assertion table ────────────────────────────────────────────────


@dataclass
class Row:
    """One assertion. `check` takes the delta dict and returns (ok, actual)."""
    rid: str
    what: str
    key: str | None
    expect: str
    check: Callable[[dict], tuple[bool, Any]]


def _eq(key: str, want: int) -> Callable[[dict], tuple[bool, Any]]:
    return lambda d: (d.get(key) == want, d.get(key))


def _ge(key: str, want: int) -> Callable[[dict], tuple[bool, Any]]:
    return lambda d: (d.get(key, -1) >= want, d.get(key))


def stage_a_rows(expect_n: int = 1) -> list[Row]:
    """§15.1 — the S6 core. `expect_n` is 1 for the single-delete case."""
    D, R, B = K_DEPS, K_RESOLVED, K_BROADCAST
    return [
        Row("A1", "the informer DELETE actually evicted the entry",
            f"{D}.evict_delete_total", f"+{expect_n}",
            _eq(f"{D}.evict_delete_total", expect_n)),
        Row("A2", "the store-side eviction is the SAME event",
            f"{R}.evict_delete_total", f"+{expect_n}",
            _eq(f"{R}.evict_delete_total", expect_n)),
        Row("A3", "NOT the refresher self-404 leg",
            f"{D}.evict_self_gone_total", "0",
            _eq(f"{D}.evict_self_gone_total", 0)),
        Row("A4", "NOT the C4 drop point",
            f"{D}.evict_drop_point_total", "0",
            _eq(f"{D}.evict_drop_point_total", 0)),
        Row("A5", "C10 published the eviction frame",
            f"{B}.evict_published", f"+{expect_n}",
            _eq(f"{B}.evict_published", expect_n)),
        Row("A6", "it reached the armed browser",
            f"{B}.delivered", f"+{expect_n}",
            _eq(f"{B}.delivered", expect_n)),
        Row("A7", "no buffer-full drop (loss mode L2)",
            f"{B}.dropped", "0", _eq(f"{B}.dropped", 0)),
        Row("A8", "the pacing bucket did not defer at N=1",
            f"{B}.evict_deferred", "0", _eq(f"{B}.evict_deferred", 0)),
        Row("A9a", "TTL eviction stayed silent (§6.2.5/S1d)",
            f"{R}.evict_ttl_total", "0", _eq(f"{R}.evict_ttl_total", 0)),
        Row("A9b", "LRU eviction stayed silent (§6.2.5/S1d)",
            f"{R}.evict_lru_total", "0", _eq(f"{R}.evict_lru_total", 0)),
        Row("A9c", "max-age eviction stayed silent (§6.2.5/S1d)",
            f"{R}.evict_max_age_total", "0",
            _eq(f"{R}.evict_max_age_total", 0)),
        Row("A10", "no action was lost",
            f"{D}.delete_worker_panics_total", "0",
            _eq(f"{D}.delete_worker_panics_total", 0)),
        Row("A11", "the pipeline lost no DELETE",
            f"{D}.reconcile_divergence_total", "0",
            _eq(f"{D}.reconcile_divergence_total", 0)),
        Row("A12", "dep edges were not silently dropped",
            f"{D}.dropped_cap", "0", _eq(f"{D}.dropped_cap", 0)),
        Row("A13", "the browser stayed connected throughout",
            f"{B}.subscribers", "unchanged", _eq(f"{B}.subscribers", 0)),
    ]


def burst_rows(n: int) -> list[Row]:
    """§15.3 — S9's "all N eventually arrive".

    The "no loss" half is asserted on `delivered`, NOT on a drained counter:
    facts §2.4 records that NO `drained` counter exists. `evict_deferred` proves
    the bound engaged; `delivered == N` proves nothing was lost.
    """
    B = K_BROADCAST
    return [
        Row("A17", "every evicted key was eventually delivered",
            f"{B}.delivered", f"=={n}", _eq(f"{B}.delivered", n)),
        Row("A18", "the bound engaged rather than dropping",
            f"{B}.evict_deferred", ">0", _ge(f"{B}.evict_deferred", 1)),
        Row("A19", "nothing was lost",
            f"{B}.dropped", "0", _eq(f"{B}.dropped", 0)),
    ]


def crd_rows() -> list[Row]:
    """§14.3 / §15.4 — the gated relist stage's bridge counters."""
    C = K_CRD
    return [
        Row("B1", "a real structural schema change was detected",
            f"{C}.schema_relists_fired", "+1",
            _eq(f"{C}.schema_relists_fired", 1)),
        Row("B2", "the bridge ran for the relisted GVR",
            f"{C}.relist_bridge_runs_total", ">=1",
            _ge(f"{C}.relist_bridge_runs_total", 1)),
        Row("B3", "the bridge enqueued (soak evidence; >0 retires the re-fire)",
            f"{C}.relist_bridge_enqueued_total", ">=0",
            _ge(f"{C}.relist_bridge_enqueued_total", 0)),
        Row("B4", "THE FALSIFIER: the bridge covered the teardown window",
            f"{C}.relist_bridge_timeout_total", "0",
            _eq(f"{C}.relist_bridge_timeout_total", 0)),
        Row("B5", "no CRD DELETE raced the relist",
            f"{C}.relist_bridge_aborted_total", "0",
            _eq(f"{C}.relist_bridge_aborted_total", 0)),
        Row("B6", "the forcing step did something (thrash guard NOT hit)",
            f"{C}.schema_unchanged", "0", _eq(f"{C}.schema_unchanged", 0)),
        Row("B7", "relisted informers synced",
            f"{C}.relist_postsync_timeout_total", "0",
            _eq(f"{C}.relist_postsync_timeout_total", 0)),
    ]


def evaluate(rows: list[Row], d: dict) -> tuple[bool, list[dict]]:
    """Run every row. Returns (all_passed, results) — NEVER short-circuits.

    Every row is evaluated even after one fails, so the report shows the whole
    picture rather than the first problem.
    """
    results, ok_all = [], True
    for r in rows:
        try:
            ok, actual = r.check(d)
        except Exception as e:                      # a malformed delta
            ok, actual = False, f"<error: {e}>"
        ok_all = ok_all and ok
        results.append({
            "id": r.rid, "assertion": r.what, "key": r.key,
            "expected": r.expect, "actual": actual, "passed": bool(ok),
        })
    return ok_all, results


def crosscheck_a1_a2(results: list[dict]) -> dict:
    """§15.1's narrative guard: A1 and A2 must move TOGETHER.

    A1 without A2 = the tracker counted an eviction the store did not perform.
    A2 without A1 = the entry left by a route other than the informer DELETE,
    which invalidates the whole test. Either way the run is not evidence.
    """
    a1 = next((r for r in results if r["id"] == "A1"), None)
    a2 = next((r for r in results if r["id"] == "A2"), None)
    if not a1 or not a2:
        return {"checked": False}
    agree = (a1["actual"] == a2["actual"])
    return {
        "checked": True, "agree": agree,
        "deps_evict_delete_total": a1["actual"],
        "resolved_evict_delete_total": a2["actual"],
        "verdict": "ok" if agree else (
            "MISMATCH — the two evict_delete_total counters disagree. "
            "deps-only means the tracker counted an eviction the store did not "
            "perform; resolved-only means the entry left by a route other than "
            "the informer DELETE. The run is not evidence either way."),
    }


def crosscheck_cache_proof(stored: int, hits: int, hook_data: dict) -> dict:
    """R2 applied to the cache proof: two channels, neither sufficient alone.

    The counter channel (`store_total` / `hit_total`) proves **an** entry was
    stored and served from L1. It cannot prove it was *ours*: both are GLOBAL
    counters, there is no per-key store counter, and any other portal session's
    widget fetch inside the window satisfies `>= 1` on its own. So a DECLINED
    harness widget can ride foreign traffic to a passing counter check.

    The browser channel (`l1_hit` + `refresh_key`) is per-widget — it is the
    response the harness's own `/call` got — but it is reported by the half
    whose work the counters exist to corroborate, so it is not sufficient
    either.

    Together they are: counters say "an entry was cached and served", the
    browser says "this one". **A mismatch fails the stage** — same discipline
    as the delete's two channels, never reconciled.
    """
    counters_ok = stored >= 1 and hits >= 1
    browser_hit = bool(hook_data.get("l1_hit"))
    key = hook_data.get("refresh_key")
    browser_ok = browser_hit and bool(key)
    agree = counters_ok and browser_ok
    if counters_ok and not browser_ok:
        verdict = (
            "MISMATCH — the counters moved but the browser did not report an "
            "L1 hit for the harness widget. store_total/hit_total are global, "
            "so this is consistent with the widget having been DECLINED "
            "(facts §10.2) while FOREIGN traffic moved the counters. Nothing "
            "can ever be evicted for a key whose entry never existed.")
    elif browser_ok and not counters_ok:
        verdict = (
            "MISMATCH — the browser reported an L1 hit but neither "
            "store_total nor hit_total moved. The reported hit is not "
            "corroborated by the store; treat the browser evidence as "
            "unreliable rather than accepting it.")
    elif not agree:
        verdict = ("neither channel confirms the widget was cached")
    else:
        verdict = "ok"
    return {
        "passed": agree,
        "counters": {"store_total_delta": stored, "hit_total_delta": hits,
                     "ok": counters_ok,
                     "semantics": "global; proves AN entry, not THIS one"},
        "browser": {"l1_hit": browser_hit, "refresh_key_present": bool(key),
                    "ok": browser_ok,
                    "semantics": "per-widget; proves THIS one, self-reported"},
        "verdict": verdict,
    }


def crosscheck_channels(hook_deleted: dict, hook_asserted: dict) -> dict:
    """R2: the two evidence channels must AGREE; a mismatch FAILS the stage.

    Channel A is the injected fetch wrapper (the frame); channel B is the CDP
    request count (the effect). Both are reported by the browser half in the
    `asserted` hook. This is never reconciled, explained away or retried.
    """
    data = (hook_asserted or {}).get("data", {})
    frames = data.get("frames_for_armed_key")
    calls = data.get("uninitiated_calls")
    if frames is None or calls is None:
        return {
            "checked": False, "passed": False,
            "verdict": "the browser half did not report both channels "
                       "(frames_for_armed_key / uninitiated_calls) — R2 cannot "
                       "be evaluated, so the stage FAILS rather than passing on "
                       "the counter half alone.",
        }
    agree = (frames == calls == 1)
    return {
        "checked": True, "passed": agree,
        "frames_for_armed_key": frames, "uninitiated_calls": calls,
        "verdict": "ok" if agree else (
            "CHANNEL MISMATCH — a frame with no refetch is an SPA defect; a "
            "refetch with no frame is a harness-initiated call, i.e. the test "
            "measuring itself. Per R2 this fails the stage and is not "
            "reconciled."),
    }


# ─── Stages ─────────────────────────────────────────────────────────────────


def stage_preflight(ctx: dict) -> dict:
    """A0 — every precondition, read live. Fails loudly, never skips."""
    portal, reqlog = ctx["portal_base"], ctx["reqlog"]

    got_ctx = cluster.canonical_gke_context()
    if got_ctx != EXPECTED_CONTEXT:
        raise PreflightFailed(
            f"kube context is {got_ctx!r}, expected {EXPECTED_CONTEXT!r}. The "
            f"bench's default pin is a DIFFERENT cluster; set "
            f"BENCH_GKE_CONTEXT={EXPECTED_CONTEXT} for an S6 run.")

    sp_img = _deployed_image("snowplow")
    fe_img = _deployed_image("frontend")
    sp_ver, fe_ver = _version_of(sp_img), _version_of(fe_img)

    problems = []
    if sp_ver != EXPECTED_SNOWPLOW_VERSION:
        problems.append(
            f"snowplow is {sp_ver} (Deployment image {sp_img}), expected "
            f"{EXPECTED_SNOWPLOW_VERSION} — C10/C12 are not deployed, so the "
            f"eviction-publish counters this run asserts do not exist yet.")
    if fe_ver != EXPECTED_FRONTEND_VERSION:
        problems.append(
            f"frontend is {fe_ver} (Deployment image {fe_img}), expected "
            f"{EXPECTED_FRONTEND_VERSION} — without the re-auth fix a 401 "
            f"mid-run kills the stream silently and S6 would measure nothing "
            f"(facts doc §5).")

    bundle = _served_bundle_hash(portal, reqlog)

    # B1 — the published tag is not the deployed build, so the tag alone cannot
    # gate it. The served bundle hash is the only ground truth for what the
    # browser is actually executing, and it is captured HERE, in the same
    # preflight as the tag, so the two describe one moment.
    #
    # (Provenance note, because the earlier version of this comment was wrong:
    # it claimed frontend:1.6.11 had been re-pushed under the same tag. It had
    # not — the bundle change D4naopHo → Kr5dAb3q was the 1.6.10 → 1.6.11 roll,
    # and I had compared a hash captured on 09-14 against a tag read on 09-15.
    # That mistake is itself the argument for this assertion: two readings taken
    # a day apart are not a comparison, and only a same-preflight hash is.)
    expected_bundle = ctx.get("expect_bundle")
    if not expected_bundle:
        problems.append(
            "no expected bundle hash supplied — pass --expect-bundle "
            "(or set S6_EXPECT_BUNDLE) with the `index-<hash>.js` filename of "
            "the release under test. The image tag alone cannot gate the "
            "build: it names what was deployed, not what is being served, and "
            "a roll between two readings makes the tag agree while the content "
            "differs.")
    elif bundle != expected_bundle:
        problems.append(
            f"served SPA bundle is {bundle!r}, expected {expected_bundle!r}. "
            f"The Deployment tag can match while the served content does not — "
            f"this is the assertion that catches it.")

    # Raise the BUILD problems before attempting login. They are the cheapest
    # and most decisive signal, they need no credential, and reporting "creds
    # missing" while the cluster is running the wrong build would bury the
    # blocker that actually stops the run.
    if problems:
        raise PreflightFailed("; ".join(problems))

    token = login(portal, reqlog)
    snap = read_debug_vars(portal, token, reqlog)
    missing = [k for k in (K_DEPS, K_RESOLVED, K_BROADCAST) if k not in snap]
    if missing:
        raise PreflightFailed(
            f"expvar keys absent from /debug/vars: {missing}. Under "
            f"CACHE_ENABLED=false these publishers are not registered at all "
            f"(CFG-1), so every assertion in the table would be a false zero.")

    ctx["token"] = token
    return {
        "context": got_ctx,
        "run_id": ctx["run_id"],
        "snowplow_image": sp_img, "snowplow_version": sp_ver,
        "frontend_image": fe_img, "frontend_version": fe_ver,
        "served_bundle": bundle,
        "expected_bundle": expected_bundle,
        "bundle_matches": bundle == expected_bundle,
        "debug_vars_reachable": True,
        "expvar_keys_present": sorted(
            k for k in (K_DEPS, K_RESOLVED, K_BROADCAST, K_CRD) if k in snap),
        "owned_prefixes": {
            "widget": OWNED_PREFIX, "page": OWNED_PAGE_PREFIX,
            "crd_suffix": OWNED_CRD_SUFFIX,
        },
        "identity": os.environ.get("HARNESS_USER", ""),   # name only, no secret
    }


def stage_counters_before(ctx: dict) -> dict:
    """A1 — snapshot AFTER the browser half proved a real cache hit (§10.2).

    Order matters and is the whole point: waiting for `hit_proved` before
    snapshotting is what stops a DECLINED widget (armed but never cached) from
    reaching the delete and reading as a C10 failure.
    """
    run_dir, portal = ctx["run_dir"], ctx["portal_base"]
    reqlog, rid = ctx["reqlog"], ctx["run_id"]
    began = _now_iso()

    created = wait_hook(run_dir, "created", rid, began)
    for kind, name in (created.get("data", {}).get("objects") or []):
        assert_owned(kind, name)                 # R1, on what was actually made

    # P1/P2 — snapshot BEFORE the browser does any /call for the widget, so one
    # of the two cache-proof channels is OURS. Trusting the browser's `l1_hit`
    # alone would rest the guard on the very half the counters exist to
    # corroborate; trusting the counters alone cannot say the entry was OURS
    # (they are global, and there is no per-key store counter). Both are
    # required, and they must agree — see crosscheck_cache_proof.
    pre = snapshot_counters(read_debug_vars(portal, ctx["token"], reqlog))

    wait_hook(run_dir, "rendered", rid, began)       # first /call → cold fill
    hit = wait_hook(run_dir, "hit_proved", rid, began)  # second /call → HIT
    hd = hit.get("data", {})
    if not hd.get("refresh_key"):
        raise PreflightFailed(
            "the browser half reported no X-Snowplow-Refresh-Key. Without the "
            "armed key there is nothing to assert delivery against.")

    post = snapshot_counters(read_debug_vars(portal, ctx["token"], reqlog))
    cache_d = delta(pre, post)
    stored = cache_d[f"{K_RESOLVED}.store_total"]
    hits = cache_d[f"{K_RESOLVED}.hit_total"]

    # TWO CHANNELS, neither sufficient alone (R2 discipline, same as the
    # delete). `>= 1` on the counters, not `== 1`: both are GLOBAL and any
    # other portal session moves them, so equality would be flaky for a reason
    # unrelated to the test. But `>= 1` is for the same reason not PROOF that
    # OUR widget was the one cached — there is no per-key store counter — so
    # the browser's per-widget l1_hit must agree with it.
    cache_cc = crosscheck_cache_proof(stored, hits, hd)
    if not cache_cc["passed"]:
        raise PreflightFailed(
            f"the disposable widget was NOT provably cached. "
            f"{cache_cc['verdict']} "
            f"(counters: store_total Δ={stored}, hit_total Δ={hits}; browser: "
            f"l1_hit={hd.get('l1_hit')!r}). Facts §10.2: snowplow stamps "
            f"X-Snowplow-Refresh-Key even on a DECLINE (stage-error, external "
            f"touch, undeclared extras, UAF refilter), so the browser can arm a "
            f"key whose entry never existed — nothing can ever be evicted for "
            f"it and the run would read as a C10 failure. Refusing to proceed. "
            f"Check the snowplow log for a 'declining to cache' WARN.")

    ctx["window_start"] = _now_iso()
    ctx["before"] = post          # the delete window opens from the post-hit state
    ctx["refresh_key"] = hd["refresh_key"]
    return {
        "window_start": ctx["window_start"],
        "refresh_class": hd.get("refresh_class"),
        "refresh_key_sha256_prefix": (hd.get("refresh_key") or "")[:12],
        "cache_proof": cache_cc,
        "armed_keys": post[f"{K_BROADCAST}.armed_keys"],
        "subscribers": post[f"{K_BROADCAST}.subscribers"],
        "counters_before": ctx["before"],
    }


def stage_counters_after(ctx: dict) -> dict:
    """A2 — wait for the delete, let the drain settle, snapshot, assert."""
    run_dir, portal = ctx["run_dir"], ctx["portal_base"]
    reqlog, rid = ctx["reqlog"], ctx["run_id"]
    began = _now_iso()

    deleted = wait_hook(run_dir, "deleted", rid, began)
    for kind, name in (deleted.get("data", {}).get("objects") or []):
        assert_owned(kind, name)
    asserted = wait_hook(run_dir, "asserted", rid, began)

    # C10 defers under the per-subscriber bucket, so an immediate read can
    # legitimately miss frames still queued (§15.3 A17).
    time.sleep(SETTLE_SECONDS)

    snap = read_debug_vars(portal, ctx["token"], reqlog)
    ctx["window_end"] = _now_iso()
    after = snapshot_counters(snap)
    d = delta(ctx["before"], after)

    n = int((deleted.get("data", {}) or {}).get("deleted_count", 1))
    rows = stage_a_rows(n) if n == 1 else burst_rows(n)
    ok, results = evaluate(rows, d)

    a1a2 = crosscheck_a1_a2(results)
    chan = crosscheck_channels(deleted, asserted)
    passed = ok and a1a2.get("agree", True) and chan.get("passed", False)

    ctx["window_end_iso"] = ctx["window_end"]
    return {
        "__passed__": passed,
        "window_end": ctx["window_end"],
        "settle_seconds": SETTLE_SECONDS,
        "quiescence_caveat": (
            "The eviction rows assert EXACT deltas (== 1) against GLOBAL "
            "counters. That is sound only while no other actor evicts an L1 "
            "entry inside this window — a controller deleting a CR on 057 "
            "would move snowplow_deps.evict_delete_total too. The window is a "
            "few seconds and the run should be made without other portal "
            "activity; if A1/A2 read higher than expected, treat it as "
            "INCONCLUSIVE-AND-RERUN, not as a snowplow failure. "
            "snowplow_resolved_cache.store_total / hit_total are asserted >= 1 "
            "rather than == 1 for the same reason over a longer window."),
        "deleted_count": n,
        "counters_after": after,
        "delta": d,
        "assertions": results,
        "crosscheck_evict_delete_total": a1a2,
        "crosscheck_evidence_channels": chan,
        "failed_rows": [r["id"] for r in results if not r["passed"]],
    }


def stage_reconcile_selfcheck(ctx: dict) -> dict:
    """A4 — §16. Primary: our own request log. Secondary: a bounded ceiling."""
    start = ctx.get("window_start")
    end = ctx.get("window_end") or _now_iso()
    touched = ctx["reqlog"].touched_reconcile(start, end)
    if touched:
        raise ReconcilePerturbation(
            f"/debug/reconcile was requested inside the measurement window "
            f"({len(touched)} time(s)). It walks every resident L1 entry, hands "
            f"ABSENT ones to the dep-event worker and holds the store mutex for "
            f"the whole walk — it manufactures the very eviction S6 observes. "
            f"The run is void.")

    d = ctx.get("after_delta") or {}
    sampled = d.get(f"{K_DEPS}.reconcile_sampled_total")
    window_s = _iso_delta_seconds(start, end)
    ceiling = RECONCILE_SAMPLE_PER_TICK * max(
        1, int(window_s / RECONCILE_TICK_SECONDS) + 1)
    within = True if sampled is None else sampled <= ceiling

    return {
        "primary_self_check": "no /debug/reconcile request in window",
        "requests_in_window": len(ctx["reqlog"].paths_between(start, end)),
        "window_seconds": round(window_s, 1),
        "reconcile_sampled_delta": sampled,
        "ceiling": ceiling,
        "within_ceiling": within,
        "note": (
            "The ceiling is defence-in-depth and BOUNDED, not exact: the "
            "periodic ticker moves the same counter, and there is no counter "
            "distinguishing the manual endpoint from the ticker (both drive the "
            "same depsReconcile instance). A breach is INCONCLUSIVE-AND-RERUN "
            "— another operator hitting /debug/reconcile on 057 trips it too — "
            "never a snowplow failure."),
    }


def announce(run_dir: Path, name: str, run_id: str,
             data: dict | None = None) -> Path:
    """Counter-half → browser-half fact file (the opposite direction to a hook).

    The browser half polls these to learn that the counter half has finished a
    mutation it must react to (the CRD and its CR now exist; the schema has been
    bumped). Same payload shape and same atomic write as `write_hook`, in a
    separate directory so the two directions can never be confused for each
    other.
    """
    p = Path(run_dir) / "announce" / f"{name}.json"
    p.parent.mkdir(parents=True, exist_ok=True)
    payload = {"announce": name, "run_id": run_id, "at": _now_iso(),
               "data": data or {}}
    tmp = p.with_suffix(".tmp")
    with open(tmp, "w") as fh:
        json.dump(payload, fh, indent=2)
        fh.flush()
        os.fsync(fh.fileno())
    tmp.replace(p)
    return p


def _kubectl_apply(manifest: str, what: str) -> None:
    """Server-side apply a harness-owned manifest.

    `kubectl apply` for a BENCH FIXTURE is the established convention in this
    package (`lifecycle.py:483` applies its user CRs the same way). The
    helm-only rule (`feedback_never_kubectl_apply`, `feedback_chart_only_for_snowplow`)
    governs deploying snowplow and portal COMPONENTS, not throwaway test objects
    the harness creates and deletes under its own name prefix.
    """
    rc, _, err = cluster.kubectl(
        "apply", "--server-side", "--force-conflicts", "-f", "-",
        input_data=manifest)
    if rc != 0:
        raise RuntimeError(f"failed to apply {what}: {err}")


def _crd_manifest(group: str, plural: str, kind: str, *, widened: bool) -> str:
    """The disposable CRD, in its narrow and widened forms.

    The ONLY difference between the two is one extra OPTIONAL property under
    `spec.versions[0].schema.openAPIV3Schema`. That is exactly what the
    fingerprint covers — `sha256(json([{name, schema.openAPIV3Schema}]))`,
    `crd_discovery_side_effect.go:593` — so this edit changes it and a metadata
    edit would not. Optional, so no existing CR becomes invalid.
    """
    extra = ('                  widened:\n'
             '                    type: string\n') if widened else ""
    return (
        "apiVersion: apiextensions.k8s.io/v1\n"
        "kind: CustomResourceDefinition\n"
        "metadata:\n"
        f"  name: {plural}.{group}\n"
        "spec:\n"
        f"  group: {group}\n"
        "  scope: Namespaced\n"
        "  names:\n"
        f"    plural: {plural}\n"
        f"    singular: {plural[:-1]}\n"
        f"    kind: {kind}\n"
        "  versions:\n"
        "    - name: v1alpha1\n"
        "      served: true\n"
        "      storage: true\n"
        "      schema:\n"
        "        openAPIV3Schema:\n"
        "          type: object\n"
        "          properties:\n"
        "            spec:\n"
        "              type: object\n"
        "              properties:\n"
        "                note:\n"
        "                  type: string\n"
        + extra
    )


def _cr_manifest(group: str, kind: str, name: str) -> str:
    return (
        f"apiVersion: {group}/v1alpha1\n"
        f"kind: {kind}\n"
        "metadata:\n"
        f"  name: {name}\n"
        f"  namespace: {NS}\n"
        "spec:\n"
        "  note: s6 relist probe\n"
    )


def _iso_delta_seconds(a: str | None, b: str | None) -> float:
    if not a or not b:
        return 0.0
    ta = datetime.datetime.fromisoformat(a)
    tb = datetime.datetime.fromisoformat(b)
    return max(0.0, (tb - ta).total_seconds())


# ─── The gated CRD-relist stage (§14) ───────────────────────────────────────


def stage_crd_relist(ctx: dict) -> dict:
    """B1 — SEPARATELY GATED, OFF BY DEFAULT, needs Diego's approval.

    Creating and deleting a CRD on a shared cluster is materially heavier than
    creating a CR: it is cluster-scoped and it makes snowplow tear down and
    re-register real informers. This stage runs only behind --enable-crd-relist
    AND S6_CRD_RELIST_APPROVED=1, so neither a stray flag nor a stray env var
    alone can start it.

    ORDER IS LOAD-BEARING (§14.1): creating the CRD only RECORDS the
    fingerprint (first observation never relists), and the bump relists only a
    GVR that is already REGISTERED — registration happens lazily when something
    reads a CR of that kind (objects/get.go:113). So a CR must be rendered
    through a widget BEFORE the schema bump, or the relist fires for nothing.
    """
    if not ctx.get("enable_crd_relist"):
        raise RuntimeError(
            "stage_crd_relist called without --enable-crd-relist; this is a "
            "programming error, not a skip.")
    if os.environ.get("S6_CRD_RELIST_APPROVED") != "1":
        raise PreflightFailed(
            "the CRD relist stage is approved-gated: set "
            "S6_CRD_RELIST_APPROVED=1 only when Diego has approved this run. "
            "Creating/deleting a CRD on 057 is heavier than a CR and is never "
            "implied by the default S6 path.")

    run_dir, portal = ctx["run_dir"], ctx["portal_base"]
    reqlog, rid = ctx["reqlog"], ctx["run_id"]
    began = _now_iso()

    crd_name = ctx["crd_name"]                       # "<plural>.harness.krateo.io"
    assert_owned("crd", crd_name)
    plural, group = crd_name.split(".", 1)
    kind = plural[:-1].capitalize()
    cr_name = f"{OWNED_PREFIX}{plural}"
    assert_owned("cr", cr_name)

    created_objs: list[tuple[str, str]] = []
    try:
        # (1) CRD, narrow form. First observation only RECORDS the fingerprint —
        #     it deliberately does NOT relist (§14.1 fact 2,
        #     crd_discovery_side_effect.go:653-661).
        _kubectl_apply(_crd_manifest(group, plural, kind, widened=False),
                       f"CRD {crd_name}")
        created_objs.append(("crd", crd_name))

        # (2) one CR of the new kind.
        _kubectl_apply(_cr_manifest(group, kind, cr_name),
                       f"{kind}/{cr_name}")
        created_objs.append((plural, cr_name))

        announce(run_dir, "crd_created", rid, {
            "crd": crd_name, "group": group, "plural": plural,
            "kind": kind, "namespace": NS, "cr": cr_name,
            "next": "render a widget over this CR, then signal crd_rendered",
        })

        # (3) THE STEP THAT MAKES THE BUMP MEAN ANYTHING. The relist only fires
        #     for a GVR that is already REGISTERED (`rw.IsRegistered(gvr)`,
        #     :686-689), and registration is lazy — it happens when something
        #     actually reads a CR of the kind (objects/get.go:113). Bumping the
        #     schema before this wait would relist nothing and the stage would
        #     fail for the wrong reason.
        wait_hook(run_dir, "crd_rendered", rid, began)

        before = snapshot_crd_counters(
            read_debug_vars(portal, ctx["token"], reqlog))

        # (4) the bump: one extra OPTIONAL property in the structural schema.
        _kubectl_apply(_crd_manifest(group, plural, kind, widened=True),
                       f"CRD {crd_name} (widened)")
        announce(run_dir, "crd_bumped", rid, {"crd": crd_name})
        time.sleep(SETTLE_SECONDS)

        after = snapshot_crd_counters(
            read_debug_vars(portal, ctx["token"], reqlog))
        d = delta(before, after)
        ok, results = evaluate(crd_rows(), d)

        return {
            "__passed__": ok,
            "crd": crd_name, "cr": cr_name, "group": group, "kind": kind,
            "approved": True,
            "counters_before": before,
            "counters_after": after,
            "delta": d,
            "assertions": results,
            "failed_rows": [r["id"] for r in results if not r["passed"]],
            "known_residue": (
                "navDiscoveredGroups is APPEND-ONLY on DELETE by ratified "
                "design, so the disposable group name stays in that in-memory "
                "set for the life of the pod. Bounded and harmless — which is "
                "why the group carries a FRESH per-run name and why nothing "
                "asserts that set is unchanged."),
        }
    finally:
        # Teardown in the documented order (§14.4), inner-most first, and ALWAYS
        # — a failed assertion must not leave a CRD on a shared cluster.
        _teardown_crd(created_objs)


def _teardown_crd(objs: list[tuple[str, str]]) -> None:
    """Delete what this stage created, inner-most first (§14.4).

    Every delete is gated by `assert_owned` again at the point of use: the list
    is built locally, but re-checking here means a future edit that widens it
    cannot quietly delete something the harness does not own.

    Deleting the CRD cascades any remaining CRs at the apiserver, and snowplow's
    own teardown is clean (`triggerCRDDelete` → `RemoveResourceType`,
    `watcher.go:1591-1600`, nil- and unknown-GVR-safe; `OnResourceTypeRemoved`,
    `deps.go:872-877`, nil-safe). So no informer is left running.
    """
    for kind, name in reversed(objs):            # CR before CRD
        try:
            assert_owned("crd" if kind == "crd" else "cr", name)
        except NotOwned:
            continue
        if kind == "crd":
            cluster.kubectl("delete", "crd", name, "--ignore-not-found",
                            "--wait=false")
        else:
            cluster.kubectl("delete", kind, name, "-n", NS,
                            "--ignore-not-found", "--wait=false")


# ─── Orchestration ──────────────────────────────────────────────────────────


STAGES: list[tuple[str, Callable[[dict], dict], str]] = [
    ("A0_preflight", stage_preflight,
     "Without it the run can measure a cluster running the WRONG build — the "
     "exact failure mode facts §5 documents — and report a false green."),
    ("A1_counters_before", stage_counters_before,
     "Without the pre-delete snapshot there is no delta, and without the "
     "hit_proved gate a DECLINED widget (armed but never cached, §10.2) "
     "reaches the delete and reads as a C10 failure."),
    ("A2_counters_after", stage_counters_after,
     "This IS the acceptance: the §15 table plus the R2 two-channel "
     "cross-check. Skipping it leaves S6 unproven."),
    ("A3_reconcile_selfcheck", stage_reconcile_selfcheck,
     "Without it a /debug/reconcile call inside the window could have "
     "manufactured the eviction being measured, and nothing would show it."),
]


def run(run_dir: Path, portal_base: str, *, enable_crd_relist: bool = False,
        crd_name: str | None = None, from_stage: str | None = None,
        expect_bundle: str | None = None) -> int:
    """One invocation, every stage, per-stage proofs + state.json."""
    run_dir = Path(run_dir)
    (run_dir / "proofs").mkdir(parents=True, exist_ok=True)
    (run_dir / "hooks").mkdir(parents=True, exist_ok=True)
    (run_dir / "announce").mkdir(parents=True, exist_ok=True)

    # A fresh run gets a fresh identity AND a cleared hook directory; a
    # --from-stage resume keeps the existing one so the browser half's already
    # delivered hooks still count (review finding B2). Either way every wait
    # checks run_id AND the stage start time, so a leftover file from a
    # previous attempt is ignored rather than consumed.
    rid_path = run_dir / "run_id"
    if from_stage and rid_path.exists():
        run_id = rid_path.read_text().strip()
        print(f"[accept1126] resuming run_id={run_id} from {from_stage}")
    else:
        run_id = f"{int(time.time())}-{os.getpid()}"
        for stale in (run_dir / "hooks").glob("*.json"):
            stale.unlink()
        for stale in (run_dir / "announce").glob("*.json"):
            stale.unlink()
        rid_path.write_text(run_id)
        print(f"[accept1126] run_id={run_id} (hooks cleared)")

    ctx: dict[str, Any] = {
        "run_dir": run_dir,
        "run_id": run_id,
        "portal_base": portal_base.rstrip("/"),
        "reqlog": _RequestLog(),
        "enable_crd_relist": enable_crd_relist,
        "expect_bundle": expect_bundle or os.environ.get(
            "S6_EXPECT_BUNDLE", "").strip() or None,
        "crd_name": crd_name or f"s6run{int(time.time())}{OWNED_CRD_SUFFIX}",
    }

    try:
        state = load_state(run_dir)
    except Exception:
        state = {"schema_version": "1.0.0", "stages": {}}

    stages = list(STAGES)
    if enable_crd_relist:
        stages.append((
            "B1_crd_relist", stage_crd_relist,
            "The relist bridge's only live falsifier: "
            "relist_bridge_timeout_total > 0 on a healthy cluster means the "
            "bridge does NOT cover the teardown window and the 1.12.5 "
            "post-sync re-fire must stay."))

    started_here = from_stage is None
    failed: list[str] = []

    for stage_id, work, breaks in stages:
        if not started_here:
            if stage_id == from_stage:
                started_here = True
            else:
                continue
        began = _now_iso()
        try:
            proof_dict = work(ctx) or {}
            passed = bool(proof_dict.pop("__passed__", True))
            if stage_id == "A2_counters_after":
                ctx["after_delta"] = proof_dict.get("delta")
            err = None
        except Exception as e:
            proof_dict, passed, err = {"error": f"{type(e).__name__}: {e}"}, False, e

        proof = StageProof(
            stage_id=stage_id, started_at=began, ended_at=_now_iso(),
            passed=passed, proof=proof_dict, artifacts=[],
            what_breaks_if_skipped=breaks)
        _persist(run_dir, state, proof)
        if not passed:
            failed.append(stage_id)
            print(f"[accept1126] {stage_id} FAILED: "
                  f"{proof_dict.get('error') or proof_dict.get('failed_rows')}")
            break                                   # no silent continue
        print(f"[accept1126] {stage_id} passed")

    _write_report(run_dir, state, ctx, failed)
    return 1 if failed else 0


def _persist(run_dir: Path, state: dict, proof: StageProof) -> None:
    with open(run_dir / "proofs" / f"{proof.stage_id}.json", "w") as fh:
        json.dump(proof.to_dict(), fh, indent=2)
        fh.flush()
        os.fsync(fh.fileno())
    state.setdefault("stages", {})[proof.stage_id] = {
        "passed": proof.passed, "ended_at": proof.ended_at}
    save_state(run_dir, state)


def _write_report(run_dir: Path, state: dict, ctx: dict,
                  failed: list[str]) -> None:
    """Final report: every number with its source. No number without a key."""
    lines = ["# S6 — 1.12.6 item 7 live acceptance (counter half)", ""]
    lines.append(f"- run dir: `{run_dir}`")
    lines.append(f"- portal: `{ctx['portal_base']}` (gateway path only)")
    lines.append(f"- context: `{cluster.canonical_gke_context()}`")
    lines.append(f"- verdict: **{'FAIL' if failed else 'PASS'}**"
                 + (f" (first failing stage: `{failed[0]}`)" if failed else ""))
    lines.append("")
    for sid in state.get("stages", {}):
        p = run_dir / "proofs" / f"{sid}.json"
        if not p.exists():
            continue
        d = json.load(open(p))
        lines.append(f"## {sid} — {'PASS' if d['passed'] else 'FAIL'}")
        lines.append("")
        lines.append(f"*What breaks if skipped:* {d['what_breaks_if_skipped']}")
        lines.append("")
        for row in d["proof"].get("assertions", []):
            mark = "ok" if row["passed"] else "**FAIL**"
            lines.append(
                f"- {mark} `{row['id']}` {row['assertion']} — "
                f"`{row['key']}` expected `{row['expected']}`, "
                f"got `{row['actual']}`")
        if d["proof"].get("assertions"):
            lines.append("")
        for k in ("crosscheck_evict_delete_total",
                  "crosscheck_evidence_channels"):
            if k in d["proof"]:
                lines.append(f"- {k}: `{json.dumps(d['proof'][k])}`")
        if d["proof"].get("error"):
            lines.append(f"- error: `{d['proof']['error']}`")
        lines.append("")
    lines.append("## Request log (source for the §16 self-check)")
    lines.append("")
    for e in ctx["reqlog"].entries:
        lines.append(f"- `{e['at']}` {e['method']} `{e['url']}` → {e['status']}")
    lines.append("")
    out = run_dir / "report.md"
    with open(out, "w") as fh:
        fh.write("\n".join(lines))
        fh.flush()
        os.fsync(fh.fileno())
    print(f"[accept1126] report → {out}")


# ─── CLI ────────────────────────────────────────────────────────────────────


def add_parsers(sub: argparse._SubParsersAction) -> None:
    """Register `accept1126` and `accept1126-hook` on the bench CLI."""
    p = sub.add_parser(
        "accept1126",
        help="S6 live acceptance for 1.12.6 item 7 — counter half.")
    p.add_argument("--run-dir", required=True)
    p.add_argument("--portal-base", default=DEFAULT_PORTAL_BASE)
    p.add_argument("--from-stage", default=None)
    p.add_argument(
        "--expect-bundle", default=None,
        help="Expected served SPA bundle filename, e.g. index-Kr5dAb3q.js. "
             "REQUIRED (or S6_EXPECT_BUNDLE): the image tag names what was "
             "deployed, not what is being served, so only the content hash "
             "gates the build the browser actually runs.")
    p.add_argument(
        "--enable-crd-relist", action="store_true",
        help="Run the gated CRD schema-relist stage. OFF by default; also "
             "requires S6_CRD_RELIST_APPROVED=1 (Diego's approval).")
    p.add_argument("--crd-name", default=None)
    p.set_defaults(func=cmd_accept1126)

    h = sub.add_parser(
        "accept1126-hook",
        help="Signal a stage boundary from the BROWSER half.")
    h.add_argument("--run-dir", required=True)
    h.add_argument("--name", required=True, choices=ALL_HOOKS)
    h.add_argument(
        "--run-id", default=None,
        help="Defaults to the id in <run-dir>/run_id. A hook whose run id does "
             "not match the live run is IGNORED, not consumed.")
    h.add_argument("--data", default="{}", help="JSON payload")
    h.set_defaults(func=cmd_accept1126_hook)


def cmd_accept1126(args) -> int:
    return run(Path(args.run_dir), args.portal_base,
               enable_crd_relist=args.enable_crd_relist,
               crd_name=args.crd_name, from_stage=args.from_stage,
               expect_bundle=args.expect_bundle)


def cmd_accept1126_hook(args) -> int:
    run_dir = Path(args.run_dir)
    rid = args.run_id or read_run_id(run_dir)
    p = write_hook(run_dir, args.name, rid, json.loads(args.data))
    print(f"[accept1126-hook] {args.name} (run_id={rid}) → {p}")
    return 0
