"""The bench reads snowplow's /debug as ONE identity of its own, and no measured user holds a /debug grant
(bench/rbac_model.py THE DEBUG READER).

Why: snowplow #1365 (Fixes #1362; in every image after 1.12.70) authorizes /debug/* through Kubernetes RBAC
as a NON-RESOURCE request: a ClusterRole rule with nonResourceURLs, bound by a ClusterRoleBinding. The bench
read /debug/vars and its profiles as ordinary scale users, whose grants are resource rules only: on 1.12.71
the Go runtime read would fail, and every arm with it, before load; mem606 would record nothing. As #1365
has the S6 harness read /debug with a separate operator token, the bench uses one dedicated identity, in a
group of its own, granted `get` on exactly the paths it reads: the measured users keep the customer's RBAC
shape. On 1.12.70 /debug needs only a valid JWT, which this identity also has.
"""

from __future__ import annotations

import json
import re
import subprocess
import sys
import types

import pytest
import yaml

from bench import data_manifests, fresh_classes, full_widen, k6_job, load606, rbac_model
from tests.test_run606_real_sequence import BENCH_DIR, harness  # noqa: F401  (fixture)

BENCH = BENCH_DIR
READER = rbac_model.DEBUG_READER


# ─── the identity and its grant ─────────────────────────────────────────────

def test_the_debug_readers_role_is_get_on_exactly_the_benchs_debug_paths_and_nothing_else():
    role, binding = list(yaml.safe_load_all(rbac_model.debug_reader_rbac_yaml()))
    assert role["kind"] == "ClusterRole" and binding["kind"] == "ClusterRoleBinding"
    # non-resource rules only: no API group, no resource, no other verb (snowplow's evaluator, like the
    # apiserver, matches a non-resource request against nonResourceURLs and verbs alone)
    assert role["rules"] == [{"nonResourceURLs": list(rbac_model.DEBUG_READ_PATHS), "verbs": ["get"]}]
    assert binding["roleRef"] == {"apiGroup": "rbac.authorization.k8s.io", "kind": "ClusterRole",
                                  "name": role["metadata"]["name"]}
    assert binding["subjects"] == [{"apiGroup": "rbac.authorization.k8s.io", "kind": "Group",
                                    "name": rbac_model.DEBUG_GROUP}]
    secret, user = data_manifests.debug_reader_docs("pw")
    assert user["metadata"]["name"] == READER and user["spec"]["groups"] == [rbac_model.DEBUG_GROUP]
    assert secret["metadata"]["name"] == f"{READER}-password"
    # its group is its own: not the portal's base group, no team, not the admins
    assert rbac_model.DEBUG_GROUP not in (rbac_model.BASE_GROUP, rbac_model.ADMIN_GROUP)
    assert not rbac_model.DEBUG_GROUP.startswith("bench-team-")
    # and the schema step validates its User with the run's other users
    assert ("the debug reader", user) in data_manifests.documents(namespaces=2, users=4)


def test_no_measured_identity_holds_a_debug_grant():
    m = rbac_model.bench_model(1000, 50)
    groups = {g for i in range(1, 1001) for g in rbac_model.user_groups(i, m=m)}
    groups |= {rbac_model.BASE_GROUP, rbac_model.team_group(1)}                    # the preflight identity
    p = fresh_classes.plan(sessions=5, routes=5, think_s=15, duration_s=3600, namespaces=50, user_prefix="scaleuser-")
    groups |= {g for s in p["sessions"] for f in s["fresh"] for g in f["groups"]}
    assert rbac_model.DEBUG_GROUP not in groups
    # nothing else the run binds carries a non-resource rule, and nothing binds the debug group
    docs = [d for t in rbac_model.rbac_yaml(50) for d in yaml.safe_load_all(t) if d]
    docs += [d for d in yaml.safe_load_all(fresh_classes.manifest(p, "pw")) if d]
    docs += [d for d in full_widen.identity_docs("scaleuser-fullwiden", "pw")]
    for d in docs:
        for r in d.get("rules") or []:
            assert "nonResourceURLs" not in r, d["metadata"]["name"]
        for s in d.get("subjects") or []:
            assert s.get("name") not in (rbac_model.DEBUG_GROUP, READER), d["metadata"]["name"]
    # the menu classes' identities take their groups from the model's (bench/menu_classes.py), so none is in it
    assert all(rbac_model.DEBUG_GROUP not in c for c in [rbac_model.menu_class(rbac_model.user_groups(i, m=m), m)
                                                         for i in range(1, 1001)])


# ─── exactly the paths the bench requests ───────────────────────────────────

#: the modules of the run606 path that request /debug: the driver, mem606, runtime606 (load606.py), the
#: full-widen watch, and the browser (k6/browser.js)
RUN606_DEBUG_READERS = ("bench/load606.py", "bench/full_widen.py", "k6/browser.js", "k6/navigate.js", "k6/spa.js")
#: a request literal: get("/debug/...") / get(f"/debug/...") in Python; `${...}/debug/...` in a k6 script
_REQ = re.compile(r"""(?:\bget\(\s*f?["']|\$\{[^}]+\})(/debug/[A-Za-z0-9_/]+)""")


# ─── every function that reaches a /debug request, found by the call graph, not by a literal ─

def _debug_readers(src: str) -> list[str]:
    """Every function (or method) of a module that reaches a /debug request: those whose own calls carry a
    "/debug/..." path argument, then, to a fixpoint, every function calling one of them, by name or method
    name. A helper that hides the literal (read_vars, mem_sample) is found through its callers."""
    import ast
    tree = ast.parse(src)
    fns = {}
    for node in tree.body:
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            fns[node.name] = node
        elif isinstance(node, ast.ClassDef):
            fns.update({f"{node.name}.{m.name}": m for m in node.body
                        if isinstance(m, (ast.FunctionDef, ast.AsyncFunctionDef))})

    def lit(n):
        if isinstance(n, ast.Constant) and isinstance(n.value, str):
            return n.value.startswith("/debug/")
        return (isinstance(n, ast.JoinedStr) and bool(n.values) and isinstance(n.values[0], ast.Constant)
                and str(n.values[0].value).startswith("/debug/"))
    hit = {q for q, f in fns.items() if any(isinstance(c, ast.Call) and any(lit(x) for x in c.args)
                                           for c in ast.walk(f))}
    while True:
        short = {q.rsplit(".", 1)[-1] for q in hit}
        more = {q for q, f in fns.items() if q not in hit and any(
            isinstance(c, ast.Call) and ((isinstance(c.func, ast.Name) and c.func.id in short)
                                         or (isinstance(c.func, ast.Attribute) and c.func.attr in short))
            for c in ast.walk(f))}
        if not more:
            return sorted(hit)
        hit |= more


#: every function of the run606 path that reaches /debug, and, for each ENTRY (where the token is chosen), the
#: test that proves it is the debug reader's. A new reader (a new call of read_vars, mem_sample, a helper that
#: builds a /debug URL) is not in this table, and turns the guard RED until it is given its own proof
DEBUG_READERS = {
    "bench/load606.py": {
        "_cmd_drive": "test_the_drive_samples_debug_vars_as_the_debug_reader",
        "_drive": "test_the_drive_samples_debug_vars_as_the_debug_reader",
        "read_vars": "(helper: reached through _drive)",
        "_cmd_fresh": "test_fresh_gates_its_start_as_the_debug_reader_and_probes_as_its_own_user",
        "wait_for_customer_load": "(helper: reached through _cmd_fresh; tests/test_measure_validity.py)",
        "_cmd_go_runtime": "test_the_go_runtime_job_reads_as_the_debug_reader (its only identity is --user)",
        "go_runtime_read": "(helper: reached through _cmd_go_runtime; its token is the caller's)",
        "BootProbe._get": "test_the_boot_probe_reads_debug_as_the_debug_reader_and_the_boot_root_as_the_preflight_user",
        "BootProbe._probe": "test_the_boot_probe_reads_debug_as_the_debug_reader_and_the_boot_root_as_the_preflight_user",
        "BootProbe._dump": "test_the_boot_probe_reads_debug_as_the_debug_reader_and_the_boot_root_as_the_preflight_user",
        "_cmd_memwatch": "test_mem606_and_the_drivers_sampler_read_as_the_debug_reader_and_the_drive_as_scale_users",
        "memwatch": "(helper: reached through _cmd_memwatch)",
        "mem_sample": "(helper: reached through memwatch)",
        "cpu_profiler": "(helper: reached through memwatch)",
    },
    "bench/full_widen.py": {
        "main": "test_the_full_widen_cli_wires_debug_to_the_debug_reader_and_the_probe_to_its_user",
        "_cmd_watch": "test_the_full_widen_cli_wires_debug_to_the_debug_reader_and_the_probe_to_its_user",
        "Watch.run": "(method: reached through _cmd_watch)",
        "Watch.tick": "test_the_full_widen_job_reads_debug_as_the_debug_reader_and_the_probe_as_its_own_identity",
        "Watch._vars": "test_the_full_widen_job_reads_debug_as_the_debug_reader_and_the_probe_as_its_own_identity",
    },
}


def test_every_function_that_reads_debug_is_one_whose_identity_is_proven():
    for rel, want in DEBUG_READERS.items():
        assert _debug_readers((BENCH / rel).read_text()) == sorted(want), rel
    # each named proof exists in this file
    here = (BENCH / "tests" / "test_debug_reader.py").read_text()
    for want in DEBUG_READERS.values():
        for proof in want.values():
            name = proof.split(" ")[0]
            assert name.startswith("(") or f"def {name}(" in here, proof
    # the k6 scripts: one function reads /debug, the browser's learned-class counters
    for f in (BENCH / "k6").glob("*.js"):
        # each top-level function's text runs to the next top-level `function` / `async function`
        code = re.sub(r"(?m)^\s*//.*$", "", f.read_text())          # code, not its comments
        chunks = re.split(r"(?m)^(?=(?:export )?(?:default )?(?:async )?function\b)", code)
        fns = [re.match(r"(?:export )?(?:default )?(?:async )?function\s*(\w*)", c).group(1) or "<default>"
               for c in chunks if re.match(r"(?:export )?(?:default )?(?:async )?function\b", c) and "/debug/" in c]
        assert fns == (["learnedCounts"] if f.name == "browser.js" else []), (f.name, fns)


def test_the_role_grants_exactly_the_debug_paths_the_run_requests():
    used = {}
    for rel in RUN606_DEBUG_READERS:
        for path in _REQ.findall((BENCH / rel).read_text()):
            used.setdefault(path, set()).add(rel)
    assert set(used) == set(rbac_model.DEBUG_READ_PATHS), used
    assert used["/debug/vars"] >= {"bench/load606.py", "bench/full_widen.py", "k6/browser.js"}


# ─── every /debug read is the debug reader's, and only /debug reads are ─────

def _job_docs(*argv):
    p = subprocess.run([sys.executable, "-m", "bench.load606_job", *argv], cwd=BENCH, capture_output=True,
                       text=True, timeout=120)
    assert p.returncode == 0, p.stderr
    return [d for d in yaml.safe_load_all(p.stdout) if d and d.get("kind") == "Job"]


def _script(job):
    return job["spec"]["template"]["spec"]["containers"][0]["args"][0]


COMMON = ("--snowplow-url", "http://snowplow:8081", "--authn-url", "http://authn:8082", "--password", "pw")


def test_the_boot_probe_reads_debug_as_the_debug_reader_and_the_boot_root_as_the_preflight_user(tmp_path):
    from tests.test_boot_window import boot_probe_identities
    boot_probe_identities(tmp_path)
    # the Job logs the two in by their own flags: the debug reader and the preflight user, nobody else
    [job] = _job_docs(*COMMON, "--boot-probe-only", "--init", "/call?x", "--boot-bound-s", "60")
    cmd = job["spec"]["template"]["spec"]["containers"][0]["command"]
    assert cmd[cmd.index("--debug-user") + 1] == READER and cmd[cmd.index("--preflight-user") + 1] == "scaleuser-0000"


def test_the_go_runtime_job_reads_as_the_debug_reader():
    [job] = _job_docs(*COMMON, "--go-runtime-only", "--go-runtime-deadline", "1794812999")
    assert f'--user "{READER}"' in _script(job) and "scaleuser-0000" not in _script(job)


def test_mem606_and_the_drivers_sampler_read_as_the_debug_reader_and_the_drive_as_scale_users():
    jobs = {j["metadata"]["name"]: j for j in _job_docs(
        *COMMON, "--users", "4", "--shards", "2", "--init", "/call?resource=layouts&name=app-shell",
        "--mem-duration-s", "60")}
    assert f'--user "{READER}"' in _script(jobs["mem606"])
    drive = _script(jobs["load606"])
    # only shard 0 samples /debug/vars, as the debug reader; every shard drives its own scale users
    assert f'VARS=0; DEBUG=""; [ "$JOB_COMPLETION_INDEX" = "0" ] && VARS=15 && DEBUG="{READER}"' in drive
    assert '--vars-interval "$VARS" --debug-user "$DEBUG"' in drive and '--user-start "$S"' in drive


def test_the_browser_reads_the_counters_as_the_debug_reader_never_as_its_session():
    cfg = k6_job.browser_config(sessions=[{"name": "s1", "user": "scaleuser-0001", "offset": 0, "cold": True}], routes=["/a"],
                                password="pw", think_s=1, duration_s=60, frontend_url="http://gw",
                                snowplow_url="http://gw/content", authn_url="http://authn:8082/",
                                debug_user=READER)
    assert cfg["debugUser"] == READER and cfg["authnUrl"] == "http://authn:8082"
    src = (BENCH / "k6" / "browser.js").read_text()
    body = src[src.index("function learnedCounts"):src.index("async function tokenOf")]
    # the /debug read carries the debug reader's token, and the session's token decides only WHETHER to read
    assert "Authorization: `Bearer ${dt}`" in body and "const dt = debugToken(renew)" in body
    assert "Bearer ${token}" not in body
    login = src[src.index("function debugToken"):src.index("function learnedCounts")]
    assert "${CFG.authnUrl}/basic/login" in login and "${CFG.debugUser}:${CFG.password}" in login
    assert "import encoding from 'k6/encoding'" in src


def test_the_full_widen_job_reads_debug_as_the_debug_reader_and_the_probe_as_its_own_identity():
    text = full_widen.job_yaml(snowplow_url="http://s", authn_url="http://a", user="scaleuser-fullwiden",
                               password="pw", bounds={}, linger_s=1, widen_wait_s=1)
    assert f'--user "scaleuser-fullwiden" --debug-user "{READER}"' in text
    seen = []

    def get(path, token):
        seen.append((path.split("?")[0], token))
        if path == "/debug/vars":
            return (200, b"{}") if token == "dbg" else (403, b"")
        return (200, b"{}") if token == "own" else (403, b"")
    w = full_widen.Watch(get=get, login=lambda: "own", debug_login=lambda: "dbg", emit=lambda r: None,
                         arm=lambda: None, widened=lambda: False, bounds={}, widen_wait_s=1)
    w.tick("sync")
    assert ("/debug/vars", "dbg") in seen and ("/call", "own") in seen
    assert all(t == "dbg" for p, t in seen if p.startswith("/debug/")) and \
        all(t == "own" for p, t in seen if not p.startswith("/debug/"))


def test_the_full_widen_cli_wires_debug_to_the_debug_reader_and_the_probe_to_its_user(monkeypatch, tmp_path):
    # #1471 review J08: the watch's own tests build its callables; THIS drives main(), as the Job runs it
    got = {}

    class Watch:
        def __init__(self, **kw):
            got.update(kw)

        def run(self):
            return 0
    monkeypatch.setattr(full_widen, "Watch", Watch)
    monkeypatch.setattr(full_widen, "STREAM", str(tmp_path / "widen.jsonl"))
    monkeypatch.setattr(load606, "login", lambda url, user, pw: f"tok:{user}")
    rc = full_widen.main(["watch", "--snowplow-url", "http://s", "--authn-url", "http://a",
                          "--user", "scaleuser-fullwiden", "--debug-user", READER, "--password", "pw",
                          "--bounds", "{}", "--widen-wait-s", "1"])
    assert rc == 0
    assert got["debug_login"]() == f"tok:{READER}" and got["login"]() == "tok:scaleuser-fullwiden"


def test_fresh_gates_its_start_as_the_debug_reader_and_probes_as_its_own_user(monkeypatch, tmp_path):
    # #1471 review: fresh606's start gate read /debug/vars as scaleuser-0001; on 1.12.71 that is a 403, and the
    # gate waited out 1800 s. It reads as the debug reader, and a refusal stops the probe at once
    seen = {}

    def gate(client, token, **kw):
        seen["gate"] = token
        return {"started": False, "waited_s": 0.0, "delta": None, "refused": 403,
                "why": "/debug/vars answered 403: the start gate cannot read snowplow's counters"}
    monkeypatch.setattr(load606, "login", lambda url, user, pw: f"tok:{user}")
    monkeypatch.setattr(load606, "wait_for_customer_load", gate)

    def past_the_gate(*a, **kw):
        raise AssertionError("the probe ran its phases past a refused start gate")
    monkeypatch.setattr(load606, "fresh_phases", past_the_gate)
    out = tmp_path / "fresh.jsonl"
    argv = ["fresh", "--mutator", "incluster", "--snowplow-url", "http://s", "--authn-url", "http://a",
            "--user", "scaleuser-0001", "--password", "pw", "--group", "g", "--version", "v", "--plural", "p",
            "--probe-name", "bench-app-01-01", "--rates", "10", "--samples", "1", "--gap-s", "0",
            "--churn-namespaces", "1", "--wait-for-load", "--no-viewer", "--out", str(out)]
    assert load606.main(argv + ["--debug-user", READER]) == 2
    assert seen["gate"] == f"tok:{READER}"
    rec = [json.loads(x) for x in out.read_text().splitlines() if x.strip()]
    assert rec[-1]["kind"] == "fresh_start_gate" and rec[-1]["refused"] == 403
    # no debug reader: the probe refuses by name, it never gates as its own user
    seen.clear()
    assert load606.main(argv) == 2 and "gate" not in seen


def test_the_fresh_job_names_the_debug_reader():
    [job] = [j for j in _job_docs(*COMMON, "--users", "4", "--shards", "1", "--init",
                                  "/call?resource=layouts&name=app-shell", "--fresh-rates", "10")
             if j["metadata"]["name"] == "fresh606"]
    s = _script(job)
    assert f'--user "scaleuser-0001" --debug-user "{READER}"' in s and "--wait-for-load" in s


def test_the_drive_samples_debug_vars_as_the_debug_reader(monkeypatch, tmp_path):
    seen = []

    class Client:
        def __init__(self, *a, **kw):
            pass

        def get(self, path, token):
            seen.append((path.split("?")[0], token))
            return load606.Response(200, b"{}", 1.0)

    class Driver:
        results: list = []

        def __init__(self, *a, **kw):
            pass

        def run(self):
            import time
            time.sleep(0.3)
    monkeypatch.setattr(load606, "HttpClient", Client)
    monkeypatch.setattr(load606, "Driver", Driver)
    monkeypatch.setattr(load606, "login", lambda url, user, pw: f"tok:{user}")
    a = types.SimpleNamespace(snowplow_url="http://s", authn_url="http://a", password="pw",
                              user_prefix="scaleuser-", user_start=1, user_end=2, init="/call?x",
                              frontend_ns="krateo-system", builders_ns="krateo-system", vars_interval=0.05,
                              debug_user=READER, burst_users=0, composite_route=None, no_viewers=True,
                              think_s=0, duration_s=0, nav_workers=1, call_workers=1, burst_at_s=0)
    rc, why = load606._drive(a, lambda r: None, [])
    debug = [t for p, t in seen if p.startswith("/debug/")]
    assert debug and set(debug) == {f"tok:{READER}"}, seen
    assert all(t != f"tok:{READER}" for p, t in seen if not p.startswith("/debug/"))
    # a sampler with no debug reader refuses the drive by name, never samples as a measured user
    a.debug_user = None
    rc, why = load606._drive(a, lambda r: None, [])
    assert rc == 2 and why == "the /debug/vars sampler (--vars-interval > 0) needs the debug reader (--debug-user)"


# ─── the real run: run606.sh against the fakes (the fake answers /debug as 1.12.71 does) ─

def _run606(env, **extra):
    return subprocess.run(["bash", str(BENCH / "run606.sh")], env=dict(env, **extra), capture_output=True,
                          text=True, timeout=600)


@pytest.mark.parametrize("authz", ["rbac", "jwt"])
def test_the_rehearsal_reads_the_go_runtime_as_the_debug_reader_on_both_snowplows(harness, authz):  # noqa: F811
    env, state, _, tmp = harness
    p = _run606(env, SNOWPLOW_ENV="GOGC=100", BENCH_606_REHEARSAL="env", FAKE_SNOWPLOW_DEBUG_AUTHZ=authz)
    log = (tmp / "out" / "run.log").read_text()
    assert p.returncode == 0 and "rehearsal complete" in log, log[-3000:]
    assert "RUN: python3 -m bench data606-debug-reader" in log
    assert log.index("data606-debug-reader") < log.index("snowplow env verified from the running process")
    rbac = [json.loads(x) for x in (state / "rbac.jsonl").read_text().splitlines()]
    # the one non-resource grant bound to a person is the debug reader's (observe606's is its ServiceAccount's)
    nonres = {d["metadata"]["name"] for d in rbac if d["kind"] == "ClusterRole"
              and any("nonResourceURLs" in r for r in d.get("rules") or [])}
    to_people = {d["roleRef"]["name"]: [s["name"] for s in d.get("subjects") or [] if s.get("kind") in ("User", "Group")]
                 for d in rbac if d["kind"] == "ClusterRoleBinding" and d["roleRef"]["name"] in nonres}
    assert {k: v for k, v in to_people.items() if v} == {"bench-debug-reader": [rbac_model.DEBUG_GROUP]}
    assert (state / "users").read_text().split() == [READER, "scaleuser-0000"]


def test_a_run_whose_debug_reader_is_missing_fails_before_load_by_the_403(harness):  # noqa: F811
    env, state, _, tmp = harness
    # the identity is made, its ClusterRole is not (the fake's apply drops it): 1.12.71 answers 403
    p = _run606(env, SNOWPLOW_ENV="GOGC=100", BENCH_606_REHEARSAL="env", FAKE_DROP_KIND="ClusterRole")
    log = (tmp / "out" / "run.log").read_text()
    assert p.returncode != 0 and "rehearsal complete" not in log
    assert "the settings the process runs with could not be read: /debug/vars answered 403" in log, log[-3000:]


def test_a_dry_pass_says_it_makes_the_debug_reader_at_the_preflight(harness):  # noqa: F811
    env, state, _, tmp = harness
    p = _run606(env, BENCH_606_DRY_RUN="1")
    log = (tmp / "out" / "run.log").read_text()
    assert p.returncode == 0, log[-3000:]
    pre = log[log.index("=== step preflight ==="):log.index("=== step data ===")]
    assert "DRY: python3 -m bench data606-debug-reader" in pre and "DRY: python3 -m bench data606-preflight-user" in pre



def test_no_test_writes_a_browser_config_by_hand():
    # #1471: a hand-written config lacked authnUrl / debugUser, so the browser never read the counters and the
    # Chrome test saw empty values. Every browser config a test runs comes from k6_job.browser_config
    for f in sorted((BENCH / "tests").glob("test_*.py")):
        src = f.read_text()
        assert not re.search(r'["\']frontendUrl["\']\s*:', src), f.name
