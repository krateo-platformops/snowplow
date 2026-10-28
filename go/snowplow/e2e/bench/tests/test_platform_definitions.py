"""The platform's create handshakes, recorded so a wedged component is provable
afterwards (a run lost an attempt to one, and the bundle kept no controller
log and no managed object's annotations).

At the end of the platform step AND at every teardown, every RestDefinition
and CompositionDefinition: the krateo.io/external-create-{pending,succeeded,
failed} markers, Ready/Synced, what it manages; WEDGED = a pending marker
newer than both terminal markers and older than provider-runtime's recovery
window (grace + 2 re-observes, its own defaults) by its own timestamp. The
oasgen-provider / core-provider logs IN FULL, redacted. The provider-runtime
each controller carries, read from its image. The verdict's platform section
counts WEDGED (expected 0) and cleared pendings per cluster.

Object shapes: 057's (2026-10-28, read-only): CompositionDefinition
status.managed{group,kind}, status.apiVersion; RestDefinition status.resource.
"""

from __future__ import annotations

import datetime
import gzip
import hashlib
import http.server
import io
import json
import subprocess
import sys
import tarfile
import threading

import pytest

from bench import diag606, imagemod
from tests.test_run606_real_sequence import BENCH_DIR, _calls, harness  # noqa: F401

NOW = 1_793_200_000.0
P = "krateo.io/external-create-"


def iso(t):
    return datetime.datetime.fromtimestamp(t, datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def ann(pending=None, succeeded=None, failed=None):
    """Markers as seconds BEFORE NOW."""
    return {f"{P}{k}": iso(NOW - v) for k, v in (("pending", pending), ("succeeded", succeeded),
                                                 ("failed", failed)) if v is not None}


# ─── the rule ────────────────────────────────────────────────────────────────


def test_the_window_is_provider_runtimes_own_defaults():
    """grace (2m) + 2 x re-observe (15s), from provider-runtime v1.5.1's
    defaultCreateRecoveryGracePeriod / defaultCreateRecoveryReobserveInterval
    (pkg/reconciler/reconciler.go:38, :40): the measured recovery, 2m0.1s,
    lands on the first re-observe after the grace period."""
    assert (diag606.CREATE_RECOVERY_GRACE_S, diag606.CREATE_RECOVERY_REOBSERVE_S) == (120.0, 15.0)
    assert diag606.WEDGE_AFTER_S == 150.0
    src = (BENCH_DIR / "bench" / "diag606.py").read_text()
    assert "defaultCreateRecoveryGracePeriod" in src and "defaultCreateRecoveryReobserveInterval" in src


@pytest.mark.parametrize("name, markers, state", [
    ("wedged", ann(pending=600), "WEDGED"),
    ("just past the bound", ann(pending=151), "WEDGED"),
    ("at the bound: still inside", ann(pending=150), "pending"),
    ("the measured recovery: inside", ann(pending=120.1), "pending"),
    ("inside the window", ann(pending=30), "pending"),
    ("recovered", ann(pending=600, succeeded=598), "cleared"),
    ("failed then succeeded", ann(pending=900, failed=880, succeeded=700), "cleared"),
    ("succeeded at the same second", ann(pending=600, succeeded=600), "cleared"),
    ("failed last", ann(pending=900, succeeded=950, failed=880), "create-failed"),
    # newer than BOTH: a pending after a failure (a retry) that never resolved
    ("retried after a failure, wedged", ann(pending=300, failed=600), "WEDGED"),
    ("retried after a failure, in the window", ann(pending=60, failed=600), "pending"),
    ("newer than succeeded, older than failed", ann(pending=500, succeeded=900, failed=400), "create-failed"),
    ("no marker", {}, "no-pending"),
    ("succeeded only", ann(succeeded=10), "no-pending"),
])
def test_create_states(name, markers, state):
    got = diag606.create_state(markers, NOW)
    assert got["state"] == state, name
    assert got["annotations"] == {k[len(P):]: v for k, v in markers.items()}


def test_an_unreadable_marker_is_said_not_guessed():
    assert diag606.create_state({f"{P}pending": "yesterday"}, NOW)["state"] == "unreadable"


# ─── the records, on 057's shapes ───────────────────────────────────────────


def _cd(name, markers, ready=True):
    return {"kind": "CompositionDefinition", "metadata": {"name": name, "namespace": "krateo-system",
                                                          "annotations": {**markers, "other": "x"}},
            "status": {"apiVersion": "composition.krateo.io/v1-8-84", "resource": "dashboardpages",
                       "managed": {"group": "composition.krateo.io", "kind": "DashboardPage",
                                   "versionInfo": [{"version": "v1-8-84"}]},
                       "conditions": [{"type": "Ready", "status": "True" if ready else "False",
                                       "reason": "Available" if ready else "Creating", "message": ""},
                                      {"type": "Synced", "status": "True", "reason": "ReconcileSuccess"}]}}


def _rd(name, markers, ready=False):
    return {"kind": "RestDefinition", "metadata": {"name": name, "namespace": "krateo-system",
                                                   "annotations": markers},
            "status": {"resource": {"apiVersion": "github.krateo.io/v2022-11-28", "kind": "Repo"},
                       "conditions": [{"type": "Ready", "status": "True" if ready else "False",
                                       "reason": "Available" if ready else "Creating",
                                       "message": "create incomplete"},
                                      {"type": "Synced", "status": "False", "reason": "ReconcileError",
                                       "message": "create incomplete"}]}}


def test_a_record_carries_markers_conditions_and_what_it_manages():
    cd = diag606.definition_record(_cd("dashboard-page", ann(pending=600, succeeded=598)),
                                   "compositiondefinitions", NOW)
    assert cd["id"] == "compositiondefinitions/krateo-system/dashboard-page" and cd["state"] == "cleared"
    assert cd["managed"] == {"group": "composition.krateo.io", "kind": "DashboardPage"}
    assert cd["apiVersion"] == "composition.krateo.io/v1-8-84"
    assert cd["ready"]["status"] == "True" and cd["synced"]["reason"] == "ReconcileSuccess"
    assert set(cd["annotations"]) == {"pending", "succeeded"}                 # only the create markers
    rd = diag606.definition_record(_rd("kog-repo", ann(pending=600)), "restdefinitions", NOW)
    assert rd["state"] == "WEDGED" and rd["pending_age_s"] == 600.0
    assert rd["resource"] == {"apiVersion": "github.krateo.io/v2022-11-28", "kind": "Repo"}
    assert rd["ready"] == {"status": "False", "reason": "Creating", "message": "create incomplete"}


class FakeKube:
    """diag606.Kube's kubectl, answering from fixtures."""

    def __init__(self, answers):
        self.answers, self.calls, self.ctx = answers, [], "gke_p_europe-west4_snowplow-bench-x"

    def kubectl(self, *args, timeout=120):
        self.calls.append(list(args))
        for key, ans in self.answers.items():
            if all(k in args for k in key):
                return ans() if callable(ans) else ans
        return 1, "", "error: the server doesn't have a resource type"


def test_a_snapshot_counts_every_state_and_says_what_it_could_not_list():
    objs = {"items": [_rd("wedged", ann(pending=600)), _rd("window", ann(pending=30)),
                      _rd("recovered", ann(pending=600, succeeded=598), True),
                      _rd("failed-then-ok", ann(pending=900, failed=880, succeeded=700), True)]}
    k = FakeKube({("restdefinitions.ogen.krateo.io",): (0, json.dumps(objs), "")})
    rec = diag606.definitions_snapshot(k, at="platform_end", now=lambda: NOW)
    assert rec["counts"] == {"WEDGED": 1, "pending": 1, "cleared": 2, "create-failed": 0, "no-pending": 0,
                             "unreadable": 0}
    assert [u["kind"] for u in rec["unlisted"]] == ["compositiondefinitions.core.krateo.io"]
    assert rec["wedge_after_s"] == 150.0 and rec["context"].endswith("snowplow-bench-x")
    line = diag606.definitions_line(rec)
    assert "WEDGED 1" in line and "WEDGED restdefinitions/krateo-system/wedged: pending" in line
    assert "(600 s old), Ready=False (Creating), Synced=False (ReconcileError)" in line


# ─── controller logs: in full, redacted ─────────────────────────────────────


def _pods():
    return {"items": [
        {"metadata": {"name": "oasgen-provider-0", "namespace": "krateo-system",
                      "labels": {"app.kubernetes.io/name": "oasgen-provider"}},
         "spec": {"containers": [{"name": "oasgen-provider"}]},
         "status": {"containerStatuses": [{"name": "oasgen-provider", "restartCount": 0}]}},
        {"metadata": {"name": "installer-core-provider-0", "namespace": "krateo-system",
                      "labels": {"app.kubernetes.io/name": "core-provider"}},
         "spec": {"containers": [{"name": "installer-core-provider"}]},
         "status": {"containerStatuses": [{"name": "installer-core-provider", "restartCount": 2}]}}]}


LOG = "\n".join([f"2026-10-28T09:40:{i % 60:02d}Z line {i}" for i in range(5000)]
                + ['2026-10-28T09:50:00Z E create failed: User "scaleuser-0001" cannot create restdefinitions '
                   "token=abcdefghijklmnop", "2026-10-28T09:50:01Z I the line after the failure"])


def test_the_controller_selector_is_exact():
    k = FakeKube({("get", "pods"): (0, json.dumps(_pods()), "")})
    diag606.controller_pods(k)
    sel = k.calls[0][k.calls[0].index("-l") + 1]
    assert sel == "app.kubernetes.io/name in (oasgen-provider,core-provider)"     # not chart-inspector


def test_controller_logs_are_kept_in_full_and_redacted(tmp_path):
    k = FakeKube({("logs",): (0, LOG, "")})
    rows = diag606.controller_logs(k, _pods()["items"], str(tmp_path))
    assert [(r["component"], r["previous"]) for r in rows] == [
        ("oasgen-provider", False), ("core-provider", False), ("core-provider", True)]   # restarted: previous too
    assert not any(a.startswith("--tail") for c in k.calls for a in c)
    assert all("--timestamps" in c for c in k.calls)
    text = open(rows[0]["file"]).read()
    assert text.count("\n") + 1 == 5002 and text.endswith("I the line after the failure")
    assert 'User "<redacted>" cannot create' in text and "scaleuser-0001" not in text
    assert "abcdefghijklmnop" not in text


def test_controller_images_name_the_provider_runtime_each_carries():
    pods = _pods()["items"]
    for p, d in zip(pods, ("a" * 64, "b" * 64)):
        p["status"]["containerStatuses"][0].update(image=f"ghcr.io/x/{p['metadata']['name']}:1",
                                                   imageID=f"ghcr.io/x/img@sha256:{d}")
    seen = []

    def read(ref, modules, command=None):
        seen.append(ref)
        if ref.endswith("a" * 64):
            return {"module": modules[0], "version": "v1.5.1", "process_binary": "/usr/bin/manager"}
        raise OSError("registry unreachable")
    rows = diag606.controller_images(pods, read=read)
    assert rows[0]["provider_runtime"] == {"module": "github.com/krateo-platformops/provider-runtime",
                                           "version": "v1.5.1", "process_binary": "/usr/bin/manager"}
    assert rows[1]["provider_runtime"]["why"] == "OSError: registry unreachable"
    assert seen == ["ghcr.io/x/img@sha256:" + "a" * 64, "ghcr.io/x/img@sha256:" + "b" * 64]


# ─── the image read: a registry over HTTP, no Docker ────────────────────────


def _tar_gz(entries):
    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode="w") as tar:
        for name, data, link in entries:
            ti = tarfile.TarInfo(name)
            if link:
                ti.type, ti.linkname = tarfile.SYMTYPE, link
                tar.addfile(ti)
            else:
                ti.size = len(data)
                tar.addfile(ti, io.BytesIO(data))
    return gzip.compress(buf.getvalue())


def _elf(deps):
    return (b"\x7fELF\x02\x01\x01" + b"\0" * 64 + b"\xff Go buildinf:\npath\tx\nmod\tx\t(devel)\t\n"
            + b"".join(f"dep\t{m}\t{v}\th1:abc=\n".encode() for m, v in deps) + b"build\t-ldflags=\n")


@pytest.fixture
def registry():
    """A registry on 127.0.0.1 (plain http, Docker's loopback rule) that asks
    for a Bearer token first, serving one image: an index -> linux/amd64
    manifest -> config (Entrypoint /bin/manager) -> a base layer where /bin is
    a symlink to usr/bin, and a top layer with the controller /usr/bin/manager
    (provider-runtime v1.5.1) and a helper built with v1.3.0."""
    blobs = {}

    def put(data):
        d = "sha256:" + hashlib.sha256(data).hexdigest()
        blobs[d] = data
        return d
    base = put(_tar_gz([("bin", b"", "usr/bin"), ("etc/os-release", b"x", None)]))
    top = put(_tar_gz([("usr/bin/manager", _elf([("github.com/krateo-platformops/provider-runtime", "v1.5.1")]),
                        None),
                       ("usr/bin/helper", _elf([("github.com/krateo-platformops/provider-runtime", "v1.3.0")]),
                        None)]))
    cfg = put(json.dumps({"config": {"Entrypoint": ["/bin/manager"]}}).encode())
    man = json.dumps({"config": {"digest": cfg}, "layers": [{"digest": base}, {"digest": top}]}).encode()
    man_d = put(man)
    index = json.dumps({"manifests": [{"digest": "sha256:" + "f" * 64, "platform": {"os": "linux",
                                                                                     "architecture": "arm64"}},
                                      {"digest": man_d, "platform": {"os": "linux", "architecture": "amd64"}}]})

    class H(http.server.BaseHTTPRequestHandler):
        def do_GET(self):
            if self.path.startswith("/token"):
                return self._send(json.dumps({"token": "t0k"}).encode())
            if self.headers.get("Authorization") != "Bearer t0k":
                self.send_response(401)
                self.send_header("WWW-Authenticate", f'Bearer realm="http://127.0.0.1:{srv.server_address[1]}'
                                                     '/token",service="reg",scope="repository:x/ctl:pull"')
                self.end_headers()
                return
            if self.path.endswith("/manifests/1.0"):
                return self._send(index.encode())
            d = self.path.rsplit("/", 1)[-1]
            if d in blobs:
                return self._send(blobs[d])
            self.send_response(404)
            self.end_headers()

        def _send(self, body):
            self.send_response(200)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, *a):
            pass
    srv = http.server.ThreadingHTTPServer(("127.0.0.1", 0), H)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    yield f"127.0.0.1:{srv.server_address[1]}"
    srv.shutdown()


def test_the_image_read_finds_the_process_binarys_provider_runtime(registry):
    got = imagemod.image_deps(f"{registry}/x/ctl:1.0", diag606.PROVIDER_RUNTIME)
    assert got["process_binary"] == "/usr/bin/manager"                # /bin -> usr/bin, the image's own link
    assert (got["module"], got["version"]) == ("github.com/krateo-platformops/provider-runtime", "v1.5.1")
    assert got["binaries"]["/usr/bin/helper"]["version"] == "v1.3.0"   # another binary, another version
    # the pod's own command wins over the image's Entrypoint
    got = imagemod.image_deps(f"{registry}/x/ctl:1.0", diag606.PROVIDER_RUNTIME, command=["/usr/bin/helper"])
    assert got["version"] == "v1.3.0"
    got = imagemod.image_deps(f"{registry}/x/ctl:1.0", ("github.com/other/module",))
    assert got["version"] is None and "lists none of github.com/other/module" in got["why"]


def test_build_deps_reads_replacements():
    deps = imagemod.build_deps(b"\ndep\tgithub.com/a/b\tv1.0.0\th1:x=\n=>\tgithub.com/c/b\tv1.5.1\th1:y=\n")
    assert deps["github.com/a/b"] == {"version": "v1.0.0",
                                      "replaced_by": {"module": "github.com/c/b", "version": "v1.5.1"}}


# ─── the verdict's platform section, through the evaluate CLI ───────────────


def _snap(at, t, objects, images=None):
    rec = diag606.definitions_snapshot(FakeKube({("restdefinitions.ogen.krateo.io",): (
        0, json.dumps({"items": objects}), "")}), at=at, now=lambda: t)
    if images:
        rec["controller_images"] = images
    return rec


def test_the_verdict_counts_wedged_and_cleared_per_cluster(tmp_path):
    end = _snap("platform_end", NOW, [_rd("kog-repo", ann(pending=600)), _rd("ok", ann(pending=600, succeeded=598))],
                images=[{"component": "oasgen-provider", "image": "ghcr.io/x/oasgen-provider:0.39.0",
                         "provider_runtime": {"version": "v1.5.1"}}])
    # by teardown the wedge resolved (its pending marker gained a succeeded)
    down = _snap("teardown", NOW + 3600, [_rd("kog-repo", ann(pending=600, succeeded=10)),
                                          _rd("ok", ann(pending=600, succeeded=598))])
    f = tmp_path / "platform-definitions.jsonl"
    f.write_text(json.dumps(end) + "\n" + json.dumps(down) + "\n")
    p = subprocess.run([sys.executable, "-m", "bench.load606", "evaluate", str(f)], cwd=BENCH_DIR,
                       capture_output=True, text=True, timeout=120)
    plat = json.loads(p.stdout)["report"]["platform"]
    assert (plat["status"], plat["wedged"], plat["expected_wedged"], plat["cleared"]) == ("present", 1, 0, 2)
    assert [s["at"] for s in plat["snapshots"]] == ["platform_end", "teardown"]
    assert plat["snapshots"][0]["wedged"][0]["id"] == "restdefinitions/krateo-system/kog-repo"
    assert plat["provider_runtime"] == [{"component": "oasgen-provider", "image": "ghcr.io/x/oasgen-provider:0.39.0",
                                         "version": "v1.5.1"}]
    assert "platform: 1 WEDGED create(s)" in p.stderr
    # none recorded: said, not zero
    assert diag606.platform_report([])["status"] == "absent"


# ─── run606: at the platform step's end AND at every teardown ───────────────


def _run(harness, **extra):
    env, state, ambient, tmp = harness
    env = dict(env, FAKE_RDS=json.dumps([_rd("kog-repo", {f"{P}pending": "2026-10-28T09:00:00Z"})]), **extra)
    p = subprocess.run(["bash", str(BENCH_DIR / "run606.sh")], env=env, capture_output=True, text=True,
                       timeout=600)
    recs = [json.loads(x) for x in (tmp / "out" / "platform-definitions.jsonl").read_text().splitlines()] \
        if (tmp / "out" / "platform-definitions.jsonl").exists() else []
    return p, recs, tmp / "out", state


def test_run606_records_the_definitions_at_the_platform_end_and_at_teardown(harness):
    p, recs, out, state = _run(harness)
    assert p.returncode == 0, p.stderr[-2000:]
    assert [r["at"] for r in recs] == ["platform_end", "teardown"], p.stderr[-2000:]
    err = p.stderr
    assert recs[0]["counts"]["WEDGED"] == 1 and "WEDGED restdefinitions/krateo-system/kog-repo" in err
    # at the platform step's end: after the wait, before the next step
    i_end = err.index("[platform] definitions at platform_end")
    assert err.index("=== step platform ===") < i_end < err.index("=== step env ===")
    # at teardown on a SUCCESSFUL run too, before the cluster goes
    assert err.index("=== step evaluate ===") < err.index("[platform] definitions at teardown") < err.index(
        "=== teardown") < err.index("deleted (gke-down OK)")
    logs = sorted(x.name for x in (out / "platform-logs" / "platform_end").iterdir())
    assert logs == ["krateo-system__installer-core-provider-0__core-provider__current.log",
                    "krateo-system__installer-core-provider-0__core-provider__previous.log",
                    "krateo-system__oasgen-provider-0__oasgen-provider__current.log"]
    text = (out / "platform-logs" / "platform_end" / logs[-1]).read_text()
    assert "line 2999" in text and text.rstrip().endswith("the line after the failure")
    assert "scaleuser-0001" not in text
    # the provider-runtime read was tried, and its failure is evidence, not a crash
    assert all((i.get("provider_runtime") or {}).get("why") for i in recs[0]["controller_images"])
    v = json.loads((out / "verdict.json").read_text())
    assert v["report"]["platform"]["wedged"] == 1


def test_the_platform_end_snapshot_is_taken_when_the_platform_never_gets_ready(harness):
    p, recs, out, state = _run(harness, FAKE_INSTALLER_READY="0")
    assert p.returncode != 0 and "TIMEOUT" in p.stdout + p.stderr
    assert "=== step env ===" not in p.stderr                     # the platform step failed
    assert [r["at"] for r in recs] == ["platform_end", "teardown"]
