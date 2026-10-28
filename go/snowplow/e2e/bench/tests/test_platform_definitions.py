"""The platform's create handshakes, recorded so a wedged component is provable
afterwards (a run lost an attempt to one, and the bundle kept no controller
log and no managed object's annotations).

At the end of the platform step AND at every teardown, every RestDefinition
and CompositionDefinition: the krateo.io/external-create-{pending,succeeded,
failed} markers, Ready/Synced, what it manages; a pending marker newer than
both terminal markers is judged by the window of the provider-runtime its
controller CARRIES (read from the image): >= v1.4.0 grace + 2 re-observes
(that release's defaults); < v1.4.0 no recovery path, WEDGED past each
reconcile's own bound; an unknown version UNKNOWN, never a default window.
The oasgen-provider / core-provider logs IN FULL, redacted (current AND
previous). The verdict's platform section counts WEDGED (expected 0),
UNKNOWN, cleared pendings and cap hits per cluster.

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


W15 = diag606.wedge_window("v1.5.1")
W13 = diag606.wedge_window("v1.3.0")
WUNK = diag606.wedge_window(None, "IOError: registry unreachable")


def test_the_window_is_the_carried_runtimes_own():
    """>= v1.4.0: grace + 2 x re-observe of that release (2m, 15s: :38, :40).
    < v1.4.0: no recovery path (v1.3.0 refuses an incomplete create for good,
    :766-773); each reconcile is bounded at timeout + grace (1m + 30s, :29-30).
    Anything else: no window."""
    for v in ("v1.4.0", "v1.5.0", "v1.5.1", "v1.5.2", "v1.5.3"):
        w = diag606.wedge_window(v)
        assert (w["wedge_after_s"], w["recovery"]) == (150.0, True), v
        assert "defaultCreateRecoveryGracePeriod" in w["source"] and v in w["source"]
    for v in ("v1.0.0", "v1.2.2", "v1.3.0", "v1.3.1"):
        w = diag606.wedge_window(v)
        assert (w["wedge_after_s"], w["recovery"]) == (90.0, False), v
        assert "reconcileTimeout = 1 * time.Minute" in w["source"]
    for v in ("v1.6.0", "v0.0.0-20261028-abcdef", None, ""):
        w = diag606.wedge_window(v)
        assert w["wedge_after_s"] is None and w["why"], v


@pytest.mark.parametrize("name, window, markers, state", [
    # provider-runtime >= v1.4.0: 150 s
    ("wedged", W15, ann(pending=600), "WEDGED"),
    ("v1.5.1: just past the bound", W15, ann(pending=151), "WEDGED"),
    ("v1.5.1: at the bound, still inside", W15, ann(pending=150), "pending"),
    ("v1.5.1: the measured recovery, inside", W15, ann(pending=120.1), "pending"),
    ("inside the window", W15, ann(pending=30), "pending"),
    ("recovered", W15, ann(pending=600, succeeded=598), "cleared"),
    ("failed then succeeded", W15, ann(pending=900, failed=880, succeeded=700), "cleared"),
    ("succeeded at the same second", W15, ann(pending=600, succeeded=600), "cleared"),
    ("failed last", W15, ann(pending=900, succeeded=950, failed=880), "create-failed"),
    ("retried after a failure, wedged", W15, ann(pending=300, failed=600), "WEDGED"),
    ("retried after a failure, in the window", W15, ann(pending=60, failed=600), "pending"),
    ("newer than succeeded, older than failed", W15, ann(pending=500, succeeded=900, failed=400), "create-failed"),
    ("no marker", W15, {}, "no-pending"),
    ("succeeded only", W15, ann(succeeded=10), "no-pending"),
    # provider-runtime < v1.4.0: 90 s, no recovery path
    ("v1.3.0: just past its reconcile bound", W13, ann(pending=91), "WEDGED"),
    ("v1.3.0: at its reconcile bound", W13, ann(pending=90), "pending"),
    ("v1.3.0: 120 s is a wedge there (no recovery)", W13, ann(pending=120), "WEDGED"),
    ("v1.3.0: recovered", W13, ann(pending=600, succeeded=598), "cleared"),
    # unknown: never judged by a default window
    ("unknown runtime: old pending", WUNK, ann(pending=6000), "UNKNOWN"),
    ("unknown runtime: fresh pending", WUNK, ann(pending=5), "UNKNOWN"),
    ("unknown runtime: resolved is still resolved", WUNK, ann(pending=600, succeeded=598), "cleared"),
])
def test_create_states(name, window, markers, state):
    got = diag606.create_state(markers, NOW, window)
    assert got["state"] == state, name
    assert got["annotations"] == {k[len(P):]: v for k, v in markers.items()}


def test_a_wedge_under_a_runtime_without_recovery_says_so_and_an_unknown_one_says_why():
    got = diag606.create_state(ann(pending=600), NOW, W13)
    assert got["note"] == "no recovery path in this runtime (provider-runtime v1.3.0)"
    assert "note" not in diag606.create_state(ann(pending=600), NOW, W15)
    got = diag606.create_state(ann(pending=600), NOW, WUNK)
    assert got["why"] == "no window to judge it by: IOError: registry unreachable"


def test_an_unreadable_marker_is_said_not_guessed():
    assert diag606.create_state({f"{P}pending": "yesterday"}, NOW, W15)["state"] == "unreadable"


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
                                   "compositiondefinitions", NOW, W15)
    assert cd["id"] == "compositiondefinitions/krateo-system/dashboard-page" and cd["state"] == "cleared"
    assert cd["managed"] == {"group": "composition.krateo.io", "kind": "DashboardPage"}
    assert cd["apiVersion"] == "composition.krateo.io/v1-8-84"
    assert cd["ready"]["status"] == "True" and cd["synced"]["reason"] == "ReconcileSuccess"
    assert set(cd["annotations"]) == {"pending", "succeeded"}                 # only the create markers
    rd = diag606.definition_record(_rd("kog-repo", ann(pending=600)), "restdefinitions", NOW, W15)
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
    rec = diag606.definitions_snapshot(k, at="platform_end", now=lambda: NOW,
                                       windows={"oasgen-provider": W15, "core-provider": W13})
    assert rec["counts"] == {"WEDGED": 1, "UNKNOWN": 0, "pending": 1, "cleared": 2, "create-failed": 0,
                             "no-pending": 0, "unreadable": 0}
    assert {o["controller"] for o in rec["objects"]} == {"oasgen-provider"}         # RDs: oasgen's window
    assert [u["kind"] for u in rec["unlisted"]] == ["compositiondefinitions.core.krateo.io"]
    assert rec["context"].endswith("snowplow-bench-x")
    line = diag606.definitions_line(rec)
    assert "WEDGED 1" in line and "WEDGED restdefinitions/krateo-system/wedged: pending" in line
    assert "(600 s old), Ready=False (Creating), Synced=False (ReconcileError)" in line
    assert "oasgen-provider: provider-runtime v1.5.1: WEDGED after 150 s" in line
    assert "core-provider: provider-runtime v1.3.0: WEDGED after 90 s (no recovery path in this runtime)" in line


def test_each_controller_window_comes_from_what_its_pods_carry():
    def img(comp, version=None, why=None):
        return {"component": comp, "provider_runtime": {k: v for k, v in (("version", version), ("why", why))
                                                        if v is not None}}
    w = diag606.component_windows([img("oasgen-provider", "v1.5.1"), img("core-provider", "v1.3.0")])
    assert (w["oasgen-provider"]["wedge_after_s"], w["core-provider"]["wedge_after_s"]) == (150.0, 90.0)
    w = diag606.component_windows([img("oasgen-provider", "v1.5.1"), img("oasgen-provider", "v1.5.2"),
                                   img("core-provider", why="URLError: refused")])
    assert w["oasgen-provider"]["wedge_after_s"] is None and "different provider-runtimes" in w["oasgen-provider"]["why"]
    assert w["core-provider"]["why"] == "URLError: refused"
    w = diag606.component_windows([])
    assert w["core-provider"]["why"] == "no core-provider pod was found"


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
    for row in rows:                                   # current AND previous (#960 review T1, Q2)
        text = open(row["file"]).read()
        assert text.count("\n") + 1 == 5002 and text.endswith("I the line after the failure"), row
        assert 'User "<redacted>" cannot create' in text and "scaleuser-0001" not in text, row
        assert "abcdefghijklmnop" not in text, row
    assert rows[2]["previous"] is True and rows[2]["file"].endswith("__previous.log")


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
    # a digest read earlier this run is reused, not read again
    seen.clear()
    rows = diag606.controller_images(pods, read=read, known={"ghcr.io/x/img@sha256:" + "a" * 64: {"version": "v1.5.1"}})
    assert seen == ["ghcr.io/x/img@sha256:" + "b" * 64] and rows[0]["provider_runtime"] == {"version": "v1.5.1"}
    # a replaced module: the replacement is what was compiled
    rows = diag606.controller_images(pods[:1], read=lambda ref, m, command=None: {
        "module": m[0], "version": "v1.3.0", "replaced_by": {"module": "x/fork", "version": "v1.5.1"}})
    assert rows[0]["provider_runtime"]["version"] == "v1.5.1"


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
    man = json.dumps({"config": {"digest": cfg, "size": len(blobs[cfg])},
                      "layers": [{"digest": base, "size": len(blobs[base])},
                                 {"digest": top, "size": len(blobs[top])}]}).encode()
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


def test_a_layer_larger_than_declared_or_undeclared_is_refused(registry, monkeypatch):
    """#960 review B2: a layer is streamed and bounded by the manifest's declared
    size; one sending more is refused loudly, one declaring none too."""
    orig = imagemod.Registry.manifest

    def lie(self, ref):
        m = orig(self, ref)
        m["layers"][-1]["size"] = 10                     # declares 10 bytes, sends more
        return m
    monkeypatch.setattr(imagemod.Registry, "manifest", lie)
    with pytest.raises(imagemod.Oversize, match="more than its declared 10 bytes; refused"):
        imagemod.image_deps(f"{registry}/x/ctl:1.0", diag606.PROVIDER_RUNTIME)

    def undeclared(self, ref):
        m = orig(self, ref)
        m["layers"][-1].pop("size", None)          # (an index resolves through this twice)
        return m
    monkeypatch.setattr(imagemod.Registry, "manifest", undeclared)
    with pytest.raises(imagemod.Oversize, match="declares no size; refused"):
        imagemod.image_deps(f"{registry}/x/ctl:1.0", diag606.PROVIDER_RUNTIME)


def test_a_dep_line_cut_by_a_chunk_boundary_is_still_read(monkeypatch):
    monkeypatch.setattr(imagemod, "_CHUNK", 64)
    blob = b"\x00" * 50 + b"\ndep\tgithub.com/krateo-platformops/provider-runtime\tv1.5.1\th1:abc=\n" + b"\x00" * 50
    got = imagemod._scan_deps(io.BytesIO(blob))
    assert got["github.com/krateo-platformops/provider-runtime"]["version"] == "v1.5.1"


def test_build_deps_reads_replacements():
    deps = imagemod.build_deps(b"\ndep\tgithub.com/a/b\tv1.0.0\th1:x=\n=>\tgithub.com/c/b\tv1.5.1\th1:y=\n")
    assert deps["github.com/a/b"] == {"version": "v1.0.0",
                                      "replaced_by": {"module": "github.com/c/b", "version": "v1.5.1"}}


# ─── the verdict's platform section, through the evaluate CLI ───────────────


def _snap(at, t, objects, images=None, windows=None):
    rec = diag606.definitions_snapshot(FakeKube({("restdefinitions.ogen.krateo.io",): (
        0, json.dumps({"items": objects}), "")}), at=at, now=lambda: t,
        windows=windows or {"oasgen-provider": W15, "core-provider": W15})
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
    f.write_text(json.dumps(end) + "\n" + json.dumps(down) + "\n"
                 + json.dumps({"kind": "platform_definitions_cap", "at": "teardown", "cap_s": 300, "t": NOW}) + "\n")
    p = subprocess.run([sys.executable, "-m", "bench.load606", "evaluate", str(f)], cwd=BENCH_DIR,
                       capture_output=True, text=True, timeout=120)
    plat = json.loads(p.stdout)["report"]["platform"]
    assert (plat["status"], plat["wedged"], plat["expected_wedged"], plat["cleared"]) == ("present", 1, 0, 2)
    assert [s["at"] for s in plat["snapshots"]] == ["platform_end", "teardown"]
    assert plat["snapshots"][0]["flagged"][0] == {"id": "restdefinitions/krateo-system/kog-repo", "state": "WEDGED",
                                                  "pending": iso(NOW - 600), "pending_age_s": 600.0}
    assert plat["provider_runtime"] == [{"component": "oasgen-provider", "image": "ghcr.io/x/oasgen-provider:0.39.0",
                                         "version": "v1.5.1"}]
    assert plat["windows"]["oasgen-provider"]["wedge_after_s"] == 150.0
    assert plat["cap_hits"] == [{"kind": "platform_definitions_cap", "at": "teardown", "cap_s": 300, "t": NOW}]
    assert "platform: 1 WEDGED create(s)" in p.stderr
    assert "the definitions snapshot at teardown hit its 300 s cap: incomplete" in p.stderr


def test_a_failed_provider_runtime_read_carries_its_why_into_the_report():
    rec = _snap("platform_end", NOW, [_rd("kog-repo", ann(pending=600))],
                images=[{"component": "oasgen-provider", "image": "ghcr.io/x/oasgen-provider:0.39.0",
                         "provider_runtime": {"why": "URLError: refused"}}],
                windows={"oasgen-provider": diag606.wedge_window(None, "URLError: refused")})
    plat = diag606.platform_report([rec])
    assert plat["provider_runtime"] == [{"component": "oasgen-provider", "image": "ghcr.io/x/oasgen-provider:0.39.0",
                                         "version": None, "why": "URLError: refused"}]
    assert (plat["unknown"], plat["wedged"]) == (1, 0)
    assert plat["snapshots"][0]["flagged"][0]["why"] == "no window to judge it by: URLError: refused"
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
    # the harness's images are on a registry nothing serves: no version, so no window -> UNKNOWN, loudly
    assert recs[0]["counts"]["UNKNOWN"] == 1 and recs[0]["counts"]["WEDGED"] == 0
    assert "UNKNOWN restdefinitions/krateo-system/kog-repo" in err
    assert "oasgen-provider: provider-runtime UNKNOWN: NO WINDOW (URLError" in err
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
    assert (v["report"]["platform"]["unknown"], v["report"]["platform"]["wedged"]) == (1, 0)
    assert any("platform: 1 UNKNOWN create(s)" in w for w in v["report"]["report_warnings"])


def test_the_platform_end_snapshot_is_taken_when_the_platform_never_gets_ready(harness):
    p, recs, out, state = _run(harness, FAKE_INSTALLER_READY="0")
    assert p.returncode != 0 and "TIMEOUT" in p.stdout + p.stderr
    assert "=== step env ===" not in p.stderr                     # the platform step failed
    assert [r["at"] for r in recs] == ["platform_end", "teardown"]


def test_the_summary_is_printed_from_the_redacted_record(harness):
    """#960 LOW: a list refused with an error naming the user: neither the
    record nor the run log's summary carries the name."""
    p, recs, out, state = _run(harness, FAKE_RDS_FORBIDDEN="1")
    assert "could not list restdefinitions.ogen.krateo.io (rc=1)" in p.stderr
    assert 'User "<redacted>" cannot list' in p.stderr
    assert "scaleuser-0001" not in p.stderr + json.dumps(recs)


def test_a_definitions_call_that_hits_its_cap_is_said_and_recorded(harness):
    """#960 review B1: the platform-end call is capped like the teardown one;
    a cap hit is loud and recorded, and the platform step goes on."""
    p, recs, out, state = _run(harness, DEFS_CAP_S="1", FAKE_CONTROLLER_LOG_LINES="3000000")
    assert "PLATFORM DEFINITIONS CAP HIT at platform_end: stopped after 1s" in p.stderr
    caps = [json.loads(x) for x in (out / "platform-definitions.jsonl").read_text().splitlines()
            if '"platform_definitions_cap"' in x]
    assert {c["at"] for c in caps} >= {"platform_end"} and all(c["cap_s"] == 1 for c in caps)
    assert "=== step env ===" in p.stderr                          # the platform step was not held
