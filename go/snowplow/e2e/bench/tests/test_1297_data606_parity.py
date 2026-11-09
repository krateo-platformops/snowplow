"""#1297: the kind parity arm's pure parts (bench/data606_parity.py), hermetic.

The arm itself (tests/kind_1297_data606_parity.sh) needs kind and the internet and is run by hand;
what it decides with is tested here: the normalization (server-set fields out, Secret values only as
their sha256), the baseline subtraction, the diff (one object fewer, one extra, one changed field, a
different controller replica count each FAIL it), and the run606.sh extraction arm B runs.
"""

from __future__ import annotations

import copy
import json
import subprocess
from pathlib import Path

import pytest

from bench import data606_parity as dp

BENCH_DIR = Path(__file__).resolve().parent.parent

ARM_B_VARS, ARM_B_FUNCTIONS = dp.ARM_B_VARS, dp.ARM_B_FUNCTIONS


def _server_set(o: dict, uid: str) -> dict:
    o = copy.deepcopy(o)
    o["metadata"].update(uid=uid, resourceVersion=uid, creationTimestamp="2026-11-09T00:00:00Z",
                         generation=1, managedFields=[{"manager": uid}])
    o["status"] = {"seen": uid}
    return o


NS = {"apiVersion": "v1", "kind": "Namespace", "metadata": {"name": "bench-ns-01"}, "spec": {"finalizers": ["kubernetes"]}}
COMP = {"apiVersion": "composition.krateo.io/v1-2-2", "kind": "GithubScaffoldingWithCompositionPage",
        "metadata": {"name": "bench-app-01-01", "namespace": "bench-ns-01"}, "spec": {"app": {"port": 31180}}}
SECRET = {"apiVersion": "v1", "kind": "Secret", "type": "kubernetes.io/basic-auth",
          "metadata": {"name": "scaleuser-0001-password", "namespace": "krateo-system"},
          "data": {"password": "c2VjcmV0LXZhbHVl"}}
SYSTEM_CR = {"apiVersion": "rbac.authorization.k8s.io/v1", "kind": "ClusterRole", "metadata": {"name": "admin"}, "rules": []}
CONTROLLER = {"spec": {"replicas": 0}}


def _listings(uid: str, comps=(COMP,), extra_cr=()) -> dict:
    return {"Namespace": {"items": [_server_set(NS, uid)]},
            "GithubScaffoldingWithCompositionPage": {"items": [_server_set(c, uid) for c in comps]},
            "Secret": {"items": [_server_set(SECRET, uid)]},
            "ClusterRole": {"items": [_server_set(SYSTEM_CR, uid)] + [_server_set(c, uid) for c in extra_cr]}}


def _baseline(uid: str) -> dict:
    return dp.build({"ClusterRole": {"items": [_server_set(SYSTEM_CR, uid)]}}, None, None)


def _arm(uid: str, **kw) -> dict:
    return dp.build(_listings(uid, **kw), CONTROLLER, _baseline(uid))


def test_two_clusters_with_the_same_objects_diff_empty_whatever_the_server_set():
    a, b = _arm("uid-a"), _arm("uid-b")
    d = dp.diff(a, b)
    assert d["verdict"] == "PASS", d
    assert d["counts"] == {"GithubScaffoldingWithCompositionPage": [1, 1], "Namespace": [1, 1], "Secret": [1, 1]}
    assert "ClusterRole/-/admin" not in a["objects"]            # in the baseline: not data606's


def test_no_secret_value_is_kept_and_a_changed_value_still_fails():
    a = _arm("uid-a")
    text = json.dumps(a)
    assert "c2VjcmV0LXZhbHVl" not in text and "secret-value" not in text
    other = copy.deepcopy(SECRET)
    other["data"]["password"] = "b3RoZXI="
    lb = _listings("uid-b")
    lb["Secret"]["items"] = [_server_set(other, "uid-b")]
    d = dp.diff(a, dp.build(lb, CONTROLLER, _baseline("uid-b")))
    assert d["verdict"] == "FAIL"
    assert d["different"] == {"Secret/krateo-system/scaleuser-0001-password": [".data.password"]}
    assert "b3RoZXI=" not in json.dumps(d)


def test_one_object_fewer_fails_with_its_name_and_unequal_counts():
    comp2 = copy.deepcopy(COMP)
    comp2["metadata"]["name"] = "bench-app-01-02"
    d = dp.diff(_arm("uid-a", comps=(COMP, comp2)), _arm("uid-b"))
    assert d["verdict"] == "FAIL" and not d["counts_equal"]
    assert d["only_in_a"] == ["GithubScaffoldingWithCompositionPage/bench-ns-01/bench-app-01-02"]


def test_an_object_the_in_cluster_arm_left_behind_fails():
    leaked = {"apiVersion": "rbac.authorization.k8s.io/v1", "kind": "ClusterRole",
              "metadata": {"name": "bench-data606"}, "rules": []}
    d = dp.diff(_arm("uid-a"), _arm("uid-b", extra_cr=(leaked,)))
    assert d["verdict"] == "FAIL" and d["only_in_b"] == ["ClusterRole/-/bench-data606"]


def test_a_changed_field_and_a_different_hold_each_fail():
    changed = copy.deepcopy(COMP)
    changed["spec"]["app"]["port"] = 1
    d = dp.diff(_arm("uid-a"), _arm("uid-b", comps=(changed,)))
    assert d["different"] == {"GithubScaffoldingWithCompositionPage/bench-ns-01/bench-app-01-01": [".spec.app.port"]}
    held, not_held = _arm("uid-a"), dp.build(_listings("uid-b"), {"spec": {"replicas": 1}}, _baseline("uid-b"))
    assert dp.diff(held, not_held)["verdict"] == "FAIL"
    assert dp.diff(held, dp.build(_listings("uid-b"), None, _baseline("uid-b")))["verdict"] == "FAIL"


def test_the_harness_namespace_is_not_compared():
    sa_cm = {"apiVersion": "v1", "kind": "Secret", "metadata": {"name": "x", "namespace": "bench-loadgen"}, "data": {}}
    lb = _listings("uid-b")
    lb["Secret"]["items"].append(_server_set(sa_cm, "uid-b"))
    lb["Namespace"]["items"].append(_server_set({"apiVersion": "v1", "kind": "Namespace",
                                                 "metadata": {"name": "bench-loadgen"}}, "uid-b"))
    assert dp.diff(_arm("uid-a"), dp.build(lb, CONTROLLER, _baseline("uid-b")))["verdict"] == "PASS"


def test_arm_b_runs_run606s_own_functions_and_defaults():
    src = (BENCH_DIR / "run606.sh").read_text()
    text = dp.assignments(src, ARM_B_VARS) + dp.functions(src, ARM_B_FUNCTIONS)
    for n in ARM_B_FUNCTIONS:
        assert f"\n{n}() {{" in "\n" + text
    assert "s_data() {\n" in text and 'render-data606 --context "$KCTX"' in text
    assert 'FRESH_MODES="${FRESH_MODES:-annotation status}"' in text
    p = subprocess.run(["bash", "-n"], input=text, text=True, capture_output=True)
    assert p.returncode == 0, p.stderr
    with pytest.raises(SystemExit):
        dp.functions(src, ["no_such_function_1297"])


def test_the_harness_extracts_the_modules_lists():
    sh = (BENCH_DIR / "tests" / "kind_1297_data606_parity.sh").read_text()
    assert "python3 -m bench.data606_parity functions --script run606.sh --arm-b" in sh
    assert "--vars" not in sh                      # no second list in the harness to drift
    assert subprocess.run(["bash", "-n", str(BENCH_DIR / "tests" / "kind_1297_data606_parity.sh")]).returncode == 0


def test_arm_b_carries_every_variable_and_function_s_datas_path_reads():
    """The guard f7aa9df3 needed: s_data's path read INC_POLL_S (run606.sh's top level) and the
    arm's list did not carry it, so arm B died on an unbound variable. Now the lists are checked
    against run606.sh itself."""
    src = (BENCH_DIR / "run606.sh").read_text()
    assert dp.arm_b_coverage(src) == {"vars_missing": [], "functions_missing": []}


@pytest.mark.parametrize("drop, field", [("INC_POLL_S", "vars_missing"), ("PW", "vars_missing"),
                                         ("kerr_line", "functions_missing"), ("data606_wait", "functions_missing")])
def test_the_guard_sees_a_dropped_variable_or_function(monkeypatch, drop, field):
    lst = "ARM_B_VARS" if field == "vars_missing" else "ARM_B_FUNCTIONS"
    monkeypatch.setattr(dp, lst, [x for x in getattr(dp, lst) if x != drop])
    assert drop in dp.arm_b_coverage((BENCH_DIR / "run606.sh").read_text())[field]
    with pytest.raises(SystemExit, match="arm B would not run what run606 runs"):
        dp.main(["functions", "--script", str(BENCH_DIR / "run606.sh"), "--arm-b"])


def test_a_new_global_read_on_s_datas_path_fails_the_guard():
    """A run606.sh where data606_wait reads a NEW top-level variable: refused until the arm carries it."""
    src = (BENCH_DIR / "run606.sh").read_text()
    assert "INC_FINISH_CALLS=6\n" in src and "data606_wait() {\n" in src
    src = src.replace("INC_FINISH_CALLS=6\n", "INC_FINISH_CALLS=6\nNEW_1297_KNOB=1\n", 1)
    src = src.replace("data606_wait() {\n", 'data606_wait() {\n  : "$NEW_1297_KNOB"\n', 1)
    assert dp.arm_b_coverage(src)["vars_missing"] == ["NEW_1297_KNOB"]


def test_an_aborted_s_data_fails_closed_and_deletes_what_it_created(tmp_path):
    """armB.sh's EXIT trap, as the harness writes it: s_data dies on an unbound variable (set -u), the
    trap says so, deletes data606's Job, ServiceAccount and RBAC (incluster_delete), and exits 1."""
    sh = (BENCH_DIR / "tests" / "kind_1297_data606_parity.sh").read_text()
    lines = [ln.strip() for ln in sh.splitlines()
             if ln.strip().startswith("echo 'on_exit()") or ln.strip() in ("echo 'trap on_exit EXIT'", "echo 'DONE=0'")]
    assert len(lines) == 3
    trap = "".join(subprocess.run(["bash", "-c", ln], capture_output=True, text=True).stdout for ln in lines)
    calls = tmp_path / "calls"
    script = ("set -Eeuo pipefail\n" + trap
              + f'incluster_delete() {{ echo "delete $1" >> {calls}; }}\n'
              + 's_data() { echo "$NO_SUCH_VARIABLE"; }\nrc=0; s_data || rc=$?\nDONE=1\necho S_DATA_RC=$rc\n')
    p = subprocess.run(["bash", "-c", script], capture_output=True, text=True)
    assert p.returncode == 1 and "S_DATA_ABORTED" in p.stdout and "S_DATA_RC" not in p.stdout, (p.stdout, p.stderr)
    assert calls.read_text() == "delete data606\n"
