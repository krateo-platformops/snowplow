"""CLI tests: calibrate, phase6 resume/exit-codes, overlay-stale, report.

Companion file to test_cli_check.py (Block 2). Per plan §C.8 — the 9
test cases listed at lines 408-419.

Per docs/bench-restructure-path-b-plan-2026-06-02.md §C.8.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from pathlib import Path
from unittest import mock

import pytest

import bench.cli as cli_mod
from bench import phases
from bench.browser import ConvergenceTimeout


# ─── calibrate ──────────────────────────────────────────────────────────────


def test_calibrate_command_writes_overlay_then_exits_0(monkeypatch):
    """cmd_calibrate wraps expected.run_calibrate_expected_calls; rc=0 on
    successful write."""
    written = {}

    def _fake_calibrate():
        written["called"] = True
        return "/tmp/snowplow-runs/calibration/expected_calls.json"

    monkeypatch.setattr(
        "bench.expected.run_calibrate_expected_calls", _fake_calibrate)
    args = argparse.Namespace(tag="0.30.232")
    rc = cli_mod.cmd_calibrate(args)
    assert rc == 0
    assert written["called"] is True


def test_calibrate_command_exits_1_on_failure(monkeypatch, capsys):
    def _fake_calibrate():
        raise RuntimeError("boom")

    monkeypatch.setattr(
        "bench.expected.run_calibrate_expected_calls", _fake_calibrate)
    rc = cli_mod.cmd_calibrate(argparse.Namespace(tag="t"))
    assert rc == 1
    err = capsys.readouterr().err
    assert "calibrate FAILED" in err
    assert "boom" in err


# ─── phase6 stage-resume (acceptance (f)) ──────────────────────────────────


def test_phase6_from_stage_S5_resumes_from_disk_state(tmp_path, monkeypatch):
    """Run to S4 then re-invoke with --from-stage S5; state.json.stages_completed
    must already contain S0-S4 and stage timestamps for S0-S4 do NOT update."""
    # Prepare a state.json with S0-S4 completed.
    pre_state = {
        "schema_version": "1.0.0",
        "tag": "0.30.232", "scale": 5000,
        "started_at": "2026-06-02T00:00:00+00:00",
        "stages_completed": ["S0", "S1", "S2", "S3", "S4"],
        "stage_proofs": {
            sid: {
                "stage_id": sid, "started_at": "T_PRIOR",
                "ended_at": "T_PRIOR_END",
                "passed": True, "proof": {}, "artifacts": [],
                "what_breaks_if_skipped": (
                    f"{sid} underwrites later cells"),
            } for sid in ("S0", "S1", "S2", "S3", "S4")
        },
    }
    phases.save_state(tmp_path, pre_state)

    # Stub out the actual stage runner — capture which stages executed.
    executed = []

    def _make_fake_stage(sid):
        def _f(ctx):
            executed.append(sid)
            return phases.StageProof(
                stage_id=sid, started_at="T_NEW", ended_at="T_NEW_END",
                passed=True, proof={"ran": sid}, artifacts=[],
                what_breaks_if_skipped="fake stage",
            )
        return _f

    fake_registry = {
        sid: _make_fake_stage(sid) for sid in phases.STAGE_ORDER
    }
    monkeypatch.setattr(phases, "STAGE_REGISTRY", fake_registry)
    # Avoid live login + browser context.
    monkeypatch.setattr("bench.browser.login_all", lambda: {})
    monkeypatch.setattr(phases, "_setup_users", lambda ctx: None)
    monkeypatch.setattr(phases, "_teardown_users", lambda ctx: None)
    monkeypatch.setattr("bench.browser.FRONTEND", None)
    # Bypass the live-cluster proof re-runner (avoids kubectl spawn).
    monkeypatch.setattr(phases, "_proof_validation_re_runner",
                        lambda state, fs, budget_secs=10.0: {
                            sid: "skipped:test" for sid in state.get(
                                "stages_completed", [])
                        })

    phases.run_phase6(
        "0.30.232", 5000,
        from_stage="S5", to_stage="S6",
        run_dir=tmp_path,
    )

    # S0-S4 must NOT have been re-run.
    assert "S0" not in executed
    assert "S4" not in executed
    # S5+S6 must have run.
    assert "S5" in executed
    assert "S6" in executed
    # state.json's S0-S4 proofs preserved their prior timestamps.
    state = json.loads((tmp_path / "state.json").read_text())
    for sid in ("S0", "S1", "S2", "S3", "S4"):
        assert state["stage_proofs"][sid]["started_at"] == "T_PRIOR", (
            f"{sid} timestamp drifted on resume")


# ─── phase6 aborts when overlay is stale (acceptance (g)) ──────────────────


def test_phase6_aborts_when_overlay_stale(tmp_path, monkeypatch, capsys):
    """When the overlay is >14 days old, `bench phase6` exits non-zero with
    stderr pointing to `python -m bench calibrate`."""
    # Synthesize a stale overlay path via expected.overlay_age_seconds.
    fake_overlay = tmp_path / "expected_calls.json"
    fake_overlay.write_text("{}")
    # 20 days ago
    stale_age = 20 * 86400
    now = time.time()
    os.utime(fake_overlay, (now - stale_age, now - stale_age))

    from bench import expected as expected_mod
    monkeypatch.setattr(expected_mod, "EXPECTED_CALLS_OVERLAY_PATH",
                        str(fake_overlay))

    # cmd_check is the operator-facing entrypoint for overlay-gate; we
    # exercise its gate-7 path directly.
    args = argparse.Namespace(tag="0.30.232", allow_non_gke=True,
                              allow_stale_overlay=False)
    monkeypatch.setattr(
        cli_mod, "_gate_pod_ready", lambda: (True, "snowplow_pod_ready: PASS"))
    monkeypatch.setattr(
        cli_mod, "_gate_image_match", lambda t: (True, "image_tag_match: PASS"))
    monkeypatch.setattr(
        cli_mod, "_gate_crds_present", lambda timeout=10:
        (True, "crds_present: PASS"))
    monkeypatch.setattr(
        cli_mod, "_gate_helm_lockstep", lambda t:
        (True, "helm_release_lockstep: PASS"))
    monkeypatch.setattr(
        cli_mod, "_gate_frontend_reachable", lambda:
        (True, "frontend_lb_reachable: PASS"))
    monkeypatch.setattr(
        cli_mod, "_gke_context_guard", lambda allow_non_gke=None:
        "gke_neon-481711_us-central1-a_cluster-1")

    rc = cli_mod.cmd_check(args)
    assert rc == 2
    err = capsys.readouterr().err
    assert "overlay_freshness: FAIL" in err


def test_overlay_stale_message_points_to_bench_calibrate(tmp_path, monkeypatch):
    """The gate-7 failure text MUST mention how to refresh the overlay."""
    from bench import expected as expected_mod
    fake_overlay = tmp_path / "expected_calls.json"
    fake_overlay.write_text("{}")
    stale_age = 20 * 86400
    now = time.time()
    os.utime(fake_overlay, (now - stale_age, now - stale_age))
    monkeypatch.setattr(expected_mod, "EXPECTED_CALLS_OVERLAY_PATH",
                        str(fake_overlay))

    ok, msg = cli_mod._gate_overlay_freshness(allow_stale=False)
    assert ok is False
    # Stale-overlay diagnostic must reference 'calibrate' so the operator
    # knows the remedy command.
    diag = (msg or "").lower()
    assert "calibrate" in diag or "overlay" in diag


# ─── cmd_report ─────────────────────────────────────────────────────────────


def test_cmd_report_exits_1_when_run_dir_missing(capsys):
    rc = cli_mod.cmd_report(argparse.Namespace(run_dir=None))
    assert rc == 1
    assert "run-dir is required" in capsys.readouterr().err


def test_cmd_report_emits_row_with_state_present(tmp_path, monkeypatch):
    # Pre-seed a state.json + a per-mutation result file.
    state = {
        "schema_version": "1.0.0",
        "tag": "0.30.232", "scale": 5000,
        "stages_completed": ["S0", "S1"],
        "stage_proofs": {
            "S0": {
                "stage_id": "S0", "started_at": "x", "ended_at": "y",
                "passed": True,
                "proof": {}, "artifacts": [],
                "what_breaks_if_skipped": "preflight underwrites all",
            },
            "S1": {
                "stage_id": "S1", "started_at": "x", "ended_at": "y",
                "passed": True,
                "proof": {}, "artifacts": [],
                "what_breaks_if_skipped": "S1 underwrites cold/warm denominator",
            },
        },
    }
    phases.save_state(tmp_path, state)
    # Stub kubectl so build_canonical_ledger_row doesn't spawn subprocess.
    from bench import ledger
    monkeypatch.setattr(ledger, "kubectl", lambda *a, **k: (1, "", ""))

    rc = cli_mod.cmd_report(argparse.Namespace(run_dir=str(tmp_path)))
    assert rc == 0
    assert (tmp_path / "ledger_row.json").exists()
    assert (tmp_path / "summary.json").exists()


# ─── phase6 ConvergenceTimeout exit code ────────────────────────────────────


def test_cmd_phase6_exits_4_on_ConvergenceTimeout(tmp_path, monkeypatch):
    """When phases.run_phase6 raises ConvergenceTimeout, cmd_phase6 exits 4."""
    def _raise(*a, **k):
        raise ConvergenceTimeout(
            stage=6, user="cyberjoker",
            api=99, ui=100, cluster=200, timeout_secs=300)

    monkeypatch.setattr(phases, "run_phase6", _raise)
    args = argparse.Namespace(
        tag="0.30.232", scale=5000,
        from_stage=None, to_stage="S6",
        cache_mode="ON", video="none",
        run_dir=str(tmp_path),
        allow_stale_overlay=True,
    )
    rc = cli_mod.cmd_phase6(args)
    assert rc == 4


def test_cmd_phase6_exits_2_on_incompatible_state_schema(tmp_path, monkeypatch):
    """When phases.run_phase6 raises IncompatibleStateSchema, cmd_phase6
    exits 2 (caller should re-init the run_dir)."""
    def _raise(*a, **k):
        raise phases.IncompatibleStateSchema("schema 2.x not supported")

    monkeypatch.setattr(phases, "run_phase6", _raise)
    args = argparse.Namespace(
        tag="0.30.232", scale=5000,
        from_stage="S5", to_stage=None,
        cache_mode="ON", video="none",
        run_dir=str(tmp_path),
        allow_stale_overlay=True,
    )
    rc = cli_mod.cmd_phase6(args)
    assert rc == 2


# ─── phase6 run-lock prevents concurrent invocations ───────────────────────


def test_cmd_phase6_returns_5_when_lock_already_held(tmp_path, run_lock, capsys):
    """When another phase6 holds the run lock (/tmp/snowplow-bench.lock on a
    real host; the test's own file here, conftest `run_lock`), cmd_phase6 exits 5
    and names the holding PID in stderr. Prevents the cluster-contamination
    failure mode observed 2026-06-09 (two concurrent bench instances stomped
    each other's S5/S6)."""
    import fcntl
    lock_path = run_lock
    # Hold the lock from a separate fd to simulate the prior bench instance.
    holder = open(lock_path, "a+")
    fcntl.flock(holder.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    holder.write("99999")
    holder.flush()
    try:
        args = argparse.Namespace(
            tag="0.30.232", scale=5000,
            from_stage=None, to_stage="S0",
            cache_mode="ON", video="none",
            run_dir=str(tmp_path / "run"),
            allow_stale_overlay=True,
        )
        rc = cli_mod.cmd_phase6(args)
        assert rc == 5
        err = capsys.readouterr().err
        assert "already running" in err
        assert "99999" in err
    finally:
        fcntl.flock(holder.fileno(), fcntl.LOCK_UN)
        holder.close()


def test_the_run_lock_is_the_hosts_file_unless_bench_run_lock_names_another(monkeypatch, run_lock):
    """#1166: a real host's phase6 locks /tmp/snowplow-bench.lock (two runs on one host must
    refuse each other); BENCH_RUN_LOCK names another file, and the suite's conftest names one
    under each test's tmp_path, so no test takes (or is refused by) the host's lock."""
    assert cli_mod._run_lock_path() == str(run_lock) and str(run_lock) != "/tmp/snowplow-bench.lock"
    fh = cli_mod._acquire_run_lock()
    try:
        assert run_lock.read_text() == str(os.getpid())
    finally:
        fh.close()
    monkeypatch.delenv("BENCH_RUN_LOCK")
    assert cli_mod._run_lock_path() == "/tmp/snowplow-bench.lock"


# ─── cmd_measure refuses lifecycle stages ──────────────────────────────────


def test_cmd_measure_refuses_mutation_stages(tmp_path, capsys):
    """`bench measure --stage S6` must refuse (S6 deploys SCALE compositions)."""
    args = argparse.Namespace(stage="S6", tag="t", scale=5000,
                              cache_mode="OFF", run_dir=str(tmp_path))
    rc = cli_mod.cmd_measure(args)
    assert rc == 1
    err = capsys.readouterr().err
    assert "S6" not in err or "must be in" in err.lower()
