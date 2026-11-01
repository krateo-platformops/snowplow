"""#1051 — what collect and evaluate completed survives `capped`'s subshell.

s_collect runs under `capped` (a subshell), so `COLLECTED=1` never reached the
parent: a run that failed AFTER "run complete" made teardown re-collect from
the cluster as it was then and overwrite $OUT/verdict.json, which the pair
reads after both runs exit. The marks are files in $OUT now:
  - after the verdict: teardown never re-collects nor re-evaluates;
  - collected, not evaluated: teardown evaluates what was collected, reads nothing again;
  - nothing collected: the partial collect of #763, as before;
  - a previous run's marks in the same $OUT never count.
"""

from __future__ import annotations

import hashlib
import subprocess

from tests.test_run606_real_sequence import BENCH_DIR, harness  # noqa: F401  (fixture)


def _run(env, **over):
    return subprocess.run(["bash", str(BENCH_DIR / "run606.sh")], env=dict(env, **over), capture_output=True,
                          text=True, timeout=300)


def _collects(log: str) -> int:
    return sum(1 for ln in log.splitlines() if "] collected: " in ln)


def _verdicts(log: str) -> int:
    return sum(1 for ln in log.splitlines() if "] verdict exit=" in ln)


def test_a_failure_after_run_complete_leaves_the_verdict_byte_identical(harness):  # noqa: F811
    """FAKE_VERDICT_SNAPSHOT: the fake kubectl copies verdict.json at its first
    call after the evaluate step wrote it (the teardown's first read): the
    bytes the run leaves must be those bytes."""
    env, state, ambient, tmp = harness
    snap = tmp / "verdict-at-evaluate.json"
    p = _run(env, BENCH_606_FAIL_AT="complete", FAKE_VERDICT_SNAPSHOT=str(snap))
    assert p.returncode == 1
    log = (tmp / "out" / "run.log").read_text()
    i_done = log.index("run complete: ")
    assert "injected failure after run complete" in log[i_done:]
    assert _verdicts(log) == 1 and _collects(log) == 1, log[-3000:]
    assert "] verdict exit=" not in log[i_done:] and "collecting partial results" not in log
    assert "nothing is re-collected or re-evaluated" in log[i_done:]
    assert snap.exists()
    assert (tmp / "out" / "verdict.json").read_bytes() == snap.read_bytes()
    assert "=== teardown " in log                      # and it still tore down


def test_collected_but_not_evaluated_evaluates_without_reading_again(harness):  # noqa: F811
    env, state, ambient, tmp = harness
    p = _run(env, BENCH_606_FAIL_AT="evaluate")
    assert p.returncode == 1
    log = (tmp / "out" / "run.log").read_text()
    assert "computing the verdict from what was collected before teardown" in log
    assert _collects(log) == 1 and _verdicts(log) == 1, log[-3000:]
    assert "collecting partial results" not in log
    assert (tmp / "out" / "verdict.json").stat().st_size > 0


def test_a_failure_before_collect_still_collects_in_teardown(harness):  # noqa: F811
    env, state, ambient, tmp = harness
    p = _run(env, BENCH_606_FAIL_AT="collect")
    assert p.returncode == 1
    log = (tmp / "out" / "run.log").read_text()
    assert "collecting partial results before teardown" in log
    assert _collects(log) == 1 and _verdicts(log) == 1
    assert (tmp / "out" / "verdict.json").stat().st_size > 0


def test_a_previous_runs_marks_never_count(harness):  # noqa: F811
    env, state, ambient, tmp = harness
    out = tmp / "out"
    out.mkdir(parents=True, exist_ok=True)
    (out / ".collected").write_text("")
    (out / ".verdict-final").write_text("")
    p = _run(env, BENCH_606_FAIL_AT="collect")
    assert p.returncode == 1
    log = (out / "run.log").read_text()
    assert "collecting partial results before teardown" in log and _collects(log) == 1


def test_no_shell_variable_carries_collect_out_of_its_subshell():
    src = (BENCH_DIR / "run606.sh").read_text()
    assert "COLLECTED=" not in src and "COLLECTED:-" not in src
    assert ': > "$COLLECTED_MARK"' in src and ': > "$VERDICT_MARK"' in src
    assert 'rm -f "$COLLECTED_MARK" "$VERDICT_MARK"' in src
