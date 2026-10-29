"""A pair stop must never interrupt a run's teardown (run 17 attempt 4: A
failed as tooling, and the pair's TERM reached B, already in its teardown,
as an operator interrupt: "signal during teardown: collect/diagnostics cut
short").

The pair now stops its partner with SIGUSR1, a PAIR STOP. Before the
teardown, the run's measurement stops and its teardown runs in full. During
the teardown it changes nothing. An operator's Ctrl-C / TERM keeps its
meaning (tests/test_run9_safety.py). Real run606.sh, fake cloud
(tests/test_run606_real_sequence.py's harness).
"""

from __future__ import annotations

import signal

from tests.test_run606_pr483 import _env483
from tests.test_run606_real_sequence import harness  # noqa: F401
from tests.test_run9_safety import _deleted, _measure_fails, _signal_at


def test_a_pair_stop_during_the_teardown_cuts_nothing_short(harness):  # noqa: F811
    env, state, ambient, tmp = harness
    env = _measure_fails(env, FAKE_SLOW_LOGS="6")
    err = _signal_at(env, [("collecting partial results", signal.SIGUSR1, False)])
    assert "pair stop received during teardown: ignored, the teardown finishes in full" in err
    assert "signal during teardown" not in err and "stopped after" not in err
    # the partial collect it arrived in finished, then the diagnostics, then the delete and its backstop
    after = err.split("pair stop received during teardown", 1)[1]
    assert "collected:" in after and "=== diagnostics before teardown" in after
    assert "[platform] definitions at teardown" in err
    _deleted(state, err)


def test_a_pair_stop_during_the_measure_stops_it_and_tears_down_in_full(harness):  # noqa: F811
    env, state, ambient, tmp = harness
    env = _env483(env)                               # as test_term_during_measure_...: a measure in progress
    env.update(FAKE_483="pass", FAKE_KOG="1", PREFLIGHT_POLL_S="0.1", FAKE_SLOW_WAIT="10")
    # sent to run606's shell only (as the pair does): it takes effect when the wait in progress returns
    err = _signal_at(env, [("bench-loadgen wait job/fresh606", signal.SIGUSR1, False)], timeout=300)
    assert "pair stop: the partner run failed as tooling; this run's measurement stops here" in err
    assert "measure FAILED or interrupted (exit 143)" in err
    assert "signal during teardown" not in err
    assert "=== diagnostics before teardown" in err and "[platform] definitions at teardown" in err
    _deleted(state, err)


def test_the_pair_stops_its_partner_with_a_pair_stop_not_a_term():
    from tests.test_run606_real_sequence import BENCH_DIR
    src = (BENCH_DIR / "run606_pair.sh").read_text()
    stop = src.split("failed as tooling: stopping run", 1)[1].split("\n        fi", 1)[0]
    assert 'kill -USR1 "${!opid}"' in stop and "kill -TERM" not in stop
    assert "trap on_pair_stop USR1" in (BENCH_DIR / "run606.sh").read_text()
