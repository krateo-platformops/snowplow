"""#824 (low follow-ups from the #823 gate): the PID guard, the control
viewer's upper-bound label, and the two surviving mutations (the '=' / quote
deny rule; arming no control viewer at all)."""

from __future__ import annotations

import json
import subprocess

import pytest

from bench import load606, logscrub
from tests.test_review823 import _functions
from tests.test_viewer_key_rotation_817 import PATH, _RotatingCell, _Stream

# `kill` / `pkill` are shimmed: these arms never signal anything real.
SHIM = 'kill() { echo "kill $*" >> "$CALLS"; }\npkill() { echo "pkill $*" >> "$CALLS"; }\n'


def _stop(tmp_path, fn, pidvar, value):
    pidfile = tmp_path / "x.pid"
    pidfile.write_text(value)
    calls = tmp_path / "calls"
    script = (SHIM + _functions("safe_pid", "take_pid", fn)
              + f'\nOUT="{tmp_path}"; CALLS="{calls}"; {pidvar}="{pidfile}"\n{fn}\n')
    subprocess.run(["bash", "-c", script], check=True, timeout=10)
    return (calls.read_text().split("\n") if calls.exists() else []), pidfile.exists()


@pytest.mark.parametrize("fn,var", [("stop_top_loop", "TOP_PIDFILE"),
                                    ("stop_log_stream", "LOG_STREAM_PIDFILE")])
@pytest.mark.parametrize("value", ["0", "-1", "1", "", "12ab", " "])
def test_a_pid_that_could_signal_a_group_or_everything_is_never_used(tmp_path, fn, var, value):
    calls, left = _stop(tmp_path, fn, var, value)
    assert [c for c in calls if c] == [], calls
    assert not left                                  # the pidfile is consumed anyway


@pytest.mark.parametrize("fn,var", [("stop_top_loop", "TOP_PIDFILE"),
                                    ("stop_log_stream", "LOG_STREAM_PIDFILE")])
def test_a_valid_pid_is_signalled_once(tmp_path, fn, var):
    calls, left = _stop(tmp_path, fn, var, "4242")
    assert any(c.startswith("kill") and c.endswith("4242") for c in calls) and not left


def test_the_control_count_is_labelled_an_upper_bound():
    rows = [{"kind": "fresh", "label": "list_dep@1", "armed": True, "push_ms": 1.0,
             "arm_gen": 1, "control_push_ms": None}]
    ctl = load606.push_report(rows)["list_dep@1"]["control"]
    assert ctl["no_push_after_rotation"] == 1
    assert ctl["no_push_after_rotation_is"] == "an upper bound"


@pytest.mark.parametrize("msg", ["retry=3 after backoff", 'list "abc" failed'])
def test_a_key_value_or_quoted_fragment_hashes_a_kept_message(msg):
    out = json.loads(logscrub.Scrubber().line(json.dumps({"msg": msg})))
    assert out["msg"].startswith("h:"), out


class _Client:
    base = "http://s"

    def get(self, path, token):
        return load606.Response(200, b"{}", 1.0, refresh=("K-old-0000000000000", "restactions"))


def test_arming_the_probe_arms_its_never_rearming_control():
    _Stream.made = []
    recs = []
    ps, viewer = load606.arm_probe_viewer(_Client(), "t", PATH, recs.append,
                                          stream_factory=_Stream)
    assert viewer.control is not None and viewer.control.role == "control"
    assert len(_Stream.made) == 2                              # the probe and the control
    cell = _RotatingCell(rotate_at=4)
    out = load606.freshness_probe(
        cell, "t", PATH, cell.mutate, samples=2, poll_ms=0, gap_s=0, budget_s=30,
        push_since=ps, viewer=viewer, clock=lambda: cell.clock[0],
        sleep=lambda s: cell.clock.__setitem__(0, cell.clock[0] + s))
    assert all("control_push_ms" in r for r in out)
