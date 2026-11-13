"""#1390 item 3: run 26e's VM was made with a 15 h TTL chosen by hand, and nothing checked it covered the arms plus
the statistics. `vm606 launch` now refuses when the VM is deleted before the arms' worst case (the runs' TTL) plus
a statistics budget MEASURED from recorded statistics steps (every statistics.jsonl on the laptop, and the
committed measurement) and scaled by this launch's input (its planned k6 seconds, its pairs), plus the launch
margin. No fixed knob: a slower recorded step, a longer planned run or more pairs each raise the budget."""

from __future__ import annotations

import json

import pytest

from bench import evidence, statstime
from tests.test_vm606 import (_env_file, _launch_args, _pin, _repo, _run, _state, _vm_ready, fake)  # noqa: F401

REC = {"kind": "statistics_step", "step": "replica N1:N1B", "command": "replica", "exit": 0, "seconds": 176.0,
       "input_bytes": 1033643132, "k6_seconds": 32341.2}


def test_the_budget_is_the_slowest_rate_times_the_planned_input():
    secs, how = statstime.budget([REC], k6_seconds_per_arm=16170, steps=2)
    assert secs == pytest.approx(2 * (2 * 16170 * (1033643132 / 32341.2)) * (176.0 / 1033643132))
    assert "2 step(s)" in how and "the slowest of 1 recorded step(s)" in how
    slow = dict(REC, seconds=7200.0)
    assert statstime.budget([REC, slow], k6_seconds_per_arm=16170, steps=2)[0] == pytest.approx(secs * 7200 / 176)
    assert statstime.budget([REC], k6_seconds_per_arm=32340, steps=2)[0] == pytest.approx(secs * 2)    # input
    assert statstime.budget([REC], k6_seconds_per_arm=16170, steps=3)[0] == pytest.approx(secs * 1.5)  # pairs


def test_no_measurement_is_no_budget():
    assert statstime.budget([], k6_seconds_per_arm=1, steps=1)[0] is None
    nothing_read = dict(REC, input_bytes=0)
    assert statstime.budget([nothing_read], k6_seconds_per_arm=1, steps=1)[0] is None
    failed = dict(REC, exit=1)
    assert statstime.load_records([]) == [] and failed not in statstime.load_records([statstime.MEASURED])


def test_the_committed_measurement_is_run_25cs_replica():
    recs = statstime.load_records([statstime.MEASURED])
    assert len(recs) == 1 and recs[0]["input_bytes"] == REC["input_bytes"] and recs[0]["seconds"] == REC["seconds"]
    assert "run 25c" in recs[0]["provenance"]


def _launch(fake, capsys, tmp_path, **env):  # noqa: F811
    repo, sha, _tag = _repo(tmp_path)
    _vm_ready(fake, hours=15.0)
    pin, pin_sha = _pin(tmp_path)
    return _run(capsys, *_launch_args(tmp_path, repo, sha, pin, pin_sha, _env_file(tmp_path, **env)))


def test_launch_records_the_budget_it_checked(fake, capsys, tmp_path):  # noqa: F811
    rc, out, err = _launch(fake, capsys, tmp_path)
    assert rc == 0, err + out
    st = _state()
    assert 150 < st["statistics_budget_s"] < 200                     # run 25c rate, 16170 k6-seconds, 1 pair
    assert "statistics budget" in st["statistics_budget"]


def test_a_slower_recorded_step_refuses_a_vm_too_short_for_it(fake, capsys, tmp_path):  # noqa: F811
    """A past launch's replica took 2 h on the same input: 13 h of arms + its 2 h + the 1 h margin exceed the
    VM's 15 h."""
    past = evidence.runs_root() / "606" / "pair-past" / "statistics.jsonl"
    past.parent.mkdir(parents=True)
    past.write_text(json.dumps(dict(REC, seconds=7200.0)) + "\n")
    rc, out, err = _launch(fake, capsys, tmp_path)
    assert rc == 2 and "the run's TTL is 13 h, its statistics budget" in err and "--ttl-hours >= 16" in err, err
    assert "launched on" not in out


def test_a_longer_planned_run_refuses_on_the_same_measurement(fake, capsys, tmp_path):  # noqa: F811
    rc, out, err = _launch(fake, capsys, tmp_path, STUB_DRIVE_S="600000")
    assert rc == 2 and "statistics budget" in err and "600000 k6-seconds" in err, err


def test_a_dry_pass_without_planned_k6_seconds_is_an_unknown_budget(fake, capsys, tmp_path):  # noqa: F811
    rc, out, err = _launch(fake, capsys, tmp_path, STUB_DRIVE_S="none", STUB_DRIVE_S2="none")
    assert rc == 2 and "the statistics budget is unknown" in err, err
