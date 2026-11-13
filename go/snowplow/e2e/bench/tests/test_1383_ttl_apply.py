"""#1383 gap 4: a TTL CronJob apply that failed only warned, and the run went on without the one teardown layer
that survives the operator's machine. gke-up now fails, so run606's EXIT trap tears the cluster down."""

from __future__ import annotations

import json
import subprocess

from bench import gke
from tests.test_gke_pvc_disks import BENCH, NAME, _calls, fakes  # noqa: F401  (fixture)
from tests.test_run606_real_sequence import harness  # noqa: F401  (fixture)


def test_a_failed_cronjob_apply_fails_gke_up(fakes, monkeypatch, capsys):
    monkeypatch.setenv("FAKE_TTL_APPLY_RC", "1")
    rc = gke.main(["gke-up", "--name", NAME, "--address-need", "1", "--yes"])
    err = capsys.readouterr().err
    assert rc == gke.EXIT_TTL_BACKSTOP_MISSING != 0
    assert "self-destruct CronJob NOT applied (rc=1): a run without its TTL backstop does not go on" in err
    assert not any(x[1:3] == ["get", "job"] for x in _calls(fakes))           # nothing after it


def test_run606_tears_down_when_the_cronjob_apply_fails(harness):  # noqa: F811
    env, state, _, tmp = harness
    p = subprocess.run(["bash", str(BENCH / "run606.sh")], env=dict(env, FAKE_TTL_APPLY_FAIL="1"),
                       capture_output=True, text=True, timeout=600)
    log = (tmp / "out" / "run.log").read_text()
    assert p.returncode != 0 and f"up FAILED: gke-up exited {gke.EXIT_TTL_BACKSTOP_MISSING}" in log, log[-3000:]
    gcl = [json.loads(x)["argv"] for x in (state / "calls.jsonl").read_text().splitlines()
           if json.loads(x)["tool"] == "gcloud"]
    assert any(a[:4] == ["container", "clusters", "delete", env["RUN_NAME"]] for a in gcl)
