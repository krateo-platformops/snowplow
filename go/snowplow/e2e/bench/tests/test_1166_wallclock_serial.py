"""#1166: a test that bounds real elapsed time is marked `serial` from its own source, so the
parallel pass (-m "not serial") leaves it to the serial pass (-m serial). tests/wallclock.py."""

from __future__ import annotations

import os
import subprocess
import sys
import textwrap
import time
from pathlib import Path

from tests.wallclock import bounds_wall_clock

BENCH_DIR = Path(__file__).resolve().parent.parent


def _direct():
    t0 = time.monotonic()
    assert time.monotonic() - t0 < 35


def _through_a_name():
    t0 = time.time()
    elapsed = time.time() - t0
    assert elapsed < 5, elapsed


def _in_a_conjunction():
    t0 = time.perf_counter()
    rc = 1
    assert rc != 0 and time.perf_counter() - t0 < 30


def _a_lower_bound_only():
    t0 = time.monotonic()
    assert time.monotonic() - t0 >= 0         # a floor is not a load-sensitive bound


def _no_clock():
    n = 3
    assert n - 1 < 5


def _a_clock_never_asserted():
    t0 = time.monotonic()
    while time.monotonic() - t0 < 0.01:       # a wait loop, not an assertion
        pass


def test_a_wall_clock_bound_is_recognised_in_each_of_its_shapes():
    assert bounds_wall_clock(_direct)
    assert bounds_wall_clock(_through_a_name)
    assert bounds_wall_clock(_in_a_conjunction)


def test_what_does_not_bound_elapsed_time_is_not_serial():
    assert not bounds_wall_clock(_a_lower_bound_only)
    assert not bounds_wall_clock(_no_clock)
    assert not bounds_wall_clock(_a_clock_never_asserted)


def test_the_two_passes_split_the_suite_exactly(tmp_path):
    """-m serial and -m "not serial" select complementary sets: nothing runs twice, nothing is lost."""
    (tmp_path / "test_inner.py").write_text(textwrap.dedent('''
        import time

        def test_bounded():
            t0 = time.monotonic()
            assert time.monotonic() - t0 < 30

        def test_plain():
            assert True
    '''))
    env = {k: v for k, v in os.environ.items() if not k.startswith("PYTEST_")}
    env["PYTHONPATH"] = str(BENCH_DIR)

    def run(sel):
        p = subprocess.run([sys.executable, "-m", "pytest", "-p", "tests.wallclock", "-p", "no:cacheprovider",
                            "-q", "-rA", "-m", sel, "test_inner.py"],
                           cwd=tmp_path, env=env, capture_output=True, text=True, timeout=120)
        assert p.returncode == 0, p.stdout + p.stderr
        return {ln.split("::")[1] for ln in p.stdout.splitlines() if ln.startswith("PASSED ")}

    assert run("serial") == {"test_bounded"}
    assert run("not serial") == {"test_plain"}
