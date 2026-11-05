"""#1166: a test that bounds real elapsed time carries an explicit `serial` mark, and
tests/wallclock.py fails collection when one does not. The parallel pass (-m "not serial")
leaves those tests to the serial pass (-m serial)."""

from __future__ import annotations

import os
import subprocess
import sys
import textwrap
import time
import time as _t
from pathlib import Path
from time import monotonic

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


def _a_module_alias():
    t0 = _t.monotonic()
    assert _t.monotonic() - t0 < 15


def _an_alias_in_the_function():
    import time as _clock
    t0 = _clock.monotonic()
    assert _clock.monotonic() - t0 < 15


def _a_function_imported_by_name():
    t0 = monotonic()
    assert monotonic() - t0 < 5


def _reversed():
    t0 = time.monotonic()
    assert 15 > time.monotonic() - t0


def _chained():
    t0 = time.monotonic()
    assert 0 <= time.monotonic() - t0 < 15


def _a_deadline():
    deadline = time.monotonic() + 10
    assert time.monotonic() < deadline


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


def _a_moment_in_the_past_compared():
    stale_at = time.monotonic() - (3600 + 60)  # a moment, not a duration
    refreshed = time.monotonic()
    assert refreshed > stale_at


def test_a_wall_clock_bound_is_recognised_in_each_of_its_shapes():
    for f in (_direct, _through_a_name, _in_a_conjunction, _a_module_alias, _an_alias_in_the_function,
              _a_function_imported_by_name, _reversed, _chained, _a_deadline):
        assert bounds_wall_clock(f), f.__name__


def test_what_does_not_bound_elapsed_time_is_not_flagged():
    for f in (_a_lower_bound_only, _no_clock, _a_clock_never_asserted, _a_moment_in_the_past_compared):
        assert not bounds_wall_clock(f), f.__name__


def _inner(tmp_path, body, *args):
    (tmp_path / "test_inner.py").write_text(textwrap.dedent(body))
    env = {k: v for k, v in os.environ.items() if not k.startswith("PYTEST_")}
    env["PYTHONPATH"] = str(BENCH_DIR)
    return subprocess.run([sys.executable, "-m", "pytest", "-p", "tests.wallclock", "-p", "no:cacheprovider",
                           "-q", "-rA", *args, "test_inner.py"],
                          cwd=tmp_path, env=env, capture_output=True, text=True, timeout=120)


def test_an_unmarked_wall_clock_bound_fails_collection(tmp_path):
    """The guard: `_t.monotonic() - t0 < 15` with no serial mark is a usage error naming the test,
    and nothing runs."""
    p = _inner(tmp_path, '''
        import time as _t

        def test_bounded():
            t0 = _t.monotonic()
            assert _t.monotonic() - t0 < 15

        def test_plain():
            assert True
    ''')
    assert p.returncode == 4, p.stdout + p.stderr                      # pytest.ExitCode.USAGE_ERROR
    assert "not marked serial" in p.stderr and "test_inner.py::test_bounded" in p.stderr, p.stderr
    assert "test_plain" not in p.stderr and "passed" not in p.stdout, p.stdout + p.stderr


def test_the_marked_bound_runs_in_the_serial_pass_only(tmp_path):
    """Marked, it collects; -m serial and -m "not serial" split the module exactly."""
    body = '''
        import time as _t
        import pytest

        @pytest.mark.serial  # bounds the wall clock
        def test_bounded():
            t0 = _t.monotonic()
            assert _t.monotonic() - t0 < 15

        def test_plain():
            assert True
    '''

    def passed(sel):
        p = _inner(tmp_path, body, "-m", sel)
        assert p.returncode == 0, p.stdout + p.stderr
        return {ln.split("::")[1] for ln in p.stdout.splitlines() if ln.startswith("PASSED ")}

    assert passed("serial") == {"test_bounded"}
    assert passed("not serial") == {"test_plain"}
