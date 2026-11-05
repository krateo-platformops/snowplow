"""A test that bounds real elapsed time runs on its own, never beside parallel workers (#1166).

`assert time.monotonic() - t0 < 35` measures the host as well as the code: beside seven other
pytest-xdist workers, each starting run606.sh and its fakes, the same code reads slower, and
the bound fails while nothing in the test changed (parallel run 2 on 84e482db:
test_run9_safety::test_every_collect_read_is_bounded_twice took 39.8 s against its 35 s bound;
30.7 s serial). Such a test is marked `serial`, found from its own source: an `assert` whose
comparison bounds a wall-clock difference (time.time / time.monotonic / time.perf_counter minus
a stamp, directly or through a name assigned from one). The suite runs in two passes:

    python3 -m pytest -n auto -m "not docker and not serial" -q -p no:cacheprovider
    python3 -m pytest -m "serial and not docker" -q -p no:cacheprovider

Loaded as a plugin (pytest.ini), so the mark exists before `-m` selects, in every process.
"""

from __future__ import annotations

import ast
import inspect
import textwrap

import pytest

_CLOCKS = {"time", "monotonic", "perf_counter"}


def _is_clock_call(n: ast.AST) -> bool:
    return (isinstance(n, ast.Call) and isinstance(n.func, ast.Attribute) and n.func.attr in _CLOCKS
            and isinstance(n.func.value, ast.Name) and n.func.value.id == "time")


def _is_clock_difference(n: ast.AST) -> bool:
    return isinstance(n, ast.BinOp) and isinstance(n.op, ast.Sub) and _is_clock_call(n.left)


def bounds_wall_clock(fn) -> bool:
    """True iff fn's own source asserts an upper bound on a wall-clock difference."""
    try:
        tree = ast.parse(textwrap.dedent(inspect.getsource(fn)))
    except (OSError, TypeError, SyntaxError):
        return False
    elapsed = {t.id for a in ast.walk(tree) if isinstance(a, ast.Assign) and _is_clock_difference(a.value)
               for t in a.targets if isinstance(t, ast.Name)}
    for node in ast.walk(tree):
        if not isinstance(node, ast.Assert):
            continue
        for c in ast.walk(node.test):
            if not isinstance(c, ast.Compare) or not any(isinstance(o, (ast.Lt, ast.LtE)) for o in c.ops):
                continue
            if _is_clock_difference(c.left) or (isinstance(c.left, ast.Name) and c.left.id in elapsed):
                return True
    return False


def pytest_configure(config):
    config.addinivalue_line(
        "markers", "serial: asserts a bound on real elapsed time, so it runs on its own (-m serial), "
                   "never beside parallel workers (set from the test's source, tests/wallclock.py)")


@pytest.hookimpl(tryfirst=True)            # before -m selects
def pytest_collection_modifyitems(config, items):
    seen: dict = {}
    for item in items:
        fn = getattr(item, "function", None)
        if fn is None:
            continue
        if fn not in seen:
            seen[fn] = bounds_wall_clock(fn)
        if seen[fn]:
            item.add_marker(pytest.mark.serial)
