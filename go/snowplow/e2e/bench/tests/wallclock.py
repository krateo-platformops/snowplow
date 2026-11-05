"""A test that bounds real elapsed time runs on its own, never beside parallel workers (#1166).

`assert time.monotonic() - t0 < 35` measures the host as well as the code. Beside seven other
pytest-xdist workers, each starting run606.sh and its fakes, the same code reads slower, and the
bound fails while nothing in the test changed. Parallel run 2 on 84e482db:
test_run9_safety::test_every_collect_read_is_bounded_twice took 39.8 s against its 35 s bound
(30.7 s serial).

Such a test carries an EXPLICIT `@pytest.mark.serial`, with a one-line reason, reviewable at the
test. The suite runs in two passes:

    python3 -m pytest -n auto -m "not docker and not serial" -q -p no:cacheprovider
    python3 -m pytest -m "serial and not docker" -q -p no:cacheprovider

This plugin (loaded from pytest.ini) is the GUARD, not the source of truth: collection FAILS when
a test's own source asserts a bound on the wall clock and the test is not marked serial. A wall-
clock reading is `time.time()`, `time.monotonic()` or `time.perf_counter()`, under any alias the
module or the function imports (`import time as _t`, `from time import monotonic`), or
`datetime.now()` / `datetime.utcnow()` likewise (`.total_seconds()` and `.timestamp()` read through).
A bound is an
assert that compares such a reading from above: `clock - t0 < X`, `X > clock - t0`, chained as in
`0 <= clock - t0 < X`, through a name assigned from such a difference (`elapsed < X`), or as a
deadline (`clock < deadline`). Some load-sensitive tests do not show their clock in their source
(they bound stamps a driver records, or a rate after a real sleep); those are marked by hand and
the guard cannot find them. #1174 moves all of them to virtual time, the real fix.
"""

from __future__ import annotations

import ast
import inspect
import textwrap

import pytest

_CLOCKS = {"time", "monotonic", "perf_counter"}
_DATETIME_CLOCKS = {"now", "utcnow"}


def _read_through(n: ast.AST) -> ast.AST:
    """`x.total_seconds()` / `x.timestamp()` is x, for what it measures."""
    if (isinstance(n, ast.Call) and not n.args and isinstance(n.func, ast.Attribute)
            and n.func.attr in ("total_seconds", "timestamp")):
        return n.func.value
    return n


class _Names:
    """The names that read the wall clock in one module + function: aliases of the time and
    datetime modules, clock functions imported by name, and the datetime class's names."""

    def __init__(self):
        self.modules = {"time"}
        self.funcs: set[str] = set()
        self.dt_modules: set[str] = set()
        self.dt_classes: set[str] = set()

    def learn(self, tree: ast.AST) -> None:
        for n in ast.walk(tree):
            if isinstance(n, ast.Import):
                for a in n.names:
                    if a.name == "time":
                        self.modules.add(a.asname or "time")
                    elif a.name == "datetime":
                        self.dt_modules.add(a.asname or "datetime")
            elif isinstance(n, ast.ImportFrom) and n.module == "time":
                for a in n.names:
                    if a.name in _CLOCKS:
                        self.funcs.add(a.asname or a.name)
            elif isinstance(n, ast.ImportFrom) and n.module == "datetime":
                for a in n.names:
                    if a.name == "datetime":
                        self.dt_classes.add(a.asname or a.name)

    def is_clock(self, n: ast.AST) -> bool:
        n = _read_through(n)
        if not isinstance(n, ast.Call):
            return False
        f = n.func
        if isinstance(f, ast.Attribute) and f.attr in _CLOCKS and isinstance(f.value, ast.Name):
            return f.value.id in self.modules
        if isinstance(f, ast.Attribute) and f.attr in _DATETIME_CLOCKS:
            v = f.value
            if isinstance(v, ast.Name):                         # datetime.now()
                return v.id in self.dt_classes
            return (isinstance(v, ast.Attribute) and v.attr == "datetime"     # datetime.datetime.now()
                    and isinstance(v.value, ast.Name) and v.value.id in self.dt_modules)
        return isinstance(f, ast.Name) and f.id in self.funcs

    def is_difference(self, n: ast.AST) -> bool:
        """`clock - stamp`, an elapsed time: the stamp a name, attribute, item or clock reading
        (not `clock - (MAX_AGE + 60)`, which is a moment in the past, not a duration)."""
        n = _read_through(n)
        return (isinstance(n, ast.BinOp) and isinstance(n.op, ast.Sub) and self.is_clock(n.left)
                and (isinstance(n.right, (ast.Name, ast.Attribute, ast.Subscript)) or self.is_clock(n.right)))


def _bounds_from_above(names: _Names, elapsed: set[str], c: ast.Compare) -> bool:
    """Any link `a OP b` of a (possibly chained) comparison that puts a wall-clock reading or
    difference on the small side of an upper bound."""
    operands = [c.left, *c.comparators]
    for op, a, b in zip(c.ops, operands, operands[1:]):
        if isinstance(op, (ast.Lt, ast.LtE)):
            small = a
        elif isinstance(op, (ast.Gt, ast.GtE)):
            small = b
        else:
            continue
        small = _read_through(small)
        if names.is_difference(small) or names.is_clock(small):
            return True
        if isinstance(small, ast.Name) and small.id in elapsed:
            return True
    return False


_module_names: dict[str, _Names] = {}


def _names_for(fn) -> _Names:
    path = inspect.getsourcefile(fn) or ""
    if path not in _module_names:
        names = _Names()
        try:
            names.learn(ast.parse(open(path).read()))
        except (OSError, SyntaxError):
            pass
        _module_names[path] = names
    return _module_names[path]


def bounds_wall_clock(fn) -> bool:
    """True iff fn's own source asserts an upper bound on the wall clock."""
    try:
        tree = ast.parse(textwrap.dedent(inspect.getsource(fn)))
    except (OSError, TypeError, SyntaxError):
        return False
    module = _names_for(fn)
    names = _Names()
    names.modules |= module.modules
    names.funcs |= module.funcs
    names.dt_modules |= module.dt_modules
    names.dt_classes |= module.dt_classes
    names.learn(tree)                                   # an import inside the function
    elapsed = {t.id for a in ast.walk(tree) if isinstance(a, ast.Assign) and names.is_difference(a.value)
               for t in a.targets if isinstance(t, ast.Name)}
    for node in ast.walk(tree):
        if isinstance(node, ast.Assert):
            for c in ast.walk(node.test):
                if isinstance(c, ast.Compare) and _bounds_from_above(names, elapsed, c):
                    return True
    return False


def pytest_configure(config):
    config.addinivalue_line(
        "markers", "serial: bounds real elapsed time, so it runs on its own (-m serial), never beside "
                   "parallel workers; carried explicitly, checked by tests/wallclock.py (#1166, #1174)")


def pytest_collection_modifyitems(config, items):
    seen: dict = {}
    unmarked = []
    for item in items:
        fn = getattr(item, "function", None)
        if fn is None:
            continue
        if fn not in seen:
            seen[fn] = bounds_wall_clock(fn)
        if seen[fn] and item.get_closest_marker("serial") is None:
            unmarked.append(item.nodeid)
    if unmarked:
        raise pytest.UsageError(
            "#1166: these tests assert a bound on the wall clock but are not marked serial, so they "
            "would run beside the parallel workers and measure the host. Mark each one "
            "`@pytest.mark.serial` with a one-line reason (and add it to #1174):\n  "
            + "\n  ".join(sorted(set(unmarked))))
