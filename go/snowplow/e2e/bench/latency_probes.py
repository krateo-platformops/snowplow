#!/usr/bin/env python3
"""Latency probes at t+5/t+15/t+25 min during the steady-state window.

Hits /call (compositions-list RestAction) for admin + cj. Each probe is
n=5 burst; reports per-call ms + p50/p95/mean + parsed items count.

Output:
    <out-dir>/latency-probes.json
    <out-dir>/latency.log

Usage:
    cd e2e/bench
    python3 latency_probes.py --out-dir ~/some-run-dir
        # default --out-dir = {runs_root}/phase6-latency-probes (bench/evidence.py)

Task #184: this file used to live only at `/tmp/phase6-0.30.245-50K/
latency_probes.py`, which referenced `bench.browser._http_get` — a
symbol that has never existed (the public helper is `browser.http_get`,
no leading underscore). Every invocation tripped AttributeError on the
first probe call. The script is now committed under `e2e/bench/` so it
survives `/tmp` clears + uses the canonical public symbol.

Bench-restructure design uses `browser.http_get` everywhere (see
bench/browser.py:283 + tests/test_browser.py:395). The underscore-
prefixed symbol `_http_get` only exists in `bench/storm.py` as a
local wrapper for retry semantics — it is NOT a module-level alias.
"""
from __future__ import annotations

import argparse
import json
import statistics
import sys
import time
from pathlib import Path

# Bench is importable when this script is run from `e2e/bench/`.
SCRIPT_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(SCRIPT_DIR))
from bench import browser, evidence  # type: ignore


# 3 probes inside a typical 30-min observation window.
PROBE_TIMES_MIN = [5, 15, 25]


def probe(user: str, token: str, label: str, n: int = 5) -> dict:
    """Hit compositions-list via the portal LB; capture per-call ms."""
    path = ("/call?apiVersion=templates.krateo.io%2Fv1"
            "&resource=restactions&name=compositions-list&namespace=krateo-system")
    samples: list[int] = []
    items_seen: int | None = None
    for _ in range(n):
        # Task #184 fix: browser.http_get (no underscore). The historic
        # _http_get reference never resolved.
        ms, code, body = browser.http_get(path, token)
        samples.append(ms)
        if items_seen is None and body and code == 200:
            try:
                if isinstance(body, bytes):
                    body = body.decode("utf-8", errors="ignore")
                doc = json.loads(body)
                items = doc.get("items") if isinstance(doc, dict) else None
                if isinstance(items, list):
                    items_seen = len(items)
            except Exception:
                pass
    samples_sorted = sorted(samples)
    p95_idx = max(0, int(0.95 * (n - 1))) if n > 1 else 0
    return {
        "user": user,
        "label": label,
        "n": n,
        "samples_ms": samples,
        "p50": int(statistics.median(samples)),
        "p95": samples_sorted[p95_idx],
        "mean": int(statistics.mean(samples)),
        "items_seen": items_seen,
    }


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Latency probes during the steady-state observation window"
    )
    parser.add_argument(
        "--out-dir",
        default=str(evidence.runs_root() / "phase6-latency-probes"),
        help="output directory (default: %(default)s)",
    )
    parser.add_argument(
        "--probe-minutes",
        type=int,
        nargs="+",
        default=PROBE_TIMES_MIN,
        help="probe trigger times in minutes from start (default: 5 15 25)",
    )
    parser.add_argument(
        "--n",
        type=int,
        default=5,
        help="samples per probe burst (default: 5)",
    )
    args = parser.parse_args()

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    out = out_dir / "latency-probes.json"
    log_path = out_dir / "latency.log"

    log = open(log_path, "a")
    log.write(f"\n=== latency_probes started at "
              f"{time.strftime('%Y-%m-%d %H:%M:%S')} "
              f"probe_minutes={args.probe_minutes} n={args.n} ===\n")
    log.flush()

    try:
        tokens = browser.login_all()
    except Exception as e:
        log.write(f"login_all failed: {e}\n")
        log.close()
        return 1
    if not tokens or "admin" not in tokens or "cyberjoker" not in tokens:
        log.write(f"missing tokens: "
                  f"{list(tokens.keys()) if tokens else []}\n")
        log.close()
        return 1

    start_ts = time.time()
    results: list[dict] = []

    for probe_min in args.probe_minutes:
        target_ts = start_ts + probe_min * 60
        sleep_s = max(0, target_ts - time.time())
        if sleep_s > 0:
            time.sleep(sleep_s)

        for user, token_key in (("admin", "admin"), ("cyberjoker", "cyberjoker")):
            r = probe(user, tokens[token_key],
                      f"t+{probe_min}min", n=args.n)
            log.write(
                f"t+{probe_min}min {user:11s} n={r['n']} items={r['items_seen']} "
                f"p50={r['p50']}ms p95={r['p95']}ms mean={r['mean']}ms "
                f"samples={r['samples_ms']}\n"
            )
            log.flush()
            results.append(r)

    with open(out, "w") as f:
        json.dump({
            "started_at": int(start_ts),
            "probe_minutes": args.probe_minutes,
            "samples_per_probe": args.n,
            "probes": results,
        }, f, indent=2, default=str)
    log.write(f"=== latency_probes done. {len(results)} probes saved to {out} ===\n")
    log.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
