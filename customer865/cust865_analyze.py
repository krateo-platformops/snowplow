"""#865 customer arm on the real refresher: per rate, rounds of (main, branch, main2).

Acceptance (team-lead): customer p95 and req/s no worse than main, multiplicatively
within the main/main spread; dirty_to_fresh no worse.

  branch/main (per round) = branch / mean(main, main2); reported as geo mean and range.
  main/main spread        = main2/main per round, made SYMMETRIC: [1/max(r,1/r), max(r,1/r)]
                            over rounds, i.e. the largest multiplicative gap two main
                            legs of one round showed, in either direction.
  verdict for "lower is better": OK if geo(branch/main) <= the spread's upper end;
          for "higher is better": OK if geo(branch/main) >= the spread's lower end.
"""
import glob
import json
import math
import os
import re
import sys

OUT = sys.argv[1]


def load(p):
    return json.load(open(p)) if os.path.exists(p) else None


def geo(xs):
    xs = [x for x in xs if x and x > 0 and math.isfinite(x)]
    return math.exp(sum(math.log(x) for x in xs) / len(xs)) if xs else float('nan')


METRICS = [
    ('dependents customer p50 ms', lambda d: d['cust_dep'].get('p50_ms'), 'lower'),
    ('dependents customer p95 ms', lambda d: d['cust_dep'].get('p95_ms'), 'lower'),
    ('dependents customer p99 ms', lambda d: d['cust_dep'].get('p99_ms'), 'lower'),
    ('dependents customer req/s', lambda d: d['cust_dep_rps'], 'higher'),
    ('unrelated customer p50 ms', lambda d: d['cust_unrel'].get('p50_ms'), 'lower'),
    ('unrelated customer p95 ms', lambda d: d['cust_unrel'].get('p95_ms'), 'lower'),
    ('unrelated customer p99 ms', lambda d: d['cust_unrel'].get('p99_ms'), 'lower'),
    ('unrelated customer req/s', lambda d: d['cust_unrel_rps'], 'higher'),
    ('dependents staleness seen p50 ms', lambda d: d['cust_dep_staleness'].get('p50_ms'), 'lower'),
    ('dependents staleness seen p95 ms', lambda d: d['cust_dep_staleness'].get('p95_ms'), 'lower'),
    ('dependents staleness seen p99 ms', lambda d: d['cust_dep_staleness'].get('p99_ms'), 'lower'),
    ('refresher dirty_to_fresh p50 ms', lambda d: d['dirty_to_fresh'].get('p50_ms'), 'lower'),
    ('refresher dirty_to_fresh p95 ms', lambda d: d['dirty_to_fresh'].get('p95_ms'), 'lower'),
    ('refresher dirty_to_fresh samples', lambda d: d['dirty_to_fresh'].get('n'), 'info'),
    ('refresher completions/s', lambda d: d['completions_s'], 'info'),
    ('process CPU (cores)', lambda d: d['cpu_cores'], 'info'),
    ('peak HeapInuse GB', lambda d: d['peak_heap_inuse'] / 1e9, 'info'),
    ('content fills', lambda d: d['fills'], 'info'),
    ('refresher yields', lambda d: d['refresh_yielded'], 'info'),
    ('refresher 5 s cap hits', lambda d: d['refresh_capped'], 'info'),
    ('refresher parked s', lambda d: d['refresh_parked_s'], 'info'),
]

rates = sorted({int(re.match(r'r(\d+)-', os.path.basename(p)).group(1)) for p in glob.glob(os.path.join(OUT, 'r*-*-main.json'))})
for rate in rates:
    rounds = sorted({int(re.match(r'r\d+-(\d+)-', os.path.basename(p)).group(1)) for p in glob.glob(os.path.join(OUT, 'r%d-*-main.json' % rate))})
    legs = []
    for r in rounds:
        m, b, m2 = (load(os.path.join(OUT, 'r%d-%d-%s.json' % (rate, r, leg))) for leg in ('main', 'branch', 'main2'))
        if m and b and m2:
            legs.append((m, b, m2))
    if not legs:
        continue
    b0 = legs[0][1]
    print('== rate %d events/s, GOMAXPROCS %s, %d rounds (main, branch, main2); customers: %d on dependents + %d on an unrelated RESTAction, think %d ms' % (
        rate, b0.get('gomaxprocs'), len(legs), b0['cust_dep_n'], b0['cust_unrel_n'], b0['cust_think_ms']))
    print('  %-34s %-44s %-44s %-44s %-26s %-16s %s' % ('metric', 'main', 'branch', 'main2', 'branch/main geo (range)', 'main/main spread', 'verdict'))
    for name, f, better in METRICS:
        mv = [f(m) for m, _, _ in legs]
        bv = [f(b) for _, b, _ in legs]
        m2v = [f(m2) for _, _, m2 in legs]
        bm = [b / ((m + m2) / 2) for m, b, m2 in zip(mv, bv, m2v) if m and m2 and b is not None and (m + m2) > 0]
        mm = [m2 / m for m, m2 in zip(mv, m2v) if m and m2]
        spread = max([max(r, 1 / r) for r in mm]) if mm else float('nan')
        g = geo(bm)
        if better == 'lower':
            verdict = 'OK' if g <= spread else 'WORSE'
        elif better == 'higher':
            verdict = 'OK' if g >= 1 / spread else 'WORSE'
        else:
            verdict = ''
        fmt = lambda xs: '[' + ', '.join(('%.2f' % x) if isinstance(x, float) else str(x) for x in xs) + ']'
        rng = '%.2f (%.2f-%.2f)' % (g, min(bm), max(bm)) if bm else 'n/a'
        sp = '%.2f-%.2f' % (1 / spread, spread) if mm else 'n/a'
        print('  %-34s %-44s %-44s %-44s %-26s %-16s %s' % (name, fmt(mv), fmt(bv), fmt(m2v), rng, sp, verdict))
    li = [b.get('list_incremental') or {} for _, b, _ in legs]
    print('  branch: divergence_total=%s unanchors=%s passes=%s refresh_skipped=%s; customer errors (main, branch, main2)=%s' % (
        [x.get('DivergenceTotal') for x in li], [x.get('Unanchors') for x in li], [x.get('Passes') for x in li],
        [x.get('RefreshSkipped') for x in li], [(m['cust_errors'], b['cust_errors'], m2['cust_errors']) for m, b, m2 in legs]))
