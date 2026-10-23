"""#854 customer arm: per rate, main / branch / main2 per round.

Acceptance (team-lead): customer p95 and req/s no worse than main,
multiplicatively within the main/main spread; dirty_to_fresh no worse.
The main/main spread for a metric is the range of main2/main over rounds;
branch/main is compared against it (the mean of the two main legs is main).
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
    ('heavy p50 ms', lambda d: d['cust_heavy'].get('p50_ms'), 'lower'),
    ('heavy p95 ms', lambda d: d['cust_heavy'].get('p95_ms'), 'lower'),
    ('heavy p99 ms', lambda d: d['cust_heavy'].get('p99_ms'), 'lower'),
    ('heavy req/s', lambda d: d['cust_heavy_rps'], 'higher'),
    ('light p50 ms', lambda d: d['cust_light'].get('p50_ms'), 'lower'),
    ('light p95 ms', lambda d: d['cust_light'].get('p95_ms'), 'lower'),
    ('light p99 ms', lambda d: d['cust_light'].get('p99_ms'), 'lower'),
    ('light req/s', lambda d: d['cust_light_rps'], 'higher'),
    ('dirty_to_fresh p50 ms', lambda d: d['dirty_to_fresh'].get('p50_ms'), 'lower'),
    ('dirty_to_fresh p95 ms', lambda d: d['dirty_to_fresh'].get('p95_ms'), 'lower'),
    ('dependent completions/s', lambda d: d['completions_s'], 'higher'),
    ('process CPU (cores)', lambda d: d['cpu_cores'], 'info'),
    ('content fills', lambda d: d['fills'], 'info'),
]

rates = sorted({int(re.match(r'r(\d+)-', os.path.basename(p)).group(1)) for p in glob.glob(os.path.join(OUT, 'r*-*-main.json'))})
for rate in rates:
    rounds = sorted({int(re.match(r'r\d+-(\d+)-', os.path.basename(p)).group(1)) for p in glob.glob(os.path.join(OUT, 'r%d-*-main.json' % rate))})
    legs = []
    for r in rounds:
        m, b, m2 = (load(os.path.join(OUT, 'r%d-%d-%s.json' % (rate, r, leg))) for leg in ('main', 'branch', 'main2'))
        if m and b and m2:
            legs.append((m, b, m2))
    print('== rate %d events/s, GOMAXPROCS %s, %d rounds (main, branch, main2)' % (rate, legs[0][1].get('gomaxprocs'), len(legs)))
    print('  %-26s %-28s %-28s %-28s %-22s %-22s %s' % ('metric', 'main', 'branch', 'main2', 'branch/main (geo)', 'main2/main range', 'verdict'))
    for name, f, better in METRICS:
        mv = [f(m) for m, _, _ in legs]
        bv = [f(b) for _, b, _ in legs]
        m2v = [f(m2) for _, _, m2 in legs]
        bm = [b / ((m + m2) / 2) for m, b, m2 in zip(mv, bv, m2v) if m and m2 and b is not None]
        mm = [m2 / m for m, m2 in zip(mv, m2v) if m and m2]
        g = geo(bm)
        lo, hi = (min(mm), max(mm)) if mm else (float('nan'), float('nan'))
        if better == 'lower':
            verdict = 'OK' if g <= max(hi, 1.0) else 'WORSE'
        elif better == 'higher':
            verdict = 'OK' if g >= min(lo, 1.0) else 'WORSE'
        else:
            verdict = ''
        fmt = lambda xs: '[' + ', '.join('%.1f' % x if isinstance(x, float) else str(x) for x in xs) + ']'
        print('  %-26s %-28s %-28s %-28s %-22s %-22s %s' % (name, fmt(mv), fmt(bv), fmt(m2v), '%.2f' % g, '%.2f-%.2f' % (lo, hi), verdict))
    li = [b.get('list_incremental') or {} for _, b, _ in legs]
    print('  branch: divergence_total=%s generations=%s items_built=%s reads=%s cust_errors main/branch/main2=%s' % (
        [x.get('DivergenceTotal') for x in li], [x.get('Generations') for x in li], [x.get('ItemsBuilt') for x in li],
        [x.get('Reads') for x in li], [(m['cust_errors'], b['cust_errors'], m2['cust_errors']) for m, b, m2 in legs]))
