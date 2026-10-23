"""#865 control: is the branch's customer p50 offset the request path or the process?

  hit_bench  sequential L1-hit GETs right after warm-up (no event, no other customer):
             the request path alone, same conditions on both trees.
  probe      a fixed CPU-only computation with the customers' think time, during the
             window: what the process's scheduling adds to a short task.
  unrel      the unrelated-RESTAction customers during the window (as in arm A).
"""
import json
import math
import os
import sys

OUT = sys.argv[1]


def geo(xs):
    xs = [x for x in xs if x and x > 0]
    return math.exp(sum(math.log(x) for x in xs) / len(xs)) if xs else float('nan')


fields = [('hit_bench', 'p50_ms'), ('hit_bench', 'p95_ms'), ('probe', 'p50_ms'), ('probe', 'p95_ms'),
          ('cust_unrel', 'p50_ms'), ('cust_unrel', 'p95_ms'), ('cust_dep', 'p50_ms')]
rows = {}
for rnd in range(1, 6):
    for leg in ('main', 'branch', 'main2'):
        p = os.path.join(OUT, 'r1-%d-%s.json' % (rnd, leg))
        if os.path.exists(p):
            rows[(rnd, leg)] = json.load(open(p))
for f, q in fields:
    vals = {leg: [rows[(r, leg)][f].get(q) for r in range(1, 6) if (r, leg) in rows] for leg in ('main', 'branch', 'main2')}
    bm = [b / ((m + m2) / 2) for m, b, m2 in zip(vals['main'], vals['branch'], vals['main2'])]
    mm = [m2 / m for m, m2 in zip(vals['main'], vals['main2'])]
    sp = max(max(r, 1 / r) for r in mm)
    print('%-11s %-7s main %-40s branch %-40s main2 %-40s branch/main %.2f (%.2f-%.2f)  main/main %.2f-%.2f' % (
        f, q, [round(x * 1000) for x in vals['main']], [round(x * 1000) for x in vals['branch']],
        [round(x * 1000) for x in vals['main2']], geo(bm), min(bm), max(bm), 1 / sp, sp))
print('(values in microseconds)')
