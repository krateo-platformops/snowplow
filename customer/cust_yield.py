"""#854 customer arm: the refresher-yield load per leg (parked worker-seconds, cap hits)."""
import json
import os
import sys

OUT = sys.argv[1]
for rate in (1, 10, 50):
    row = {}
    for leg in ('main', 'branch', 'main2'):
        parked, capped = [], []
        for rnd in (1, 2, 3):
            p = os.path.join(OUT, 'r%d-%d-%s.json' % (rate, rnd, leg))
            if os.path.exists(p):
                d = json.load(open(p))
                parked.append(round(d.get('yield_parked_s', 0), 1))
                capped.append(d.get('yield_capped', 0))
        row[leg] = (parked, capped)
    print('rate %2d: parked worker-s per 30 s  main %s branch %s main2 %s | 5 s cap hits main %s branch %s main2 %s' % (
        rate, row['main'][0], row['branch'][0], row['main2'][0], row['main'][1], row['branch'][1], row['main2'][1]))
