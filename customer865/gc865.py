"""#865: GC cycles, allocation and refresher completions per leg (what drives customer latency)."""
import json
import os
import sys

OUT = sys.argv[1]
rates = [int(r) for r in sys.argv[2].split(',')] if len(sys.argv) > 2 else [1, 10, 50]
for rate in rates:
    print('== rate %d' % rate)
    for leg in ('main', 'branch', 'main2'):
        gc, al, comp, p50, cpu = [], [], [], [], []
        for rnd in range(1, 6):
            p = os.path.join(OUT, 'r%d-%d-%s.json' % (rate, rnd, leg))
            if not os.path.exists(p):
                continue
            d = json.load(open(p))
            gc.append(d['num_gc'])
            al.append(round(d['alloc_bytes'] / 1e9, 1))
            comp.append(d['refresh_completed'])
            p50.append(d['cust_unrel'].get('p50_ms'))
            cpu.append(round(d['cpu_cores'], 2))
        print('  %-7s GC cycles/30s %-24s alloc GB %-28s refreshes %-24s unrelated p50 ms %-34s cores %s' % (leg, gc, al, comp, p50, cpu))
