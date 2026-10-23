"""#854 ABAB: per-completion CPU / allocation (a SHARE: work per unit of output),
paired per round, plus the background load sampled during each run."""
import json
import math
import os
import sys

D = os.path.dirname(os.path.abspath(__file__))
R = os.path.join(sys.argv[1] if len(sys.argv) > 1 else os.path.join(D, 'runs'))


def geo(xs):
    return math.exp(sum(math.log(x) for x in xs) / len(xs))


def hms(s):
    h, m, x = s.split(':')
    return int(h) * 3600 + int(m) * 60 + int(x)


ends = {}
for line in open(os.path.join(D, 'abab.log')):
    p = line.split()
    if len(p) == 3 and p[1].startswith('exit='):
        ends[p[0]] = hms(p[2])
load = []
lp = os.path.join(R, 'load.log')
if os.path.exists(lp):
    for line in open(lp):
        p = line.split()
        load.append((hms(p[0]), int(p[1].split('=')[1])))


def other_load(tag):
    e = ends.get(tag)
    if e is None or not load:
        return float('nan')
    xs = [v for t, v in load if e - 32 <= t <= e]
    return sum(xs) / len(xs) if xs else float('nan')


for rate in (1, 10, 50):
    print('== rate %d events/s' % rate)
    cpu_r, alloc_r = [], []
    for rnd in (1, 2, 3):
        row = {}
        for side in ('main', 'branch'):
            d = json.load(open(os.path.join(R, 'r%d-%d-%s.json' % (rate, rnd, side))))
            comp = d['completions_s'] * 30.0
            row[side] = (d['cpu_cores'] * 30.0 / comp, d['alloc_bytes'] / comp, other_load('r%d-%d-%s' % (rate, rnd, side)))
        cr = row['branch'][0] / row['main'][0]
        ar = row['branch'][1] / row['main'][1]
        cpu_r.append(cr)
        alloc_r.append(ar)
        print('  round %d: CPU/completion main %.3f s branch %.3f s (x%.2f); alloc/completion main %.0f MB branch %.0f MB (x%.2f); other-process CPU main %.0f%% branch %.0f%%' % (
            rnd, row['main'][0], row['branch'][0], cr, row['main'][1] / 1e6, row['branch'][1] / 1e6, ar, row['main'][2], row['branch'][2]))
    print('  paired geo: CPU/completion x%.2f (range %.2f-%.2f); alloc/completion x%.2f (range %.2f-%.2f)' % (
        geo(cpu_r), min(cpu_r), max(cpu_r), geo(alloc_r), min(alloc_r), max(alloc_r)))
