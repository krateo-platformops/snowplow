"""#854 paired ABAB analysis: per rate, branch vs main, paired ratios."""
import json
import math
import os
import re
import subprocess

D = os.path.dirname(os.path.abspath(__file__))
R = os.path.join(D, 'runs')


def label_share(prof, tag):
    """CPU seconds carrying pprof label `tag` and the profile total."""
    try:
        out = subprocess.run(['go', 'tool', 'pprof', '-tags', prof], capture_output=True, text=True, timeout=300).stdout
    except Exception:
        return None, None
    m = re.search(r'^\s*' + re.escape(tag) + r': Total ([0-9.]+)(m?s) of ([0-9.]+)(m?s)', out, re.M)
    if not m:
        tot = re.search(r'Total ([0-9.]+)(m?s)', out)
        return 0.0, None
    def sec(v, u):
        return float(v) / (1000.0 if u == 'ms' else 1.0)
    return sec(m.group(1), m.group(2)), sec(m.group(3), m.group(4))


def geo(xs):
    xs = [x for x in xs if x and x > 0]
    return math.exp(sum(math.log(x) for x in xs) / len(xs)) if xs else float('nan')


rows = []
for rate in (1, 10, 50):
    runs = {'main': [], 'branch': []}
    for rnd in (1, 2, 3):
        for side in ('main', 'branch'):
            p = os.path.join(R, 'r%d-%d-%s.json' % (rate, rnd, side))
            if not os.path.exists(p):
                continue
            d = json.load(open(p))
            fs, tot = label_share(os.path.join(R, 'r%d-%d-%s.cpu.pb.gz' % (rate, rnd, side)), 'apistage_fill_gvr')
            ds, _ = label_share(os.path.join(R, 'r%d-%d-%s.cpu.pb.gz' % (rate, rnd, side)), 'dep')
            d['fill_cpu_s'] = fs
            d['dep_cpu_s'] = ds
            d['prof_total_s'] = tot
            runs[side].append(d)
    if not runs['main'] or not runs['branch']:
        continue
    print('== rate %d events/s (50K objects, 20 dependents, 4 workers, 30 s per run)' % rate)
    keys = [('cpu_cores', 'process CPU (cores)'), ('alloc_bytes', 'TotalAlloc (bytes)'),
            ('peak_heap_inuse', 'peak HeapInuse (bytes)'), ('fills', 'content fills'),
            ('completions_s', 'dependent completions/s'), ('events', 'events applied'),
            ('fill_cpu_s', 'CPU in fill (label apistage_fill_gvr, s)'), ('dep_cpu_s', 'CPU in dependents (label dep, s)'),
            ('gc_pause_s', 'GC pause (s)')]
    for k, name in keys:
        mv = [r.get(k) for r in runs['main']]
        bv = [r.get(k) for r in runs['branch']]
        ratios = [b / m for m, b in zip(mv, bv) if m and b is not None and m > 0]
        print('  %-42s main %-40s branch %-40s paired ratio (geo) %s' % (
            name, [round(x, 3) if isinstance(x, float) else x for x in mv],
            [round(x, 3) if isinstance(x, float) else x for x in bv],
            ('%.3f' % geo(ratios)) if ratios else 'n/a'))
    li = [r.get('list_incremental') or {} for r in runs['branch']]
    print('  branch list_incremental: divergence_total=%s generations=%s refresh_skipped=%s gen_alloc_max=%s rawjson_built=%s unanchors=%s' % (
        [x.get('DivergenceTotal') for x in li], [x.get('Generations') for x in li], [x.get('RefreshSkipped') for x in li],
        [x.get('GenAllocBytesMax') for x in li], [x.get('RawJSONBuilt') for x in li], [x.get('Unanchors') for x in li]))
    print('  per-generation bytes (branch): %s' % [
        round((x.get('GenAllocBytesTotal') or 0) / max(1, x.get('GenAllocCount') or 1)) for x in li])
