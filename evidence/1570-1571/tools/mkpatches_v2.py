"""mkpatches_v2.py WT OUTDIR: write one unified-diff patch per #1570 v2 mutant (each a single
exact replacement against WT's committed file), without touching WT. Uses difflib."""
import difflib, os, sys
wt, out = sys.argv[1], sys.argv[2]
os.makedirs(out, exist_ok=True)
REF = "go/snowplow/internal/cache/refresher.go"
RES = "go/snowplow/internal/cache/resolved.go"
muts = {
    "floor-ignored": (REF, "\tif floor := r.rateFloor(entry, tier); floor > 0 && ok && entry != nil {",
                      "\tif floor := r.rateFloor(entry, tier); false && floor > 0 && ok && entry != nil { // MUTANT"),
    "fixed-2s-floor": (REF, "\tif entry == nil || entry.RefreshCost <= 0 {\n\t\treturn 0\n\t}",
                       "\treturn 2 * time.Second // MUTANT\n\tif entry == nil || entry.RefreshCost <= 0 {\n\t\treturn 0\n\t}"),
    "unknown-cost-gets-a-fixed-floor": (REF, "\tif entry == nil || entry.RefreshCost <= 0 {\n\t\treturn 0\n\t}",
                                        "\tif entry == nil || entry.RefreshCost <= 0 {\n\t\treturn 2 * time.Second // MUTANT\n\t}"),
    "cost-not-recorded": (RES, "\t\tentry.RefreshCost = max(windowNow().Sub(rc.startedAt), 0)", "\t\t_ = rc // MUTANT"),
    "backlog-factor-removed": (REF, "\treturn time.Duration(int64(entry.RefreshCost) * int64(k) / int64(s))",
                               "\treturn entry.RefreshCost // MUTANT"),
    "sides-not-separated": (REF, "\tif tier == tierViewed {\n\t\tif r.viewedQueue != nil {",
                            "\tif false && tier == tierViewed { // MUTANT\n\t\tif r.viewedQueue != nil {"),
}
for name, (f, old, new) in muts.items():
    s = open(os.path.join(wt, f)).read()
    assert s.count(old) == 1, (name, s.count(old))
    t = s.replace(old, new)
    d = difflib.unified_diff(s.splitlines(True), t.splitlines(True), fromfile="a/" + f, tofile="b/" + f)
    open(os.path.join(out, name + ".diff"), "w").write("diff --git a/%s b/%s\n" % (f, f) + "".join(d))
    print(name)
