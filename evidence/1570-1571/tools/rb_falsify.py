"""rb_falsify.py SHA PATCHDIR OUT.json: the #1570 v2 falsifiers on the build VM, one rbuild job
per mutant (the patch applied on the pushed SHA), plus the unmutated arms. A mutant is KILLED
when its arm fails at an assertion (the failing test line is recorded)."""
import json, os, subprocess, sys, time
sha, pdir, out = sys.argv[1], sys.argv[2], sys.argv[3]
rb = os.path.join(os.path.dirname(os.path.abspath(__file__)), "rbuild.sh")
P = "TestRefresher1570_"
plan = [
    ("unmutated", None, "TestRefresher1570"),
    ("floor-ignored", "floor-ignored", P + "ExpensiveCellKeepsCoalescing$"),
    ("fixed-2s-floor", "fixed-2s-floor", P + "(CheapCellIsNotFlooredTwoSeconds|UnmeasuredCellFirstRefreshRunsAtOnce)$"),
    ("unknown-cost-gets-a-fixed-floor", "unknown-cost-gets-a-fixed-floor", P + "UnmeasuredCellFirstRefreshRunsAtOnce$"),
    ("cost-not-recorded", "cost-not-recorded", P + "(TerminalPutRecordsItsCost|ExpensiveCellKeepsCoalescing)$"),
    ("backlog-factor-removed", "backlog-factor-removed", P + "CheapCellsDoNotWaitBehindBackToBackExpensiveCells$"),
    ("sides-not-separated", "sides-not-separated", P + "ViewedCellIsNotFlooredByTheUnviewedBacklog$"),
]
res = {"sha": sha, "started_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()), "mutants": []}
for name, patch, run in plan:
    cmd = [rb, "fals-" + name, "--sha", sha]
    if patch:
        cmd += ["--patch", os.path.join(pdir, patch + ".diff")]
    script = ("export GOTOOLCHAIN=go1.27.3; echo MUTANT_LINES=$(grep -c MUTANT internal/cache/refresher.go internal/cache/resolved.go | tr '\\n' ' '); "
              f"go test -race -count=1 -v -run '{run}' ./internal/cache/ 2>&1 | grep -E '^(--- |ok|FAIL)|_test.go:[0-9]+:'")
    p = subprocess.run(cmd + ["--", "sh", "-c", script], capture_output=True, text=True)
    lines = [l for l in p.stdout.splitlines() if l.startswith(("---", "ok", "FAIL", "    ", "MUTANT_LINES"))]
    failed = any(l.startswith("--- FAIL") for l in lines)
    verdict = ("PASS" if not failed and any(l.startswith("ok") for l in lines) else "FAIL") if not patch else ("KILLED" if failed else "SURVIVED")
    r = {"name": name, "run": run, "verdict": verdict, "lines": lines}
    res["mutants"].append(r)
    print(json.dumps({"name": name, "verdict": verdict}), flush=True)
res["finished_utc"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
json.dump(res, open(out, "w"), indent=1)
