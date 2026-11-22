"""falsify.py SPEC.json: for each mutant, a fresh `git archive` of REPO@REF, one exact
replacement (must match once), then `go test -race -count=N -run RUN PKG`; the mutant must
FAIL (KILLED). Also runs the unmutated archive once (must PASS). Writes a JSON result."""
import json, os, subprocess, sys, tempfile, time
spec = json.load(open(sys.argv[1]))
repo, ref, out = spec["repo"], spec["ref"], spec["out"]
base = tempfile.mkdtemp(prefix="falsify-", dir=spec.get("tmp"))
sha = subprocess.check_output(["git", "-C", repo, "rev-parse", ref], text=True).strip()
results = {"ref": sha, "started": time.strftime("%Y-%m-%dT%H:%M:%S%z"), "mutants": []}

def extract(name):
    d = os.path.join(base, name)
    os.makedirs(d)
    tar = subprocess.run(["git", "-C", repo, "archive", sha], check=True, capture_output=True).stdout
    subprocess.run(["tar", "-x", "-C", d], input=tar, check=True)
    return d

def gotest(d, run, pkg, count):
    env = dict(os.environ, GOFLAGS="-mod=mod")
    t0 = time.time()
    p = subprocess.run(["nice", "-n", "10", "go", "test", "-race", f"-count={count}", "-run", run, pkg],
                       cwd=os.path.join(d, "go/snowplow"), capture_output=True, text=True, env=env)
    tail = [l for l in (p.stdout + p.stderr).splitlines() if l.startswith(("--- FAIL", "--- PASS", "ok", "FAIL", "    "))]
    return p.returncode, round(time.time() - t0, 1), tail[-12:]

for m in [{"name": "unmutated", "edits": []}] + spec["mutants"]:
    d = extract(m["name"])
    ok = True
    for e in m["edits"]:
        p = os.path.join(d, e["file"])
        s = open(p).read()
        n = s.count(e["old"])
        if n != 1:
            ok = False
            results["mutants"].append({"name": m["name"], "error": f"old string matched {n} times in {e['file']}"})
            break
        open(p, "w").write(s.replace(e["old"], e["new"]))
    if not ok:
        continue
    for arm in m.get("arms", spec["arms"]):
        rc, secs, tail = gotest(d, arm["run"], arm["pkg"], arm.get("count", 1))
        want_fail = m["name"] != "unmutated" and arm.get("expect", "FAIL") == "FAIL"
        verdict = ("KILLED" if rc != 0 else "SURVIVED") if want_fail else ("PASS" if rc == 0 else "FAIL")
        r = {"name": m["name"], "arm": arm["run"], "rc": rc, "seconds": secs, "verdict": verdict, "tail": tail}
        results["mutants"].append(r)
        print(json.dumps({k: r[k] for k in ("name", "arm", "verdict", "seconds")}), flush=True)
results["finished"] = time.strftime("%Y-%m-%dT%H:%M:%S%z")
json.dump(results, open(out, "w"), indent=1)
