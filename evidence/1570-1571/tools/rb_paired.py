"""rb_paired.py ARMS_JSON OUT_DIR TEST ROUNDS [ENV=V ...]: the alternated A/B runner on the
cloud build VM (rbuild.sh), ONE job at a time, in a VM-exclusive window.

ARMS_JSON: {"order": [...], "arms": {"main": {"sha": "<40hex>", "overlay": "<dir or null>"}, ...}}
Round r runs every arm once, in `order` rotated by r. Each run is one rbuild.sh job: a fresh
checkout of the arm's PUSHED sha (+ overlay), GOTOOLCHAIN=go1.27.3 (go.mod), GOMAXPROCS=4, the
harness test, its JSON printed between markers and saved locally. The job also prints the VM's
/proc/loadavg and the number of running rjob jobs before and after the test (the VM shares CPU
between jobs; a CPU-measuring A/B is only valid when ours is the only job).
Records go to OUT_DIR/runs.jsonl (RUN / RUN-FAILED, sha, start/end UTC, VM load, other jobs)."""
import json, os, subprocess, sys, time
spec = json.load(open(sys.argv[1]))
out, test, rounds = sys.argv[2], sys.argv[3], int(sys.argv[4])
extra = dict(a.split("=", 1) for a in sys.argv[5:])
here = os.path.dirname(os.path.abspath(__file__))
rbuild = os.path.join(here, "rbuild.sh")
os.makedirs(out, exist_ok=True)
outvar = "PFRESH_OUT" if test.startswith("TestPairedFresh") else "PAIRED865_OUT"
envs = " ".join(f"{k}={v}" for k, v in sorted(extra.items()))
# The command run inside the VM checkout (go/snowplow). Markers delimit the machine-read parts.
script = (
    'set -u; o=$(mktemp); l=$(mktemp); '
    'J(){ ps -eo args | grep -o "[b]in/rjob [^ ]*" | sort -u | tr "\\n" "," ; }; '
    'echo "===VM-BEFORE=== $(cat /proc/loadavg) jobs=[$(J)] nproc=$(nproc)"; '
    f'env GOTOOLCHAIN=go1.27.3 GOMAXPROCS=4 {outvar}=$o {envs} '
    f'go test -tags paired854,pairedfresh -count=1 -timeout 40m -run \'^{test}$\' ./internal/handlers/dispatchers/ >$l 2>&1; rc=$?; '
    'echo "===VM-AFTER=== $(cat /proc/loadavg) jobs=[$(J)]"; '
    'echo ===JSON-BEGIN===; cat $o 2>/dev/null; echo; echo ===JSON-END===; '
    'grep -v "^{" $l | tail -8; exit $rc'
)
log = open(os.path.join(out, "runs.jsonl"), "a")
order = spec["order"]
for r in range(rounds):
    for a in order[r % len(order):] + order[: r % len(order)]:
        arm = spec["arms"][a]
        cmd = [rbuild, f"{test}-{a}-r{r + 1}", "--sha", arm["sha"]]
        if arm.get("overlay"):
            cmd += ["--overlay", arm["overlay"]]
        cmd += ["--", "sh", "-c", script]
        rec = {"test": test, "arm": a, "round": r + 1, "sha": arm["sha"], "overlay": bool(arm.get("overlay")),
               "env": extra, "start_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())}
        t0 = time.time()
        p = subprocess.run(cmd, capture_output=True, text=True)
        txt = p.stdout
        rec["seconds"] = round(time.time() - t0, 1)
        rec["rc"] = p.returncode
        rec["end_utc"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        for line in txt.splitlines():
            if line.startswith("===VM-BEFORE==="):
                rec["vm_before"] = line.split("===VM-BEFORE===", 1)[1].strip()
            if line.startswith("===VM-AFTER==="):
                rec["vm_after"] = line.split("===VM-AFTER===", 1)[1].strip()
        dest = os.path.join(out, f"{test}-{a}-r{r + 1}.json")
        if "===JSON-BEGIN===" in txt and "===JSON-END===" in txt:
            body = txt.split("===JSON-BEGIN===", 1)[1].split("===JSON-END===", 1)[0].strip()
            try:
                json.loads(body)
                open(dest, "w").write(body)
                rec["out"] = dest
            except ValueError:
                rec["out"] = None
        label = f"bin/rjob {test}-{a}-r{r + 1}"
        others = set()
        for k in ("vm_before", "vm_after"):
            v = rec.get(k, "")
            if "jobs=[" in v:
                others |= {j for j in v.split("jobs=[", 1)[1].split("]", 1)[0].split(",") if j and j != label}
        rec["other_jobs"] = sorted(others)
        rec["alone"] = not others and "vm_before" in rec
        if p.returncode == 0 and rec.get("out"):
            rec["status"] = "RUN" if rec["alone"] else "RUN-EXCLUDED-NOT-ALONE"
        else:
            rec["status"] = "RUN-FAILED"
        rec["tail"] = [l for l in (txt.split("===JSON-END===")[-1] + p.stderr).splitlines() if l.strip()][-8:]
        log.write(json.dumps(rec) + "\n")
        log.flush()
        print(json.dumps({k: rec.get(k) for k in ("arm", "round", "status", "seconds", "alone", "other_jobs", "vm_before")}), flush=True)
