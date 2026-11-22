"""run_paired.py TREES_DIR OUT_DIR TEST ROUNDS ARM[,ARM...] [ENV=V ...]: compile one test binary
per arm tree (tags paired854,pairedfresh), then run the arms alternated, ROUNDS times
(round r runs every arm once, in the listed order, rotated by r so no arm always runs first),
each with GOMAXPROCS=4, nice 10, and the extra env.

LOAD GATE (shared laptop): a run starts only when the host's 1-minute load average is under
LOAD_GATE (8); until then the runner waits, re-reading every 30 s, and records how long. The
load is sampled every 10 s during the run (mean / max recorded: they include the run itself).

Each run writes OUT_DIR/<test>-<arm>-r<k>.json (the harness's own JSON, via PFRESH_OUT /
PAIRED865_OUT) and a line in OUT_DIR/runs.jsonl (RUN record: arm, tree sha, start/end, rc,
load at start, during and after, the wait for the gate)."""
import json, os, subprocess, sys, threading, time
LOAD_GATE = 8.0
trees, out, test, rounds, arms = sys.argv[1], sys.argv[2], sys.argv[3], int(sys.argv[4]), sys.argv[5].split(",")
extra = dict(a.split("=", 1) for a in sys.argv[6:])
os.makedirs(out, exist_ok=True)
pkg = "./internal/handlers/dispatchers/"
outvar = "PFRESH_OUT" if test.startswith("TestPairedFresh") else "PAIRED865_OUT"

def load():
    return os.getloadavg()[0]

def gate():
    t0 = time.time()
    while load() >= LOAD_GATE:
        time.sleep(30)
    return round(time.time() - t0, 1)

bins = {}
for a in arms:
    root = os.path.join(trees, a, "go/snowplow")
    b = os.path.join(out, f"bin-{a}.test")
    if not os.path.exists(b):
        gate()
        subprocess.run(["nice", "-n", "10", "go", "test", "-c", "-tags", "paired854,pairedfresh", "-o", b, pkg],
                       cwd=root, check=True)
    bins[a] = (b, root, open(os.path.join(trees, a, "TREE_SHA")).read().strip())
log = open(os.path.join(out, "runs.jsonl"), "a")
for r in range(rounds):
    order = arms[r % len(arms):] + arms[: r % len(arms)]
    for a in order:
        b, root, sha = bins[a]
        dest = os.path.join(out, f"{test}-{a}-r{r + 1}.json")
        env = dict(os.environ, GOMAXPROCS="4", **extra)
        env[outvar] = dest
        waited = gate()
        rec = {"test": test, "arm": a, "round": r + 1, "tree_sha": sha, "start": time.strftime("%Y-%m-%dT%H:%M:%S%z"),
               "load_gate": LOAD_GATE, "gate_wait_s": waited, "load_before": round(load(), 1), "env": extra}
        samples, stop = [], threading.Event()

        def sample():
            while not stop.wait(10):
                samples.append(load())
        th = threading.Thread(target=sample, daemon=True)
        th.start()
        t0 = time.time()
        p = subprocess.run(["nice", "-n", "10", b, "-test.run", f"^{test}$", "-test.count", "1", "-test.timeout", "40m"],
                           cwd=os.path.join(root, pkg), env=env, capture_output=True, text=True)
        stop.set()
        th.join()
        rec.update({"rc": p.returncode, "seconds": round(time.time() - t0, 1), "load_after": round(load(), 1),
                    "load_during_mean": round(sum(samples) / len(samples), 1) if samples else None,
                    "load_during_max": round(max(samples), 1) if samples else None,
                    "end": time.strftime("%Y-%m-%dT%H:%M:%S%z"), "out": dest if os.path.exists(dest) else None,
                    "status": "RUN" if p.returncode == 0 and os.path.exists(dest) else "RUN-FAILED",
                    "tail": [l for l in (p.stdout + p.stderr).splitlines() if not l.startswith("{")][-6:]})
        log.write(json.dumps(rec) + "\n")
        log.flush()
        print(json.dumps({k: rec[k] for k in ("arm", "round", "status", "seconds", "gate_wait_s", "load_before",
                                              "load_during_mean", "load_during_max")}), flush=True)
