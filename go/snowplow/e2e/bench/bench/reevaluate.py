"""Evaluate a run directory again with this bench (#1494): a fetched durable copy, its evidence as it is.

    python3 -m bench.reevaluate <run dir> --out <new verdict file> [the run's settings, each only to confirm]

A run from #1494 on keeps the exact evaluate argv with its evidence (run606 s_evaluate: evaluate-argv.json,
its run dir as {OUT}); that argv is used as it is. A run from before keeps none, so every setting that feeds
evaluate is DERIVED from the run's own evidence, never from a default:

  driver            loadgen-job.yaml, the Jobs run606 applied: k6load / k6browser (k6) or load606
  snowplow digest   image-digest.jsonl, the digest gate's `expected` (EXPECTED_IMAGE_DIGEST)
  frontend          frontend-digest.jsonl (written only with FRONTEND_TAG), its `expected`
                    (EXPECTED_FRONTEND_DIGEST), and installer-values.yaml's componentValues.frontend.image.tag
                    (FRONTEND_TAG, as run606 rendered it); neither: no override
  browser sessions  k6: loadgen-job.yaml's browser-config.json, its sessions (BROWSER_SESSIONS), checked against
                    browser/browser-summary.json's when the browser left one
  viewers           k6: loadgen-job.yaml's k6 config `viewers` and its `viewer` sidecar (VIEWERS), checked against
                    the viewer's output (k6/viewer.jsonl, viewer-exit.json): output without a sidecar is a
                    contradiction
  run 8 report      the run's own verdict.json: its report holds `run8` iff evaluate ran with --run8
                    (RUN8_REPORT)
  settle            the run's own run.log: it names the settle step (#1511 on), and then --settle is passed,
                    whether settle.json is there or not (a missing one reads INVALID, undecided)

A setting the evidence cannot decide is REFUSED, named, unless the operator states it (--driver,
--expect-digest, --frontend-tag + --expect-frontend-digest or --no-frontend, --browser-sessions, --viewers,
--run8); a stated value the evidence contradicts is refused too. The new verdict carries `reevaluated`: each
setting, its value and where it came from, and the argv. Never written over the run's own verdict.json.
"""

from __future__ import annotations

import argparse
import contextlib
import glob
import io
import json
import os
import sys

#: run606 s_evaluate's inputs (its nullglob globs, in its order)
EVALUATE_GLOBS = ("nav-*.jsonl", "fresh-*.jsonl", "preflight.jsonl", "pod-*.jsonl", "mem-*.jsonl", "top.jsonl",
                  "top-boot.jsonl", "boot.jsonl", "flowcontrol-config.jsonl", "image-digest.jsonl", "job-pods.jsonl",
                  "cluster-shape*.jsonl", "api-outages*.jsonl", "platform-definitions.jsonl", "command-failures.jsonl",
                  "cert-manager*.jsonl", "host-load.jsonl", "incluster-placement.jsonl", "observer-pod.jsonl",
                  "netns*.jsonl", "frontend-digest*.jsonl")


class Refused(ValueError):
    pass


def _jsonl_last(path: str, kind: str) -> dict | None:
    try:
        with open(path) as fh:
            rows = [json.loads(ln) for ln in fh if ln.lstrip().startswith("{")]
    except (OSError, ValueError):
        return None
    return next((r for r in reversed(rows) if isinstance(r, dict) and r.get("kind") == kind), None)


def _manifest(d: str) -> list[dict] | None:
    import yaml
    try:
        with open(os.path.join(d, "loadgen-job.yaml")) as fh:
            return [x for x in yaml.safe_load_all(fh) if isinstance(x, dict)]
    except (OSError, yaml.YAMLError):
        return None


def _jobs(docs: list[dict]) -> dict[str, dict]:
    return {(x.get("metadata") or {}).get("name"): x for x in docs if x.get("kind") == "Job"}


def _cm_file(docs: list[dict], name: str):
    for x in docs:
        if x.get("kind") == "ConfigMap" and name in (x.get("data") or {}):
            try:
                return json.loads(x["data"][name])
            except ValueError:
                return None
    return None


def derive(d: str) -> dict:
    """{setting: (value, source)} from the run's evidence; value None: the evidence cannot decide (source: why)."""
    out: dict = {}
    docs = _manifest(d)
    jobs = _jobs(docs) if docs is not None else {}
    if docs is None:
        out["driver"] = (None, "no readable loadgen-job.yaml")
    elif {"k6load", "k6browser"} & set(jobs):
        out["driver"] = ("k6", "loadgen-job.yaml: the k6load / k6browser Jobs")
    elif "load606" in jobs:
        out["driver"] = ("load606", "loadgen-job.yaml: the load606 Job")
    else:
        out["driver"] = (None, "loadgen-job.yaml holds neither the k6 Jobs nor the load606 Job")
    dg = _jsonl_last(os.path.join(d, "image-digest.jsonl"), "image_digest")
    out["expect_digest"] = ((dg["expected"], "image-digest.jsonl: the digest gate's expected")
                            if dg and dg.get("expected") else (None, "no image-digest.jsonl record with its expected"))
    # the frontend override: its digest gate's record exists only with FRONTEND_TAG
    fds = sorted(glob.glob(os.path.join(d, "frontend-digest*.jsonl")))
    fd = next((r for r in (_jsonl_last(p, "frontend_image_digest") for p in fds) if r), None)
    tag = None
    try:
        import yaml
        with open(os.path.join(d, "installer-values.yaml")) as fh:
            vals = yaml.safe_load(fh) or {}
        tag = (((vals.get("componentValues") or {}).get("frontend") or {}).get("image") or {}).get("tag")
        vals_read = True
    except (OSError, ValueError, AttributeError):
        vals_read = False
    if fd and fd.get("expected") and tag:
        out["frontend"] = ((str(tag), fd["expected"]), "frontend-digest.jsonl's expected and installer-values.yaml's "
                                                        "componentValues.frontend.image.tag")
    elif not fds and vals_read and not tag:
        out["frontend"] = ("none", "no frontend-digest.jsonl and no frontend tag in installer-values.yaml")
    else:
        out["frontend"] = (None, f"frontend-digest.jsonl {'present' if fds else 'absent'}"
                                 f"{' with its expected' if fd and fd.get('expected') else ''}, installer-values.yaml "
                                 f"{'unreadable' if not vals_read else 'tag ' + repr(tag)}: the override is not decided")
    if out["driver"][0] == "k6":
        cfg = _cm_file(docs, "browser-config.json")
        n = len(cfg["sessions"]) if isinstance(cfg, dict) and isinstance(cfg.get("sessions"), list) else None
        summ = None
        try:
            with open(os.path.join(d, "browser", "browser-summary.json")) as fh:
                s = json.load(fh)
            summ = len(s["sessions"]) if isinstance(s.get("sessions"), list) else None
        except (OSError, ValueError, TypeError, AttributeError):
            pass
        if n is None:
            out["browser_sessions"] = (None, "loadgen-job.yaml holds no browser-config.json with its sessions")
        elif summ is not None and summ != n:
            out["browser_sessions"] = (None, f"loadgen-job.yaml's browser config has {n} sessions, the browser's "
                                             f"summary {summ}")
        else:
            out["browser_sessions"] = (n, "loadgen-job.yaml's browser-config.json sessions"
                                       + (" (the browser's summary agrees)" if summ is not None else ""))
        k6cfg = _cm_file(docs, "config.json")
        k6job = jobs.get("k6load") or {}
        names = {c.get("name") for c in (((k6job.get("spec") or {}).get("template") or {}).get("spec") or {}).get(
            "containers") or []}
        sidecar = "viewer" in names
        said = k6cfg.get("viewers") if isinstance(k6cfg, dict) else None
        output = any(os.path.exists(os.path.join(d, "k6", f)) for f in ("viewer.jsonl", "viewer-exit.json"))
        if said is None or bool(said) != sidecar:
            out["viewers"] = (None, f"loadgen-job.yaml: k6 config viewers {said!r}, viewer sidecar {sidecar}")
        elif output and not sidecar:
            out["viewers"] = (None, "the viewer left output, and the manifest has no viewer sidecar")
        else:
            out["viewers"] = (sidecar, "loadgen-job.yaml: the k6 config's viewers and its viewer sidecar")
    try:
        with open(os.path.join(d, "verdict.json")) as fh:
            v = json.load(fh)
        out["run8"] = ("run8" in (v.get("report") or {}), "the run's own verdict.json: its report "
                       + ("holds run8" if "run8" in (v.get("report") or {}) else "holds no run8"))
    except (OSError, ValueError, AttributeError):
        out["run8"] = (None, "no readable verdict.json of the run")
    # #1422 (the settle step, #1511): decided by what the run DID, its own run.log naming the step, never by whether
    # settle.json is there: a run that had the step and lost its record must read INVALID (undecided), not unchecked
    try:
        with open(os.path.join(d, "run.log"), errors="replace") as fh:
            ran = "=== step settle ===" in fh.read()
        out["settle"] = (ran, "the run's own run.log: " + ("its settle step ran (a bench from #1511 on)" if ran else
                                                           "no settle step (a bench before #1511)"))
    except OSError:
        out["settle"] = (None, "no readable run.log of the run: whether it had a settle step is not known")
    return out


def settings(d: str, stated: dict) -> dict:
    """{setting: (value, source)}: each derived, confirmed by what the operator stated; Refused, naming every
    setting the evidence cannot decide and the operator did not state, and every stated one it contradicts."""
    der = derive(d)
    out, why = {}, []
    names = ["driver", "expect_digest", "frontend", "run8", "settle"] + (
        ["browser_sessions", "viewers"] if (stated.get("driver") or der["driver"][0]) == "k6" else [])
    for k in names:
        dv, src = der.get(k, (None, "not derived"))
        sv = stated.get(k)
        if sv is not None and dv is not None and sv != dv:
            why.append(f"{k}: stated {sv!r}, the run's evidence says {dv!r} ({src})")
        elif sv is None and dv is None:
            why.append(f"{k}: not decided by the run's evidence ({src}); state it")
        else:
            out[k] = (dv, src) if dv is not None else (sv, f"stated by the operator (the evidence: {src})")
    if why:
        raise Refused("; ".join(why))
    return out


def browser_logs(d: str) -> list[str]:
    """#1508: `--browser-log F` for each browser driver log the run collected (run606 s_evaluate's glob): where
    k6 logged which session's iteration threw, for a browser.js that recorded no session end."""
    return [x for p in sorted(glob.glob(os.path.join(d, "driver-k6browser-*.log"))) for x in ("--browser-log", p)]


def argv_for(d: str, s: dict) -> list[str]:
    """run606 s_evaluate's argv for run dir `d` with settings `s`."""
    files = [f for g in EVALUATE_GLOBS for f in sorted(glob.glob(os.path.join(d, g)))]
    argv = ["evaluate", *files, *(["--run8"] if s["run8"][0] else []),
            "--expect-digest", s["expect_digest"][0], "--profile-dir", os.path.join(d, "heap")]
    if s["driver"][0] == "k6":
        argv += ["--k6-out", os.path.join(d, "k6"), "--browser-out", os.path.join(d, "browser"),
                 "--browser-sessions", str(s["browser_sessions"][0]), *([] if s["viewers"][0] else ["--no-viewers"]),
                 # #1525 review: the renderer watch the run was configured with, from its own manifest (the one
                 # browser_sessions was derived from)
                 "--loadgen-job", os.path.join(d, "loadgen-job.yaml")]
        argv += browser_logs(d)
    # #1422: a run that had the settle step is judged on its settle.json, there or not (derive: its run.log)
    settle = os.path.join(d, "settle.json")
    argv += ["--tracing", os.path.join(d, "tracing.json"), "--cluster-operations",
             os.path.join(d, "cluster-operations.json"), *(["--settle", settle] if s["settle"][0] else []),
             "--snowplow-env", os.path.join(d, "snowplow-env.json"),
             "--apf", os.path.join(d, "apf.jsonl.gz")]
    if s["frontend"][0] != "none":
        tag, dig = s["frontend"][0]
        argv += ["--frontend-tag", tag, "--expect-frontend-digest", dig]
    return argv


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="python3 -m bench.reevaluate")
    ap.add_argument("run_dir")
    ap.add_argument("--out", required=True, help="the new verdict (never the run's own verdict.json)")
    ap.add_argument("--driver", choices=["k6", "load606"], default=None)
    ap.add_argument("--expect-digest", default=None)
    ap.add_argument("--frontend-tag", default=None)
    ap.add_argument("--expect-frontend-digest", default=None)
    ap.add_argument("--no-frontend", action="store_true", help="the run had no frontend override")
    ap.add_argument("--browser-sessions", type=int, default=None)
    ap.add_argument("--viewers", choices=["on", "off"], default=None)
    ap.add_argument("--run8", choices=["on", "off"], default=None)
    ap.add_argument("--settle", choices=["on", "off"], default=None, help="the run had the settle step (#1511)")
    a = ap.parse_args(argv)
    d = os.path.abspath(a.run_dir)
    if os.path.abspath(a.out) == os.path.join(d, "verdict.json"):
        print("reevaluate: --out must not be the run's own verdict.json", file=sys.stderr)
        return 2
    if bool(a.frontend_tag) != bool(a.expect_frontend_digest) or (a.no_frontend and a.frontend_tag):
        print("reevaluate: --frontend-tag and --expect-frontend-digest go together, and not with --no-frontend",
              file=sys.stderr)
        return 2
    rec = os.path.join(d, "evaluate-argv.json")
    used: dict = {}
    if os.path.exists(rec):
        with open(rec) as fh:
            ev = ["evaluate", *(x.replace("{OUT}", d) for x in json.load(fh))]
        used = {"argv": {"value": "evaluate-argv.json", "source": "the run's own record of its evaluate argv"}}
        if "--browser-out" in ev and "--browser-log" not in ev and browser_logs(d):
            # a run recorded before #1508 passed no browser log; the run collected it
            ev += browser_logs(d)
            used["browser_log"] = {"value": [os.path.basename(p) for p in browser_logs(d)[1::2]],
                                   "source": "the run's collected driver-k6browser-*.log (#1508; its recorded argv "
                                             "predates the flag)"}
    else:
        stated = {"driver": a.driver, "expect_digest": a.expect_digest,
                  "frontend": ("none" if a.no_frontend else (a.frontend_tag, a.expect_frontend_digest)
                               if a.frontend_tag else None),
                  "browser_sessions": a.browser_sessions,
                  "viewers": None if a.viewers is None else a.viewers == "on",
                  "run8": None if a.run8 is None else a.run8 == "on",
                  "settle": None if a.settle is None else a.settle == "on"}
        try:
            s = settings(d, stated)
        except Refused as e:
            print(f"reevaluate: REFUSED: {e}", file=sys.stderr)
            return 2
        ev = argv_for(d, s)
        used = {k: {"value": v, "source": src} for k, (v, src) in s.items()}
    for k, u in used.items():
        print(f"reevaluate: {k} = {u['value']!r} ({u['source']})", file=sys.stderr)
    print("reevaluate: python3 -m bench.load606 " + " ".join(ev), file=sys.stderr, flush=True)
    from bench import load606
    buf = io.StringIO()
    with contextlib.redirect_stdout(buf):
        rc = load606.main(ev)
    v = json.loads(buf.getvalue())
    v["reevaluated"] = {"settings": used, "argv": ev}
    with open(a.out + ".part", "w") as fh:
        json.dump(v, fh, indent=2, default=str)
    os.replace(a.out + ".part", a.out)
    return rc


if __name__ == "__main__":
    sys.exit(main())
