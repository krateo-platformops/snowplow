"""A run's DECLARED snowplow environment, per arm: rendered through the installer's own values, proven
from the running pod before any load, compared between the arms of a launch.

Why: the owner approved a paired run of ONE snowplow image at GOGC=50 against the Go default. Until
now an arm got its snowplow environment from whatever the installer chart it installed carried (run
26i: GOGC=50 and GOMEMLIMIT=7GiB from installer 0.3.616's snowplow chart), so two arms could differ
only in their image.

The declaration: SNOWPLOW_ENV="GOGC=100" (run606.sh; SNOWPLOW_ENV_<arm> in the launcher's env file),
space-separated KEY=VALUE, Go runtime keys only (bench/goruntime.py is_go_key: the keys the bench
can READ BACK from the pod; a key it could not verify is refused, not trusted).

  - Always an explicit value. There is no "unset": the bench only does fresh installs, and on a
    fresh install the installer's own default flows in whatever a values file leaves out (measured
    by the installer's maintainer on 0.3.641, and reproduced by this module's render check: no
    declaration renders GOGC "50"; its componentValuesUnset only clears a key of a LIVE cluster's
    CR). The Go default is declared by its value, GOGC=100, and said so: "the Go default, set
    explicitly". snowplow itself never reads GOGC nor calls debug.SetGCPercent.
  - Applied ONLY through the installer: componentValues.snowplow.env in the run's installer values
    (bench/installer_values.py). Never a patch of the Deployment or of its ConfigMap.
  - Rendered before anything is created (`render-check`, in the dry pass too): `helm template` of
    the pulled installer chart with the run's values must carry each declared key, with its value,
    in the Installer CR it would apply (components-patch.json: spec.componentValues.snowplow.env).
  - Proven from the pod before the measure (`check`): the run's own reading of the live snowplow
    container's Go runtime env (bench/tracing.py + bench/goruntime.py: the container's env and the
    ConfigMaps it reads through envFrom, as the API server holds them, never the rendered values)
    must show every declared key SET to the declared value. A mismatch, or a key that cannot be
    read, fails the arm before any load.
  - Compared between arms before the measure (`agree`): every Go runtime key NO arm declared
    (GOMEMLIMIT, GOMAXPROCS, anything the reading finds) must read the same on every arm of the
    launch that reached its reading; one that differs fails the arm before load. The launcher
    refuses, before anything is created, a launch whose arms do not declare the same set of keys.

Report only beyond that: evaluate shows the record (report["snowplow_env"]), and the pair says in
one line whether its arms differ in image, in environment, or in both.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
import time

from bench import goruntime

KIND = "snowplow_env"
_VALUE = re.compile(r"^[A-Za-z0-9._:/+-]+$")
#: what a declared value is, when it is the runtime's own default (said in every line that names it)
GO_DEFAULT_VALUES = {"GOGC": "100"}
SAID_DEFAULT = "the Go default, set explicitly"


class Refused(ValueError):
    """A declaration the bench does not take, with the reason."""


def parse(text: str | None) -> dict[str, str]:
    """SNOWPLOW_ENV as {key: value}; {} for none. Refused: a token that is not KEY=VALUE, a key the
    bench cannot read back from the pod (not a Go runtime key), a key named twice, an empty value or
    one with characters a values file would need quoting rules for."""
    out: dict[str, str] = {}
    for tok in (text or "").split():
        key, eq, value = tok.partition("=")
        if not eq:
            raise Refused(f"SNOWPLOW_ENV: '{tok}' is not KEY=VALUE")
        if not goruntime.is_go_key(key):
            raise Refused(f"SNOWPLOW_ENV: {key} is not a Go runtime key (GOGC, GOMEMLIMIT, GOMAXPROCS, ...): the "
                          f"bench can only prove from the pod the keys its Go runtime reading reads")
        if key in out:
            raise Refused(f"SNOWPLOW_ENV names {key} twice")
        if not _VALUE.match(value):
            raise Refused(f"SNOWPLOW_ENV: {key}'s value {value!r} is empty or holds a character outside "
                          f"[A-Za-z0-9._:/+-] (there is no 'unset': declare the value, e.g. GOGC=100 for the Go "
                          f"default)")
        out[key] = value
    return out


def said(key: str, value: str) -> str:
    return f"{key}={value}" + (f" ({SAID_DEFAULT})" if GO_DEFAULT_VALUES.get(key) == value else "")


def declared_text(declared: dict[str, str]) -> str:
    return ", ".join(said(k, v) for k, v in sorted(declared.items())) or "nothing declared"


# ─── rendered: what the installer would apply ───────────────────────────────

PATCH_KEY = "components-patch.json"


def rendered_env(docs: list) -> dict:
    """spec.componentValues.snowplow.env of the Installer CR the chart applies, from its rendered
    documents: the one ConfigMap that carries components-patch.json (the merge patch its bootstrap
    Job applies to the CR). Raises Refused when the render holds none, or more than one."""
    found = [d["data"][PATCH_KEY] for d in docs if isinstance(d, dict) and d.get("kind") == "ConfigMap"
             and isinstance(d.get("data"), dict) and PATCH_KEY in d["data"]]
    if len(found) != 1:
        raise Refused(f"the rendered installer chart holds {len(found)} ConfigMap(s) with {PATCH_KEY}, not one: "
                      f"what it would apply to the Installer CR cannot be read")
    try:
        patch = json.loads(found[0])
        cv = patch["spec"]["componentValues"]
    except (ValueError, KeyError, TypeError) as e:
        raise Refused(f"{PATCH_KEY} is not a JSON patch with spec.componentValues ({type(e).__name__})") from None
    env = (cv.get("snowplow") or {}).get("env") if isinstance(cv, dict) else None
    return dict(env) if isinstance(env, dict) else {}


def render_check(installer_pulled: str, values_path: str, declared: dict[str, str], helm: str = "helm") -> dict:
    """The run's installer values, rendered by the pulled chart: (record). `ok` only when every
    declared key is in the CR the chart would apply, with the declared value, as a string."""
    import yaml

    from bench import ghcr_pull
    root = os.path.join(installer_pulled, "installer")
    rec = {"declared": declared, "rendered": None, "ok": False, "why": ""}
    try:
        with open(values_path) as fh:
            values = yaml.safe_load(fh) or {}
        docs = ghcr_pull.render(ghcr_pull._chart_with(root, {}), ghcr_pull.INSTALLER_RELEASE, values, helm,
                                args=("--kube-version", ghcr_pull.KUBE_VERSION))
        rec["rendered"] = rendered_env(docs)
    except (Refused, ghcr_pull._Refused, OSError, yaml.YAMLError) as e:
        rec["why"] = f"the installer chart could not be rendered with this run's values: {e}"
        return rec
    wrong = [f"{k} renders as {rec['rendered'].get(k)!r}, not {v!r}" if k in rec["rendered"] else
             f"{k} is not in the rendered componentValues.snowplow.env" for k, v in sorted(declared.items())
             if rec["rendered"].get(k) != v]
    rec["ok"] = not wrong
    rec["why"] = ("; ".join(wrong) if wrong else
                  f"componentValues.snowplow.env of the Installer CR the chart applies: {json.dumps(rec['rendered'], sort_keys=True)}")
    return rec


# ─── observed: what the running pod has ─────────────────────────────────────

def reading_of(tracing_path: str, phase: str = "before") -> tuple[dict | None, str]:
    """(the Go runtime env of the live snowplow container at `phase`, "") or (None, why not)."""
    try:
        with open(tracing_path) as fh:
            doc = json.load(fh)
    except OSError as e:
        return None, f"the run took no reading of the pod ({tracing_path}: {e.strerror})"
    except ValueError:
        return None, f"the run's reading of the pod is not JSON ({tracing_path})"
    g = (((doc.get("readings") or {}).get(phase) or {}).get("go_env")) if isinstance(doc, dict) else None
    if not isinstance(g, dict):
        return None, f"the run's reading of the pod {phase} the measure holds no Go runtime env"
    if g.get("state") != "read":
        return None, f"the pod's Go runtime env could not be read {phase} the measure ({g.get('why')})"
    return g, ""


def observed(g: dict) -> dict[str, dict]:
    """Each Go runtime key of the reading as {"state": set|unset|UNKNOWN, "value", "from"}."""
    return {k: {"state": r.get("state"), "value": r.get("value"), "from": r.get("from"),
                **({"why": r.get("why")} if r.get("state") == goruntime.UNKNOWN else {})}
            for k, r in sorted((g.get("effective") or {}).items())}


def key_text(k: str, r: dict) -> str:
    if r.get("state") == "set":
        return f"{k}={r.get('value')}"
    return f"{k} unset" if r.get("state") == "unset" else f"{k} UNKNOWN ({r.get('why')})"


def check(declared: dict[str, str], tracing_path: str, *, arm: str = "", now=time.time) -> dict:
    """The run's record: what was declared, what the running container has, and whether every
    declared key is SET there to the declared value. Fails closed: no reading, an UNKNOWN key."""
    rec: dict = {"kind": KIND, "arm": arm, "t": now(), "declared": declared, "observed": None, "verified": False,
                 "mismatches": [], "undeclared": {},
                 "source": "the live snowplow container's env and the ConfigMaps it reads (the run's Go runtime "
                           "reading before the measure), never the rendered values"}
    g, why = reading_of(tracing_path)
    if g is None:
        rec["why"] = why
        return rec
    obs = rec["observed"] = observed(g)
    if g.get("secret_source"):
        rec["mismatches"].append(f"{g['secret_source']}: what the container's Go runtime env is cannot be proven")
    for k, v in sorted(declared.items()):
        r = obs.get(k) or {"state": "unset"}
        if r.get("state") != "set" or r.get("value") != v:
            rec["mismatches"].append(f"declared {k}={v}, the pod has {key_text(k, r)}")
    rec["undeclared"] = {k: r for k, r in obs.items() if k not in declared}
    unknown = [k for k, r in rec["undeclared"].items() if r.get("state") == goruntime.UNKNOWN]
    if unknown:
        rec["mismatches"].append(f"undeclared key(s) the pod's reading cannot decide: {', '.join(unknown)}")
    rec["verified"] = not rec["mismatches"]
    rec["why"] = ("; ".join(rec["mismatches"]) if rec["mismatches"] else
                  f"the pod has every declared key as declared ({declared_text(declared)}); undeclared: "
                  + (", ".join(key_text(k, r) for k, r in rec["undeclared"].items()) or "none"))
    return rec


# ─── agreed: the arms of one launch ─────────────────────────────────────────

def same_keys(by_arm: dict[str, dict[str, str]]) -> str:
    """"" when every arm declares the same set of keys; else why the launch is refused."""
    sets = {arm: tuple(sorted(d)) for arm, d in by_arm.items()}
    if len(set(sets.values())) <= 1:
        return ""
    return ("the runs do not declare the same snowplow env keys ("
            + "; ".join(f"{arm}: {' '.join(ks) or 'none'}" for arm, ks in sorted(sets.items()))
            + "): a run that declares no value for a key another run sets would take the installer's default by "
              "silence; declare every key on every run (SNOWPLOW_ENV_<run>)")


def undeclared_differences(recs: dict[str, dict]) -> list[str]:
    """The Go runtime keys no arm declared that do not read the same on every arm (records: check's)."""
    keys = sorted({k for r in recs.values() for k in (r.get("undeclared") or {})})
    out = []
    for k in keys:
        seen = {arm: key_text(k, (r.get("undeclared") or {}).get(k) or {"state": "unset"})
                for arm, r in sorted(recs.items()) if k not in (r.get("declared") or {})}
        if len(set(seen.values())) > 1:
            out.append(f"{k} is declared on no run and differs between them: "
                       + ", ".join(f"{arm}: {t}" for arm, t in seen.items()))
    return out


def agree(shared_dir: str, arm: str, arms: list[str], wait_s: float, *, clock=time.monotonic, sleep=time.sleep,
          poll_s: float = 5.0) -> dict:
    """Before `arm` starts its load: wait (at most `wait_s`) for the reading of every other arm of the
    launch in `shared_dir` (<arm>.json, check's record; <arm>.gone: that run ended without one), then
    compare the undeclared keys. {"ok", "why", "compared", "not_compared"}: not ok only on a difference;
    an arm with no reading within the wait is NOT compared, and is named (the pair says it again)."""
    deadline = clock() + wait_s
    recs: dict[str, dict] = {}
    pending = list(arms)
    while True:
        for x in list(pending):
            p = os.path.join(shared_dir, f"{x}.json")
            if os.path.exists(p):
                try:
                    with open(p) as fh:
                        recs[x] = json.load(fh)
                    pending.remove(x)
                except ValueError:
                    pass                         # being written: read again
            elif os.path.exists(os.path.join(shared_dir, f"{x}.gone")):
                pending.remove(x)
        if not pending or clock() >= deadline:
            break
        sleep(min(poll_s, max(deadline - clock(), 0.01)))
    diffs = undeclared_differences(recs) if arm in recs else [f"{arm}'s own reading is not in {shared_dir}"]
    compared = sorted(x for x in recs if x != arm)
    absent = sorted(x for x in arms if x not in recs and x != arm)
    return {"ok": not diffs, "why": "; ".join(diffs), "compared": compared, "not_compared": absent,
            "line": (f"undeclared Go runtime keys: {'DIFFER: ' + '; '.join(diffs) if diffs else 'the same'} on "
                     f"{arm} and {', '.join(compared) or 'no other run'}"
                     + (f"; NOT compared with {', '.join(absent)} (no reading within {wait_s:g} s, or the run "
                        f"ended without one)" if absent else ""))}


# ─── reported ───────────────────────────────────────────────────────────────

def load(path: str | None) -> dict | None:
    try:
        with open(path) as fh:
            rec = json.load(fh)
    except (OSError, ValueError, TypeError):
        return None
    return rec if isinstance(rec, dict) and rec.get("kind") == KIND else None


def report(path: str | None) -> dict:
    """evaluate's report["snowplow_env"]: the record, and its INFO lines. No record: UNKNOWN, said."""
    rec = load(path)
    if rec is None:
        why = "the run has no snowplow-env record (no declaration was checked against the pod)"
        return {"state": "UNKNOWN", "why": why, "lines": [f"INFO snowplow env: UNKNOWN ({why})"]}
    state = "VERIFIED" if rec.get("verified") else "NOT_VERIFIED"
    und = ", ".join(key_text(k, r) for k, r in (rec.get("undeclared") or {}).items()) or "none"
    return dict(rec, state=state, lines=[
        f"INFO snowplow env declared: {declared_text(rec.get('declared') or {})}; "
        + (f"read from the running pod before the measure: as declared; undeclared Go runtime keys: {und}"
           if rec.get("verified") else f"NOT verified from the pod: {rec.get('why')}")])


def _arm(v: dict | None) -> dict | None:
    sec = ((v or {}).get("report") or {}).get(KIND) if isinstance(v, dict) else None
    return sec if isinstance(sec, dict) and sec.get("state") == "VERIFIED" else None


def _digest(v: dict | None):
    return (((v or {}).get("report") or {}).get("image_digest") or {}).get("expected") if isinstance(v, dict) else None


def pair(va: dict | None, vb: dict | None, labels: tuple[str, str], not_comparable: str = "") -> dict:
    """The pair's one statement of what its arms differ in: the image (each verdict's expected digest),
    the declared snowplow env (each arm's record, verified from its pod), both, or neither; and the
    undeclared keys that differ. An arm without a verified record: UNKNOWN, never "the same"."""
    da, db = _digest(va), _digest(vb)
    image = ("the image is UNKNOWN on an arm" if not da or not db else
             f"the same image ({da})" if da == db else f"different images ({labels[0]}: {da}; {labels[1]}: {db})")
    ra, rb = _arm(va), _arm(vb)
    out: dict = {"result": "INFO", "same_image": (da == db) if da and db else None, "env_differs": None,
                 "undeclared_differ": None, "a": ra, "b": rb}
    if ra is None or rb is None:
        who = ", ".join(lab for lab, r in zip(labels, (ra, rb), strict=True) if r is None)
        out["lines"] = [f"INFO arms: {image}; snowplow env UNKNOWN: no record verified from the pod for {who}"]
    else:
        ea, eb = ra.get("declared") or {}, rb.get("declared") or {}
        keys = sorted(set(ea) | set(eb))
        diff = [k for k in keys if ea.get(k) != eb.get(k)]
        und = undeclared_differences({labels[0]: ra, labels[1]: rb})
        out.update(env_differs=bool(diff), undeclared_differ=und)

        def one(e, k):
            return said(k, e[k]) if k in e else f"{k} not declared"
        env = ("the same declared snowplow env (" + (declared_text(ea) if ea else "nothing declared") + ")"
               if not diff else "they differ in snowplow env: "
               + "; ".join(f"{one(ea, k)} on {labels[0]}, {one(eb, k)} on {labels[1]}" for k in diff))
        only = (" (in env only, not in image)" if diff and da and da == db else
                " (in image only)" if not diff and da and db and da != db else "")
        out["lines"] = [f"INFO arms: {image}; {env}{only}; each read from the running pod before the measure"
                        + ("; UNDECLARED keys differ: " + "; ".join(und) if und else "")]
    if not_comparable:
        out["lines"].append(f"INFO not comparable with {not_comparable}")
    return out


# ─── the one-cluster rehearsal's report ─────────────────────────────────────

def _json_file(path: str):
    try:
        with open(path) as fh:
            return json.load(fh)
    except (OSError, ValueError):
        return None


def rehearsal_lines(out_dir: str) -> list[str]:
    """Everything a rehearsal run (run606.sh BENCH_606_REHEARSAL=env) exists to show, from the files
    it wrote, in one block: the maintenance exclusion as GKE printed it back, the seconds its step
    took and GKE's refusal text if it was refused once (#1422), the cluster's operations record and
    its control, the declared snowplow env as the chart rendered it, the pod's Go runtime reading and
    the comparison's verdict. A file that is not there is said, never skipped."""
    from bench import clusterops
    j = lambda name: _json_file(os.path.join(out_dir, name))  # noqa: E731
    lines = ["=== rehearsal report ==="]
    ex = j("maintenance-exclusion.json")
    if not isinstance(ex, dict):
        lines.append("maintenance exclusion: NO RECORD (maintenance-exclusion.json): gke-up did not reach the step")
    else:
        lines += [f"maintenance exclusion: {'VERIFIED' if ex.get('verified') else 'NOT VERIFIED'}: {ex.get('why')}",
                  f"  asked: {json.dumps(ex.get('requested'), sort_keys=True)}",
                  f"  read back, as GKE prints it: {json.dumps(ex.get('read_back'), sort_keys=True)}",
                  f"  the cluster's createTime, as GKE prints it: {ex.get('cluster_created')!r}",
                  f"  the step took {ex.get('seconds')} s",
                  ("  the update was refused once: " + json.dumps(ex["refused_once"], sort_keys=True)
                   if ex.get("refused_once") else "  the update was not refused (no operation was in flight)")]
    ops = j("cluster-operations.json")
    if not isinstance(ops, dict):
        lines.append("cluster operations: NO RECORD (cluster-operations.json)")
    elif not ops.get("readable"):
        lines.append(f"cluster operations: {ops.get('unreadable')}")
    else:
        c = ops.get("control") or {}
        lines.append(f"cluster operations: readable; control {c.get('type')} {c.get('operation')} (started "
                     f"{c.get('start')}, cluster created {c.get('cluster_created')}); {len(ops['operations'])} "
                     f"recorded of {ops.get('listed_in_region')} listed in the region:")
        lines += [f"  {clusterops.op_text(o)}" for o in ops["operations"]]
    rn = j("snowplow-env-render.json")
    lines.append("snowplow env, rendered by the installer chart: " + (
        f"{'as declared' if rn.get('ok') else 'NOT as declared'}: {rn.get('why')}" if isinstance(rn, dict)
        else "no render record (nothing was declared)"))
    g, why = reading_of(os.path.join(out_dir, "tracing.json"))
    lines.append("the pod's Go runtime env, read before any load: "
                 + (goruntime.reading_text(g) if g is not None else f"NOT READ ({why})"))
    rec = load(os.path.join(out_dir, "snowplow-env.json"))
    lines.append("comparison with the declaration: " + (
        "NO RECORD (snowplow-env.json)" if rec is None else
        f"{'VERIFIED' if rec.get('verified') else 'NOT AS DECLARED'}: declared {declared_text(rec.get('declared') or {})}; "
        f"{rec.get('why')}"))
    lines.append("=== end of rehearsal report ===")
    return lines


# ─── CLI ────────────────────────────────────────────────────────────────────

def _write(path: str, rec: dict) -> None:
    tmp = path + ".part"
    with open(tmp, "w") as fh:
        json.dump(rec, fh, indent=1)
        fh.write("\n")
    os.replace(tmp, path)


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="python3 -m bench.snowplow_env")
    sub = ap.add_subparsers(dest="cmd", required=True)
    p = sub.add_parser("parse", help="exit 2 with the reason unless the declaration is one the bench takes")
    p.add_argument("--declared", default="")
    k = sub.add_parser("same-keys", help="exit 2 unless every run declares the same keys (ARM=declaration ...)")
    k.add_argument("runs", nargs="+")
    r = sub.add_parser("render-check", help="exit 1 unless the pulled installer chart renders the declaration")
    r.add_argument("--installer-pulled", required=True)
    r.add_argument("--values", required=True)
    r.add_argument("--declared", default="")
    r.add_argument("--out", required=True)
    c = sub.add_parser("check", help="exit 1 unless the running pod has every declared key as declared")
    c.add_argument("--declared", default="")
    c.add_argument("--tracing", required=True)
    c.add_argument("--arm", default="")
    c.add_argument("--out", required=True)
    g = sub.add_parser("agree", help="exit 1 when a key no run declared differs between the launch's runs")
    g.add_argument("--dir", required=True)
    g.add_argument("--arm", required=True)
    g.add_argument("--arms", nargs="+", required=True)
    g.add_argument("--wait-s", type=float, required=True)
    g.add_argument("--out", required=True)
    h = sub.add_parser("rehearsal-report", help="the one block a rehearsal run prints, from its files")
    h.add_argument("--out-dir", required=True)
    a = ap.parse_args(argv)
    if a.cmd == "rehearsal-report":
        print("\n".join(rehearsal_lines(a.out_dir)))
        return 0
    try:
        if a.cmd == "same-keys":
            by_arm = {}
            for item in a.runs:
                arm, _, text = item.partition("=")
                by_arm[arm] = parse(text)
            why = same_keys(by_arm)
            if why:
                print(f"refusing: {why}", file=sys.stderr)
                return 2
            print("snowplow env keys, the same on every run: "
                  + (" ".join(sorted(next(iter(by_arm.values())))) or "none declared"))
            return 0
        declared = parse(getattr(a, "declared", ""))
    except Refused as e:
        print(f"refusing: {e}", file=sys.stderr)
        return 2
    if a.cmd == "parse":
        print(declared_text(declared))
        return 0
    if a.cmd == "render-check":
        rec = render_check(a.installer_pulled, a.values, declared)
        _write(a.out, dict(rec, kind="snowplow_env_render"))
        print(("rendered: " if rec["ok"] else "NOT rendered as declared: ") + rec["why"])
        return 0 if rec["ok"] else 1
    if a.cmd == "check":
        rec = check(declared, a.tracing, arm=a.arm)
        _write(a.out, rec)
        print(("verified from the pod: " if rec["verified"] else "NOT as declared: ") + rec["why"])
        return 0 if rec["verified"] else 1
    res = agree(a.dir, a.arm, a.arms, a.wait_s)
    _write(a.out, res)
    print(res["line"])
    return 0 if res["ok"] else 1


if __name__ == "__main__":
    sys.exit(main())
