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
  - Proven from the pod at the measure's start (`check`): the run's own reading of the live snowplow
    container's Go runtime env (bench/tracing.py + bench/goruntime.py: the container's env and the
    ConfigMaps it reads through envFrom, as the API server holds them, never the rendered values)
    must show every declared key SET to the declared value. A mismatch, or a key that cannot be
    read, makes the run INVALID, named (evaluate's snowplow_env.verified). The owner's ruling
    (2026-11-16): start-up is over when the pod is Ready, so the load starts right after Ready and
    this reading runs alongside it, never before it (its deadline: the new pod's Ready + the read's
    budget, the same point in every arm).
  - Compared between arms AFTER the measure (`agreement`, in custlat pair and replica): every Go
    runtime key NO arm declared (GOMEMLIMIT, GOMAXPROCS, anything the reading finds) must read the
    same on both arms of a pair; one that differs makes the pair INVALID, named. The launcher
    refuses, before anything is created, a launch whose arms do not declare the same set of keys.

THE EFFECTIVE SETTINGS. What a pod's env says is not what its process runs with: snowplow chart 1.12.70
sets no GOMEMLIMIT, the pod's env reads "unset (the runtime's default, no limit)", and the process
runs with a soft memory limit all the same (snowplow derives it in-process, 7/8 of the container's
cgroup limit). So the record's facts are the PROCESS's: snowplow publishes what its Go runtime has
on /debug/vars (snowplow_go_runtime: gogc, memory_limit_bytes, memory_limit_source,
cgroup_memory_limit_bytes), read alongside the load's start by the bench's debug reader from a short Job
on the loadgen node (run606 go_runtime_start/finish; no credential from outside the cluster). The record
holds both the declared env and the effective values, and:
  - the effective GOGC must equal the declared one (a declared GOMEMLIMIT its effective bytes), or
    the arm fails before load; so does an arm whose effective settings could not be read;
  - between the arms of a launch, the effective memory limit and its source must be equal (unless
    the arms declare GOMEMLIMIT themselves), and the effective GOGC when no arm declares it;
  - every line that states a setting states the effective value.

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


def rendered_component_values(docs: list) -> dict:
    """spec.componentValues of the Installer CR the chart applies, from its rendered documents: the
    one ConfigMap that carries components-patch.json (the merge patch its bootstrap Job applies to
    the CR). Raises Refused when the render holds none, or more than one."""
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
    return cv if isinstance(cv, dict) else {}


def rendered_env(docs: list) -> dict:
    """spec.componentValues.snowplow.env of the Installer CR the chart applies (rendered_component_values)."""
    env = (rendered_component_values(docs).get("snowplow") or {}).get("env")
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


GOGC_OFF = -1                      # the runtime's GOGC when GC is off (GOGC=off)
NO_LIMIT = 2 ** 63 - 1             # the runtime's memory limit when there is none (math.MaxInt64)
_SIZE = re.compile(r"^(\d+)(B|KiB|MiB|GiB|TiB)?$")
_UNIT = {None: 1, "B": 1, "KiB": 2 ** 10, "MiB": 2 ** 20, "GiB": 2 ** 30, "TiB": 2 ** 40}


def gogc_value(text: str) -> int | None:
    """A GOGC value as the runtime holds it: "off" is -1, else the integer; None when it is neither."""
    if text == "off":
        return GOGC_OFF
    return int(text) if re.fullmatch(r"\d+", text) else None


def memlimit_bytes(text: str) -> int | None:
    """A GOMEMLIMIT value in bytes, as Go parses it (an integer with an optional B, KiB, MiB, GiB or
    TiB; "off" is no limit); None when it is none of those."""
    if text == "off":
        return NO_LIMIT
    m = _SIZE.match(text)
    return int(m.group(1)) * _UNIT[m.group(2)] if m else None


def size_text(n) -> str:
    """Bytes as a message says them: no limit, or binary units with the exact byte count."""
    if not isinstance(n, int) or isinstance(n, bool):
        return f"{n!r}"
    if n >= NO_LIMIT:
        return "no limit"
    return f"{n / 2 ** 30:.2f} GiB ({n} bytes)" if n >= 2 ** 20 else f"{n} bytes"


def effective_of(path: str | None) -> tuple[dict | None, str]:
    """(the settings the snowplow process runs with, "") from the runtime606 Job's output (its one
    go_runtime line, load606 go-runtime), or (None, why they are not known)."""
    if not path:
        return None, "no reading of the process's settings was taken"
    try:
        with open(path) as fh:
            rows = [json.loads(ln) for ln in fh if ln.lstrip().startswith("{")]
    except OSError as e:
        return None, f"the reading of the process's settings is not there ({path}: {e.strerror})"
    except ValueError:
        return None, f"the reading of the process's settings is not JSON ({path})"
    rec = next((r for r in rows if isinstance(r, dict) and r.get("kind") == "go_runtime"), None)
    if rec is None:
        tried = [r for r in rows if isinstance(r, dict) and r.get("kind") == "go_runtime_attempt"]
        if tried:
            from bench import load606
            return None, (f"the Job that reads the process's settings printed no reading: its {len(tried)} "
                          f"attempt(s) so far had no answer, over {round(tried[-1]['t_end'] - tried[0]['t'], 1)} s "
                          f"(the last: {load606.attempt_text(tried[-1])}), and the step's wait ended first")
        return None, "the Job that reads the process's settings printed no reading"
    if not rec.get("ok") or not isinstance(rec.get("effective"), dict):
        return None, str(rec.get("why") or "the reading says neither ok nor why not")
    return rec["effective"], ""


def read_of(path: str | None) -> dict | None:
    """How the runtime606 Job's read of /debug/vars went (load606 read_summary: attempts, no_answer,
    answered, seconds_to_answer, stall), from its record, or rebuilt from its attempt lines when the
    record never came; None when neither is there (no reading, or a Job from before the attempts)."""
    try:
        with open(path) as fh:
            rows = [json.loads(ln) for ln in fh if ln.lstrip().startswith("{")]
    except (OSError, TypeError, ValueError):
        return None
    rec = next((r for r in rows if isinstance(r, dict) and r.get("kind") == "go_runtime"), None)
    if rec is not None and isinstance(rec.get("read"), dict):
        return rec["read"]
    tried = [r for r in rows if isinstance(r, dict) and r.get("kind") == "go_runtime_attempt"]
    if not tried:
        return None
    no = [a for a in tried if not a.get("status")]
    ans = next((a for a in tried if a.get("status")), None)
    secs = round(ans["t_end"] - tried[0]["t"], 1) if ans else None
    return {"attempts": len(tried), "no_answer": len(no), "answered": ans["status"] if ans else None,
            "answered_at": ans["t_end"] if ans else None,
            "seconds_to_answer": secs, "unfinished": True,
            "stall": {"no_answer_attempts": len(no), "seconds_to_answer": secs, "answered": ans["status"] if ans else None,
                      "first": no[0], "last": no[-1]} if no else None}


def read_text(read: dict | None) -> str:
    """The read's own finding, one line: answered at once, STALLED (answered only after attempts with no
    answer: how many, the seconds from the first attempt to the answer), or no answer at all."""
    from bench import load606
    if not isinstance(read, dict):
        return "its attempts were not recorded (no reading, or a runtime606 Job that predates them)"
    st = read.get("stall")
    if not st:
        a = read.get("answered")
        return f"answered {a} on its first attempt" if a else "not one attempt was made: " + str(read.get("why"))
    if read.get("answered") is None:
        return (f"NO ANSWER: {st['no_answer_attempts']} attempt(s), none answered (the first: "
                f"{load606.attempt_text(st['first'])}; the last: {load606.attempt_text(st['last'])})")
    return (f"STALLED: {st['no_answer_attempts']} attempt(s) with no answer (the first: "
            f"{load606.attempt_text(st['first'])}), then HTTP {st['answered']} "
            f"{st['seconds_to_answer']} s after the first attempt began")


def effective_text(e: dict | None) -> str:
    if not isinstance(e, dict):
        return "the process's effective settings: NOT READ"
    gogc = "off" if e.get("gogc") == GOGC_OFF else e.get("gogc")
    return (f"GOGC {gogc}, memory limit {size_text(e.get('memory_limit_bytes'))} (source "
            f"{e.get('memory_limit_source')}; the container's limit {size_text(e.get('cgroup_memory_limit_bytes'))})")


def check(declared: dict[str, str], tracing_path: str, *, arm: str = "", now=time.time,
          effective_path: str | None = None, boot_window_path: str | None = None,
          load_start_path: str | None = None) -> dict:
    """The run's record: what was declared, what the running container's env has, what the PROCESS
    runs with (`effective_path`: the runtime606 Job's output), and whether every declared key is SET
    in the env to the declared value and is the effective one. Fails closed: no reading of the pod,
    an UNKNOWN key, no reading of the process."""
    rec: dict = {"kind": KIND, "arm": arm, "t": now(), "declared": declared, "observed": None, "verified": False,
                 "mismatches": [], "undeclared": {}, "effective": None,
                 "source": "the live snowplow container's env and the ConfigMaps it reads (the run's Go runtime "
                           "reading before the measure), never the rendered values"}
    eff, eff_why = effective_of(effective_path)
    rec["effective"] = eff
    # the read's own finding (a boot-window stall), kept apart from the verdict on the settings
    rec["runtime_read"] = read_of(effective_path)
    # the read's boot window (bench/bootwindow.py, report-only): carried into the record, never judged
    rec["boot_window"] = boot_window_of(boot_window_path)
    # when the load started (the drivers applied right after the reboot, the digest gate and the pod record:
    # the owner's ruling, 2026-11-16) and when the read answered, both from the new pod's Ready
    rec["load_start"] = load_start_of(load_start_path, rec["runtime_read"])
    if eff is None:
        rec["mismatches"].append(f"the settings the process runs with could not be read: {eff_why}")
    else:
        if "GOGC" in declared and gogc_value(declared["GOGC"]) != eff.get("gogc"):
            rec["mismatches"].append(f"declared GOGC={declared['GOGC']}, the process runs with GOGC "
                                     f"{'off' if eff.get('gogc') == GOGC_OFF else eff.get('gogc')}")
        if "GOMEMLIMIT" in declared and memlimit_bytes(declared["GOMEMLIMIT"]) != eff.get("memory_limit_bytes"):
            rec["mismatches"].append(f"declared GOMEMLIMIT={declared['GOMEMLIMIT']}, the process runs with a memory "
                                     f"limit of {size_text(eff.get('memory_limit_bytes'))}")
    g, why = reading_of(tracing_path)
    if g is None:
        rec["mismatches"].append(why)
        rec["why"] = "; ".join(rec["mismatches"])
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
                  f"the process runs with {effective_text(eff)}; the pod's env has every declared key as declared "
                  f"({declared_text(declared)}); undeclared in the env: "
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


def effective_differences(recs: dict[str, dict]) -> list[str]:
    """What the processes of the launch's arms run with that must be the same and is not: the memory
    limit and its source (unless the arms declare GOMEMLIMIT themselves: then each arm's own check
    holds it), and the GOGC when no arm declares it."""
    out = []
    eff = {arm: r.get("effective") for arm, r in sorted(recs.items()) if isinstance(r.get("effective"), dict)}
    declared = {k for r in recs.values() for k in (r.get("declared") or {})}
    fields = ([] if "GOMEMLIMIT" in declared else ["memory_limit_bytes", "memory_limit_source"]) + (
        [] if "GOGC" in declared else ["gogc"])
    for f in fields:
        seen = {arm: e.get(f) for arm, e in eff.items()}
        if len({json.dumps(v) for v in seen.values()}) > 1:
            said = {"memory_limit_bytes": "the memory limit the process runs with",
                    "memory_limit_source": "where the process's memory limit comes from",
                    "gogc": "the GOGC the process runs with (declared on no run)"}[f]
            out.append(f"{said} differs between the runs: " + ", ".join(
                f"{arm}: {size_text(v) if f == 'memory_limit_bytes' else v}" for arm, v in seen.items()))
    return out


def agreement(ra: dict | None, rb: dict | None, labels: tuple[str, str]) -> list[str]:
    """AFTER the measure, from the two arms' recorded readings (their verdicts' snowplow_env sections): what
    their processes run with that must be the same and is not (effective_differences, undeclared_differences).
    [] when they agree, or when an arm has no verified reading (that arm is INVALID on its own)."""
    if not (isinstance(ra, dict) and ra.get("verified") and isinstance(rb, dict) and rb.get("verified")):
        return []
    recs = {labels[0]: ra, labels[1]: rb}
    return effective_differences(recs) + undeclared_differences(recs)


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
    read = rec.get("runtime_read")
    return dict(rec, state=state, runtime_read_stall=(read or {}).get("stall"), lines=[
        f"INFO snowplow Go runtime, effective (read from the running process alongside the load's start): "
        f"{effective_text(rec.get('effective'))}",
        f"INFO snowplow env declared: {declared_text(rec.get('declared') or {})}; "
        + (f"the process and the pod's env have it as declared; undeclared Go runtime keys in the env: {und}"
           if rec.get("verified") else f"NOT verified: {rec.get('why')}"),
        f"INFO {READ_FINDING}: {read_text(read)}"] + load_start_lines(rec.get("load_start"))
        + boot_window_lines(rec.get("boot_window")))


def load_start_of(path: str | None, read: dict | None) -> dict | None:
    """run606's load-start.json (the drivers applied: ready_at, applied_at, offset_s), with when the Go
    runtime read answered (its answering attempt's end), both as offsets from the new pod's Ready."""
    try:
        with open(path) as fh:
            ls = json.load(fh)
    except (OSError, TypeError, ValueError):
        return None
    if not isinstance(ls, dict) or ls.get("kind") != "load_start" or not isinstance(ls.get("ready_at"), (int, float)):
        return None
    out = dict(ls, read_answered_at=None, read_answered_from_ready_s=None)
    if isinstance(read, dict) and isinstance(read.get("answered_at"), (int, float)):
        out["read_answered_at"] = read["answered_at"]
        out["read_answered_from_ready_s"] = round(read["answered_at"] - ls["ready_at"], 1)
    return out


def load_start_lines(ls: dict | None) -> list[str]:
    """The run report's load-start lines: Ready, the load start, the read's answer, and the rule."""
    if not isinstance(ls, dict):
        return ["INFO load start: NO RECORD (load-start.json: no load was applied, or a run that predates it)"]
    ans = ls.get("read_answered_from_ready_s")
    return [f"INFO load start: Ready at {_utc(ls['ready_at'])}; the drivers applied at {_utc(ls['applied_at'])} = "
            f"Ready + {ls['offset_s']} s; the Go runtime read answered at "
            + (f"Ready + {ans} s" if ans is not None else "NO TIME (it never answered)"),
            f"INFO load start: load starts at Ready + {ls['offset_s']} s in this arm, and in every arm right after "
            f"{ls.get('after') or 'the reboot, the digest gate and the pod record'}: the Go runtime read runs alongside "
            f"the load and never delays it"]


def _utc(t) -> str:
    return time.strftime("%H:%M:%SZ", time.gmtime(t)) if isinstance(t, (int, float)) else str(t)


def validity(env: dict | None) -> dict:
    """evaluate's snowplow_env.verified validity check: FAIL (the run is INVALID, named) when the process's
    settings were not read within the read's deadline or are not as declared."""
    if not isinstance(env, dict) or env.get("state") == "UNKNOWN":
        why = (env or {}).get("why") or "the run has no snowplow-env record"
        return {"check": "snowplow_env.verified", "result": "FAIL", "value": "no record", "bar": "verified",
                "why": f"the run's snowplow env was not checked: {why}"}
    ok = env.get("state") == "VERIFIED"
    return {"check": "snowplow_env.verified", "result": "PASS" if ok else "FAIL",
            "value": env.get("state"), "bar": "VERIFIED",
            "why": None if ok else f"snowplow env NOT as declared (the Go runtime read's deadline is the new pod's "
                                   f"Ready + its budget): {env.get('why')}"}


def boot_window_of(path: str | None) -> dict | None:
    """$OUT/boot-window.json (bench/bootwindow.py said), or None when there is none."""
    try:
        with open(path) as fh:
            rec = json.load(fh)
    except (OSError, TypeError, ValueError):
        return None
    return rec if isinstance(rec, dict) and rec.get("kind") == "boot_window" else None


def boot_window_lines(bw: dict | None) -> list[str]:
    """The run report's "boot window" block (report-only)."""
    if not isinstance(bw, dict):
        return ["INFO boot window: NO RECORD (boot-window.json: no Go runtime read, or one that predates it)"]
    return [ln for ln in bw.get("lines") or [] if isinstance(ln, str)]


#: the stall's own finding, named so in the record, the report and the run log
READ_FINDING = "the measure's first snowplow read (runtime606, /debug/vars as the debug reader)"


def _arm(v: dict | None) -> dict | None:
    sec = ((v or {}).get("report") or {}).get(KIND) if isinstance(v, dict) else None
    return sec if isinstance(sec, dict) and sec.get("state") == "VERIFIED" else None


def _digest(v: dict | None):
    return (((v or {}).get("report") or {}).get("image_digest") or {}).get("expected") if isinstance(v, dict) else None


def pair(va: dict | None, vb: dict | None, labels: tuple[str, str], not_comparable: str = "") -> dict:
    """The pair's one statement of what its arms differ in: the image (each verdict's expected digest)
    and the GC settings their PROCESSES ran with (each arm's record: the effective values, read from
    the running process before the measure), with what each arm declared beside them. An arm
    without a verified record: UNKNOWN, never "the same"."""
    da, db = _digest(va), _digest(vb)
    image = ("the image is UNKNOWN on an arm" if not da or not db else
             f"the same image ({da})" if da == db else f"different images ({labels[0]}: {da}; {labels[1]}: {db})")
    ra, rb = _arm(va), _arm(vb)
    out: dict = {"result": "INFO", "same_image": (da == db) if da and db else None, "env_differs": None,
                 "effective_differs": None, "undeclared_differ": None, "a": ra, "b": rb}
    if ra is None or rb is None:
        who = ", ".join(lab for lab, r in zip(labels, (ra, rb), strict=True) if r is None)
        out["lines"] = [f"INFO arms: {image}; snowplow's GC settings UNKNOWN: no record verified from the running "
                        f"process for {who}"]
    else:
        ea, eb = ra.get("declared") or {}, rb.get("declared") or {}
        fa, fb = ra.get("effective") or {}, rb.get("effective") or {}
        diff = [k for k in sorted(set(ea) | set(eb)) if ea.get(k) != eb.get(k)]
        eff = [f for f in ("gogc", "memory_limit_bytes", "memory_limit_source") if fa.get(f) != fb.get(f)]
        und = undeclared_differences({labels[0]: ra, labels[1]: rb})
        out.update(env_differs=bool(diff), effective_differs=eff, undeclared_differ=und)

        def gogc(e):
            return "off" if e.get("gogc") == GOGC_OFF else e.get("gogc")

        def limit(e):
            return f"{size_text(e.get('memory_limit_bytes'))} from {e.get('memory_limit_source')}"

        def declared(e):
            return declared_text(e) if e else "nothing declared"
        gc = (f"GOGC {gogc(fa)} on both" if "gogc" not in eff else
              f"GOGC {gogc(fa)} on {labels[0]}, GOGC {gogc(fb)} on {labels[1]}")
        mem = (f"memory limit {limit(fa)} on both" if not {"memory_limit_bytes", "memory_limit_source"} & set(eff) else
               f"memory limit {limit(fa)} on {labels[0]}, {limit(fb)} on {labels[1]}")
        what = ("their processes ran with the same GC settings" if not eff else
                "their processes ran with different GC settings")
        only = (" (they differ in GC settings only, not in image)" if eff and da and da == db else
                " (they differ in image only)" if not eff and da and db and da != db else "")
        out["lines"] = [
            f"INFO arms: {image}; {what}: {gc}; {mem}{only}; each read from the running process alongside the load's start "
            f"(declared: {labels[0]} {declared(ea)}; {labels[1]} {declared(eb)})"
            + ("; UNDECLARED env keys differ: " + "; ".join(und) if und else "")]
    sa, sb = ((x or {}).get("load_start") if isinstance(x, dict) else None for x in (_sec(va), _sec(vb)))
    oa, ob = ((s_ or {}).get("offset_s") for s_ in (sa, sb))
    out["load_start_offsets_s"] = {labels[0]: oa, labels[1]: ob}
    out["lines"].append(
        f"INFO load start: {labels[0]} at Ready + {oa if oa is not None else 'UNKNOWN'} s, {labels[1]} at Ready + "
        f"{ob if ob is not None else 'UNKNOWN'} s"
        + (f": a difference of {round(abs(oa - ob), 1)} s (the bench holds no tolerance for it: reported, not judged)"
           if oa is not None and ob is not None else ": the difference is UNKNOWN")
        + "; in every arm the load starts right after the reboot, the digest gate and the pod record")
    out["disagree"] = agreement(_sec(va), _sec(vb), labels)
    if out["disagree"]:
        out["lines"].append("INVALID arms: snowplow env, compared after the measure from each arm's reading: "
                            + "; ".join(out["disagree"]))
    if not_comparable:
        out["lines"].append(f"INFO not comparable with {not_comparable}")
    return out


def _sec(v: dict | None) -> dict | None:
    sec = ((v or {}).get("report") or {}).get(KIND) if isinstance(v, dict) else None
    return sec if isinstance(sec, dict) else None


# ─── the one-cluster rehearsal's report ─────────────────────────────────────

def _json_file(path: str):
    try:
        with open(path) as fh:
            return json.load(fh)
    except (OSError, ValueError):
        return None


def _last_json_line(path: str, kind: str):
    """The last record of `kind` in a JSON-lines file (a Job's log holds other lines too); None when
    the file is not there or holds none."""
    rec = None
    try:
        with open(path) as fh:
            for line in fh:
                if line.lstrip().startswith("{"):
                    try:
                        r = json.loads(line)
                    except ValueError:
                        continue
                    if isinstance(r, dict) and r.get("kind") == kind:
                        rec = r
    except OSError:
        return None
    return rec


def image_lines(rec: dict | None) -> list[str]:
    """The snowplow pod's image, as the kubelet resolved it, against the expected digest: the record
    of the run's one digest check (bench.load606 image_digest_gate), for a report."""
    if not isinstance(rec, dict):
        return ["the snowplow pod's image: NO RECORD (image-digest.jsonl): the digest check did not run"]
    out = [f"the snowplow pod's image against the expected digest {rec.get('expected')}: {rec.get('result')}"
           + (f" ({rec.get('why')})" if rec.get("why") else "")]
    out += [f"  {p.get('container')}: image {p.get('image')!r}, imageID {p.get('image_id')!r}, digest "
            f"{p.get('digest') or 'none'}" + (" (a pod on its way out, not judged)" if p.get("terminating") else "")
            for p in rec.get("pods") or []]
    return out


def readers_lines(rec: dict | None) -> list[str]:
    """Who the pinned charts grant the portal to (bench/portal_readers.py's record), for a report."""
    if not isinstance(rec, dict):
        return ["portal readers, from the charts: NO RECORD (portal-readers.json)"]
    if not rec.get("ok"):
        return [f"portal readers, from the charts: REFUSED: {rec.get('why')}"]
    from bench import portal_readers
    return [ln.replace("portal readers:", "portal readers, from the charts:", 1) for ln in portal_readers.said(rec)]


def preflight_lines(rec: dict | None) -> list[str]:
    """The preflight user's boot call, the menu's routes and its composite pages (bench.load606's
    preflight record, already through the bench's identity redaction), for a report."""
    if not isinstance(rec, dict):
        return ["preflight (as the bench's preflight user): NO RECORD (preflight.jsonl): the preflight Job gave "
                "no result"]
    routes = rec.get("routes") or []
    out = [f"preflight (as the bench's preflight user): {'OK' if rec.get('ok') else 'REFUSED'}; boot root "
           f"{rec.get('init')} answered {rec.get('init_status')}; {len(routes)} route(s) in the menu; composite "
           f"page(s): {', '.join(rec.get('composite_routes') or []) or 'NONE'}"]
    out += [f"  {r.get('path')}: {r.get('status')}, {r.get('tree_calls')} call(s), depth {r.get('tree_depth')}, "
            f"{r.get('tree_errors')} error(s)" for r in routes]
    out += [f"  REFUSED {x.get('path')} -> {x.get('status')}: {str(x.get('body') or '')[:300]}"
            for x in rec.get("refused") or []]
    out += [f"  product error (recorded, not fatal) {x.get('path')} -> {x.get('status')}"
            for x in rec.get("product_errors") or []]
    return out


def rehearsal_lines(out_dir: str) -> list[str]:
    """Everything a rehearsal run (run606.sh BENCH_606_REHEARSAL=env) exists to show, from the files
    it wrote, in one block: the maintenance exclusion as GKE printed it back, the seconds its step
    took and GKE's refusal text if it was refused once (#1422), the cluster's operations record and
    its control, the declared snowplow env as the chart rendered it, the pod's Go runtime reading and
    the comparison's verdict, the image the snowplow pod runs against the expected digest (#1468),
    who the charts grant the portal to, and the preflight user's boot call, routes and composite
    pages (run 26j). A file that is not there is said, never skipped."""
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
    eff, eff_why = effective_of(os.path.join(out_dir, "go-runtime.jsonl"))
    lines.append("the snowplow process's effective GC settings, read before any load (/debug/vars "
                 "snowplow_go_runtime, as the bench's debug reader): "
                 + (f"{effective_text(eff)}; raw: {json.dumps(eff, sort_keys=True)}" if eff is not None
                    else f"NOT READ ({eff_why})"))
    lines.append(f"{READ_FINDING}: {read_text(read_of(os.path.join(out_dir, 'go-runtime.jsonl')))}")
    lines += boot_window_lines(boot_window_of(os.path.join(out_dir, "boot-window.json")))
    lines.append("the pod's Go runtime env, read before any load: "
                 + (goruntime.reading_text(g) if g is not None else f"NOT READ ({why})"))
    rec = load(os.path.join(out_dir, "snowplow-env.json"))
    lines.append("comparison with the declaration: " + (
        "NO RECORD (snowplow-env.json)" if rec is None else
        f"{'VERIFIED' if rec.get('verified') else 'NOT AS DECLARED'}: declared {declared_text(rec.get('declared') or {})}; "
        f"{rec.get('why')}"))
    lines += image_lines(_last_json_line(os.path.join(out_dir, "image-digest.jsonl"), "image_digest"))
    lines += readers_lines(j("portal-readers.json"))
    lv = j("portal-readers-live.json")
    from bench import portal_readers
    lines.append(portal_readers.live_said(lv) if isinstance(lv, dict) else
                 "portal readers, LIVE: NO RECORD (portal-readers-live.json): the live grant was not read")
    lines += preflight_lines(_last_json_line(os.path.join(out_dir, "preflight.jsonl"), "preflight"))
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
    c.add_argument("--effective", default=None,
                   help="the runtime606 Job's output (run606: $OUT/go-runtime.jsonl): the settings the snowplow "
                        "process runs with; without it the check fails")
    c.add_argument("--boot-window", default=None,
                   help="run606's boot-window.json (bench/bootwindow.py): carried into the record, report-only")
    c.add_argument("--load-start", default=None,
                   help="run606's load-start.json: when the drivers were applied, from the new pod's Ready")
    c.add_argument("--arm", default="")
    c.add_argument("--out", required=True)
    h = sub.add_parser("rehearsal-report", help="the one block a rehearsal run prints, from its files")
    h.add_argument("--out-dir", required=True)
    rs = sub.add_parser("read-said", help="the runtime606 read's own finding, one line (run606 logs it)")
    rs.add_argument("--effective", required=True)
    a = ap.parse_args(argv)
    if a.cmd == "rehearsal-report":
        print("\n".join(rehearsal_lines(a.out_dir)))
        return 0
    if a.cmd == "read-said":
        print(f"{READ_FINDING}: {read_text(read_of(a.effective))}")
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
        rec = check(declared, a.tracing, arm=a.arm, effective_path=a.effective, boot_window_path=a.boot_window,
                    load_start_path=a.load_start)
        _write(a.out, rec)
        print(("verified from the running process: " if rec["verified"] else "NOT as declared: ") + rec["why"])
        return 0 if rec["verified"] else 1
    return 2


if __name__ == "__main__":
    sys.exit(main())
