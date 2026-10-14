"""#606 installer source checks for the bench.

oci-preflight  THE install path (run 5+): the published OCI chart must
               resolve, and its pinned components must satisfy the #483 gate.

LOCAL-CHART MODE CANNOT WORK for this installer (run 4, 2026-10-14): the
installer's own self-CompositionDefinition resolves its chart from the
registry at .Chart.Version (templates/self-bootstrap.yaml:215), and the cdc
ServiceAccount / CR version are derived from .Chart.Version too
(self-bootstrap.yaml:71-72, ordered-teardown.yaml:30). A locally stamped
version (0.0.0-pr483) does not exist in the registry, so the install can
never converge. run606.sh therefore refuses INSTALLER_GIT_REF /
INSTALLER_CHART_DIR; `prepare` / `preflight` stay only as building blocks
(oci-preflight reuses `preflight` on the pulled chart).

STDLIB + PyYAML only, no bench imports (it runs before any cluster exists).

prepare    git clone -b <ref> <url> <dest>; stamp CHART_VERSION in
           chart/Chart.yaml; `helm dependency update <dest>/chart`.
preflight  Refuse the run unless the chart directory is the branch we mean:
           - chart/Chart.yaml has a concrete version (CHART_VERSION stamped);
           - the dependency charts are vendored under chart/charts/;
           - every --require-pin assertion holds against
             chart/files/component-pins.yaml. For #483:
               alert-provider-crd.feature == portal
               krateo-observability.deps contains alert-provider-crd
           Runs BEFORE the cluster is created, so a wrong branch costs nothing.
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys

import yaml

__all__ = ["PR483_REQUIREMENTS", "check_pins", "preflight", "prepare", "main"]

INSTALLER_REPO = "https://github.com/krateo-platformops/installer.git"

# (component, field, op, value)
PR483_REQUIREMENTS = (
    ("alert-provider-crd", "feature", "==", "portal"),
    ("krateo-observability", "deps", "contains", "alert-provider-crd"),
)


def _parse_req(s: str) -> tuple[str, str, str, str]:
    """`comp.field==value` or `comp.field~=value` (contains)."""
    op = "contains" if "~=" in s else "=="
    lhs, value = s.split("~=" if op == "contains" else "==", 1)
    comp, field = lhs.rsplit(".", 1)
    return comp, field, op, value


def check_pins(pins: dict, requirements) -> list[str]:
    """Return a list of failures (empty = all requirements hold)."""
    comps = {c.get("name"): c for c in (pins or {}).get("components") or []}
    fails = []
    for comp, field, op, value in requirements:
        c = comps.get(comp)
        if c is None:
            fails.append(f"{comp}: not in component-pins.yaml")
            continue
        got = c.get(field)
        if op == "==" and got != value:
            fails.append(f"{comp}.{field} == {got!r}, required {value!r}")
        if op == "contains" and value not in (got or []):
            fails.append(f"{comp}.{field} = {got!r}, required to contain {value!r}")
    return fails


def preflight(chart_dir: str, requirements=PR483_REQUIREMENTS) -> dict:
    chart_yaml = os.path.join(chart_dir, "Chart.yaml")
    pins_yaml = os.path.join(chart_dir, "files", "component-pins.yaml")
    fails: list[str] = []
    meta: dict = {}
    if not os.path.isfile(chart_yaml):
        fails.append(f"{chart_yaml} missing (pass the repo's chart/ directory)")
    else:
        meta = yaml.safe_load(open(chart_yaml)) or {}
        if "CHART_VERSION" in str(meta.get("version")):
            fails.append("Chart.yaml version still CHART_VERSION (not stamped)")
        deps = [d.get("name") for d in meta.get("dependencies") or []]
        vendored = os.listdir(os.path.join(chart_dir, "charts")) \
            if os.path.isdir(os.path.join(chart_dir, "charts")) else []
        for d in deps:
            if not any(v.startswith(d) for v in vendored):
                fails.append(f"dependency {d} not vendored (run helm dependency update)")
    if not os.path.isfile(pins_yaml):
        fails.append(f"{pins_yaml} missing")
    else:
        fails += check_pins(yaml.safe_load(open(pins_yaml)), requirements)
    return {"chart_dir": chart_dir, "version": meta.get("version"),
            "requirements": [list(r) for r in requirements],
            "ok": not fails, "failures": fails}


def oci_preflight(chart_ref: str, version: str, workdir: str,
                  requirements=PR483_REQUIREMENTS, run=subprocess.run) -> dict:
    """Registry preflight for the ORDINARY install path (run 5+).

    1. `helm show chart <ref> --version <v>` must resolve (published, pullable);
    2. the pulled chart's files/component-pins.yaml must satisfy `requirements`
       (default: the #483 gate — alert-provider-crd.feature == portal,
       krateo-observability.deps contains alert-provider-crd).
    Read-only (ghcr only); runs before any cluster exists.
    """
    res: dict = {"chart": chart_ref, "version": version, "ok": False, "failures": []}
    show = run(["helm", "show", "chart", chart_ref, "--version", version],
               capture_output=True, text=True)
    if show.returncode != 0:
        res["failures"].append(f"helm show chart {chart_ref} --version {version} "
                               f"failed: {(show.stderr or '').strip()[:300]}")
        return res
    meta = yaml.safe_load(show.stdout or "") or {}
    res["resolved_version"] = meta.get("version")
    if str(meta.get("version")) != str(version):
        res["failures"].append(f"resolved version {meta.get('version')!r} != {version!r}")
        return res
    os.makedirs(workdir, exist_ok=True)
    pull = run(["helm", "pull", chart_ref, "--version", version, "--untar",
                "-d", workdir], capture_output=True, text=True)
    if pull.returncode != 0:
        res["failures"].append(f"helm pull failed: {(pull.stderr or '').strip()[:300]}")
        return res
    sub = [d for d in os.listdir(workdir) if os.path.isdir(os.path.join(workdir, d))]
    if len(sub) != 1:
        res["failures"].append(f"unexpected pull layout in {workdir}: {sub}")
        return res
    inner = preflight(os.path.join(workdir, sub[0]), requirements)
    res["failures"] += inner["failures"]
    res["requirements"] = inner["requirements"]
    res["ok"] = not res["failures"]
    return res


def prepare(ref: str, dest: str, version: str, repo: str = INSTALLER_REPO,
            run=subprocess.run) -> str:
    """Clone + stamp + vendor. Returns the chart directory."""
    if os.path.exists(dest):
        raise RuntimeError(f"{dest} exists; use a fresh directory")
    run(["git", "clone", "--depth", "1", "-b", ref, repo, dest], check=True)
    chart = os.path.join(dest, "chart")
    cy = os.path.join(chart, "Chart.yaml")
    with open(cy) as fh:
        txt = fh.read()
    with open(cy, "w") as fh:
        fh.write(txt.replace("CHART_VERSION", version))
    run(["helm", "dependency", "update", chart], check=True)
    head = run(["git", "-C", dest, "rev-parse", "HEAD"], check=True,
               capture_output=True, text=True)
    with open(os.path.join(dest, "SOURCE.json"), "w") as fh:
        json.dump({"repo": repo, "ref": ref, "commit": (head.stdout or "").strip(),
                   "version": version}, fh)
    return chart


def main(argv=None) -> int:
    p = argparse.ArgumentParser(prog="python -m bench.installer_src")
    sub = p.add_subparsers(dest="cmd", required=True)
    pr = sub.add_parser("prepare")
    pr.add_argument("--ref", required=True)
    pr.add_argument("--dest", required=True)
    pr.add_argument("--version", required=True)
    pr.add_argument("--repo", default=INSTALLER_REPO)
    op = sub.add_parser("oci-preflight")
    op.add_argument("--chart", required=True)
    op.add_argument("--version", required=True)
    op.add_argument("--workdir", required=True)
    op.add_argument("--out", default=None)
    op.add_argument("--no-pins", action="store_true",
                    help="only check the chart resolves (no #483 pin assertions)")
    pf = sub.add_parser("preflight")
    pf.add_argument("--chart-dir", required=True)
    pf.add_argument("--require-pin", action="append", default=None,
                    help="comp.field==value or comp.field~=value; default: the #483 set")
    pf.add_argument("--out", default=None)
    a = p.parse_args(argv)
    if a.cmd == "prepare":
        print(prepare(a.ref, a.dest, a.version, a.repo))
        return 0
    if a.cmd == "oci-preflight":
        res = oci_preflight(a.chart, a.version, a.workdir,
                            () if a.no_pins else PR483_REQUIREMENTS)
        if a.out:
            with open(a.out, "w") as fh:
                json.dump(res, fh, indent=2)
        print(json.dumps(res, indent=2), file=sys.stderr)
        if not res["ok"]:
            print("installer registry preflight REFUSED: " + "; ".join(res["failures"]),
                  file=sys.stderr)
            return 2
        return 0
    reqs = ([_parse_req(r) for r in a.require_pin] if a.require_pin
            else PR483_REQUIREMENTS)
    res = preflight(a.chart_dir, reqs)
    if a.out:
        with open(a.out, "w") as fh:
            json.dump(res, fh, indent=2)
    print(json.dumps(res, indent=2), file=sys.stderr)
    if not res["ok"]:
        print("installer preflight REFUSED: " + "; ".join(res["failures"]),
              file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    sys.exit(main())
