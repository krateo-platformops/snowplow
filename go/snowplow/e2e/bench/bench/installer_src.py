"""#606 installer source for the bench: the published OCI chart, or a LOCAL
chart built from an installer git branch (run 4: installer#483, branch
fix/482-alert-crd-gate, unreleased).

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
    pf = sub.add_parser("preflight")
    pf.add_argument("--chart-dir", required=True)
    pf.add_argument("--require-pin", action="append", default=None,
                    help="comp.field==value or comp.field~=value; default: the #483 set")
    pf.add_argument("--out", default=None)
    a = p.parse_args(argv)
    if a.cmd == "prepare":
        print(prepare(a.ref, a.dest, a.version, a.repo))
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
