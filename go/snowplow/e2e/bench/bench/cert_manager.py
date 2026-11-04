"""cert-manager before the installer, exactly as krateo-057 has it (installer#614).

WHY. Installer 0.3.506 ships cert-manager only under `features.ingress`. The
ClickHouse and MongoDB operators need the `cert-manager.io` API group, so on a
fresh cluster with ingress off they, and the 5 components that depend on them,
are never created: paired run 21 reported 33 of 40 declared components
created (installer#614). krateo-057 works only because cert-manager was
installed there BY HAND, outside the installer. The bench mirrors that hand
install, and nothing else: this module and its two steps in run606.sh go away
once the installer ships cert-manager itself (installer#614).

WHAT 057 HAS (read read-only, 2026-11-04): helm release `cert-manager`, chart
`cert-manager-v1.21.1` (app v1.21.1), namespace `cert-manager`, user values
exactly `crds: {enabled: true}`. Those four are pinned below and nothing else
is set; the chart's own defaults do the rest, as on 057.

THE CHART. The upstream jetstack chart, from the registry cert-manager's own
install documentation names (oci://quay.io/jetstack/charts/cert-manager), at
v1.21.1, verified by the sha256 of its archive, the way the bench verifies
every other artifact it runs (EXPECTED_IMAGE_DIGEST, bench/ghcr_pull.py's
digest-verified chart layers): the archive is pulled once, before any cluster
exists, and run606 installs THAT verified file, never a second pull. The same
archive is what https://charts.jetstack.io serves for v1.21.1 (byte-identical,
checked 2026-11-04 when the digest was pinned; its .prov signs it with the
cert-manager maintainers' key 1020CF3C033D4F35BAE1C19E1226061C665DF13E).

preflight  (pre-cluster, dry pass too) pull, verify the digest and the
           archive's own Chart.yaml, write the values file, and derive from
           the verified chart, rendered with exactly those values, the
           Deployments and CRDs run606 waits for. Refused unless they include
           the cert-manager.io group (the reason it is installed at all).
record     (after the install) the run's record: what helm READS BACK from
           the cluster (release, chart, app version, status, user values),
           compared field by field with 057's reading. A mismatch fails the
           step: the run would not mirror 057.

STDLIB + PyYAML only, no bench imports (it runs before any cluster exists).
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import subprocess
import sys
import tarfile
import time

import yaml

__all__ = ["CHART_REF", "CHART_NAME", "CHART_VERSION", "CHART_SHA256", "RELEASE",
           "NAMESPACE", "VALUES", "AT_057", "preflight", "readback", "main"]

# installer#614: the pins (see the module docstring for where each comes from)
CHART_REF = "oci://quay.io/jetstack/charts/cert-manager"
CHART_NAME = "cert-manager"
CHART_VERSION = "v1.21.1"
# sha256 of cert-manager-v1.21.1.tgz (helm pull of CHART_REF at CHART_VERSION,
# 2026-11-04; its OCI manifest digest was sha256:15c0b46d9006ce8eb9ff14d1bf54d1bb
# fcc587bb9e24cd9fe186fb8fec56af1f)
CHART_SHA256 = "c27101f3f3e2349fb4a9e704316105bf7b52ad73b8c8257d3498ef7f2f6a4adc"
RELEASE = "cert-manager"
NAMESPACE = "cert-manager"
VALUES = {"crds": {"enabled": True}}
# the API group the ClickHouse and MongoDB operators require (installer#614)
REQUIRED_GROUP = "cert-manager.io"

#: krateo-057's hand install, as read there (read-only, 2026-11-04)
AT_057 = {"release": "cert-manager", "namespace": "cert-manager", "chart": "cert-manager-v1.21.1",
            "app_version": "v1.21.1", "values": {"crds": {"enabled": True}}}
MIRRORS = ("krateo-057's hand install, outside the installer (helm release cert-manager, chart "
           "cert-manager-v1.21.1, app v1.21.1, namespace cert-manager, user values crds.enabled=true; "
           "read read-only 2026-11-04)")
REASON = ("installer#614: installer 0.3.506 ships cert-manager only under features.ingress; the ClickHouse "
          "and MongoDB operators need cert-manager.io, so with ingress off they and their 5 dependents are "
          "never created (run 21: 33 of 40 declared components). Goes away once the installer ships "
          "cert-manager itself.")


def _sha256(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 16), b""):
            h.update(chunk)
    return h.hexdigest()


def _chart_yaml(tgz: str) -> dict:
    """The archive's own <name>/Chart.yaml (read from the verified bytes)."""
    with tarfile.open(tgz, "r:gz") as tf:
        member = tf.getmember(f"{CHART_NAME}/Chart.yaml")
        return yaml.safe_load(tf.extractfile(member).read()) or {}


def rendered_waits(manifest: str) -> tuple[list[str], list[str]]:
    """(Deployments, CRDs) of a rendered chart, sorted: what run606 waits for."""
    deps, crds = set(), set()
    for d in yaml.safe_load_all(manifest):
        if not isinstance(d, dict):
            continue
        name = (d.get("metadata") or {}).get("name")
        if d.get("kind") == "Deployment" and name:
            deps.add(name)
        elif d.get("kind") == "CustomResourceDefinition" and name:
            crds.add(name)
    return sorted(deps), sorted(crds)


def preflight(workdir: str, run=subprocess.run) -> dict:
    """Pull the pinned chart into `workdir`, verify it, render it with VALUES.
    The record says what was refused, if anything (`ok` False)."""
    res: dict = {"kind": "cluster_prerequisite", "name": "cert-manager", "chart_ref": CHART_REF,
                 "version": CHART_VERSION, "chart_sha256": CHART_SHA256, "release": RELEASE,
                 "namespace": NAMESPACE, "values": VALUES, "mirrors": MIRRORS, "reason": REASON,
                 "ok": False, "failures": []}
    os.makedirs(workdir, exist_ok=True)
    for f in os.listdir(workdir):                 # a previous run's archive never counts
        if f.endswith(".tgz"):
            os.remove(os.path.join(workdir, f))
    pull = run(["helm", "pull", CHART_REF, "--version", CHART_VERSION, "-d", workdir],
               capture_output=True, text=True)
    if pull.returncode != 0:
        res["failures"].append(f"helm pull {CHART_REF} --version {CHART_VERSION} failed: "
                               f"{(pull.stderr or '').strip()[:300]}")
        return res
    tgzs = sorted(f for f in os.listdir(workdir) if f.endswith(".tgz"))
    if tgzs != [f"{CHART_NAME}-{CHART_VERSION}.tgz"]:
        res["failures"].append(f"unexpected pull layout in {workdir}: {tgzs}")
        return res
    tgz = os.path.join(workdir, tgzs[0])
    got = _sha256(tgz)
    res["pulled_sha256"] = got
    if got != CHART_SHA256:
        res["failures"].append(f"{tgzs[0]} sha256 {got} != the pinned {CHART_SHA256}")
        os.remove(tgz)                            # never installable by mistake
        return res
    meta = _chart_yaml(tgz)
    res["app_version"] = meta.get("appVersion")
    if meta.get("name") != CHART_NAME or str(meta.get("version")) != CHART_VERSION:
        res["failures"].append(f"the archive's Chart.yaml is {meta.get('name')!r} {meta.get('version')!r}, "
                               f"not {CHART_NAME!r} {CHART_VERSION!r}")
        return res
    values = os.path.join(workdir, "values.yaml")
    with open(values, "w") as fh:
        yaml.safe_dump(VALUES, fh, default_flow_style=False)
    tpl = run(["helm", "template", RELEASE, tgz, "--namespace", NAMESPACE, "-f", values],
              capture_output=True, text=True)
    if tpl.returncode != 0:
        res["failures"].append(f"helm template of the verified chart failed: {(tpl.stderr or '').strip()[:300]}")
        return res
    deps, crds = rendered_waits(tpl.stdout or "")
    res["deployments"], res["crds"] = deps, crds
    if not deps:
        res["failures"].append("the verified chart, rendered with its values, has no Deployment")
    if not any(c.endswith("." + REQUIRED_GROUP) and c.count(".") == REQUIRED_GROUP.count(".") + 1
               for c in crds):
        res["failures"].append(f"the verified chart, rendered with its values, has no {REQUIRED_GROUP} CRD")
    res["chart_file"], res["values_file"] = tgz, values
    res["ok"] = not res["failures"]
    return res


def readback(pre: dict, helm_list: list, helm_values: dict | None) -> dict:
    """The run's record, from what helm reads back from the cluster, compared
    with 057's reading field by field."""
    rel = next((r for r in helm_list or [] if r.get("name") == RELEASE), None) or {}
    seen = {"release": rel.get("name"), "namespace": rel.get("namespace"), "chart": rel.get("chart"),
            "app_version": rel.get("app_version"), "values": helm_values}
    mism = [f"{k}: {seen[k]!r} (057: {AT_057[k]!r})" for k in AT_057 if seen[k] != AT_057[k]]
    if rel.get("status") != "deployed":
        mism.append(f"status: {rel.get('status')!r} (expected 'deployed')")
    rec = {k: pre.get(k) for k in ("kind", "name", "chart_ref", "version", "chart_sha256", "app_version",
                                   "mirrors", "reason", "deployments", "crds")}
    rec.update(t=time.time(), installed=dict(seen, status=rel.get("status")),
               mirrors_057=not mism, mismatches=mism)
    return rec


def main(argv=None) -> int:
    p = argparse.ArgumentParser(prog="python -m bench.cert_manager")
    sub = p.add_subparsers(dest="cmd", required=True)
    pf = sub.add_parser("preflight")
    pf.add_argument("--workdir", required=True)
    pf.add_argument("--out", required=True)
    rc = sub.add_parser("record")
    rc.add_argument("--preflight", required=True, help="the preflight's record")
    rc.add_argument("--helm-list", required=True, help="helm list -n cert-manager -o json")
    rc.add_argument("--helm-values", required=True, help="helm get values cert-manager -n cert-manager -o json")
    rc.add_argument("--out", required=True, help="the run's record (jsonl, appended)")
    a = p.parse_args(argv)
    if a.cmd == "preflight":
        res = preflight(a.workdir)
        with open(a.out, "w") as fh:
            json.dump(res, fh, indent=2)
        if not res["ok"]:
            print("cert-manager preflight REFUSED: " + "; ".join(res["failures"]), file=sys.stderr)
            return 2
        print(f"cert-manager {CHART_VERSION} (app {res['app_version']}): {CHART_REF}, sha256 {CHART_SHA256} "
              f"verified; mirrors krateo-057's hand install (installer#614); waits for deployments "
              f"{' '.join(res['deployments'])} and CRDs {' '.join(res['crds'])}", file=sys.stderr)
        return 0
    with open(a.preflight) as fh:
        pre = json.load(fh)

    def _json(path):
        try:
            with open(path) as fh:
                return json.load(fh)
        except (OSError, ValueError):
            return None
    rec = readback(pre, _json(a.helm_list) or [], _json(a.helm_values))
    with open(a.out, "a") as fh:
        fh.write(json.dumps(rec) + "\n")
    if not rec["mirrors_057"]:
        print("cert-manager does NOT mirror krateo-057's hand install: " + "; ".join(rec["mismatches"]),
              file=sys.stderr)
        return 2
    print(f"cert-manager installed: release {RELEASE}, chart {rec['installed']['chart']}, app "
          f"{rec['installed']['app_version']}, namespace {NAMESPACE}, values crds.enabled=true: mirrors "
          f"krateo-057's hand install (installer#614)", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
