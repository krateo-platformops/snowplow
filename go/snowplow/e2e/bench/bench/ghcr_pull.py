"""The read-only ghcr pull token the bench installs with, exactly as 057 does
(run 19: the agentgateway charts are PRIVATE, krateo-agentiko/*).

Run 19 failed at the platform step. CompositionDefinitions
agentgateway-controller 0.2.0 and agentgateway-policies 0.2.18 stayed
Synced=False on `401 unauthorized` from oci://ghcr.io/krateo-agentiko/charts.
057 pulls them through the installer's `registryAuth`: enabled, a username,
and a passwordRef to a Secret holding a read:packages token. The installer
stamps every CompositionDefinition's spec.chart.credentials from it
(_helpers.tpl inst.chartExtras) and derives an image-pull Secret from it
(templates/imagepull-secret.yaml). The bench does the same.

THE FILE, written by the operator (never by the bench), mode 0600:

    ~/.config/snowplow-bench/ghcr-pull.env
        GHCR_PULL_USERNAME=<github user>
        GHCR_PULL_TOKEN=<a token with read:packages only>

The TOKEN never enters argv, a shell variable, a log, the run dir or a
diagnostic. `token` writes it to stdout for a pipe into `kubectl create
secret ... --from-file=token=/dev/stdin`, and `username` writes the username
into a private values file outside the run dir. The preflight authenticates
with the token in-process.

    python3 -m bench.ghcr_pull check --env-file F              # exit 2 + what to create
    python3 -m bench.ghcr_pull preflight --env-file F --installer-pulled D --values V --out OUT.json
    python3 -m bench.ghcr_pull username --env-file F > PRIVATE.yaml
    python3 -m bench.ghcr_pull token --env-file F | kubectl create secret generic ... --from-file=token=/dev/stdin
"""

from __future__ import annotations

import argparse
import base64
import json
import os
import stat
import sys
import urllib.error
import urllib.request

KEYS = ("GHCR_PULL_USERNAME", "GHCR_PULL_TOKEN")
#: the Secret registryAuth.passwordRef names, as on 057 (installer-platform values): name, key, namespace
SECRET_NAME, SECRET_KEY, SECRET_NS = "krateo-agentiko-pull", "token", "krateo-system"
#: the registry API (a test seam: the suite's fake registry)
API = os.environ.get("GHCR_API_URL", "https://ghcr.io")
WHAT_TO_CREATE = ("create it with a GitHub token that has ONLY read:packages:\n"
                  "  mkdir -p ~/.config/snowplow-bench && umask 077 && cat > {path} <<'EOF'\n"
                  "  GHCR_PULL_USERNAME=<your github user>\n  GHCR_PULL_TOKEN=<the token>\n  EOF\n"
                  "  chmod 600 {path}")


def _parse(path: str) -> dict:
    out = {}
    with open(path) as fh:
        for ln in fh:
            ln = ln.strip()
            if not ln or ln.startswith("#") or "=" not in ln:
                continue
            k, _, v = ln.partition("=")
            out[k.strip()] = v.strip().strip('"').strip("'")
    return out


def check(path: str) -> str | None:
    """None when the file is usable, else why not (never the token)."""
    if not os.path.isfile(path):
        return f"{path} does not exist"
    mode = stat.S_IMODE(os.stat(path).st_mode)
    if mode != 0o600:
        return f"{path} has mode {mode:04o}, not 0600"
    try:
        kv = _parse(path)
    except OSError as e:
        return f"{path} is not readable: {e.strerror}"
    missing = [k for k in KEYS if not kv.get(k)]
    if missing:
        return f"{path} lacks {' and '.join(missing)}"
    return None


def creds(path: str) -> tuple[str, str]:
    kv = _parse(path)
    return kv["GHCR_PULL_USERNAME"], kv["GHCR_PULL_TOKEN"]


def private_pins(installer_pulled: str, values: str) -> list[dict]:
    """The pinned charts this install pulls from a repo of their own (the
    repo-override pins: private krateo-agentiko/*), for the features the
    install enables: the installer's defaults, overridden by the bench's
    values."""
    import yaml
    root = os.path.join(installer_pulled, "installer")
    with open(os.path.join(root, "files", "component-pins.yaml")) as fh:
        pins = yaml.safe_load(fh) or {}
    try:
        with open(os.path.join(root, "values.yaml")) as fh:
            features = dict((yaml.safe_load(fh) or {}).get("features") or {})
    except OSError:
        features = {}
    with open(values) as fh:
        features.update((yaml.safe_load(fh) or {}).get("features") or {})
    out = []
    for c in pins.get("components") or []:
        if isinstance(c, dict) and c.get("repo") and features.get(c.get("feature", ""), False):
            out.append({"name": c["name"], "chart": c.get("chart") or c["name"], "repo": c["repo"],
                        "version": str(c.get("version"))})
    return out


def resolve(repo: str, chart: str, version: str, user: str, token: str) -> str | None:
    """None when oci://<repo>/<chart>:<version> resolves with these credentials
    (the registry's token exchange, then the manifest), else why not."""
    if not repo.startswith("oci://"):
        return f"{repo} is not an OCI repo"
    host, _, path = repo[len("oci://"):].partition("/")
    name = f"{path}/{chart}".strip("/")
    basic = base64.b64encode(f"{user}:{token}".encode()).decode()
    try:
        req = urllib.request.Request(f"{API}/token?scope=repository:{name}:pull&service={host}",
                                     headers={"Authorization": f"Basic {basic}"})
        with urllib.request.urlopen(req, timeout=30) as r:
            bearer = json.loads(r.read() or b"{}").get("token")
        if not bearer:
            return f"{host} gave no token for {name}"
        req = urllib.request.Request(
            f"{API}/v2/{name}/manifests/{version}", method="HEAD",
            headers={"Authorization": f"Bearer {bearer}",
                     "Accept": "application/vnd.oci.image.manifest.v1+json"})
        with urllib.request.urlopen(req, timeout=30):
            return None
    except urllib.error.HTTPError as e:
        return f"oci://{host}/{name}:{version}: HTTP {e.code} {e.reason}"
    except (urllib.error.URLError, OSError) as e:
        return f"oci://{host}/{name}:{version}: {getattr(e, 'reason', e)}"


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="bench.ghcr_pull")
    sub = ap.add_subparsers(dest="cmd", required=True)
    for n in ("check", "username", "token"):
        sub.add_parser(n).add_argument("--env-file", required=True)
    pf = sub.add_parser("preflight")
    pf.add_argument("--env-file", required=True)
    pf.add_argument("--installer-pulled", required=True)
    pf.add_argument("--values", required=True)
    pf.add_argument("--out", required=True)
    a = ap.parse_args(argv)
    why = check(a.env_file)
    if why:
        print(f"refusing: the ghcr pull token: {why}; " + WHAT_TO_CREATE.format(path=a.env_file), file=sys.stderr)
        return 2
    if a.cmd == "check":
        return 0
    user, token = creds(a.env_file)
    if a.cmd == "username":
        sys.stdout.write("registryAuth:\n  username: " + json.dumps(user) + "\n")
        return 0
    if a.cmd == "token":
        sys.stdout.write(token)
        return 0
    pins = private_pins(a.installer_pulled, a.values)
    res = [{"chart": f"{p['repo']}/{p['chart']}", "version": p["version"],
            "error": resolve(p["repo"], p["chart"], p["version"], user, token)} for p in pins]
    with open(a.out, "w") as fh:
        json.dump({"kind": "ghcr_pull_preflight", "charts": res}, fh, indent=1)
    bad = [r for r in res if r["error"]]
    for r in res:
        print(f"[ghcr] {r['chart']} {r['version']}: {'OK' if not r['error'] else r['error']}", file=sys.stderr)
    if bad:
        print("refusing: the ghcr pull token does not resolve every private chart this install pulls: "
              + "; ".join(r["error"] for r in bad), file=sys.stderr)
        return 3
    return 0


if __name__ == "__main__":
    sys.exit(main())
