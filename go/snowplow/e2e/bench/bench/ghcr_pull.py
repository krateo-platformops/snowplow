"""The read-only ghcr pull token the bench installs with, exactly as 057 does
(run 19: the agentgateway charts are PRIVATE, krateo-agentiko/*).

Run 19 failed at the platform step. CompositionDefinitions
agentgateway-controller 0.2.0 and agentgateway-policies 0.2.18 stayed
Synced=False on `401 unauthorized` from oci://ghcr.io/krateo-agentiko/charts.
057 pulls them through the installer's `registryAuth`: enabled, a username,
and a passwordRef to a Secret holding a read:packages token. The installer
stamps every CompositionDefinition's spec.chart.credentials from it
(_helpers.tpl inst.chartExtras) and derives an image-pull Secret from it
(templates/imagepull-secret.yaml). The bench does the same for the charts.

THE IMAGES (run 19 attempt 2): installer 0.3.468 renders its derived
image-pull Secret only when coreAgents or specialistAgents is on, yet injects
its name into agentgateway-policies' private hop pods (ui-token-hop,
cert-replay-hop): with only agentGateway on they sat in ImagePullBackOff for
43 min (fixed in the installer's PR 599). The bench uses the installer's
documented opt-out: it creates a kubernetes.io/dockerconfigjson Secret
(IMAGE_PULL_SECRET, krateo-system, host ghcr.io) from the same file and sets
registryAuth.imagePullSecretName to it, so the installer derives none and
injects that name. Once PR 599 ships the derived path works too; switching
back is a later decision.

THE FILE, written by the operator (never by the bench), mode 0600:

    ~/.config/snowplow-bench/ghcr-pull.env   (or the path GHCR_PULL_ENV_FILE names)
        GHCR_PULL_USERNAME=<github user>
        GHCR_PULL_TOKEN=<a token with read:packages only>

The TOKEN never enters argv, a shell variable, a log, the run dir or a
diagnostic. `token` writes it to stdout for a pipe into `kubectl create
secret ... --from-file=token=/dev/stdin`, and `username` writes a values
document for a pipe into `helm install ... -f -`: no file holds either. The
preflight authenticates with the token in-process, over https only, and
follows no redirect (an Authorization header is never sent to a host the
registry redirects to). bench.diag606 redacts both, as literals, from every
bundle and log it writes, and redacts the username by key from the objects
that carry it (the Installer CR's registryAuth, chart.credentials).

    python3 -m bench.ghcr_pull check --env-file F              # exit 2 + what to create
    python3 -m bench.ghcr_pull preflight --env-file F --installer-pulled D --values V --out OUT.json
    python3 -m bench.ghcr_pull username --env-file F | helm install ... -f values.yaml -f -
    python3 -m bench.ghcr_pull token --env-file F | kubectl create secret generic ... --from-file=token=/dev/stdin
    python3 -m bench.ghcr_pull dockerconfigjson --env-file F \
      | kubectl create secret generic ... --type=kubernetes.io/dockerconfigjson --from-file=.dockerconfigjson=/dev/stdin

The dockerconfigjson (and its base64 user:token) is built in-process and
written only to that pipe.

The preflight resolves, before any cluster exists:
  - every repo-override chart the installer turns on (see private_pins);
  - every ghcr.io IMAGE those charts' own values name (chart_images): the
    chart is fetched in-process from the registry with the token (manifest,
    then its helm layer, digest-verified), and every `image` its values.yaml
    (subcharts included) names is resolved, an empty tag being the chart's
    appVersion, as the charts template it. An image the token cannot read is
    refused here, never as an ImagePullBackOff on a paid cluster.
"""

from __future__ import annotations

import argparse
import base64
import hashlib
import io
import json
import os
import stat
import sys
import tarfile
import urllib.error
import urllib.request

KEYS = ("GHCR_PULL_USERNAME", "GHCR_PULL_TOKEN")
DEFAULT_ENV_FILE = os.path.expanduser("~/.config/snowplow-bench/ghcr-pull.env")
#: the Secret registryAuth.passwordRef names, as on 057 (installer-platform values): name, key, namespace
SECRET_NAME, SECRET_KEY, SECRET_NS = "krateo-agentiko-pull", "token", "krateo-system"
#: the bench's image-pull Secret (kubernetes.io/dockerconfigjson, in SECRET_NS), named by
#: registryAuth.imagePullSecretName: the installer's documented opt-out of deriving its own
IMAGE_PULL_SECRET = "bench-ghcr-image-pull"
#: the registry the token authenticates: a GitHub token is a ghcr.io credential, nothing else's
REGISTRY_HOST = "ghcr.io"
MANIFEST_TYPES = ", ".join(("application/vnd.oci.image.index.v1+json", "application/vnd.oci.image.manifest.v1+json",
                            "application/vnd.docker.distribution.manifest.list.v2+json",
                            "application/vnd.docker.distribution.manifest.v2+json"))
HELM_CHART_LAYER = "application/vnd.cncf.helm.chart.content.v1.tar+gzip"
#: the registry API. GHCR_API_URL may point it elsewhere (the suite's fake
#: registry), over https only: the token is never sent in clear text.
API = os.environ.get("GHCR_API_URL", "https://ghcr.io")


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    """A redirect is an error: urllib would carry the Authorization header to
    whatever host the registry names (#1007 review F6)."""
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        raise urllib.error.HTTPError(req.full_url, code, f"redirect to {newurl.split('?')[0]} refused",
                                     headers, fp)


_OPEN = urllib.request.build_opener(_NoRedirect).open
WHAT_TO_CREATE = ("create it with a GitHub token that has ONLY read:packages:\n"
                  "  mkdir -p {dir} && umask 077 && cat > {path} <<'EOF'\n"
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
    """Every pinned chart this install pulls from a repo of its OWN (the
    repo-override pins: krateo-agentiko/*, krateo-blueprints/*, ...), whose
    component the installer enables, by its own rule (inst.featureEnabled):
    a pin with no feature is on; a feature in the chart's inst.coreFeatures is
    always on; else the install's features (the installer's defaults,
    overridden by the bench's values). The token must resolve each one: a
    public chart resolves with any valid token, a private one only with
    access."""
    import yaml

    from bench import diag606
    root = os.path.join(installer_pulled, "installer")
    core = set(diag606.core_features(installer_pulled))
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
        if not (isinstance(c, dict) and c.get("repo")):
            continue
        f = c.get("feature") or ""
        if not f or f in core or features.get(f, False):
            out.append({"name": c["name"], "chart": c.get("chart") or c["name"], "repo": c["repo"],
                        "version": str(c.get("version"))})
    return out


class _Refused(Exception):
    """Why a reference does not resolve (never carries a credential)."""


def _exchange(host: str, name: str, user: str, token: str) -> str:
    """The registry's bearer for pulling `name` (its token exchange)."""
    if not API.startswith("https://"):
        raise _Refused(f"the registry API {API!r} is not https: the token is never sent in clear text")
    basic = base64.b64encode(f"{user}:{token}".encode()).decode()
    req = urllib.request.Request(f"{API}/token?scope=repository:{name}:pull&service={host}",
                                 headers={"Authorization": f"Basic {basic}"})
    with _OPEN(req, timeout=30) as r:
        try:
            bearer = json.loads(r.read() or b"{}").get("token")
        except ValueError:
            raise _Refused(f"{host} answered the token exchange for {name} with no JSON") from None
    if not bearer:
        raise _Refused(f"{host} gave no token for {name}")
    return bearer


def _ref_error(ref: str, fn):
    """fn() with every failure as `ref: why` (an error string), never raised."""
    try:
        return fn()
    except _Refused as e:
        return f"{ref}: {e}" if not str(e).startswith(("the registry API", ref)) else str(e)
    except urllib.error.HTTPError as e:
        return f"{ref}: HTTP {e.code} {e.reason}"
    except (urllib.error.URLError, OSError) as e:
        return f"{ref}: {getattr(e, 'reason', e)}"


def _head_manifest(host: str, name: str, tag: str, user: str, token: str) -> None:
    bearer = _exchange(host, name, user, token)
    req = urllib.request.Request(f"{API}/v2/{name}/manifests/{tag}", method="HEAD",
                                 headers={"Authorization": f"Bearer {bearer}", "Accept": MANIFEST_TYPES})
    with _OPEN(req, timeout=30):
        return None


def _split_repo(repo: str, chart: str) -> tuple[str, str]:
    host, _, path = repo[len("oci://"):].partition("/")
    return host, f"{path}/{chart}".strip("/")


def resolve(repo: str, chart: str, version: str, user: str, token: str) -> str | None:
    """None when oci://<repo>/<chart>:<version> resolves with these credentials
    (the registry's token exchange, then the manifest), else why not."""
    if not repo.startswith("oci://"):
        return f"{repo} is not an OCI repo"
    host, name = _split_repo(repo, chart)
    return _ref_error(f"oci://{host}/{name}:{version}", lambda: _head_manifest(host, name, version, user, token))


def split_image(ref: str) -> tuple[str, str, str]:
    """(host, name, tag-or-digest) of an image reference with an explicit registry host."""
    host, _, rest = ref.partition("/")
    if "@" in rest:
        name, _, tag = rest.partition("@")
    else:
        name, _, tag = rest.rpartition(":") if ":" in rest else (rest, "", "latest")
    return host, name, tag


def resolve_image(ref: str, user: str, token: str) -> str | None:
    """None when the image's manifest resolves with these credentials, else why not."""
    host, name, tag = split_image(ref)
    return _ref_error(ref, lambda: _head_manifest(host, name, tag, user, token))


def _blob(host: str, name: str, digest: str, bearer: str) -> bytes:
    """A blob, digest-verified. The registry answers with a redirect to its
    blob storage: followed over https only and WITHOUT the Authorization
    header (the signed URL is the credential), like every registry client."""
    req = urllib.request.Request(f"{API}/v2/{name}/blobs/{digest}", headers={"Authorization": f"Bearer {bearer}"})
    try:
        with _OPEN(req, timeout=60) as r:
            data = r.read()
    except urllib.error.HTTPError as e:
        loc = e.headers.get("Location") if e.code in (301, 302, 303, 307, 308) and e.headers else None
        if not loc:
            raise
        if not loc.startswith("https://"):
            raise _Refused(f"blob {digest[:19]} redirected off https") from None
        with _OPEN(urllib.request.Request(loc), timeout=60) as r:
            data = r.read()
    algo, _, want = digest.partition(":")
    if algo != "sha256" or hashlib.sha256(data).hexdigest() != want:
        raise _Refused(f"blob {digest[:19]} does not match its digest")
    return data


def images_in_values(values, app_version: str) -> list[str]:
    """Every image a chart's values name, as the charts template them: a dict
    under an `image` key with a `repository` (an optional `registry` before
    it; its `tag`, else its `digest`, else the chart's appVersion), or a
    string under an `image` key. Sorted, unique."""
    out = set()

    def walk(o, key):
        if isinstance(o, dict):
            if key == "image" and isinstance(o.get("repository"), str) and o["repository"]:
                repo = o["repository"]
                if isinstance(o.get("registry"), str) and o["registry"]:
                    repo = f"{o['registry'].rstrip('/')}/{repo}"
                if o.get("tag"):
                    out.add(f"{repo}:{o['tag']}")
                elif o.get("digest"):
                    out.add(f"{repo}@{o['digest']}")
                else:
                    out.add(f"{repo}:{app_version}")
            for k, v in o.items():
                walk(v, k)
        elif isinstance(o, list):
            for v in o:
                walk(v, key)
        elif key == "image" and isinstance(o, str) and "/" in o:
            out.add(o if (":" in o.rpartition("/")[2] or "@" in o) else f"{o}:latest")
    walk(values, None)
    return sorted(out)


def chart_images(repo: str, chart: str, version: str, user: str, token: str) -> tuple[list[str], str | None]:
    """(the images oci://<repo>/<chart>:<version> names, None) or ([], why
    not): the chart is fetched in-process (manifest, its helm layer) and read
    in memory, never written anywhere; every values.yaml in it counts (a
    subchart's own appVersion for its empty tags)."""
    import yaml
    host, name = _split_repo(repo, chart)
    ref = f"oci://{host}/{name}:{version}"

    def fetch():
        bearer = _exchange(host, name, user, token)
        req = urllib.request.Request(f"{API}/v2/{name}/manifests/{version}",
                                     headers={"Authorization": f"Bearer {bearer}",
                                              "Accept": "application/vnd.oci.image.manifest.v1+json"})
        with _OPEN(req, timeout=30) as r:
            manifest = json.loads(r.read() or b"{}")
        layers = [x for x in manifest.get("layers") or [] if x.get("mediaType") == HELM_CHART_LAYER]
        if len(layers) != 1:
            raise _Refused(f"{len(layers)} helm chart layers in its manifest")
        data = _blob(host, name, layers[0]["digest"], bearer)
        charts, values = {}, {}
        with tarfile.open(fileobj=io.BytesIO(data), mode="r:gz") as tf:
            for m in tf.getmembers():
                d, _, base = m.name.rpartition("/")
                if m.isfile() and base in ("Chart.yaml", "values.yaml"):
                    doc = yaml.safe_load(tf.extractfile(m).read()) or {}
                    (charts if base == "Chart.yaml" else values)[d] = doc
        if not charts:
            raise _Refused("no Chart.yaml in its chart layer")
        imgs = set()
        for d, v in values.items():
            c = charts.get(d) or {}
            imgs.update(images_in_values(v, str(c.get("appVersion") or c.get("version") or version)))
        return sorted(imgs)

    res = _ref_error(ref, fetch)
    return (res, None) if isinstance(res, list) else ([], f"its images unreadable: {res}")


def dockerconfigjson(user: str, token: str) -> str:
    """The image-pull Secret's .dockerconfigjson for REGISTRY_HOST, the shape
    the installer's own imagepull-secret.yaml renders."""
    auth = base64.b64encode(f"{user}:{token}".encode()).decode()
    return json.dumps({"auths": {REGISTRY_HOST: {"username": user, "password": token, "auth": auth}}})


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="bench.ghcr_pull")
    sub = ap.add_subparsers(dest="cmd", required=True)
    for n in ("check", "username", "token", "dockerconfigjson"):
        sub.add_parser(n).add_argument("--env-file", required=True)
    pf = sub.add_parser("preflight")
    pf.add_argument("--env-file", required=True)
    pf.add_argument("--installer-pulled", required=True)
    pf.add_argument("--values", required=True)
    pf.add_argument("--out", required=True)
    a = ap.parse_args(argv)
    why = check(a.env_file)
    if why:
        print(f"refusing: the ghcr pull token: {why}; " + WHAT_TO_CREATE.format(path=a.env_file, dir=os.path.dirname(os.path.abspath(a.env_file))), file=sys.stderr)
        return 2
    if a.cmd == "check":
        return 0
    user, token = creds(a.env_file)
    if a.cmd == "username":
        sys.stdout.write("registryAuth:\n  username: " + json.dumps(user) + "\n")   # a values document, for -f -
        return 0
    if a.cmd == "token":
        sys.stdout.write(token)
        return 0
    if a.cmd == "dockerconfigjson":
        sys.stdout.write(dockerconfigjson(user, token))
        return 0
    try:
        pins = private_pins(a.installer_pulled, a.values)
    except ValueError as e:              # the installer's enable rule unreadable: nothing to resolve against
        print(f"refusing: {e}", file=sys.stderr)
        return 2
    res, images, other = [], {}, {}
    for p in pins:
        err = resolve(p["repo"], p["chart"], p["version"], user, token)
        if not err:
            imgs, err = chart_images(p["repo"], p["chart"], p["version"], user, token)
            for i in imgs:
                (images if i.startswith(REGISTRY_HOST + "/") else other).setdefault(i, []).append(p["name"])
        res.append({"chart": f"{p['repo']}/{p['chart']}", "version": p["version"], "error": err})
    img_res = [{"image": i, "charts": c, "error": resolve_image(i, user, token)} for i, c in sorted(images.items())]
    with open(a.out, "w") as fh:
        json.dump({"kind": "ghcr_pull_preflight", "charts": res, "images": img_res,
                   "images_elsewhere": [{"image": i, "charts": c} for i, c in sorted(other.items())]}, fh, indent=1)
    for r in res:
        print(f"[ghcr] {r['chart']} {r['version']}: {'OK' if not r['error'] else r['error']}", file=sys.stderr)
    for r in img_res:
        print(f"[ghcr] image {r['image']} ({', '.join(r['charts'])}): {'OK' if not r['error'] else r['error']}",
              file=sys.stderr)
    if other:
        print(f"[ghcr] {len(other)} image(s) on other registries (not this token's): " + ", ".join(sorted(other)),
              file=sys.stderr)
    bad = [r["error"] for r in res + img_res if r["error"]]
    if bad:
        print("refusing: the ghcr pull token does not resolve every chart and image this install pulls: "
              + "; ".join(bad), file=sys.stderr)
        return 3
    return 0


if __name__ == "__main__":
    sys.exit(main())
