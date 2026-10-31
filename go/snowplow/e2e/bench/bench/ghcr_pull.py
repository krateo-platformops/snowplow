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
  - every ghcr.io IMAGE those charts run (chart_images). The chart is fetched
    in-process from the registry with the token (manifest, then its helm
    layer, digest-verified) and RENDERED with `helm template`, with the
    values the installer gives that component (its componentValues: the
    installer's own, overridden by the bench's): every `image` the rendered
    manifests name, a template-only one included (#1023 A; run 19's dry pass
    found ui-token-hop missing from the values-only list, because the
    installer turns it on through componentValues). The images the chart's
    values.yaml files name count too (a component the installer could still
    turn on). An image the token cannot read is refused here, never as an
    ImagePullBackOff on a paid cluster.
  - the installer's own list of private-image components
    (inst.privateImageComponents): each one this install enables must render,
    and must render at least one ghcr.io image, or the derivation is not to
    be trusted and the run is refused. A render failure of any other chart is
    recorded, not refused.

Every registry read is bounded: in size (a blob at its declared size, a
manifest or a token answer at the distribution spec's 4 MiB) and in time (each
call's whole answer within its own bound, not only each socket read); the
render is bounded too. The step's bound is the sum of its calls' bounds and is
recorded with the preflight.
"""

from __future__ import annotations

import argparse
import base64
import hashlib
import io
import json
import os
import re
import stat
import subprocess
import sys
import tarfile
import tempfile
import time
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
#: the OCI distribution spec's manifest bound (registries SHOULD accept manifests up to 4 MiB): a
#: manifest, or a token answer, larger than that is not one
MANIFEST_MAX = 4 * 1024 * 1024
#: each call's bound (seconds): a registry call, a blob, a helm render. A call's answer is checked
#: against its bound after every socket read, and each socket read waits at most the bound: a call
#: takes at most twice its bound, however its answer is dripped
HTTP_BOUND_S, BLOB_BOUND_S, RENDER_BOUND_S = 30.0, 60.0, 120.0
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
        bench = yaml.safe_load(fh) or {}
    features.update(bench.get("features") or {})
    try:
        with open(os.path.join(root, "values.yaml")) as fh:
            cv = dict((yaml.safe_load(fh) or {}).get("componentValues") or {})
    except OSError:
        cv = {}
    out = []
    for c in pins.get("components") or []:
        if not (isinstance(c, dict) and c.get("repo")):
            continue
        f = c.get("feature") or ""
        if not f or f in core or features.get(f, False):
            out.append({"name": c["name"], "chart": c.get("chart") or c["name"], "repo": c["repo"],
                        "version": str(c.get("version")),
                        # what the installer renders this component with: its own componentValues,
                        # overridden by the bench's
                        "values": _merge(cv.get(c["name"]) or {}, (bench.get("componentValues") or {}).get(c["name"]) or {})})
    return out


def _merge(base: dict, over: dict) -> dict:
    """helm's values merge: maps merge key by key, anything else is replaced."""
    out = dict(base)
    for k, v in over.items():
        out[k] = _merge(out[k], v) if isinstance(v, dict) and isinstance(out.get(k), dict) else v
    return out


_PRIVATE_IMAGE_RX = re.compile(
    r'\{\{-?\s*define\s+"inst\.privateImageComponents"\s*-?\}\}(.*?)\{\{-?\s*end\s*-?\}\}', re.S)


def private_image_components(installer_pulled: str) -> set[str] | None:
    """The installer's own list of components with PRIVATE images (its
    inst.privateImageComponents define: the components it injects image-pull
    Secrets into). None when this installer has no such define; raises
    ValueError when the define is not a static list (it is then not the
    installer's rule this mirrors)."""
    import yaml
    tdir = os.path.join(installer_pulled, "installer", "templates")
    bodies = []
    for n in sorted(os.listdir(tdir)) if os.path.isdir(tdir) else []:
        if n.endswith((".tpl", ".yaml")):
            with open(os.path.join(tdir, n)) as fh:
                bodies += _PRIVATE_IMAGE_RX.findall(fh.read())
    if not bodies:
        return None
    if len(bodies) != 1 or "{{" in bodies[0]:
        raise ValueError("inst.privateImageComponents is not one static list: its rule cannot be read")
    doc = yaml.safe_load(bodies[0]) or {}
    return {c["name"] for c in doc.get("components") or [] if isinstance(c, dict) and c.get("name")}


class _Refused(Exception):
    """Why a reference does not resolve (never carries a credential)."""


def _read(req, cap: int, bound_s: float) -> bytes:
    """The answer to `req`, at most `cap` bytes, and whole within its bound:
    checked after every socket read (read1 returns after ONE), each of which
    waits at most `bound_s`. A registry that drips its answer is cut at the
    bound, one that goes silent at the socket read's: at most 2 x bound_s."""
    end = time.monotonic() + bound_s
    with _OPEN(req, timeout=bound_s) as r:
        out = bytearray()
        while True:
            if time.monotonic() >= end:
                raise _Refused(f"no whole answer within its {bound_s:g} s bound")
            try:
                chunk = r.read1(min(65536, cap + 1 - len(out)))
            except TimeoutError:
                raise _Refused(f"no whole answer within its {bound_s:g} s bound") from None
            if not chunk:
                return bytes(out)
            out += chunk
            if len(out) > cap:
                raise _Refused(f"an answer larger than its {cap} byte bound")


def _exchange(host: str, name: str, user: str, token: str) -> str:
    """The registry's bearer for pulling `name` (its token exchange)."""
    if not API.startswith("https://"):
        raise _Refused(f"the registry API {API!r} is not https: the token is never sent in clear text")
    basic = base64.b64encode(f"{user}:{token}".encode()).decode()
    req = urllib.request.Request(f"{API}/token?scope=repository:{name}:pull&service={host}",
                                 headers={"Authorization": f"Basic {basic}"})
    try:
        bearer = json.loads(_read(req, MANIFEST_MAX, HTTP_BOUND_S) or b"{}").get("token")
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
    _read(req, 0, HTTP_BOUND_S)
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


def _blob(host: str, name: str, digest: str, size: int, bearer: str) -> bytes:
    """A blob of its DECLARED size, digest-verified. The registry answers with
    a redirect to its blob storage: followed over https only and WITHOUT the
    Authorization header (the signed URL is the credential), like every
    registry client."""
    if not isinstance(size, int) or size < 0:
        raise _Refused(f"blob {digest[:19]} declares no size")
    def get(r):                          # the one read of a blob, from the registry or its storage
        return _read(r, size, BLOB_BOUND_S)
    req = urllib.request.Request(f"{API}/v2/{name}/blobs/{digest}", headers={"Authorization": f"Bearer {bearer}"})
    try:
        data = get(req)
    except urllib.error.HTTPError as e:
        loc = e.headers.get("Location") if e.code in (301, 302, 303, 307, 308) and e.headers else None
        if not loc:
            raise
        if not loc.startswith("https://"):
            raise _Refused(f"blob {digest[:19]} redirected off https") from None
        data = get(urllib.request.Request(loc))
    algo, _, want = digest.partition(":")
    if algo != "sha256" or hashlib.sha256(data).hexdigest() != want:
        raise _Refused(f"blob {digest[:19]} does not match its digest")
    return data


def images_in_values(values, app_version: str) -> list[str]:
    """Every image a chart's values name, as the charts template them: a dict
    under an `image` key with a `repository` (an optional `registry` before
    it; its `tag`, else its `digest`, else the chart's appVersion), or a
    string under an `image` key. Sorted, unique."""
    out, seen = set(), set()

    def walk(o, key):
        if isinstance(o, (dict, list)):
            # a YAML alias is the SAME object again: each one is walked once (an alias chain
            # would otherwise double the walk at every level)
            if (id(o), key) in seen:
                return
            seen.add((id(o), key))
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


def images_in_manifests(docs) -> list[str]:
    """Every image the rendered manifests run: each string under an `image`
    key, at any depth (containers, init containers, CRs that carry one).
    Sorted, unique; YAML aliases walked once."""
    out, seen = set(), set()

    def walk(o, key):
        if isinstance(o, (dict, list)):
            if id(o) in seen:
                return
            seen.add(id(o))
        if isinstance(o, dict):
            for k, v in o.items():
                walk(v, k)
        elif isinstance(o, list):
            for v in o:
                walk(v, key)
        elif key == "image" and isinstance(o, str) and o.strip():
            out.add(o.strip())
    walk(docs, None)
    return sorted(out)


def render_images(data: bytes, name: str, values: dict, helm: str = "helm") -> list[str]:
    """`helm template` of the chart `data` with `values` (what the installer
    gives this component), bounded by RENDER_BOUND_S: the images its
    manifests run. Raises _Refused with helm's own reason when it cannot
    render. The chart (never a credential) is written to a private temporary
    directory removed at once; the values go on stdin."""
    import yaml
    with tempfile.TemporaryDirectory(prefix="bench-render-") as d:
        path = os.path.join(d, "chart.tgz")
        with open(path, "wb") as fh:
            fh.write(data)
        try:
            p = subprocess.run([helm, "template", name, path, "--namespace", SECRET_NS, "-f", "-"],
                               input=yaml.safe_dump(values or {}), capture_output=True, text=True,
                               timeout=RENDER_BOUND_S)
        except subprocess.TimeoutExpired:
            raise _Refused(f"helm template did not finish within its {RENDER_BOUND_S:g} s bound") from None
        except OSError as e:
            raise _Refused(f"helm template could not run: {e}") from None
    if p.returncode != 0:
        why = (p.stderr.strip().splitlines() or [f"exit {p.returncode}"])[-1]
        raise _Refused(f"helm template failed: {why[:300]}")
    try:
        docs = [x for x in yaml.safe_load_all(p.stdout) if x is not None]
    except yaml.YAMLError as e:
        raise _Refused(f"helm template rendered no YAML: {str(e).splitlines()[0][:200]}") from None
    return images_in_manifests(docs)


def chart_images(repo: str, chart: str, version: str, user: str, token: str, values: dict | None = None,
                 helm: str = "helm") -> dict:
    """What oci://<repo>/<chart>:<version> runs: {images, rendered,
    render_error, error}. The chart is fetched in-process (manifest, its helm
    layer); `rendered` is what `helm template` with `values` (the installer's
    componentValues for it) runs, None with `render_error` when it cannot
    render; `images` adds what every values.yaml in it names (a subchart's own
    appVersion for its empty tags). `error` when the chart itself cannot be
    read: then nothing is known about its images."""
    import yaml
    host, name = _split_repo(repo, chart)
    ref = f"oci://{host}/{name}:{version}"
    out = {"images": [], "rendered": None, "render_error": None, "error": None}

    def fetch():
        bearer = _exchange(host, name, user, token)
        req = urllib.request.Request(f"{API}/v2/{name}/manifests/{version}",
                                     headers={"Authorization": f"Bearer {bearer}",
                                              "Accept": "application/vnd.oci.image.manifest.v1+json"})
        try:
            manifest = json.loads(_read(req, MANIFEST_MAX, HTTP_BOUND_S) or b"{}")
        except ValueError:
            raise _Refused("its manifest is not JSON") from None
        layers = [x for x in manifest.get("layers") or [] if x.get("mediaType") == HELM_CHART_LAYER]
        if len(layers) != 1:
            raise _Refused(f"{len(layers)} helm chart layers in its manifest")
        data = _blob(host, name, layers[0]["digest"], layers[0].get("size"), bearer)
        charts, vals = {}, {}
        with tarfile.open(fileobj=io.BytesIO(data), mode="r:gz") as tf:
            for m in tf.getmembers():
                d, _, base = m.name.rpartition("/")
                if m.isfile() and base in ("Chart.yaml", "values.yaml"):
                    doc = yaml.safe_load(tf.extractfile(m).read()) or {}
                    (charts if base == "Chart.yaml" else vals)[d] = doc
        if not charts:
            raise _Refused("no Chart.yaml in its chart layer")
        imgs = set()
        for d, v in vals.items():
            c = charts.get(d) or {}
            imgs.update(images_in_values(v, str(c.get("appVersion") or c.get("version") or version)))
        try:
            out["rendered"] = render_images(data, chart, values or {}, helm)
            imgs.update(out["rendered"])
        except _Refused as e:
            out["render_error"] = str(e)
        return sorted(imgs)

    res = _ref_error(ref, fetch)
    if isinstance(res, list):
        out["images"] = res
    else:
        out["error"] = f"{ref}: its images unreadable: " + (res[len(ref) + 2:] if res.startswith(f"{ref}: ") else res)
    return out


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
    pf.add_argument("--helm", default="helm", help="the helm that renders each chart (helm template)")
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
        private = private_image_components(a.installer_pulled)
    except ValueError as e:              # the installer's enable rule unreadable: nothing to resolve against
        print(f"refusing: {e}", file=sys.stderr)
        return 2
    res, images, other = [], {}, {}
    for p in pins:
        ref = f"{p['repo']}/{p['chart']}"
        err, render = resolve(p["repo"], p["chart"], p["version"], user, token), None
        if not err:
            ci = chart_images(p["repo"], p["chart"], p["version"], user, token, p["values"], a.helm)
            err = ci["error"]
            render = (None if ci["error"] else ("failed: " + ci["render_error"]) if ci["render_error"]
                      else f"ok ({len(ci['rendered'])} images)")
            if not err and private is not None and p["name"] in private:
                # the installer injects image-pull Secrets into this one: its images MUST be known
                if ci["render_error"]:
                    err = (f"{ref}:{p['version']}: the installer lists it as a private-image component "
                           f"(inst.privateImageComponents) and it cannot be rendered: {ci['render_error']}")
                elif not any(i.startswith(REGISTRY_HOST + "/") for i in ci["rendered"]):
                    err = (f"{ref}:{p['version']}: the installer lists it as a private-image component "
                           f"(inst.privateImageComponents), yet it renders no {REGISTRY_HOST} image with the "
                           f"installer's values for it")
            for i in ci["images"]:
                (images if i.startswith(REGISTRY_HOST + "/") else other).setdefault(i, []).append(p["name"])
        res.append({"chart": ref, "version": p["version"], "render": render,
                    "private_image_component": bool(private and p["name"] in private), "error": err})
    img_res = [{"image": i, "charts": c, "error": resolve_image(i, user, token)} for i, c in sorted(images.items())]
    # the step's bound: every call it made at twice its own bound (see _read; a resolve = 2 calls; a
    # chart read = 2 calls and its blob; an image = 2 calls) and each render at its own
    bound = (len(pins) * (2 * (4 * HTTP_BOUND_S + BLOB_BOUND_S) + RENDER_BOUND_S)
             + len(img_res) * 2 * (2 * HTTP_BOUND_S))
    how = (f"{len(pins)} charts x (2 x (4 x {HTTP_BOUND_S:g} + {BLOB_BOUND_S:g}) + {RENDER_BOUND_S:g} s) + "
           f"{len(img_res)} images x 2 x (2 x {HTTP_BOUND_S:g} s)")
    with open(a.out, "w") as fh:
        json.dump({"kind": "ghcr_pull_preflight", "bound_s": bound, "bound": how,
                   "private_image_components": sorted(private) if private is not None else None,
                   "charts": res, "images": img_res,
                   "images_elsewhere": [{"image": i, "charts": c} for i, c in sorted(other.items())]}, fh, indent=1)
    print(f"[ghcr] the step's bound: {bound:g} s ({how})", file=sys.stderr)
    if private is None:
        print("[ghcr] this installer declares no inst.privateImageComponents: no private-image self-check",
              file=sys.stderr)
    for r in res:
        print(f"[ghcr] {r['chart']} {r['version']}: {'OK' if not r['error'] else r['error']}"
              + (f" (render {r['render']})" if r["render"] else ""), file=sys.stderr)
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
