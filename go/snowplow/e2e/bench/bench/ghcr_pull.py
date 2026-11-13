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

The path must survive a reboot: run606.sh refuses one in a temporary directory (#1194, the file
went with /tmp on 2026-11-06), so a run never depends on a token the machine can clear.

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
    (inst.privateImageComponents), RENDERED from the installer chart the run
    installs (#1395, private_image_paths): each one this install enables must
    render, and must render at least one ghcr.io image, or the derivation is
    not to be trusted and the run is refused. A list that cannot be rendered
    and read refuses the run with the reason. A render failure of any other
    chart is recorded, not refused.
  - every component whose image the run overrides (--override COMPONENT=TAG;
    run606's FRONTEND_TAG, run 26), whatever its repo: this install must
    enable it, it must render with the run's values, and a ghcr.io image it
    renders must carry that tag (then resolved like every other image), or
    the run is refused: the overridden image is the one the run will pull.

Every registry read is bounded: in size (a blob at its declared size, a
manifest or a token answer at the distribution spec's 4 MiB) and in time (each
call's whole answer within its own bound, not only each socket read); the
render is bounded too. The step's bound is the sum of its calls' bounds and is
recorded with the preflight.

NO ANSWER is not a refusal of the token (run 25: one manifest read's 30 s
timeout, from one region, refused a run that another region's identical check
had passed minutes earlier, as "does not resolve"). A call that ends with no
answer (a timeout, a connection reset or dropped, HTTP 429 / 5xx: _NoAnswer) is
asked again within the step's own bound (Budget, _phase: once every reference
had its first attempt, its reference again while what is left of the bound
holds the reference's whole share; the step never runs past its bound). Only
when the bound has no room left is it refused, as NO ANSWER, naming the call
(the token exchange, the manifest read, the chart blob read, the chart files
read, helm template), its attempts and the bound: exit 4 (EXIT_NO_ANSWER),
apart from the registry's own answers (a 401 / 403 / 404, a manifest that does
not parse: exit 3, EXIT_REFUSED, as before).

WHEN it runs: run606.sh's `registry` step, before `reap` and `up`: before this
run's cluster exists, in the dry pass too. run606_pair.sh runs every arm's dry
pass, this check included, before ANY arm starts, so a launch the token cannot
serve creates nothing; each arm's real run checks again before its own `up`.
"""

from __future__ import annotations

import argparse
import base64
import hashlib
import http.client
import io
import json
import multiprocessing
import os
import secrets
import stat
import subprocess
import sys
import tarfile
import tempfile
import threading
import time
import urllib.error
import urllib.request

from bench.gke import DEFAULT_CLUSTER_VERSION
from bench.noanswer import (NO_ANSWER_ERRORS, NO_ANSWER_STATUS, SSL_DROP_REASONS,  # noqa: F401
                            SSL_DROPS, TEMPORARY_EAI, Budget, NoAnswer, attempt, phase, unanswered)

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
#: each call's WHOLE bound (seconds), connect, status line, headers and body together (_call): a
#: registry call, a blob, a helm render (run, its output read, and parsed), the chart's own files
#: read (its tar walked, its Chart.yaml / values.yaml parsed in-process: read_chart_files). The parse
#: has the render's bound: both are an in-process YAML parse of at most RENDER_MARGIN x a measured
#: real size (below).
HTTP_BOUND_S, BLOB_BOUND_S, RENDER_BOUND_S = 30.0, 60.0, 120.0
PARSE_BOUND_S = RENDER_BOUND_S
#: Size caps (#1041 review F3, round 2).
#: - A chart's files: helm's OWN loader limits (helm v3 pkg/chart/loader: MaxDecompressedChartSize
#:   100 MiB, MaxDecompressedFileSize 5 MiB): a chart past them is one helm itself refuses to load.
#: - A blob's ceiling: the same 100 MiB. A gzip stream inflates to at least about its own size, so a
#:   larger blob cannot hold a chart helm loads. Held in memory whole; the tar headers are read one
#:   at a time and never accumulate (read_chart_files), so the WALK's memory is the blob plus one
#:   file; what is parsed is capped below (`parsed`).
#: - helm template's stdout: helm sets no limit. 16 x the largest render measured (2,665,855 B,
#:   agentgateway-controller 0.2.0): 16 = four doublings of growth before a real chart is refused.
#:   The YAML parse costs about 8 bytes of memory per byte of output (the round-1 review: 52 MB ->
#:   +415 MiB), so at the cap the parse peaks near 340 MiB, inside the operator machine's. Near that
#:   cap the TIME bound refuses first (a 42 MB render parses for about 150 s, past RENDER_BOUND_S):
#:   the size cap bounds the memory, the bound the wait.
#: - The chart's own Chart.yaml + values.yaml, parsed in-process for the images its values name: all
#:   of them together, 16 x the largest such total measured (`parsed`), the same rule (#1041 review
#:   round 3, M1: within helm's own 5 MiB / 100 MiB, two 5 MiB values.yaml took 941 s and +496 MiB).
#:   Counted before any of it is parsed, so a chart past it is refused without the parse.
#: - The chart's tar MEMBERS (files and directories): each costs the walk a header and each
#:   Chart.yaml / values.yaml a parse and a kept document, whatever its size (#1041 review round 4,
#:   M1b: 2,000,000 empty values.yaml in a 9.4 MB blob took 69 s, +619 MiB): 16 x the largest real
#:   chart's member count (`members`), the same rule, counted header by header. Probe
#:   (2026-11-01, the review's shape `kN: {a: 1, b: [x, y]}`): the review's two 5 MiB values.yaml
#:   are refused in 0.01 s, +1.7 MiB; just under the cap (2,260,389 B) parses in 2.9 s, +203 MiB
#:   with libyaml, 10.3 s, +273 MiB without: inside PARSE_BOUND_S either way.
#: Measured 2026-10-31 by this preflight's own `measured` record over the charts run 20 pulls, on the
#: real installer 0.3.468 with the bench's values (largest blob / largest file / inflated / render,
#: bytes): agentgateway-controller 0.2.0 298,188 / 1,041,093 / 2,753,178 / 2,665,855;
#: agentgateway-policies 0.2.18 152,818 / 141,400 / 593,530 / 99,113; the pinned alert-provider
#: 0.2.53 9,429 / 10,094 / 31,001 / 15,495; github-provider-kog 0.6.0 88,778 / 15,708 / 386,177 /
#: 361,843; krateo-marketplace-catalog 0.1.2 42,296 / 284,927 / 363,923 / 390,039; and the installer
#: 0.3.468 itself 363,138 / 833,982 / 1,622,254 / 32,104. Parsed in-process (Chart.yaml +
#: values.yaml, all of a chart's, bytes; 2026-11-01): agentgateway-policies 0.2.18 141,293;
#: agentgateway-controller 0.2.0 17,621; alert-provider 0.2.53 7,451; github-provider-kog 0.6.0 1,644;
#: krateo-marketplace-catalog 0.1.2 681. Tar members (2026-11-01): github-provider-kog 0.6.0 125;
#: agentgateway-policies 0.2.18 41; agentgateway-controller 0.2.0 30; alert-provider 0.2.53 8;
#: krateo-marketplace-catalog 0.1.2 6. Deepest (maps and lists, the top one counted: NEST_MAX below;
#: a chart's own files / its render, 2026-11-12, the same versions from a local helm cache rendered
#: with installer 0.3.468's componentValues): agentgateway-controller 0.2.0 4 / 40 (its CRDs'
#: schemas); agentgateway-policies 0.2.18 6 / 10; alert-provider 0.2.53 4 / 10; github-provider-kog
#: 0.6.0 2 / 10; installer 0.3.468 6 / 7; krateo-marketplace-catalog 0.1.2 1 / 3.
#: tests/fixtures/ghcr_measured_0_3_468.json holds that record, and a test pins REAL_LARGEST to it. Every run records its own under
#: `measured`, and the preflight says so when one exceeds REAL_LARGEST (a stale input, #1041 R31).
HELM_MAX_CHART, HELM_MAX_FILE = 100 * 1024 * 1024, 5 * 1024 * 1024
REAL_LARGEST = {"blob": 363_138, "file": 1_041_093, "inflated": 2_753_178, "render": 2_665_855,
                "parsed": 141_293, "members": 125, "deepest": 40}
RENDER_MARGIN = 16
CHART_INFLATED_MAX = HELM_MAX_CHART
CHART_FILE_MAX = HELM_MAX_FILE
BLOB_MAX = HELM_MAX_CHART
RENDER_OUT_MAX = RENDER_MARGIN * REAL_LARGEST["render"]
CHART_PARSED_MAX = RENDER_MARGIN * REAL_LARGEST["parsed"]
CHART_MEMBERS_MAX = RENDER_MARGIN * REAL_LARGEST["members"]
#: Nesting cap (#1356): every YAML document the preflight reads (the installer's files, the bench's
#: values, a chart's Chart.yaml / values.yaml, helm template's output) nests at most NEST_MAX maps
#: and lists deep, as built (an alias counts the depth of the node it names): _load_yaml, the
#: preflight's one YAML reader. The depth is read from the parser's events, which nest nothing,
#: before any document is built (_nest_check), and the document is then built by libyaml, in C
#: (_yaml_loader; no libyaml is a named refusal, never PyYAML's pure-Python composer, which recurses
#: two frames a level and reaches Python's limit near 490). Nothing the preflight does with a
#: document recurses either (its walks, _merge, with_markers, _dump_yaml keep stacks of their own),
#: so no depth ever nears Python's recursion limit and no RecursionError is ever relied on. One was:
#: a values.yaml 1,000 deep was walked recursively to the limit and the RecursionError caught;
#: whatever else ran at that depth (a finalizer the garbage collector chose that moment for, and
#: pytest's hook reporting it) failed for want of stack (CI run 43110378256, "Failed to process
#: unraisable exception"). The same rule as the size caps: 16 x the deepest real document
#: (REAL_LARGEST["deepest"]: agentgateway-controller 0.2.0's render, its CRDs' schemas), measured
#: by the preflight itself (`measured`: `deepest` of a chart's own files, `render_deepest` of its
#: render) and re-checked by every run like the sizes (stale_inputs).
NEST_MAX = RENDER_MARGIN * REAL_LARGEST["deepest"]
CAPS = {"helm_max_chart": HELM_MAX_CHART, "helm_max_file": HELM_MAX_FILE, "render_margin": RENDER_MARGIN,
        "real_largest": REAL_LARGEST, "blob_max": BLOB_MAX, "chart_inflated_max": CHART_INFLATED_MAX,
        "chart_file_max": CHART_FILE_MAX, "render_out_max": RENDER_OUT_MAX,
        "chart_parsed_max": CHART_PARSED_MAX, "chart_members_max": CHART_MEMBERS_MAX, "nest_max": NEST_MAX}
#: a `measured` key -> the REAL_LARGEST input it is compared with
MEASURED_INPUT = {"blob": "blob", "largest_file": "file", "inflated": "inflated", "render": "render",
                  "parsed": "parsed", "members": "members", "deepest": "deepest",
                  "render_deepest": "deepest"}
#: preflight's exits: the registry's own answer refuses (a 401 / 403 / 404, a manifest that does not
#: parse, ...); no answer within the step's bound is a refusal of its own, said as such (run 25)
EXIT_REFUSED, EXIT_NO_ANSWER = 3, 4
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


def private_pins(installer_pulled: str, values: str, include: set[str] | frozenset = frozenset()) -> list[dict]:
    """Every pinned chart this install pulls from a repo of its OWN (the
    repo-override pins: krateo-agentiko/*, krateo-blueprints/*, ...), whose
    component the installer enables, by its own rule (inst.featureEnabled):
    a pin with no feature is on; a feature in the chart's inst.coreFeatures is
    always on; else the install's features (the installer's defaults,
    overridden by the bench's values). The token must resolve each one: a
    public chart resolves with any valid token, a private one only with
    access.

    `include`: components preflighted WHATEVER their repo (the installer's
    private-image components, #1041 review F1): a pin with no repo of its own
    comes from the installer's ociRepo (`default $top.Values.ociRepo $c.repo`,
    inst.imagePullAuths' rule), the bench's ociRepo overriding the
    installer's.

    `values` per pin: what the installer gives the component, as far as it
    can be known offline: its componentValues (the installer's values.yaml,
    overridden by the bench's), merged as helm merges values files (a null
    deletes the key). Known differences from the installer's real spec
    (compositions.yaml at 0.3.468, #1041 review F5), none of which names an
    image in the charts pulled today: the spec the installer COMPUTES wins
    over componentValues (`merge $spec (deepCopy $custom)`), and its
    fill-if-absent and forced writes come after (cors.enabled,
    gateway.internalDataPlane, platformRoutes, subAgents, mcpBackendHash; the
    image pull secrets, mirrored by with_markers). The release name is the
    component's own name, as the installer's. Every file read by _load_yaml
    (NEST_MAX)."""
    from bench import diag606
    root = os.path.join(installer_pulled, "installer")
    core = set(diag606.core_features(installer_pulled))
    with open(os.path.join(root, "files", "component-pins.yaml"), "rb") as fh:
        pins = _load_yaml(fh.read(), "the installer's files/component-pins.yaml: ") or {}
    try:
        with open(os.path.join(root, "values.yaml"), "rb") as fh:
            inst_values = _load_yaml(fh.read(), "the installer's values.yaml: ") or {}
    except OSError:
        inst_values = {}
    features = dict(inst_values.get("features") or {})
    with open(values, "rb") as fh:
        bench = _load_yaml(fh.read(), f"{values}: ") or {}
    features.update(bench.get("features") or {})
    cv = dict(inst_values.get("componentValues") or {})
    oci = bench.get("ociRepo") or inst_values.get("ociRepo")
    out = []
    for c in pins.get("components") or []:
        if not (isinstance(c, dict) and (c.get("repo") or c.get("name") in include)):
            continue
        f = c.get("feature") or ""
        if not f or f in core or features.get(f, False):
            repo = c.get("repo") or oci
            if not repo:
                raise ValueError(f"{c['name']}: no repo of its own and the installer has no ociRepo")
            out.append({"name": c["name"], "chart": c.get("chart") or c["name"], "repo": repo,
                        "version": str(c.get("version")),
                        # what the installer renders this component with: its own componentValues,
                        # overridden by the bench's
                        "values": _merge(cv.get(c["name"]) or {}, (bench.get("componentValues") or {}).get(c["name"]) or {})})
    return out


def _merge(base: dict, over: dict) -> dict:
    """helm's values merge: maps merge key by key, anything else is replaced,
    and a null deletes the key. A stack of its own (NEST_MAX): each map
    merged into is a copy, `base` and `over` are left as they are."""
    out = dict(base)
    todo = [(out, over)]
    while todo:
        into, frm = todo.pop()
        for k, v in frm.items():
            if v is None:
                into.pop(k, None)
            elif isinstance(v, dict) and isinstance(into.get(k), dict):
                into[k] = dict(into[k])
                todo.append((into[k], v))
            else:
                into[k] = v
    return out


#: #1395: the installer's own list of its private-image components, as a define of its chart
PRIVATE_DEFINE = "inst.privateImageComponents"
#: the Kubernetes version the installer chart is rendered for: the one every bench cluster is created
#: at (gke-up --cluster-version); the chart's kubeVersion constraint refuses helm's own default
KUBE_VERSION = DEFAULT_CLUSTER_VERSION
#: the release the run installs the installer as (run606.sh s_install: `helm install installer`)
INSTALLER_RELEASE = "installer"
_PROBE_KEY = "privateImageComponents"


def _probe_template(name: str) -> str:
    """A template that renders the installer's own list into one ConfigMap value. It reads the
    define exactly as the installer's consumers do (`include "inst.privateImageComponents" $top |
    fromYaml`: inst.imagePullAuths, compositions.yaml), then hands it over as JSON, so no form of
    the define's source changes what is read: a comment, a delegation, a rule over values."""
    return ("apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: " + name + "\ndata:\n  " + _PROBE_KEY
            + ': {{ include "' + PRIVATE_DEFINE + '" . | fromYaml | toJson | quote }}\n')


def _chart_with(root: str, extra: dict[str, str]) -> bytes:
    """The chart directory `root` as a helm archive (its files under one top directory, as helm
    packs it) with `extra` (path in the chart -> text) added. In memory: the directory, which
    s_install installs from, is never written to."""
    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode="w:gz", compresslevel=1) as tf:
        tf.add(root, arcname="installer")
        for rel, text in extra.items():
            data = text.encode()
            ti = tarfile.TarInfo("installer/" + rel)
            ti.size, ti.mode, ti.mtime = len(data), 0o644, int(time.time())
            tf.addfile(ti, io.BytesIO(data))
    return buf.getvalue()


def _read_probe(docs: list) -> dict[str, list[list[str]]]:
    """The rendered list, by component: each entry a name and a non-empty values path of non-empty
    keys. Raises _Refused naming what is not that."""
    cms = [d for d in docs if isinstance(d, dict) and d.get("kind") == "ConfigMap"]
    if len(docs) != 1 or len(cms) != 1:
        raise _Refused(f"the render gave {len(docs)} document(s), not the one probe")
    raw = (cms[0].get("data") or {}).get(_PROBE_KEY)
    if not isinstance(raw, str):
        raise _Refused("the render holds no rendered list")
    try:
        doc = json.loads(raw)
    except ValueError as e:
        raise _Refused(f"the rendered list is not JSON ({e})") from None
    if not isinstance(doc, dict):
        raise _Refused(f"the define renders {type(doc).__name__}, not a map")
    if "Error" in doc:                 # helm's fromYaml: a map holding only the parse error
        raise _Refused(f"the define does not render as YAML: {str(doc['Error'])[:300]}")
    comps = doc.get("components")
    if not isinstance(comps, list) or not comps:
        raise _Refused("the define renders no `components` list, or an empty one: the self-check would "
                       "check nothing")
    out: dict[str, list[list[str]]] = {}
    for i, c in enumerate(comps):
        name = c.get("name") if isinstance(c, dict) else None
        path = c.get("path") if isinstance(c, dict) else None
        if not (isinstance(name, str) and name and isinstance(path, list) and path
                and all(isinstance(k, str) and k for k in path)):
            raise _Refused(f"its entry {i} is not a name and a non-empty path of keys: {json.dumps(c)[:200]}")
        out.setdefault(name, []).append(list(path))
    return out


def private_image_paths(installer_pulled: str, values: dict, helm: str = "helm") -> dict[str, list[list[str]]]:
    """The installer's own list of components with PRIVATE images (its inst.privateImageComponents
    define: the components it injects image-pull Secrets into, and at which values paths), by
    component, RENDERED (#1395): `helm template` of the pulled chart (`installer_pulled`/installer,
    the copy s_install installs) as the run installs it (release INSTALLER_RELEASE, namespace
    SECRET_NS, KUBE_VERSION) with `values`, plus one probe template (_probe_template) under a name
    of its own, shown alone. Bounded as every render is (render). Run 26f: a regex over the
    template source read the comment installer 0.3.616 put inside the define as an unknown rule.

    Raises _Refused (fail closed) with the reason whenever the render does not answer: helm fails
    (the define gone, a value the chart refuses, ...), does not finish, or what it renders is not
    a non-empty list of names and paths."""
    root = os.path.join(installer_pulled, "installer")
    why = ("the installer's private-image components (inst.privateImageComponents) cannot be read from its "
           "rendered chart")
    if not os.path.isfile(os.path.join(root, "Chart.yaml")):
        raise _Refused(f"{why}: no pulled installer chart at {root}")
    probe = f"templates/zz-bench-probe-{secrets.token_hex(4)}.yaml"
    try:
        data = _chart_with(root, {probe: _probe_template("bench-private-image-probe")})
        docs = render(data, INSTALLER_RELEASE, values, helm,
                      args=("--kube-version", KUBE_VERSION, "--show-only", probe))
        return _read_probe(docs)
    except _Refused as e:
        raise _Refused(f"{why}: {e}") from None
    except OSError as e:
        raise _Refused(f"{why}: the chart could not be read ({e})") from None


class _Refused(Exception):
    """Why a reference does not resolve (never carries a credential)."""


class _NoAnswer(_Refused, NoAnswer):
    """NO answer within a call's bound (run 25; bench/noanswer.py): not evidence about the token. Its
    reference is asked again while the step's bound has room (Budget, _phase); refused only when it
    has none, and then said as "no answer", never as "does not resolve"."""


# the classification and the bounded retry are bench/noanswer.py's, shared with every other
# pre-cluster registry read (run 26: cert-manager's helm pull)
_unanswered, _attempt, _phase = unanswered, attempt, phase


def _call(fn, bound_s: float, on_timeout=None, whole: float | None = None):
    """fn() within `bound_s` of wall clock, WHOLE: the caller is answered at
    the bound whatever fn is doing. A thread cannot be killed, so this bounds
    the WAIT, not the work (#1041 review round 4): it is used only where the
    work left after the bound is itself capped. A registry read (_read,
    _blob): the thread only waits on its socket, each read at most the bound,
    the total at most the call's byte cap (MANIFEST_MAX / BLOB_MAX, and
    http.client's own header limits), CPU only per byte received; a daemon,
    gone with the preflight. helm template: `on_timeout` kills helm and the
    reading ends at its EOF. Work that is not capped that way (a YAML parse)
    runs in a killed child instead (_in_child). fn's own exception is raised
    here; past the bound, _NoAnswer (naming `whole`, the call's own bound,
    when this wait is what is left of it)."""
    box: dict = {}

    def run():
        try:
            box["v"] = fn()
        except BaseException as e:          # noqa: BLE001 - re-raised in the caller
            box["e"] = e
    t = threading.Thread(target=run, daemon=True)
    t.start()
    t.join(bound_s)
    if t.is_alive():
        if on_timeout:
            on_timeout()
        raise _NoAnswer(f"no whole answer within its {bound_s if whole is None else whole:g} s bound")
    if "e" in box:
        raise box["e"]
    return box["v"]


def _read(req, cap: int, bound_s: float, whole: float | None = None) -> bytes:
    """The answer to `req`, at most `cap` bytes, the whole call (connect,
    status, headers, body) within `bound_s` (_call). No answer (a timeout, a
    reset, HTTP 429 or 5xx) is _NoAnswer; any other HTTP status is the
    registry's answer (HTTPError)."""
    def call():
        try:
            with _OPEN(req, timeout=bound_s) as r:
                out = bytearray()
                while True:
                    chunk = r.read1(min(65536, cap + 1 - len(out)))
                    if not chunk:
                        return bytes(out)
                    out += chunk
                    if len(out) > cap:
                        raise _Refused(f"an answer larger than its {cap} byte bound")
        except urllib.error.HTTPError as e:
            if e.code in NO_ANSWER_STATUS or e.code >= 500:
                raise _NoAnswer(f"HTTP {e.code} {e.reason}: the registry did not answer for it now") from None
            raise
        except urllib.error.URLError as e:
            why = _unanswered(e.reason) if isinstance(e.reason, BaseException) else None
            if why:
                raise _NoAnswer(why) from None
            raise
        except (OSError, http.client.IncompleteRead) as e:
            why = _unanswered(e)
            if why:
                raise _NoAnswer(why) from None
            raise
    return _call(call, bound_s, whole=whole)


def _exchange(host: str, name: str, user: str, token: str, budget: Budget | None = None) -> str:
    """The registry's bearer for pulling `name` (its token exchange)."""
    if not API.startswith("https://"):
        raise _Refused(f"the registry API {API!r} is not https: the token is never sent in clear text")
    basic = base64.b64encode(f"{user}:{token}".encode()).decode()
    req = urllib.request.Request(f"{API}/token?scope=repository:{name}:pull&service={host}",
                                 headers={"Authorization": f"Basic {basic}"})
    try:
        bearer = json.loads(_attempt("token exchange", HTTP_BOUND_S, lambda: _read(req, MANIFEST_MAX, HTTP_BOUND_S),
                                     budget) or b"{}").get("token")
    except ValueError:
        raise _Refused(f"{host} answered the token exchange for {name} with no JSON") from None
    if not bearer:
        raise _Refused(f"{host} gave no token for {name}")
    return bearer


class Why(str):
    """An error string (`ref: why`) that also says whether it is NO ANSWER (`no_answer`: the registry
    did not answer within the step's bound) or the registry's own answer (a refusal)."""
    no_answer = False


def _why(text: str, no_answer: bool) -> Why:
    w = Why(text)
    w.no_answer = no_answer
    return w


def _ref_error(ref: str, fn):
    """fn() with every failure as `ref: why` (a Why), never raised."""
    try:
        return fn()
    except _Refused as e:
        return _why(f"{ref}: {e}" if not str(e).startswith(("the registry API", ref)) else str(e),
                    isinstance(e, _NoAnswer))
    except urllib.error.HTTPError as e:
        return _why(f"{ref}: HTTP {e.code} {e.reason}", False)
    except (urllib.error.URLError, OSError) as e:
        return _why(f"{ref}: {getattr(e, 'reason', e)}", False)


def _head_manifest(host: str, name: str, tag: str, user: str, token: str, budget: Budget | None = None) -> None:
    bearer = _exchange(host, name, user, token, budget)
    req = urllib.request.Request(f"{API}/v2/{name}/manifests/{tag}", method="HEAD",
                                 headers={"Authorization": f"Bearer {bearer}", "Accept": MANIFEST_TYPES})
    _attempt("manifest read", HTTP_BOUND_S, lambda: _read(req, 0, HTTP_BOUND_S), budget)
    return None


def _split_repo(repo: str, chart: str) -> tuple[str, str]:
    host, _, path = repo[len("oci://"):].partition("/")
    return host, f"{path}/{chart}".strip("/")


def resolve(repo: str, chart: str, version: str, user: str, token: str, budget: Budget | None = None) -> str | None:
    """None when oci://<repo>/<chart>:<version> resolves with these credentials
    (the registry's token exchange, then the manifest), else why not (a Why)."""
    if not repo.startswith("oci://"):
        return _why(f"{repo} is not an OCI repo", False)
    host, name = _split_repo(repo, chart)
    return _ref_error(f"oci://{host}/{name}:{version}",
                      lambda: _head_manifest(host, name, version, user, token, budget))


def split_image(ref: str) -> tuple[str, str, str]:
    """(host, name, tag-or-digest) of an image reference with an explicit registry host."""
    host, _, rest = ref.partition("/")
    if "@" in rest:
        name, _, tag = rest.partition("@")
    else:
        name, _, tag = rest.rpartition(":") if ":" in rest else (rest, "", "latest")
    return host, name, tag


def resolve_image(ref: str, user: str, token: str, budget: Budget | None = None) -> str | None:
    """None when the image's manifest resolves with these credentials, else why not (a Why)."""
    host, name, tag = split_image(ref)
    return _ref_error(ref, lambda: _head_manifest(host, name, tag, user, token, budget))


def _blob(host: str, name: str, digest: str, size: int, bearer: str) -> bytes:
    """A blob of its DECLARED size, digest-verified. The registry answers with
    a redirect to its blob storage: followed over https only and WITHOUT the
    Authorization header (the signed URL is the credential), like every
    registry client."""
    if not isinstance(size, int) or size < 0:
        raise _Refused(f"blob {digest[:19]} declares no size")
    if size > BLOB_MAX:
        raise _Refused(f"blob {digest[:19]} declares {size} bytes, over the {BLOB_MAX} byte ceiling of a chart")
    t0 = time.monotonic()

    def get(r):                          # a read of the blob, from the registry or its storage: both
        left = BLOB_BOUND_S - (time.monotonic() - t0)       # hops within the blob's ONE bound
        if left <= 0:
            raise _NoAnswer(f"no whole answer within its {BLOB_BOUND_S:g} s bound")
        return _read(r, size, left, whole=BLOB_BOUND_S)
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
    string under an `image` key. Sorted, unique. Walked with a stack of its
    own, never Python's: no depth nears the recursion limit (NEST_MAX)."""
    out, seen = set(), set()
    todo = [(values, None)]
    while todo:
        o, key = todo.pop()
        if isinstance(o, (dict, list)):
            # a YAML alias is the SAME object again: each one is walked once (an alias chain
            # would otherwise double the walk at every level)
            if (id(o), key) in seen:
                continue
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
            todo.extend((v, k) for k, v in o.items())
        elif isinstance(o, list):
            todo.extend((v, key) for v in o)
        elif key == "image" and isinstance(o, str) and "/" in o:
            out.add(o if (":" in o.rpartition("/")[2] or "@" in o) else f"{o}:latest")
    return sorted(out)


def images_in_manifests(docs) -> list[str]:
    """Every image the rendered manifests run: each string under an `image`
    key, at any depth (containers, init containers, CRs that carry one).
    Sorted, unique; YAML aliases walked once; a stack of its own (NEST_MAX)."""
    out, seen = set(), set()
    todo = [(docs, None)]
    while todo:
        o, key = todo.pop()
        if isinstance(o, (dict, list)):
            if id(o) in seen:
                continue
            seen.add(id(o))
        if isinstance(o, dict):
            todo.extend((v, k) for k, v in o.items())
        elif isinstance(o, list):
            todo.extend((v, key) for v in o)
        elif key == "image" and isinstance(o, str) and o.strip():
            out.add(o.strip())
    return sorted(out)


#: the refusal of a YAML document nested past NEST_MAX (a values.yaml 1,000 deep), named
DEEP = f"it nests deeper than its {NEST_MAX} level cap ({RENDER_MARGIN} x the deepest real document's)"


def _nest_check(text, loader, name: str = "") -> int:
    """The deepest level any document of a YAML stream reaches as built (maps
    and lists, the top one counted; 0 for scalars only); refuses (`name` +
    DEEP) past NEST_MAX. Read from the parser's events, which nest nothing,
    before any document is built. An alias counts the depth of the node it
    names (a chain of aliases builds depth no event shows)."""
    import yaml
    height, open_, depth, top = {}, [], 0, 0  # anchor -> its node's depth; per open map / list: [anchor, start, deepest]
    for ev in yaml.parse(text, Loader=loader):
        if isinstance(ev, yaml.DocumentStartEvent):
            height.clear()
            continue
        if isinstance(ev, (yaml.MappingStartEvent, yaml.SequenceStartEvent)):
            depth += 1
            open_.append([ev.anchor, depth, depth])
            reach = depth
        elif isinstance(ev, (yaml.MappingEndEvent, yaml.SequenceEndEvent)):
            anchor, start, deepest = open_.pop()
            if anchor:
                height[anchor] = deepest - start + 1
            depth -= 1
            reach = deepest
        elif isinstance(ev, yaml.AliasEvent):
            reach = depth + height.get(ev.anchor, 0)
        else:
            if isinstance(ev, yaml.ScalarEvent) and ev.anchor:
                height.pop(ev.anchor, None)  # the name now names a scalar
            continue
        if open_:
            open_[-1][2] = max(open_[-1][2], reach)
        if reach > NEST_MAX:
            raise _Refused(f"{name}{DEEP}")
        top = max(top, reach)
    return top


def _libyaml(what: str):
    """PyYAML's libyaml class `what` (CSafeLoader, CSafeDumper), else a named
    refusal: the pure-Python composer and representer recurse (NEST_MAX)."""
    import yaml
    cls = getattr(yaml, what, None)
    if cls is None:
        raise _Refused(f"libyaml not available: this PyYAML has no {what}, and its pure-Python one recurses "
                       f"(Python's limit near 400-490 levels, under the {NEST_MAX} level cap): install PyYAML "
                       f"built with libyaml (pip's wheels are)")
    return cls


def _parse_yaml(text, name: str = "", all_docs: bool = False):
    """(the document, or every document, of `text`; the deepest level it reaches): the preflight's
    one YAML reader. Within NEST_MAX (_nest_check), then built by libyaml (_yaml_loader). `name`
    prefixes a refusal."""
    import yaml
    loader = _yaml_loader()
    deepest = _nest_check(text, loader, name)
    if all_docs:
        return list(yaml.load_all(text, Loader=loader)), deepest   # noqa: S506 - a safe loader
    return yaml.load(text, Loader=loader), deepest                  # noqa: S506 - a safe loader


def _load_yaml(text, name: str = ""):
    """_parse_yaml's document."""
    return _parse_yaml(text, name)[0]


def _dump_yaml(data) -> str:
    """`data` (a map, as values are) as yaml.safe_dump writes it (keys sorted when they sort, block
    style, a map or list met again an alias of its first), emitted by libyaml
    from events built with a stack of their own: PyYAML's representer and
    serializer recurse a few frames a level (Python's limit near 400 levels,
    under NEST_MAX). Each scalar is still PyYAML's representation."""
    import io
    import yaml
    seen, again, todo = set(), set(), [data]      # maps and lists met more than once: anchored at the first
    while todo:
        o = todo.pop()
        if isinstance(o, (dict, list)):
            if id(o) in seen:
                again.add(id(o))
                continue
            seen.add(id(o))
            todo.extend(o.values() if isinstance(o, dict) else o)
    stream = io.StringIO()
    d = _libyaml("CSafeDumper")(stream)
    anchors = {}
    todo = [("event", yaml.StreamEndEvent()), ("event", yaml.DocumentEndEvent(explicit=False)), ("node", data)]
    d.emit(yaml.StreamStartEvent())
    d.emit(yaml.DocumentStartEvent(explicit=False))
    while todo:
        kind, o = todo.pop()
        if kind == "event":
            d.emit(o)
        elif isinstance(o, (dict, list)):
            if id(o) in anchors:
                d.emit(yaml.AliasEvent(anchors[id(o)]))
                continue
            anchor = None
            if id(o) in again:
                anchor = anchors[id(o)] = f"id{len(anchors) + 1:03d}"
            if isinstance(o, dict):
                items = list(o.items())
                try:
                    items = sorted(items)                 # as SafeRepresenter.represent_mapping
                except TypeError:
                    pass
                d.emit(yaml.MappingStartEvent(anchor, None, True, flow_style=False))
                todo.append(("event", yaml.MappingEndEvent()))
                for k, v in reversed(items):
                    todo += [("node", v), ("node", k)]
            else:
                d.emit(yaml.SequenceStartEvent(anchor, None, True, flow_style=False))
                todo.append(("event", yaml.SequenceEndEvent()))
                todo += [("node", v) for v in reversed(o)]
        else:
            node = d.represent_data(o)
            if not isinstance(node, yaml.ScalarNode):
                raise yaml.representer.RepresenterError(f"cannot represent {type(o).__name__} as a values scalar")
            implicit = (node.tag == d.resolve(yaml.ScalarNode, node.value, (True, False)),
                        node.tag == d.resolve(yaml.ScalarNode, node.value, (False, True)))
            d.emit(yaml.ScalarEvent(None, node.tag, implicit, node.value, style=node.style))
    d.dispose()
    return stream.getvalue()


def _child_main(conn, fn_name: str, args: tuple) -> None:
    """_in_child's other end: fn(*args), its answer (or its refusal) sent back."""
    try:
        conn.send(("ok", globals()[fn_name](*args)))
    except _Refused as e:
        conn.send(("refused", str(e)))
    except Exception as e:                  # noqa: BLE001 - reported to the caller, never lost
        conn.send(("error", f"{type(e).__name__}: {str(e)[:300]}"))
    finally:
        conn.close()


def _in_child(fn_name: str, args: tuple, bound_s: float, late: str):
    """fn(*args) in a CHILD process (spawned), killed at `bound_s`: unlike a
    thread (_call), the work itself ends at the bound, with its memory
    (#1041 review round 4, M1b: an abandoned parse ran on for 57 s). The
    answer comes back pickled; a child that dies without one is a refusal.
    `late` is the refusal at the bound: no answer (_NoAnswer)."""
    ctx = multiprocessing.get_context("spawn")
    rd, wr = ctx.Pipe(duplex=False)
    pr = ctx.Process(target=_child_main, args=(wr, fn_name, args), daemon=True)
    pr.start()
    wr.close()
    try:
        if not rd.poll(bound_s):
            raise _NoAnswer(late)
        try:
            kind, v = rd.recv()
        except (EOFError, OSError):
            raise _Refused(f"its reader ended with no answer (exit {pr.exitcode})") from None
    finally:
        if pr.is_alive():
            pr.kill()
        pr.join()
        rd.close()
    if kind != "ok":
        raise _Refused(v)
    return v


#: how much of helm's stderr is read: its last line is the reason
STDERR_TAIL = 4096


def _tail(fh, n: int = STDERR_TAIL) -> str:
    """The last `n` bytes of a binary file, decoded: helm's reason, never its whole stderr (#1041
    review N3: a 200 MB fail message cost +434 MiB)."""
    fh.seek(0, os.SEEK_END)
    fh.seek(max(0, fh.tell() - n))
    return fh.read(n).decode("utf-8", "replace")


def render(data: bytes, name: str, values: dict, helm: str = "helm", meta: dict | None = None,
           args: tuple = ()) -> list:
    """`helm template` of the chart `data` with `values`: its manifests. The
    WHOLE of it within RENDER_BOUND_S: helm run and its stdout read streamed
    and capped at RENDER_OUT_MAX (_call; past the bound helm is killed, and
    the reading ends with it), then parsed in a child process killed at what
    is left of the bound (_in_child: the parse stops, not only the wait). Raises _Refused with helm's own reason when it cannot render.
    The chart (never a credential) is written to a private temporary
    directory removed at once; the values go on stdin (_dump_yaml), stderr
    to a file. `meta` gets `render` (bytes) and `render_deepest` (levels). `args`: helm template's
    own flags, after the chart (private_image_paths: --kube-version, --show-only)."""
    procs: list = []

    def kill():
        for pr in procs:
            if pr.poll() is None:
                pr.kill()
                pr.wait()

    def run():
        with tempfile.TemporaryDirectory(prefix="bench-render-") as d:
            path, err = os.path.join(d, "chart.tgz"), os.path.join(d, "stderr")
            with open(path, "wb") as fh:
                fh.write(data)
            with open(err, "w+b") as ef:
                try:
                    pr = subprocess.Popen([helm, "template", name, path, "--namespace", SECRET_NS, *args, "-f", "-"],
                                          stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=ef)
                except OSError as e:
                    raise _Refused(f"helm template could not run: {e}") from None
                procs.append(pr)
                unread = False
                try:
                    pr.stdin.write(_dump_yaml(values or {}).encode())
                    pr.stdin.close()
                except OSError:             # helm exited before reading its values: its own reason follows
                    unread = True
                out = bytearray()
                while True:
                    chunk = pr.stdout.read1(65536)
                    if not chunk:
                        break
                    out += chunk
                    if len(out) > RENDER_OUT_MAX:
                        kill()
                        raise _Refused(f"helm template rendered more than its {RENDER_OUT_MAX} byte cap")
                rc = pr.wait()
                # helm's reason is its last `Error:` line: after a template error helm adds a hint
                # ("Use --debug flag to render out invalid YAML"), which is not one (#1395)
                said = _tail(ef).strip().splitlines() or [f"exit {rc}"]
                why = ([x for x in said if x.startswith("Error")] or said)[-1]
                if meta is not None:
                    meta["render"] = len(out)
        if rc != 0:
            raise _Refused(f"helm template failed: {why[:300]}")
        if unread:
            raise _Refused("helm template exited without reading its values")
        return bytes(out)
    late = f"helm template did not finish within its {RENDER_BOUND_S:g} s bound"
    t0 = time.monotonic()
    try:
        out = _call(run, RENDER_BOUND_S, on_timeout=kill)
    except _NoAnswer:                       # helm did not finish: no answer, not a render failure
        raise _NoAnswer(late) from None
    docs, deepest = _in_child("_parse_manifests", (out,), max(0.0, RENDER_BOUND_S - (time.monotonic() - t0)), late)
    if meta is not None:
        meta["render_deepest"] = deepest
    return docs


def _parse_manifests(out: bytes) -> tuple[list, int]:
    """(helm template's output as documents, the deepest level they nest):
    run in a child (render), each within NEST_MAX (_parse_yaml)."""
    import yaml
    try:
        docs, deepest = _parse_yaml(out.decode(), "helm template rendered YAML: ", all_docs=True)
        return [x for x in docs if x is not None], deepest
    except (yaml.YAMLError, UnicodeDecodeError) as e:
        raise _Refused(f"helm template rendered no YAML: {str(e).splitlines()[0][:200]}") from None


def render_images(data: bytes, name: str, values: dict, helm: str = "helm") -> list[str]:
    """The images the chart's manifests run, rendered with `values`."""
    return images_in_manifests(render(data, name, values, helm))


def _secret_names(xs) -> set:
    return {x.get("name") for x in xs or [] if isinstance(x, dict) and x.get("name")}


def pods(docs) -> list[dict]:
    """Every pod spec in the manifests (a map with a `containers` list, at
    any depth: a Deployment's template, a Job's, a CR that embeds one):
    {pull_secrets, images}. A pod's pull secrets are, as Kubernetes applies
    them, its own imagePullSecrets PLUS those of the ServiceAccount it runs
    as (serviceAccountName, else `default`) among the rendered manifests
    (#1041 review N1: alert-provider 0.2.53 puts its secret on its
    ServiceAccount), in the pod's OWN namespace (round 3, F): a manifest's
    metadata.namespace, else the release's (helm template --namespace).
    YAML aliases walked once; a stack of its own, in the manifests' order
    (NEST_MAX)."""
    docs = docs if isinstance(docs, list) else [docs]

    def ns_of(d):
        return ((d.get("metadata") or {}).get("namespace") if isinstance(d, dict) else None) or SECRET_NS
    accounts = {}
    for d in docs:
        if isinstance(d, dict) and d.get("kind") == "ServiceAccount":
            accounts[(ns_of(d), (d.get("metadata") or {}).get("name"))] = _secret_names(d.get("imagePullSecrets"))
    out, seen = [], set()
    # depth first, each node's children pushed last first: the pods in the order a recursive
    # walk met them
    todo = [(d, ns_of(d)) for d in reversed(docs)]
    while todo:
        o, ns = todo.pop()
        if isinstance(o, (dict, list)):
            if id(o) in seen:
                continue
            seen.add(id(o))
        if isinstance(o, dict):
            if isinstance(o.get("containers"), list):
                cs = [c for c in (o.get("initContainers") or []) + o["containers"] if isinstance(c, dict)]
                sa = o.get("serviceAccountName") or o.get("serviceAccount") or "default"
                out.append({"pull_secrets": sorted(_secret_names(o.get("imagePullSecrets"))
                                                   | accounts.get((ns, sa), set())),
                            "service_account": sa, "namespace": ns,
                            "images": sorted({c["image"] for c in cs if isinstance(c.get("image"), str)})})
            todo.extend((v, ns) for v in reversed(list(o.values())))
        elif isinstance(o, list):
            todo.extend((v, ns) for v in reversed(o))
    return out


def marker(i: int) -> str:
    """The pull-secret name the preflight injects at a component's i-th path."""
    return f"bench-preflight-path-{i}"


def with_markers(values: dict, paths: list[list[str]]) -> dict:
    """`values` with each path's leaf set to its own marker pull secret, in
    the shape the installer injects (`list (dict "name" ...)`,
    compositions.yaml): which pod carries which path's secret is then read
    from the render. The leaf is set whatever it held: the image a path
    serves does not depend on the secret's name. `values` is left as it is:
    each map on a path is copied before it is set (copy.deepcopy would
    recurse a frame or two a level: NEST_MAX), the rest is shared."""
    out = dict(values or {})
    for i, path in enumerate(paths):
        cur = out
        for seg in path[:-1]:
            nxt = cur.get(seg)
            nxt = cur[seg] = dict(nxt) if isinstance(nxt, dict) else {}
            cur = nxt
        cur[path[-1]] = [{"name": marker(i)}]
    return out


def path_results(docs, paths: list[list[str]], effective: dict) -> list[dict]:
    """Per pull-secret path of a private-image component: the rendered pods
    that carry its marker and their ghcr.io images. `ok` when such a pod runs
    a ghcr.io image. `off` when the path's own sub-component is turned off in
    the values the chart rendered with (an ancestor map with `enabled: false`,
    helm's convention) AND no rendered pod runs a ghcr.io image without any
    of the component's markers: an "off" hop that still runs (its template
    ignores `enabled`), or a pod that reads its secret from another key, is
    not off (#1041 review N2). Else `missing`, refused: the installer injects
    a secret there for an image the preflight never saw run with it."""
    ps = pods(docs)
    mine = {marker(i) for i in range(len(paths))}
    unmarked = sorted({im for pd in ps if not (mine & set(pd["pull_secrets"]))
                       for im in pd["images"] if im.startswith(REGISTRY_HOST + "/")})
    out = []
    for i, path in enumerate(paths):
        imgs = sorted({im for pd in ps if marker(i) in pd["pull_secrets"] for im in pd["images"]})
        ghcr = [im for im in imgs if im.startswith(REGISTRY_HOST + "/")]
        off, cur = None, effective
        for k, seg in enumerate(path[:-1]):
            cur = cur.get(seg) if isinstance(cur, dict) else None
            if isinstance(cur, dict) and cur.get("enabled") is False:
                off = ".".join(path[:k + 1]) + ".enabled: false"
                break
        res = "ok" if ghcr else ("off" if off and not unmarked else "missing")
        out.append({"path": ".".join(path), "result": res, "images": imgs,
                    **({"why": off} if res == "off" else {}),
                    **({"unmarked_ghcr_pods": unmarked} if res == "missing" and unmarked else {})})
    return out


def chart_images(repo: str, chart: str, version: str, user: str, token: str, values: dict | None = None,
                 helm: str = "helm", name: str | None = None, paths: list[list[str]] | None = None,
                 budget: Budget | None = None) -> dict:
    """What oci://<repo>/<chart>:<version> runs: {images, rendered,
    render_error, error}. The chart is fetched in-process (manifest, its helm
    layer); `rendered` is what `helm template` with `values` (the installer's
    componentValues for it) runs, None with `render_error` when it cannot
    render; `images` adds what every values.yaml in it names (a subchart's own
    appVersion for its empty tags). `error` when the chart itself cannot be
    read: then nothing is known about its images.

    `name` is the release name (the installer renders a component under its
    own name, $c.name). `paths`: the component's pull-secret paths when it is
    a private-image component; then `paths` gets each one's pods (see
    path_results). `budget`: the step's (each call asked again after no
    answer while it has room); `no_answer` / `render_no_answer` say that
    `error` / `render_error` is NO ANSWER, not the registry's or helm's."""
    import yaml
    release = name or chart
    host, name = _split_repo(repo, chart)
    ref = f"oci://{host}/{name}:{version}"
    out = {"images": [], "rendered": None, "render_error": None, "error": None, "paths": None, "measured": None,
           "no_answer": False, "render_no_answer": False}

    def fetch():
        bearer = _exchange(host, name, user, token, budget)
        req = urllib.request.Request(f"{API}/v2/{name}/manifests/{version}",
                                     headers={"Authorization": f"Bearer {bearer}",
                                              "Accept": "application/vnd.oci.image.manifest.v1+json"})
        try:
            manifest = json.loads(_attempt("manifest read", HTTP_BOUND_S,
                                           lambda: _read(req, MANIFEST_MAX, HTTP_BOUND_S), budget) or b"{}")
        except ValueError:
            raise _Refused("its manifest is not JSON") from None
        layers = [x for x in manifest.get("layers") or [] if x.get("mediaType") == HELM_CHART_LAYER]
        if len(layers) != 1:
            raise _Refused(f"{len(layers)} helm chart layers in its manifest")
        data = _attempt("chart blob read", BLOB_BOUND_S,
                        lambda: _blob(host, name, layers[0]["digest"], layers[0].get("size"), bearer), budget)
        charts, vals, inflated, largest, parsed, members, deepest = _attempt(
            "chart files read", PARSE_BOUND_S, lambda: _in_child(
                "read_chart_files", (data, chart_caps()), PARSE_BOUND_S,
                f"its chart files were not read within their {PARSE_BOUND_S:g} s bound"), budget)
        if not charts:
            raise _Refused("no Chart.yaml in its chart layer")
        out["measured"] = {"blob": len(data), "largest_file": largest, "inflated": inflated, "parsed": parsed,
                           "members": members, "deepest": deepest}
        return walk(charts, vals, data)

    def walk(charts, vals, data):
        imgs = set()
        for d, v in vals.items():
            c = charts.get(d) or {}
            imgs.update(images_in_values(v, str(c.get("appVersion") or c.get("version") or version)))
        try:
            given = with_markers(values or {}, paths) if paths else (values or {})
            docs = _attempt("helm template", RENDER_BOUND_S,
                            lambda: render(data, release, given, helm, meta=out["measured"]), budget)
            out["rendered"] = images_in_manifests(docs)
            imgs.update(out["rendered"])
            if paths:
                top = min(vals, key=len) if vals else None
                out["paths"] = path_results(docs, paths, _merge(vals.get(top) or {}, given))
        except _Refused as e:
            out["render_error"], out["render_no_answer"] = str(e), isinstance(e, _NoAnswer)
        return sorted(imgs)

    res = _ref_error(ref, fetch)
    if isinstance(res, list):
        out["images"] = res
    else:
        why = res[len(ref) + 2:] if res.startswith(f"{ref}: ") else res
        out["no_answer"] = res.no_answer
        out["error"] = f"{ref}: " + ("its images unknown: " if res.no_answer else "its images unreadable: ") + why
    return out


def _yaml_loader():
    """libyaml's safe loader (CSafeLoader), else a named refusal ("libyaml not
    available"): it builds a document in C, where PyYAML's pure-Python
    composer recurses two frames a level and reaches Python's limit near 490,
    under NEST_MAX (#1356). The probe in the caps comment: libyaml parses
    about 4x faster besides."""
    return _libyaml("CSafeLoader")


def chart_caps() -> dict:
    """The caps read_chart_files applies, as this process holds them (passed to its child)."""
    return {"inflated": CHART_INFLATED_MAX, "file": CHART_FILE_MAX, "parsed": CHART_PARSED_MAX,
            "members": CHART_MEMBERS_MAX}


def read_chart_files(data: bytes, caps: dict | None = None) -> tuple[dict, dict, int, int, int, int, int]:
    """(Chart.yaml by dir, values.yaml by dir, bytes inflated, largest file,
    bytes parsed, members, the deepest level any of them nests: _nest_check)
    of a chart blob. The tar is walked header by header,
    streamed, stopping at the first cap passed; the headers never accumulate
    (tarfile would keep every one). The Chart.yaml / values.yaml bytes are
    kept raw while walking and counted against CHART_PARSED_MAX, then
    parsed: a chart past it is refused before any parse. Memory: the blob,
    those raw bytes (at most CHART_PARSED_MAX) and their parsed documents
    (structured YAML costs about 100 bytes per byte parsed: +203 MiB at the
    cap, the probe in the caps comment), and every member counts against
    CHART_MEMBERS_MAX as its header is read: an empty file costs a walk and
    a parse too (round 4, M1b). The caller runs this in a child process
    killed at PARSE_BOUND_S (chart_images, _in_child). `caps`: chart_caps()."""
    import yaml
    cap = caps or chart_caps()
    charts, vals, inflated, largest, parsed, members = {}, {}, 0, 0, 0, 0
    raw = []
    with tarfile.open(fileobj=io.BytesIO(data), mode="r:gz") as tf:
        while True:
            m = tf.next()
            if m is None:
                break
            tf.members = []
            members += 1
            if members > cap["members"]:
                raise _Refused(f"its chart has more than its {cap['members']} member cap "
                               f"({RENDER_MARGIN} x the largest real chart's)")
            inflated += max(0, m.size)
            largest = max(largest, m.size if m.isfile() else 0)
            if inflated > cap["inflated"]:
                raise _Refused(f"its chart inflates past {cap['inflated']} bytes")
            d, _, base = m.name.rpartition("/")
            if m.isfile() and base in ("Chart.yaml", "values.yaml"):
                if m.size > cap["file"]:
                    raise _Refused(f"{m.name} inflates to {m.size} bytes, past helm's own "
                                   f"{cap['file']} byte file limit")
                parsed += max(0, m.size)
                if parsed > cap["parsed"]:
                    raise _Refused(f"its Chart.yaml and values.yaml files come to more than its "
                                   f"{cap['parsed']} byte parse cap ({RENDER_MARGIN} x the largest real total)")
                raw.append((d, base, tf.extractfile(m).read(cap["file"] + 1)))
    deepest = 0
    for d, base, body in raw:
        try:
            doc, deep = _parse_yaml(body, f"{d + '/' if d else ''}{base}: ")
        except (yaml.YAMLError, UnicodeDecodeError) as e:
            raise _Refused(f"{d + '/' if d else ''}{base} is not YAML: {str(e).splitlines()[0][:200]}") from None
        (charts if base == "Chart.yaml" else vals)[d] = doc or {}
        deepest = max(deepest, deep)
    return charts, vals, inflated, largest, parsed, members, deepest


def stale_inputs(charts: list[dict]) -> list[dict]:
    """Every measured size of this run that exceeds the REAL_LARGEST input
    the caps derive from (#1041 review R31): that input is hand-copied from
    an earlier run, so it shows itself stale here, long before a real chart
    reaches RENDER_MARGIN x it. A report, not a refusal."""
    out = []
    for c in charts:
        for k, v in sorted((c.get("measured") or {}).items()):
            key = MEASURED_INPUT.get(k)
            if key and isinstance(v, int) and v > REAL_LARGEST[key]:
                out.append({"chart": c["chart"], "version": c["version"], "measured": k, "value": v,
                            "input": key, "real_largest": REAL_LARGEST[key]})
    return out


def stale_notes(stale: list[dict]) -> list[str]:
    """The preflight's report line per stale input (stale_inputs): bytes, or levels for a depth."""
    def unit(x):
        return "levels" if x["input"] == "deepest" else "B"
    return [f"[ghcr] NOTE: {x['chart']} {x['version']} measured {x['measured']} = {x['value']} {unit(x)}, over the "
            f"caps' measured input REAL_LARGEST[{x['input']!r}] = {x['real_largest']} {unit(x)}: re-measure it (the "
            f"{RENDER_MARGIN} x margin is over a stale input; reported, not refused)" for x in stale]


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
    pf.add_argument("--budget-file", default=None, help="the run's one bound of its pre-cluster registry reads "
                    "(bench/noanswer.py Budget.shared): this step adds its share")
    pf.add_argument("--override", action="append", default=[], metavar="COMPONENT=TAG",
                    help="a component whose image this run overrides (its componentValues in --values): "
                         "preflighted whatever its repo; refused unless a ghcr.io image it renders has TAG")
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
    overrides = {}
    for o in a.override:
        comp, eq, tag = o.partition("=")
        if not (comp and eq and tag) or comp in overrides:
            print(f"refusing: --override {o!r}: COMPONENT=TAG, one per component", file=sys.stderr)
            return 2
        overrides[comp] = tag
    try:
        # #1395: rendered with the values the run installs with: the bench's file, then the
        # username document s_install pipes after it (`-f values -f -`), merged as helm merges them
        with open(a.values, "rb") as fh:
            run_values = _merge(_load_yaml(fh.read(), f"{a.values}: ") or {}, {"registryAuth": {"username": user}})
        paths = private_image_paths(a.installer_pulled, run_values, a.helm)
        private = set(paths)
        pins = private_pins(a.installer_pulled, a.values, include=private | set(overrides))
    except (ValueError, _Refused) as e:  # the installer's enable rule unreadable: nothing to resolve against
        print(f"refusing: {str(e).replace(user, '<registry user>') if user else e}", file=sys.stderr)
        return 2
    absent = sorted(set(overrides) - {p["name"] for p in pins})
    if absent:
        print(f"refusing: the run overrides the image of {', '.join(absent)}, which this install does not "
              f"enable (no pin, or its feature is off): the override would run nowhere", file=sys.stderr)
        return 2
    # the step's bound, the one it always derived: every call it makes at its own WHOLE bound (_call;
    # a resolve = 2 calls; a chart read = 2 calls, its blob and its files' parse; an image = 2 calls)
    # and each render at its own. Run 25: it now also BOUNDS the step (Budget): every reference is
    # tried once, then one with NO ANSWER again while what is left holds its whole share (_phase).
    chart_s, image_s = 4 * HTTP_BOUND_S + BLOB_BOUND_S + PARSE_BOUND_S + RENDER_BOUND_S, 2 * HTTP_BOUND_S
    budget = Budget.shared(a.budget_file) if a.budget_file else Budget()
    before = budget.total                      # the run's earlier registry reads' share, if shared

    def one_chart(p):
        ref = f"{p['repo']}/{p['chart']}"
        err = resolve(p["repo"], p["chart"], p["version"], user, token, budget)
        render, ci, no_answer = None, None, bool(err) and err.no_answer
        if not err:
            ci = chart_images(p["repo"], p["chart"], p["version"], user, token, p["values"], a.helm,
                              name=p["name"], paths=paths.get(p["name"]), budget=budget)
            err, no_answer = ci["error"], ci["no_answer"]
            render = (None if ci["error"] else ("failed: " + ci["render_error"]) if ci["render_error"]
                      else f"ok ({len(ci['rendered'])} images)")
            if not err and p["name"] in private:
                # the installer injects image-pull Secrets into this one: its images MUST be known
                missing = [r["path"] for r in ci["paths"] or [] if r["result"] == "missing"]
                if ci["render_error"] and ci["render_no_answer"]:
                    err, no_answer = (f"{ref}:{p['version']}: the installer lists it as a private-image "
                                      f"component (inst.privateImageComponents) and its render gave no answer: "
                                      f"{ci['render_error']}"), True
                elif ci["render_error"]:
                    err = (f"{ref}:{p['version']}: the installer lists it as a private-image component "
                           f"(inst.privateImageComponents) and it cannot be rendered: {ci['render_error']}")
                elif missing:
                    err = (f"{ref}:{p['version']}: the installer lists it as a private-image component "
                           f"(inst.privateImageComponents), yet no rendered pod running a {REGISTRY_HOST} "
                           f"image carries the pull secret it injects at {', '.join(missing)} (the image "
                           f"behind it was never seen)")
            if not err and p["name"] in overrides:
                # the run overrides this one's image: the image it renders must be the one overridden
                want = overrides[p["name"]]
                hits = [i for i in ci["rendered"] or []
                        if i.startswith(REGISTRY_HOST + "/") and split_image(i)[2] == want]
                if ci["render_error"] and ci["render_no_answer"]:
                    err, no_answer = (f"{ref}:{p['version']}: the run overrides its image (tag {want}) and its "
                                      f"render gave no answer: {ci['render_error']}"), True
                elif ci["render_error"]:
                    err = (f"{ref}:{p['version']}: the run overrides its image (tag {want}) and it cannot be "
                           f"rendered: {ci['render_error']}")
                elif not hits:
                    err = (f"{ref}:{p['version']}: the run overrides its image (tag {want}), yet no "
                           f"{REGISTRY_HOST} image it renders carries that tag (rendered: "
                           f"{', '.join(ci['rendered'] or []) or 'none'})")
        return ({"chart": ref, "version": p["version"], "render": render,
                 "private_image_component": p["name"] in private,
                 **({"image_override": overrides[p["name"]]} if p["name"] in overrides else {}),
                 "paths": ci["paths"] if ci else None, "measured": ci["measured"] if ci else None,
                 "error": err, "no_answer": bool(err) and no_answer}, ci)

    charts, chart_tries = _phase(budget, pins, chart_s, one_chart, lambda r: r[0]["no_answer"])
    res, images, other = [], {}, {}
    for p, (r, ci), n in zip(pins, charts, chart_tries):
        res.append({**r, "attempts": n})
        for i in (ci or {}).get("images") or []:
            (images if i.startswith(REGISTRY_HOST + "/") else other).setdefault(i, []).append(p["name"])

    def one_image(item):
        i, c = item
        err = resolve_image(i, user, token, budget)
        return {"image": i, "charts": c, "error": err, "no_answer": bool(err) and err.no_answer}

    imgs, img_tries = _phase(budget, sorted(images.items()), image_s, one_image, lambda r: r["no_answer"])
    img_res = [{**r, "attempts": n} for r, n in zip(imgs, img_tries)]
    for r in res + img_res:
        if r["no_answer"]:
            r["error"] = (f"{r['error']}; {r['attempts']} attempt(s), the step's {budget.total:g} s bound spent: "
                          f"whether the token resolves it is unknown")
    bound = budget.total - before                # this step's own share of the bound
    how = (f"{len(pins)} charts x (4 x {HTTP_BOUND_S:g} + {BLOB_BOUND_S:g} + {PARSE_BOUND_S:g} + "
           f"{RENDER_BOUND_S:g} s) + {len(img_res)} images x (2 x {HTTP_BOUND_S:g} s)")
    stale = stale_inputs(res)
    with open(a.out, "w") as fh:
        json.dump({"kind": "ghcr_pull_preflight", "bound_s": bound, "bound": how, "caps": CAPS,
                   "stale_inputs": stale,
                   "private_image_components": sorted(private),
                   "charts": res, "images": img_res,
                   "images_elsewhere": [{"image": i, "charts": c} for i, c in sorted(other.items())]}, fh, indent=1)
    print(f"[ghcr] the step's bound: {bound:g} s ({how}); a call with no answer is asked again within it",
          file=sys.stderr)
    print(f"[ghcr] the installer's private-image components (inst.privateImageComponents, rendered): "
          f"{', '.join(sorted(private))}", file=sys.stderr)
    for r in res:
        print(f"[ghcr] {r['chart']} {r['version']}: {'OK' if not r['error'] else r['error']}"
              + (f" (render {r['render']})" if r["render"] else ""), file=sys.stderr)
    for r in img_res:
        print(f"[ghcr] image {r['image']} ({', '.join(r['charts'])}): {'OK' if not r['error'] else r['error']}",
              file=sys.stderr)
    if other:
        print(f"[ghcr] {len(other)} image(s) on other registries (not this token's): " + ", ".join(sorted(other)),
              file=sys.stderr)
    for line in stale_notes(stale):
        print(line, file=sys.stderr)
    refused = [r["error"] for r in res + img_res if r["error"] and not r["no_answer"]]
    unanswered = [r["error"] for r in res + img_res if r["error"] and r["no_answer"]]
    if refused:
        print("refusing: the ghcr pull token does not resolve every chart and image this install pulls: "
              + "; ".join(refused)
              + (f" (and NO ANSWER for {len(unanswered)} more: " + "; ".join(unanswered) + ")" if unanswered else ""),
              file=sys.stderr)
        return EXIT_REFUSED
    if unanswered:
        print(f"refusing: NO ANSWER from the registry within the step's {bound:g} s bound for "
              f"{len(unanswered)} of this install's charts and images, so whether the ghcr pull token resolves "
              f"them is unknown (not that it does not): " + "; ".join(unanswered), file=sys.stderr)
        return EXIT_NO_ANSWER
    return 0


if __name__ == "__main__":
    sys.exit(main())
