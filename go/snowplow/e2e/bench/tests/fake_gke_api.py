"""The GKE API and IAM as gke-up's TTL proof Job meets them (run 26h), for the suite's fakes.

The proof Job (bench.gke.ttl_proof_manifest) calls GET and DELETE on two cluster names that do not exist,
as the node SA. What GKE answers depends on what IAM grants that SA on the cluster's resource name, so this
model evaluates the IAM condition itself: a binding's condition, written in IAM's condition language
(https://docs.cloud.google.com/iam/docs/conditions-attribute-reference), the subset bench.gke writes:

    resource.type == "<type>"
    resource.name.extract("<template>") != ""        (and == "")
    a && b, a || b, ( ... )

extract() as IAM documents it: the template is <prefix>{identifier}<suffix>, each part optional; the result is
the text after the FIRST occurrence of the prefix up to the NEXT occurrence of the suffix (to the end without a
suffix, from the start without a prefix), and "" when the prefix or the suffix is not found.

GKE's answers are shaped as Google API errors are (code, message, status, details[] ErrorInfo); a 403's message
names the caller, as Google's do, so a test can check it never reaches a log. What GKE does first for a name that
does not exist (the permission, or the lookup) is the module's assumption, AIP-211's: the permission (the first
real run settles it; bench.gke.ttl_proof_manifest).

The suite's fake kubectl / gcloud are scripts with no PYTHONPATH: LOADER is the source to put in front of a
script's body; it defines ttl_proof_logs(applied_text, mode).
"""

from __future__ import annotations

import json
import re
import sys
from pathlib import Path

BENCH_DIR = Path(__file__).resolve().parent.parent
if str(BENCH_DIR) not in sys.path:
    sys.path.insert(0, str(BENCH_DIR))

from bench import gke  # noqa: E402

CALLER = "snowplow-bench-node@operations-dev-krateo-io.iam.gserviceaccount.com"
# in every error message: an error's free text never reaches a log (bench.gke.probe_answer names codes and reasons only)
MESSAGE_SENTINEL = "free-text-of-the-error-message"
_PROBE = re.compile(r"^\s*probe (\S+) (GET|DELETE) '([^']+)' (\S+)\s*$", re.M)
_TOKEN = re.compile(r'\s*(==|!=|&&|\|\||\(|\)|"(?:[^"\\]|\\.)*"|resource\.type|resource\.name\.extract)')


def extract(name: str, template: str) -> str:
    """IAM's resource.name.extract(template), as documented (module doc)."""
    m = re.fullmatch(r"(.*?)\{[A-Za-z0-9_]+\}(.*)", template)
    if not m:
        raise ValueError(f"not an extraction template: {template!r}")
    prefix, suffix = m.groups()
    start = 0
    if prefix:
        i = name.find(prefix)
        if i < 0:
            return ""
        start = i + len(prefix)
    if not suffix:
        return name[start:]
    j = name.find(suffix, start)
    return "" if j < 0 else name[start:j]


def evaluate(expression: str, *, rtype: str, name: str) -> bool:
    """The condition's value for one resource (the subset in the module doc; anything else raises)."""
    toks = []
    pos = 0
    while pos < len(expression):
        m = _TOKEN.match(expression, pos)
        if not m:
            if expression[pos:].strip() == "":
                break
            raise ValueError(f"not in the modelled subset at {expression[pos:pos + 30]!r}")
        toks.append(m.group(1))
        pos = m.end()
    it = iter(toks + [None])
    cur = [next(it)]

    def take(want=None):
        t = cur[0]
        if want is not None and t != want:
            raise ValueError(f"expected {want!r}, got {t!r}")
        cur[0] = next(it)
        return t

    def lit() -> str:
        t = take()
        if not (t and t.startswith('"')):
            raise ValueError(f"expected a string, got {t!r}")
        return json.loads(t)

    def atom() -> bool:
        if cur[0] == "(":
            take("(")
            v = disj()
            take(")")
            return v
        if cur[0] == "resource.type":
            take()
            op = take()
            v = lit()
            return (rtype == v) if op == "==" else (rtype != v) if op == "!=" else _bad(op)
        if cur[0] == "resource.name.extract":
            take()
            take("(")
            got = extract(name, lit())
            take(")")
            op = take()
            v = lit()
            return (got == v) if op == "==" else (got != v) if op == "!=" else _bad(op)
        raise ValueError(f"unexpected {cur[0]!r}")

    def conj() -> bool:
        v = atom()
        while cur[0] == "&&":
            take()
            v = atom() and v
        return v

    def disj() -> bool:
        v = conj()
        while cur[0] == "||":
            take()
            v = conj() or v
        return v

    out = disj()
    if cur[0] is not None:
        raise ValueError(f"trailing {cur[0]!r}")
    return out


def _bad(op):
    raise ValueError(f"operator {op!r} is not modelled")


def granted(mode: str, resource_name: str) -> bool:
    """Whether IAM grants the node SA a REAPER_PERMISSIONS call on `resource_name`: "granted" is node-sa's grant
    (the role under bench.gke.reaper_condition()), "unlimited" the same role with no condition, "denied" none."""
    if mode == "unlimited":
        return True
    if mode == "granted":
        return evaluate(gke.reaper_condition()["expression"], rtype=gke.CLUSTER_RESOURCE_TYPE, name=resource_name)
    return False


def answer(mode: str, perm: str, url: str, cluster: str) -> tuple[int, dict]:
    """(HTTP code, body) GKE answers one probe on a cluster that does not exist."""
    resource = url.split("/v1/", 1)[1] + "/" + cluster
    if mode == "disabled":
        return 403, {"error": {"code": 403, "status": "PERMISSION_DENIED",
                               "message": f"Kubernetes Engine API has not been used in project 1 before or it is "
                                          f"disabled (caller {CALLER}). {MESSAGE_SENTINEL}",
                               "details": [{"@type": "type.googleapis.com/google.rpc.ErrorInfo",
                                            "reason": "SERVICE_DISABLED",
                                            "metadata": {"service": "container.googleapis.com"}}]}}
    if granted(mode, resource):
        return 404, {"error": {"code": 404, "status": "NOT_FOUND",
                               "message": f"Not found: {resource}. {MESSAGE_SENTINEL}"}}
    return 403, {"error": {"code": 403, "status": "PERMISSION_DENIED",
                           "message": f"Required \"{perm}\" permission(s) for \"{resource}\" (caller {CALLER}). {MESSAGE_SENTINEL}",
                           "details": [{"@type": "type.googleapis.com/google.rpc.ErrorInfo",
                                        "reason": "IAM_PERMISSION_DENIED",
                                        "metadata": {"permission": perm}}]}}


def ttl_proof_logs(applied_text: str, mode: str) -> str:
    """The proof pod's log for the LAST proof Job applied (its own probe calls, read from its manifest), as its
    script prints GKE's answers: "TTL_PROOF <permission> <name> <code> <body>". mode: granted | denied |
    unlimited | disabled (module doc), silent (no line: the token failed)."""
    if mode == "silent":
        return ""
    docs = [d for d in applied_text.split("\n---\n") if gke.TTL_PROOF_JOB in d and "probe()" in d]
    if not docs:
        return ""
    lines = []
    for perm, _method, url, cluster in _PROBE.findall(docs[-1]):
        code, body = answer(mode, perm, url, cluster)
        lines.append(f"{gke.TTL_PROOF_MARK}{perm} {cluster} {code} {json.dumps(body)}")
    return "\n".join(lines) + "\n"


LOADER = (
    "import importlib.util as _ilu\n"
    f"_fgs = _ilu.spec_from_file_location('fake_gke_api', {str(Path(__file__).resolve())!r})\n"
    "_fgm = _ilu.module_from_spec(_fgs)\n"
    "_fgs.loader.exec_module(_fgm)\n"
    "def ttl_proof_logs(applied_text, mode):\n"
    "    return _fgm.ttl_proof_logs(applied_text, mode)\n"
)
