"""The bench's one identity redaction (bench/identity.py) and every place it
runs (#960 review, MEDIUM): its e-mail rule `[^\\s"'(),]+@[^\\s"'(),]+` also
matched an OCI digest reference, so `ghcr.io/…/core-provider@sha256:…`
became <redacted>. The platform record lost which digest a provider-runtime
was read from, and the teardown's reuse of that read never hit. An e-mail now
needs a mail-shaped domain (dot-separated labels, a letters-only TLD) and no
`<algorithm>:` right after the "@".
"""

from __future__ import annotations

import json
import subprocess
import sys

import pytest

from bench import diag606, identity, load606, logscrub, sampler
from tests.test_run606_real_sequence import BENCH_DIR

D256 = "sha256:" + "0123456789abcdef" * 4
DIGESTS = [
    f"ghcr.io/krateo-platformops/core-provider@{D256}",
    f"127.0.0.1:5000/krateo/oasgen-provider@{D256}",
    f"registry.example.com:443/org/team/img@{D256}",
    f"docker-pullable://ghcr.io/krateo-platformops/snowplow@{D256}",
    f"oasgen-provider@{D256}",
    "img@sha512:" + "ab" * 64,
]
EMAILS = [
    ("someone@example.invalid", "<redacted>"),
    ("first.last+tag@sub.example.co.uk", "<redacted>"),
    ("bench-sa@operations-dev-krateo-io.iam.gserviceaccount.com", "<redacted>"),
    ("E1028 09:50:00 create failed for op@corp.example: forbidden", "E1028 09:50:00 create failed for <redacted>: forbidden"),
    ('{"owner": "x.y@example.org"}', '{"owner": "<redacted>"}'),
    ("(requested by a@b.io)", "(requested by <redacted>)"),
    ("mailto:x@y.com,", "mailto:<redacted>,"),
    ("This command is authenticated as op@corp.example which is the active account",
     "This command is authenticated as <redacted> which is the active account"),
]


@pytest.mark.parametrize("ref", DIGESTS)
def test_a_digest_reference_survives(ref):
    assert identity.redact_identity(ref) == ref
    assert identity.redact_identity(f"image_id={ref} pulled") == f"image_id={ref} pulled"
    assert identity.redact_identity(json.dumps({"image_id": ref})) == json.dumps({"image_id": ref})


@pytest.mark.parametrize("line, want", EMAILS)
def test_an_email_is_still_redacted(line, want):
    assert identity.redact_identity(line) == want


# ─── every call site: a digest survives, an e-mail and a user do not ────────

LINE = (f'pulled {DIGESTS[0]} for User "scaleuser-0001" (owner someone@example.invalid); '
        f"next {DIGESTS[1]}")


def _check(out: str):
    assert DIGESTS[0] in out and DIGESTS[1] in out, out
    assert "someone@example.invalid" not in out and "scaleuser-0001" not in out, out


def _script(line):
    """run606.sh's kerr_line pipes kubectl's error line through bench/identity.py."""
    return subprocess.run([sys.executable, str(BENCH_DIR / "bench" / "identity.py")], input=line + "\n",
                          capture_output=True, text=True, timeout=60).stdout


SITES = {
    "identity.redact_identity": identity.redact_identity,
    "bench/identity.py (run606 kerr_line)": _script,
    "logscrub.Scrubber.kept (snowplow's log)": lambda s: logscrub.Scrubber().kept(s),
    "diag606._redact_log (controller logs)": diag606._redact_log,
    "diag606._redact_obj (the platform record)": lambda s: json.dumps(diag606._redact_obj({"v": [s]})),
    "load606._redact_strings (the preflight record)": lambda s: json.dumps(
        load606._redact_strings({"menu": {"error": s}}, identity.redact_identity)),
    "sampler._first_line (a failed sample's error line)": lambda s: sampler._first_line(s.encode()),
}


@pytest.mark.parametrize("site", sorted(SITES))
def test_every_redaction_site_keeps_digests_and_drops_identities(site):
    _check(SITES[site](LINE))


def test_error_summary_never_carries_free_text():
    """gke's and load606's kubectl/gcloud error summaries: the code only."""
    s = identity.error_summary(f"ERROR: (gcloud.x) ResponseError: code=403, message=denied for someone@example.invalid "
                               f"on {DIGESTS[0]}")
    assert s == "ResponseError: code=403"


def test_the_call_sites_are_the_ones_swept():
    """A new place that redacts is added to SITES, not left out of the sweep."""
    src = {p.name: p.read_text() for p in (BENCH_DIR / "bench").glob("*.py")}
    users = sorted(n for n, t in src.items() if "redact_identity" in t and n != "identity.py")
    assert users == ["diag606.py", "load606.py", "logscrub.py", "sampler.py"]
    assert "bench/identity.py" in (BENCH_DIR / "run606.sh").read_text()


# ─── the platform record keeps its digests, and the teardown reuses the read ─


def test_the_platform_record_keeps_the_digest_and_the_teardown_reuses_its_read(tmp_path):
    rec = {"kind": "platform_definitions", "controller_images": [
        {"component": "core-provider", "image": "ghcr.io/krateo-platformops/core-provider:2.13.39",
         "image_id": DIGESTS[0], "provider_runtime": {"version": "v1.5.2"}}]}
    out = diag606._redact_obj(rec)
    assert out["controller_images"][0]["image_id"] == DIGESTS[0]
    f = tmp_path / "platform-definitions.jsonl"
    f.write_text(json.dumps(out) + "\n")
    assert diag606._known_images(str(f)) == {DIGESTS[0]: {"version": "v1.5.2"}}
    pods = [{"metadata": {"name": "c-0", "labels": {"app.kubernetes.io/name": "core-provider"}},
             "spec": {"containers": [{"name": "c"}]},
             "status": {"containerStatuses": [{"name": "c", "image": "x", "imageID": DIGESTS[0]}]}}]

    def never(*a, **k):
        raise AssertionError("the registry was read again for a digest already read this run")
    rows = diag606.controller_images(pods, read=never, known=diag606._known_images(str(f)))
    assert rows[0]["provider_runtime"] == {"version": "v1.5.2"}
