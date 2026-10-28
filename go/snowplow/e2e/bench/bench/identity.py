"""The bench's ONE identity redaction for tool error text (#898, #900).

kubectl and gcloud name the operator in their errors: a GKE Forbidden says
`User "<account>" cannot …`, gcloud's 404 / 503 say "This command is
authenticated as <account> …". None of it may reach a log, a record or
verdict.json. Two policies, one place:

- redact_identity(line): kubectl's error line, kept for its meaning
  (run606.sh kerr_line pipes through this file) with every identity
  replaced by <redacted>: `User "…"`, `users "…"`, whatever follows
  "authenticated as" (an e-mail, a numeric id, system:serviceaccount:…),
  any e-mail address, a Secret's name (`secrets/…`, `secrets "…"`) and a
  certificate's `CN=…`. A closing ")" or "," stays. bench/logscrub.py runs
  it over snowplow's non-JSON (klog) log lines too (#919).
- error_summary(stderr): a failed gcloud / kubectl call as bench.gke logs
  and records it — its code only (`ResponseError: code=<n>`, or `Error from
  server (<Reason>)`), never its free text (#900 B1), and passed through
  redact_identity all the same.

Imports nothing from bench (bench.gke imports it and must stay free of
bench.cluster's import-time context guard). As a script:
`python3 bench/identity.py < lines` redacts each line of stdin.
"""

from __future__ import annotations

import re
import sys

REDACTED = "<redacted>"
_RULES = (
    (re.compile(r'User "[^"]*"'), 'User "<redacted>"'),
    (re.compile(r'users "[^"]*"'), 'users "<redacted>"'),
    (re.compile(r"authenticated as:? [^)\s,]+"), "authenticated as <redacted>"),
    # an e-mail address: a local part, "@", and a MAIL-SHAPED domain (dot-separated
    # labels, a letters-only TLD). Not an OCI digest reference: `name@sha256:<hex>`
    # has an algorithm and a colon right after the "@" and no dotted domain. The
    # old `[^\s"'(),]+@[^\s"'(),]+` ate every digest the bench records (#960
    # review: an image_id became <redacted>).
    (re.compile(r"""[^\s"'(),<>@/:]+@(?![A-Za-z0-9]+:)(?:[A-Za-z0-9-]+\.)+[A-Za-z]{2,}\b"""), "<redacted>"),
    # #919: a Secret's name (the bench's per-user `<user>-clientconfig`, any
    # other) in an API path or an API error, and a certificate subject's CN
    # (client certificates name their user)
    (re.compile(r"\b(secrets?)/[^/\s?&,\"']+"), r"\1/<redacted>"),
    (re.compile(r'\b(secrets?) "[^"]*"'), r'\1 "<redacted>"'),
    (re.compile(r"\bCN=[^,/\s\"')]+"), "CN=<redacted>"),
    # a user named in prose ("token for user carol expired"): the word after
    # "user" / "username" (the quoted form is the first rule's)
    (re.compile(r"(?i)\b(user(?:name)?)([:=]?\s+)(?![\"<])[^\s,;)\"]+"), r"\1\2<redacted>"),
)

_ERR_CODE = re.compile(r"\bcode=(\d{3})\b")
_ERR_REASON = re.compile(r"Error from server \(([A-Za-z]+)\)")


def redact_identity(line: str) -> str:
    for rx, rep in _RULES:
        line = rx.sub(rep, line)
    return line


def error_summary(stderr: str | None) -> str:
    s = stderr or ""
    m = _ERR_CODE.search(s)
    if m:
        out = f"ResponseError: code={m.group(1)}"
    elif (m := _ERR_REASON.search(s)):
        out = f"Error from server ({m.group(1)})"
    elif s.strip():
        out = "no error code; the error text is withheld (it may name the account)"
    else:
        out = "no error text"
    return redact_identity(out)


def main() -> int:
    for line in sys.stdin:
        sys.stdout.write(redact_identity(line.rstrip("\n")) + "\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
