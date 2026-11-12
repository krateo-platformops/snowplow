"""NO ANSWER is not a refusal: the one classification and bounded retry for every remote read that
can refuse a run, before any cluster exists (run 25: ghcr_pull; run 26: cert-manager's helm pull).

Run 25's N1 refused the whole run on one manifest read's 30 s timeout, as "the token does not
resolve". Run 26's N1B refused it on one `helm pull` of cert-manager from quay ("failed to perform
"Fetch" on source: Get "https://cdn01.quay.io/...?X-Amz-...": <cause>"), its recorded reason cut at
300 characters inside the signed URL, so the cause itself was lost. The other arms passed both.

  - CLASSIFY. A call that ends with NO ANSWER (its whole bound passed; the connection timed out, was
    reset, aborted or dropped; a TLS connection that ended; a name resolution that failed for now;
    HTTP 408 / 429 / 5xx: the registry cannot answer now) is not evidence about what it asked for.
    `unanswered` reads a Python exception; `unanswered_text` reads a helm / Go error's text. Any other
    failure is the registry's ANSWER (401, 403, 404, manifest unknown, a refused connection, a name
    that does not exist, a certificate that does not verify): refused at once, as before.
  - RETRY within the bound the reads already derive (every call at its own whole bound), never a new
    knob: `Budget` turns it into a deadline. A read with no answer is asked again while what is left
    holds another whole attempt; a call with no answer counts its whole bound (the rest of it is waited
    out after a fast reset), so how often follows from the bound alone. Only when the bound is spent
    is it refused, as NO ANSWER, naming the call, its attempts and the bound. The pre-cluster registry
    reads of one run (the installer source, cert-manager's source, the CRD charts, the ghcr pull
    token's charts and images) spend ONE bound together (`Budget.shared`, a file in the run's OUT, each
    step adding its own calls' share): what one step's answered calls did not use pays for another's
    retry.
  - SAY THE CAUSE. `cause` keeps a failure's whole text, its signed query strings and credentials
    redacted (`redact`); when it must be shortened, the END is kept: a helm / Go error ends with its
    cause ("...: context deadline exceeded", "...: 503 Service Unavailable").
"""

from __future__ import annotations

import http.client
import json
import os
import re
import socket
import ssl
import subprocess
import time

__all__ = ["NoAnswer", "Refused", "Budget", "unanswered", "unanswered_text", "redact", "cause", "attempt",
           "phase", "helm"]


class NoAnswer(Exception):
    """No answer within a call's bound: not evidence about what was asked."""


class Refused(Exception):
    """The registry's own answer: what was asked for does not resolve (its cause, redacted)."""


#: what ends a call with no answer: a timeout; a connection reset, aborted or dropped mid-answer; a TLS
#: connection that ended (every call is https: a dropped connection usually surfaces as an SSL EOF); a
#: name resolution that failed for now (EAI_AGAIN, getaddrinfo's own "temporary failure"). Never a
#: certificate that does not verify, a refused connection or a name that does not exist: answers.
NO_ANSWER_ERRORS = (TimeoutError, ConnectionResetError, ConnectionAbortedError, BrokenPipeError,
                    http.client.IncompleteRead)
SSL_DROPS = (ssl.SSLEOFError, ssl.SSLZeroReturnError, ssl.SSLSyscallError)
SSL_DROP_REASONS = frozenset({"UNEXPECTED_EOF_WHILE_READING"})
TEMPORARY_EAI = frozenset({socket.EAI_AGAIN})
#: HTTP statuses that say the registry cannot answer NOW (request timeout, too many requests) besides
#: every 5xx (its own failure): no answer about the reference
NO_ANSWER_STATUS = frozenset({408, 429})


def unanswered(e: BaseException) -> str | None:
    """Why the exception `e` is no answer, or None when it is an answer."""
    if isinstance(e, TimeoutError):
        return "timed out"
    if isinstance(e, NO_ANSWER_ERRORS):
        return f"the connection ended ({type(e).__name__})"
    if isinstance(e, SSL_DROPS) or (isinstance(e, ssl.SSLError) and getattr(e, "reason", None) in SSL_DROP_REASONS):
        reason = getattr(e, "reason", None)
        return f"the TLS connection ended ({type(e).__name__}{': ' + reason if reason else ''})"
    if isinstance(e, socket.gaierror) and e.errno in TEMPORARY_EAI:
        return f"name resolution failed for now ({e.strerror or 'EAI_AGAIN'})"
    return None


#: the same classes as Go's net/http and helm say them (helm pull / show over OCI or a chart repo)
_NO_ANSWER_TEXT = [
    re.compile(p, re.I) for p in (
        r"context deadline exceeded", r"i/o timeout", r"Client\.Timeout exceeded",
        r"TLS handshake timeout", r"timeout awaiting response headers",
        r"connection reset by peer", r"broken pipe", r"unexpected EOF", r"(?:^|[\s:])EOF\s*$",
        r"read: connection timed out",                         # ETIMEDOUT on an open connection
        r"stream error: stream ID \d+; INTERNAL_ERROR",         # an HTTP/2 RST_STREAM from the CDN
        r"http2: client connection lost",
        r"server sent GOAWAY", r"server closed idle connection",
        r"temporary failure in name resolution", r"server misbehaving",
        r"status code:? (?:408|429|5\d\d)\b", r"\b(?:408|429|5\d\d) (?:Request Timeout|Too Many Requests|"
        r"Internal Server Error|Bad Gateway|Service Unavailable|Gateway Timeout|Network Authentication Required)")]


def unanswered_text(text: str) -> str | None:
    """The no-answer phrase in a helm / Go error's text, or None when the text is an answer."""
    for rx in _NO_ANSWER_TEXT:
        for line in (text or "").splitlines() or [""]:
            m = rx.search(line)
            if m:
                return m.group(0).strip(" :")
    return None


_URL_QUERY = re.compile(r"(https?://[^\s\"'?]+)\?[^\s\"']*")
_USERINFO = re.compile(r"(https?://)[^/\s\"'@]+@")
_AUTH = re.compile(r"\b(Basic|Bearer)\s+[A-Za-z0-9._~+/=-]+", re.I)
_SECRET_PARAM = re.compile(r"\b(X-Amz-[A-Za-z-]+|token|access_token|signature|sig|Signature|Expires|Policy|"
                           r"Key-Pair-Id)=[^\s&\"']+", re.I)


def redact(text: str) -> str:
    """`text` without what authenticates a read: every URL's query string (a signed CDN URL's
    X-Amz-* / token / signature), any URL userinfo, Basic / Bearer credentials, a loose secret
    parameter. Everything else, the cause above all, is kept."""
    t = _URL_QUERY.sub(r"\1?<redacted>", text or "")
    t = _USERINFO.sub(r"\1<redacted>@", t)
    t = _AUTH.sub(r"\1 <redacted>", t)
    return _SECRET_PARAM.sub(lambda m: f"{m.group(1)}=<redacted>", t)


#: how much of a failure's text a record keeps: a whole helm error, redacted, fits many times over;
#: past it the START is cut, never the end, where helm / Go say the cause
CAUSE_MAX = 4096


def cause(stderr: str) -> str:
    """A failure's text for the record: redacted, its lines joined, and, when longer than CAUSE_MAX,
    shortened from the START (the cause is at the end)."""
    t = " | ".join(ln.strip() for ln in redact(stderr).splitlines() if ln.strip())
    return t if len(t) <= CAUSE_MAX else "..." + t[-CAUSE_MAX:]


class Budget:
    """The reads' bound, as a deadline (see the module doc). `extend` adds a share once its calls are
    known; `holds(s)` says whether what is left holds `s` more seconds. `shared(path)`: the one bound
    of a run's pre-cluster registry reads, kept in `path` across their processes (wall clock)."""

    def __init__(self, clock=time.monotonic, sleep=time.sleep, path: str | None = None):
        self.clock, self.sleep, self.path = clock, sleep, path
        self.deadline: float | None = None
        self.total = 0.0

    @classmethod
    def shared(cls, path: str, sleep=time.sleep) -> "Budget":
        b = cls(clock=time.time, sleep=sleep, path=path)
        try:
            with open(path) as fh:
                d = json.load(fh)
            b.deadline, b.total = float(d["deadline"]), float(d["total"])
        except (OSError, ValueError, KeyError, TypeError):
            pass                                   # the first step of the run: a new bound
        return b

    def extend(self, s: float) -> None:
        now = self.clock()
        self.deadline = max(now, self.deadline if self.deadline is not None else now) + s
        self.total += s
        if self.path:
            tmp = f"{self.path}.part"
            with open(tmp, "w") as fh:
                json.dump({"kind": "registry_read_bound", "deadline": self.deadline, "total": self.total}, fh)
            os.replace(tmp, self.path)

    def holds(self, s: float) -> bool:
        return self.deadline is not None and self.deadline - self.clock() >= s


def attempt(what: str, bound_s: float, fn, budget: Budget | None):
    """fn(): one call, `what`, bounded by `bound_s`. Within a budget, no answer names the call and
    counts as the call's whole bound (the rest of it is waited out after a fast reset). Without one,
    fn's own NoAnswer is raised as it is. The NoAnswer raised is of fn's own class."""
    if budget is None:
        return fn()
    t0 = budget.clock()
    try:
        return fn()
    except NoAnswer as e:
        budget.sleep(max(0.0, t0 + bound_s - budget.clock()))
        raise type(e)(f"no answer from the {what} ({e})") from None


def phase(budget: Budget, items: list, share: float, one, no_answer) -> tuple[list, list[int]]:
    """`one(item)` for every item within the bound, extended by `share` per item: each once, then each
    whose result is NO ANSWER (`no_answer(result)`) again, the least-tried first, while what is left
    holds one more whole share. Returns the results (in `items`' order) and each one's attempts. An
    answer (a refusal) is never asked again."""
    budget.extend(len(items) * share)
    out = [one(x) for x in items]
    tries = [1] * len(items)
    while True:
        open_ = [k for k in range(len(items)) if no_answer(out[k])]
        if not open_ or not budget.holds(share):
            return out, tries
        k = min(open_, key=lambda k: (tries[k], k))
        out[k] = one(items[k])
        tries[k] += 1


def helm_read_bound() -> float:
    """One helm read of a registry (pull, show) at its WHOLE bound, from the registry calls' own
    bounds (bench/ghcr_pull.py): its token exchange and its manifest at a registry call's, its chart
    blob at a blob's. A chart repo's read (index.yaml, then the archive) is within it too."""
    from bench import ghcr_pull
    return 2 * ghcr_pull.HTTP_BOUND_S + ghcr_pull.BLOB_BOUND_S


def helm(argv: list[str], what: str, bound_s: float, budget: Budget | None = None, run=subprocess.run):
    """`argv`, a helm read of a registry (pull, show), bounded by `bound_s`: its CompletedProcess once
    it succeeds. A definitive failure raises Refused(its cause); no answer (the bound passed, or the
    error's text is no answer) is asked again, the attempt counted at its whole bound, while `budget`
    holds another; then NoAnswer naming `what`, the cause, the attempts and the bound. The caller's
    share of the budget includes this call's `bound_s`."""
    clock = budget.clock if budget else time.monotonic
    n = 0
    while True:
        n += 1
        t0 = clock()
        try:
            p = run(argv, capture_output=True, text=True, timeout=bound_s)
        except subprocess.TimeoutExpired:
            why = f"no whole answer within its {bound_s:g} s bound"
        else:
            if p.returncode == 0:
                return p
            why = cause(p.stderr or f"exit {p.returncode}")
            if unanswered_text(why) is None:
                raise Refused(why)
        if budget is None:
            raise NoAnswer(f"no answer from the {what} ({why})")
        budget.sleep(max(0.0, t0 + bound_s - clock()))
        if not budget.holds(bound_s):
            raise NoAnswer(f"NO ANSWER from the {what} ({why}) in {n} attempt(s) of its {bound_s:g} s bound, the "
                           f"registry reads' {budget.total:g} s bound spent: whether it resolves is unknown")
