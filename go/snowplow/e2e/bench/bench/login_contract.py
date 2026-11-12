"""The frontend's login form contract (frontend#659), and the run's refusal of a frontend
older than it (paid run 26d: 0 of 5 browser sessions signed in).

THE CONTRACT. frontend#659 ("fix(login): four #657 follow-ups") renamed the antd form from the
literal 'basic' to a name derived from its /strategies entry (ui/src/pages/Login/LoginForm/
LoginForm.tsx, loginFormNameOf: `login_<kind>` for basic, `login_<kind>_<name>` for an
LDAPConfig named <name>). antd prefixes that name to every field id, so basic sign-in is
`#login_basic_username` / `#login_basic_password` inside `form#login_basic`, submitted with that
form's "Sign in" button; frontend pins it in ui/src/pages/Login/AuthMethods/AuthMethods.ids.test.tsx.
Labels and autocomplete are NOT selectors: with two strategies on the page both repeat.
k6/browser.js signs in through exactly these (SELECTORS; a test holds the two equal).

THE FIRST RELEASE CARRYING IT, read from the frontend repository, not from dates (2026-11-12):
#659's merge commit is e46198b8a57bf34db3b51d10290979430743afd9; `git tag --contains` it lists
1.8.36 .. 1.8.40, and the compare API says 1.8.35...e46198b8 is `ahead` by 10 (1.8.35 does not
contain it) and e46198b8...1.8.36 `ahead` by 17 (1.8.36 does). The frontend image is
ghcr.io/krateo-platformops/frontend, its chart's appVersion (the default image tag) the git tag the
release workflow ran on (.github/workflows/release-oci.yaml).

WHICH FRONTEND A RUN RUNS, read the way the bench reads every pin (ghcr_pull.private_pins): the
installer's files/component-pins.yaml `frontend` entry, with what the installer renders it with
(its own componentValues, overridden by the run's installer values). `image.tag` there (run606.sh
writes FRONTEND_TAG into componentValues.frontend.image.tag) is the image; without it, the
chart's appVersion, which is the pin's version. One rule for the override and the default: a
default older than the contract (installer 0.3.545 pins frontend 1.8.34) is refused exactly as an
override is. A tag that is not a release version (X.Y.Z) is refused too: whether it carries the
contract cannot be told from it.

No dual-selector fallback: the bench measures the UI it names, and refuses the one it cannot sign
in to, BEFORE any cluster exists (run606.sh's login-contract step, in the dry pass too; the pair
launcher runs every run's dry pass before any run starts).

    python3 -m bench.login_contract check --installer-pulled D --values V --var FRONTEND_TAG_A --out F
"""

from __future__ import annotations

import argparse
import json
import re
import sys

__all__ = ["CONTRACT_PR", "FIRST_RELEASE", "SELECTORS", "check", "frontend_tag", "main", "release"]

CONTRACT_PR = "#659"
FIRST_RELEASE = "1.8.36"
#: what k6/browser.js signs in through (basic login); a test holds browser.js to these strings
SELECTORS = {"form": "form#login_basic", "username": "#login_basic_username",
             "password": "#login_basic_password", "submit": "Sign in"}
_RELEASE = re.compile(r"^(\d+)\.(\d+)\.(\d+)$")


class Refused(Exception):
    """The run cannot sign in through this frontend; the message says why and what to set."""


def release(tag: str) -> tuple[int, int, int] | None:
    """X.Y.Z as numbers; None for anything else (a pre-release, a digest, a branch build)."""
    m = _RELEASE.match(str(tag))
    return tuple(int(x) for x in m.groups()) if m else None  # type: ignore[return-value]


def frontend_tag(installer_pulled: str, values: str) -> dict:
    """{tag, source}: the frontend image tag this install runs (module doc)."""
    from bench import ghcr_pull
    pins = [p for p in ghcr_pull.private_pins(installer_pulled, values, include={"frontend"})
            if p["name"] == "frontend"]
    if not pins:
        raise Refused("this install runs no frontend (no `frontend` pin, or its feature is off): "
                      "the browser sessions have no login form to sign in through")
    p = pins[0]
    tag = ((p.get("values") or {}).get("image") or {}).get("tag")
    if tag:
        return {"tag": str(tag), "source": "componentValues.frontend.image.tag", "pin": p["version"]}
    return {"tag": p["version"], "source": "the installer's frontend pin (its chart's appVersion)",
            "pin": p["version"]}


def check(tag: str, var: str) -> str | None:
    """None when `tag` carries the contract, else the refusal, naming what to set."""
    want = release(FIRST_RELEASE)
    got = release(tag)
    if got is None:
        return (f"frontend {tag} is not a release version (X.Y.Z): whether it carries the login form contract "
                f"{CONTRACT_PR} (first in {FIRST_RELEASE}) is unknown: set {var} ≥ {FIRST_RELEASE}")
    if got < want:
        return (f"frontend {tag} predates the login form contract {CONTRACT_PR} (first in {FIRST_RELEASE}): "
                f"set {var} ≥ {FIRST_RELEASE}")
    return None


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="bench.login_contract")
    sub = ap.add_subparsers(dest="cmd", required=True)
    c = sub.add_parser("check", help="refuse a frontend older than the login form contract")
    c.add_argument("--installer-pulled", required=True)
    c.add_argument("--values", required=True, help="the run's installer values (run606 installer-values.yaml)")
    c.add_argument("--var", required=True, help="the variable that sets this run's frontend (named in a refusal)")
    c.add_argument("--out", required=True)
    a = ap.parse_args(argv)
    if not re.fullmatch(r"FRONTEND_TAG(_[A-Za-z0-9]+)?", a.var):
        print(f"refusing: --var {a.var!r}: FRONTEND_TAG or FRONTEND_TAG_<run>", file=sys.stderr)
        return 2
    rec = {"kind": "login_contract", "contract": CONTRACT_PR, "first_release": FIRST_RELEASE,
           "selectors": SELECTORS}
    try:
        rec.update(frontend_tag(a.installer_pulled, a.values))
        why = check(rec["tag"], a.var)
    except Refused as e:
        why = str(e)
    rec.update(ok=why is None, why=why)
    with open(a.out, "w") as fh:
        json.dump(rec, fh, indent=1)
        fh.write("\n")
    if why:
        print(f"refusing: {why}", file=sys.stderr)
        return 2
    print(f"login form contract {CONTRACT_PR}: frontend {rec['tag']} ({rec['source']}) ≥ {FIRST_RELEASE}: "
          f"the browser signs in through {SELECTORS['form']}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
