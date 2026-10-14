"""Credentials PLANTED in redaction tests, built at RUNTIME.

No credential-shaped literal may appear in source: CI's secret scanner reads
every live branch, and a JWT-shaped string in a test fails the security check
on every PR. These helpers produce real-shaped values (a three-segment
base64url JWT, a random bearer token, a random password) fresh per call.
"""

from __future__ import annotations

import base64
import json
import secrets


def _b64url(raw: bytes) -> str:
    return base64.urlsafe_b64encode(raw).rstrip(b"=").decode()


def jwt() -> str:
    header = _b64url(json.dumps({"alg": "RS256", "typ": "JWT"}).encode())
    payload = _b64url(json.dumps({"sub": "scaleuser-" + secrets.token_hex(4)}).encode())
    signature = _b64url(secrets.token_bytes(32))
    return ".".join((header, payload, signature))


def bearer() -> str:
    return secrets.token_hex(16)


def opaque() -> str:
    return "v" + secrets.token_hex(10)
