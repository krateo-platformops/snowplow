"""The installer's values for one run: ONE source, read by run606.sh (write_installer_values, the
values its registry preflight renders with and s_install installs with) and by bench.vm606's
laptop-side chart check, which renders the installer's private-image component list with the
same values before any paid step exists (#1395: run 26f's dry pass, on the VM, was the first
place that list was read).

The document is the one run606.sh wrote from its own heredoc until #1395, byte for byte
(tests/test_1395_private_render.py holds it to two runs' files written by that heredoc): its
comments say why each value is what it is.

    python3 -m bench.installer_values --snowplow-tag T [--frontend-tag F] --out installer-values.yaml

`--frontend-tag` (run 26: FRONTEND_TAG) appends the frontend's image override to componentValues;
empty or absent, the document is the one without it. The tags are written as the heredoc wrote
them, inside double quotes, unescaped.
"""

from __future__ import annotations

import argparse
import os
import sys

TEMPLATE = """\
bootstrap:
  coreProvider:
    enabled: true
# The portal stack behind ONE origin, as krateo-057's installer-platform
# release has it (helm get values, read-only 2026-10-29): exposure Gateway,
# path mode (/ frontend, /auth authn, /content snowplow, /events sse-proxy,
# each prefix rewritten to /), the two-gateway split, the agentgateway. 057
# also sets tls (its own hostname and certificate): the bench has neither, so
# its origin is http://<platform gateway ip>:<platformPort>. 057's
# exposure.port: 80 is left out: inert in Gateway mode (installer 0.3.468
# values.schema.json: "Ignored when type=Gateway"; compositions.yaml reads it
# only for LoadBalancer/NodePort and outside the path-mode branch).
# Comparability: from run 19 on, the browser reaches the portal through the
# agentgateway (runs <= 17 went direct, through a localhost forwarder), so
# browser figures are not comparable across that line; A/B inside one run is
# unaffected, and k6's users still reach snowplow by its Service DNS.
exposure:
  type: Gateway
  gatewayMode: path
  gatewayRef:
    name: krateo-agent-gateway
  internalDataPlane: true
  platformPort: 80
features:
  agentGateway: true
  coreAgents: false
  specialistAgents: false
vertexAI:
  enabled: false
# Installer 0.3.422 schema: top-level additionalProperties=false and the
# engine subchart key is core-provider (the fresh-install runbook's
# krateo-core-provider key is rejected by helm schema validation).
core-provider:
  resources:
    limits:
      memory: 4Gi
componentValues:
  snowplow:
    image:
      tag: "{snowplow_tag}"
    nodeSelector:
      bench.krateo.io/pool: snowplow
    tolerations:
    - key: dedicated
      operator: Equal
      value: snowplow
      effect: NoSchedule{frontend}
registryAuth:
  enabled: true
  # username: set at install from the pull-token file, piped into helm's -f - (not kept)
  passwordRef:
    name: krateo-agentiko-pull
    namespace: krateo-system
    key: token
  imagePullSecretName: bench-ghcr-image-pull
  insecureSkipVerifyTLS: false
  registries: []
"""


def document(snowplow_tag: str, frontend_tag: str = "") -> str:
    """The values document of a run whose snowplow image tag is `snowplow_tag` (SNOWPLOW_TAG) and
    whose frontend image tag override is `frontend_tag` (FRONTEND_TAG; empty: none)."""
    frontend = f'\n  frontend:\n    image:\n      tag: "{frontend_tag}"' if frontend_tag else ""
    return TEMPLATE.format(snowplow_tag=snowplow_tag, frontend=frontend)


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="bench.installer_values")
    ap.add_argument("--snowplow-tag", required=True)
    ap.add_argument("--frontend-tag", default="")
    ap.add_argument("--out", required=True)
    a = ap.parse_args(argv)
    if not a.snowplow_tag:
        print("refusing: --snowplow-tag is empty", file=sys.stderr)
        return 2
    tmp = a.out + ".part"
    with open(tmp, "w") as fh:
        fh.write(document(a.snowplow_tag, a.frontend_tag))
    os.replace(tmp, a.out)
    return 0


if __name__ == "__main__":
    sys.exit(main())
