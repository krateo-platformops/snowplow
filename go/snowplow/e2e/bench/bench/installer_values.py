"""The installer's values for one run: ONE source, read by run606.sh (write_installer_values, the
values its registry preflight renders with and s_install installs with) and by bench.vm606's
laptop-side chart check, which renders the installer's private-image component list with the
same values before any paid step exists (#1395: run 26f's dry pass, on the VM, was the first
place that list was read).

The document is the one run606.sh wrote from its own heredoc until #1395, byte for byte
(tests/test_1395_private_render.py holds it to two runs' files written by that heredoc): its
comments say why each value is what it is.

    python3 -m bench.installer_values --snowplow-tag T [--frontend-tag F] --out installer-values.yaml

Its `features:` are krateo-057's (bench/installer_features.py: ONE record of 057's Installer CR,
every feature written, the owner's deviations named; never a list picked here; until run 30 the
heredoc's three hand-picked lines). Every run's document grants the bench's base group the portal's
shell (componentValues.portal.readerGroups: the group is bench/rbac_model.py's BASE_GROUP, never a second
literal). `--snowplow-env` (SNOWPLOW_ENV) adds the run's declared snowplow environment under
componentValues.snowplow.env. `--frontend-tag` (run 26: FRONTEND_TAG) appends the frontend's image override to componentValues;
empty or absent, the document is the one without it. The tags are written as the heredoc wrote
them, inside double quotes, unescaped.
"""

from __future__ import annotations

import argparse
import os
import sys

from bench import installer_features, rbac_model

#: the installer's component whose chart ships the portal's shell: the key of its componentValues
PORTAL_COMPONENT = "portal"
#: the group granted the shell: the one every bench user is in (ONE definition, bench/rbac_model.py)
READER_GROUP = rbac_model.BASE_GROUP

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
# The installer's features are krateo-057's, every one written (bench/installer_features.py: the
# record of 057's Installer CR, and the deviations the owner rules on, named in every check).
{features}vertexAI:
  enabled: false
# Installer 0.3.422 schema: top-level additionalProperties=false and the
# engine subchart key is core-provider (the fresh-install runbook's
# krateo-core-provider key is rejected by helm schema validation).
core-provider:
  resources:
    limits:
      memory: 4Gi
# componentValues: krateo-057's recorded switches and sizes (bench/installer_component_values.py: the
# record of 057's Installer CR, less the deviations the owner rules on, named in every check), under the
# bench's own: snowplow's image, its node and the declared snowplow environment, the frontend's image,
# and the portal's reader group. The bench's base group reads the portal's shell (the layout, the
# theme, the header, the sidebar Menu and the shared RESTActions) through the portal chart's own
# value for it: every bench user is in that group (bench/rbac_model.py BASE_GROUP) and nothing else
# grants it the shell. Up to installer 0.3.625 the grant came with the portal's demo user, on by a
# default of the installer's values schema; from 0.3.630 that default is gone, and run 26j's three
# clusters answered 403 on the boot root for every bench user. No demo user is installed.
# bench/portal_readers.py holds the grant against the pinned charts before any cluster exists.
{component_values}registryAuth:
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


def document(snowplow_tag: str, frontend_tag: str = "", snowplow_env: dict[str, str] | None = None) -> str:
    """The values document of a run whose snowplow image tag is `snowplow_tag` (SNOWPLOW_TAG), whose
    frontend image tag override is `frontend_tag` (FRONTEND_TAG; empty: none) and whose declared
    snowplow environment is `snowplow_env` (SNOWPLOW_ENV, bench/snowplow_env.py: Go runtime keys,
    each an explicit value, written as strings under componentValues.snowplow.env; none: the
    document is the one without it, and the installer's own values apply)."""
    import yaml
    from bench import installer_component_values as icv
    own: dict = {"snowplow": {"image": {"tag": snowplow_tag},
                              "nodeSelector": {"bench.krateo.io/pool": "snowplow"},
                              "tolerations": [{"key": "dedicated", "operator": "Equal", "value": "snowplow",
                                               "effect": "NoSchedule"}]},
                 PORTAL_COMPONENT: {"readerGroups": [READER_GROUP]}}
    if snowplow_env:
        own["snowplow"]["env"] = {k: str(v) for k, v in sorted(snowplow_env.items())}
    if frontend_tag:
        own["frontend"] = {"image": {"tag": frontend_tag}}
    cv = icv.merge(icv.nest(icv.bench_paths()), own)
    return TEMPLATE.format(features=installer_features.values_block(installer_features.bench_features()),
                           component_values=yaml.safe_dump({"componentValues": cv}, sort_keys=True,
                                                           default_flow_style=False, width=1000))


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="bench.installer_values")
    ap.add_argument("--snowplow-tag", required=True)
    ap.add_argument("--frontend-tag", default="")
    ap.add_argument("--snowplow-env", default="",
                    help="SNOWPLOW_ENV: the run's declared snowplow environment (bench/snowplow_env.py)")
    ap.add_argument("--out", required=True)
    a = ap.parse_args(argv)
    if not a.snowplow_tag:
        print("refusing: --snowplow-tag is empty", file=sys.stderr)
        return 2
    from bench import snowplow_env
    try:
        env = snowplow_env.parse(a.snowplow_env)
    except snowplow_env.Refused as e:
        print(f"refusing: {e}", file=sys.stderr)
        return 2
    tmp = a.out + ".part"
    with open(tmp, "w") as fh:
        fh.write(document(a.snowplow_tag, a.frontend_tag, env))
    os.replace(tmp, a.out)
    return 0


if __name__ == "__main__":
    sys.exit(main())
