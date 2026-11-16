---
type: Runbook
title: snowplow — release
description: How a release ships — one plain-semver tag drives the image build and the OCI chart publish; what lands where and what to verify.
resource: oci://ghcr.io/krateo-platformops/charts/snowplow
tags: [release, ci, oci]
timestamp: 2026-08-06T00:00:00Z
---

# Release

One tag ships everything. This monorepo has a single version line: the app image, the
`snowplow` chart and the `snowplow-crds` chart all release at the tag's version.

## The runbook

1. **Merge to `main`** with PR CI green
   ([`release-pullrequest.yaml`](../.github/workflows/release-pullrequest.yaml):
   validate-only multi-arch image build, Go tests + CRD-drift guard, the snowplow AST
   lint gates; plus chart lint, security scanning, and docs lint).
2. **Tag with plain semver — `X.Y.Z`, no `v` prefix.** Both release workflows trigger
   on `[0-9]+.[0-9]+.[0-9]+` only; a `v`-prefixed tag ships **nothing**, silently
   (it has happened: `v1.8.0` produced no artifacts; `1.9.0` is the release that
   followed).

   ```sh
   git tag 1.9.1 && git push origin 1.9.1
   ```

3. **CI builds and publishes**, no manual steps:
   - [`release-tag.yaml`](../.github/workflows/release-tag.yaml) → the shared
     `component-image-build` workflow (`krateo-platformops/.github`) builds the
     multi-arch (amd64+arm64) image from `go/snowplow/` →
     `ghcr.io/krateo-platformops/snowplow:X.Y.Z`.
   - [`release-oci.yaml`](../.github/workflows/release-oci.yaml) (the canonical,
     byte-identical org workflow) discovers every first-class chart under `helm/`,
     substitutes the `Chart.yaml` placeholders (`CHART_VERSION` and `APP_VERSION`
     → the tag being released, passed explicitly so that pushing several tags at
     once cannot give an older chart a newer `appVersion`), packages, and pushes →
     `oci://ghcr.io/krateo-platformops/charts/snowplow:X.Y.Z` and
     `oci://ghcr.io/krateo-platformops/charts/snowplow-crds:X.Y.Z`.

4. **Verify** the artifacts exist and pair up:

   ```sh
   helm show chart oci://ghcr.io/krateo-platformops/charts/snowplow --version X.Y.Z
   # appVersion in the output must equal X.Y.Z
   ```

5. **Roll it out** by bumping the Krateo installer's snowplow chart pin (both charts
   move together — the installer pins them at one version), or `helm upgrade` on a
   standalone install ([usage](./usage.md)). Never mutate the running Deployment out
   of band.

## CRD changes

CRDs are generated from the Go types (`make generate` in `go/snowplow/`), drift-gated
in PR CI, and shipped via the committed copy under
[`helm/snowplow-crds/templates/`](../helm/snowplow-crds/templates/) — the old
cross-repo CRD publish job is gone with the monorepo fold. If you change
`apis/templates/`, regenerate and update the crds chart template in the same PR.

## Docs

`docs/llms.txt` pins this bundle to the release tag — update the pin (and
[log](./log.md), when the release is notable) as part of the release PR.
