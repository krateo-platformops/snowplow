# snowplow

The content API of Krateo PlatformOps: it resolves `RESTAction` and frontend `Widget`
custom resources into the JSON the Krateo portal renders, served over `GET /call`.

## What is this

A content bridge, not a BFF: snowplow holds no product state — it composes portal
content on demand from Kubernetes CRs (declarative REST-call chains, widget
definitions), enforces the caller's own RBAC, and serves any client that presents a
Krateo JWT. One monorepo, one version line: the app (`go/snowplow/`) and its Helm
charts (`helm/`) ship together from a single tag.
Full picture: [docs/index.md](docs/index.md).

## Install

Normally installed by the **Krateo installer**, which pins the chart. Standalone:

```sh
# CRDs first (RESTAction + the authn ServiceAccount CRD the app chart hard-requires):
helm install snowplow-crds oci://ghcr.io/krateo-platformops/charts/snowplow-crds --version 1.9.0
helm install authn-crds oci://ghcr.io/krateo-platformops/charts/authn-crds

helm install snowplow oci://ghcr.io/krateo-platformops/charts/snowplow \
  --version 1.9.0 --namespace krateo-system
```

Details, dependencies and the Kind quickstart: [docs/usage.md](docs/usage.md).

> **Supported configuration: Kubernetes RBAC must be the only authorizer that can deny.**
> With the cache on (the default), snowplow decides what each user may see by evaluating
> Kubernetes RBAC in memory. It does not consult other authorizers in the apiserver chain.
> Authorizers that only *grant* extra access (for example Google IAM on GKE, which runs
> after RBAC) are supported; users then see at most what RBAC allows. An authorizer that can
> **deny** a request RBAC allows, placed before RBAC (for example a deny-capable webhook),
> is **not supported** with the cache on. snowplow would not apply that deny. Such clusters
> must run with `env.CACHE_ENABLED: "false"`, where every access decision is checked by the apiserver for that user
> (a SelfSubjectAccessReview through the full authorizer chain). Details: [docs/configuration.md](docs/configuration.md#supported-authorization-configuration).

## Configure

See [docs/configuration.md](docs/configuration.md). Most used:

| Setting | Default | Effect |
|---|---|---|
| `env.CACHE_ENABLED` | `"true"` | The single cache master gate; `false` = transparent direct-apiserver fallback (same data, slower). |
| `env.GOMEMLIMIT` | `7GiB` | Go GC back-pressure ceiling — must stay strictly below `resources.limits.memory` (8Gi) or Linux OOM-kills instead. |
| `service.type` / `service.port` | `LoadBalancer` / `8081` | One port serves content, probes and debug surfaces. |

## Examples

- [examples/cluster-namespaces](examples/cluster-namespaces) — a `RESTAction` listing
  cluster namespaces via the Kubernetes API, JQ-filtered to names.
- [examples/external-api](examples/external-api) — chained GET→POST against an external
  service through an `Endpoint` Secret.

## Docs

- [docs/index.md](docs/index.md) — the map (bundle + the code-adjacent deep corpus)
- [docs/overview.md](docs/overview.md) — what it does and how it works
- [docs/usage.md](docs/usage.md) — how to install / consume it
- [docs/configuration.md](docs/configuration.md) — the whole config surface
- [docs/api.md](docs/api.md) — the `RESTAction` CRD + the HTTP surface
- [docs/examples.md](docs/examples.md) — examples index
- [docs/release.md](docs/release.md) — how a release ships
- [docs/log.md](docs/log.md) — curated history

Internals (code-traced): [go/snowplow/ARCHITECTURE.md](go/snowplow/ARCHITECTURE.md)
and the deep dives it links.

## Develop & release

`cd go/snowplow && go test ./...` (keep `KUBECONFIG` unset — see
[CONTRIBUTING](go/snowplow/CONTRIBUTING.md)); local cluster workflow via
`go/snowplow/scripts/`. Tag `X.Y.Z` (no `v`) ships image + charts — release runbook:
[docs/release.md](docs/release.md).
