---
type: Component
title: snowplow — index
description: The map of the snowplow doc bundle — the Krateo content API that resolves RESTAction and Widget CRs into portal JSON.
resource: oci://ghcr.io/krateo-platformops/charts/snowplow
tags: [portal, content-api, restaction]
timestamp: 2026-08-06T00:00:00Z
---

# snowplow

snowplow is the **content API of the Krateo portal**: it resolves `RESTAction` and
frontend `Widget` custom resources into the JSON the frontend renders, served over
`GET /call` — a content bridge, not a BFF. This monorepo carries the app
(`go/snowplow/`), its Helm charts (`helm/snowplow/`, `helm/snowplow-crds/`) and one
version line: image and charts ship together from a single plain-semver tag.

## The bundle (start here)

- [overview](./overview.md) — what it does and how it works: `/call`, the resolver
  layering, the removable cache, its place between authn / frontend / sse-proxy.
- [usage](./usage.md) — install via the Krateo installer pin or direct
  `helm install oci://…`; the hard authn-CRD dependency; Kind quickstart.
- [configuration](./configuration.md) — the whole config surface: values, the env
  ConfigMap contract, probes, tuning.
- [api](./api.md) — the `RESTAction` CRD and the HTTP surface; OpenAPI spec pointer.
- [examples](./examples.md) — the runnable examples under `examples/`.
- [release](./release.md) — how a release ships (tag → image + charts on GHCR).
- [log](./log.md) — curated history.
- [llms.txt](./llms.txt) — the version-pinned agent index of this bundle.

## The deep corpus (code-adjacent, authoritative for internals)

The internals documentation lives next to the code under `go/snowplow/` and stays
there — it is versioned, code-traced (`file:line`) and consumed at the deployed tag via
[`go/snowplow/docs/llms.txt`](../go/snowplow/docs/llms.txt).

**Map & contracts**

- [ARCHITECTURE.md](../go/snowplow/ARCHITECTURE.md) — the one-page code-traced map;
  read before touching internals.
- [CONTRIBUTING.md](../go/snowplow/CONTRIBUTING.md) — build/run on kind, the rbac
  TestMain trap, review gate, invariants.

**Architecture deep dives** ([go/snowplow/docs/architecture/](../go/snowplow/docs/architecture/))

- [request-lifecycle](../go/snowplow/docs/architecture/request-lifecycle.md) — `/call` →
  dispatch → resolve → RBAC filter → serialize; the layering contract.
- [caching](../go/snowplow/docs/architecture/caching.md) — the three tiers, the two L1
  keys, invalidation, the transparent-fallback invariant.
- [prewarm](../go/snowplow/docs/architecture/prewarm.md) — boot seed, the
  frontend-nav walker, the opt-in prewarm engine.
- [rbac-uaf](../go/snowplow/docs/architecture/rbac-uaf.md) — per-binding-UID keying,
  the subject index, serve-time user filtering.
- [north-star](../go/snowplow/docs/architecture/north-star.md) — the performance
  contract and the harness that enforces it.
- [observability](../go/snowplow/docs/architecture/observability.md) — expvars, slog
  events, pprof.

**Decisions** ([go/snowplow/docs/adr/](../go/snowplow/docs/adr/)) — 0001 decouple
authn for testing; 0002 per-binding-UID L1 keying; 0003 identity-free content key;
0004 cache is provisional and removable; 0005 walker-driven informer; 0006
snowplow-owned external fetch (YAML acceptance).

**How-tos** ([go/snowplow/howto/](../go/snowplow/howto/)) — authoring:
[restactions](../go/snowplow/howto/restactions.md),
[widgets](../go/snowplow/howto/widgets.md),
[endpoints](../go/snowplow/howto/endpoints.md),
[extras](../go/snowplow/howto/extras.md),
[migrate-call-stage-to-direct-path](../go/snowplow/howto/migrate-call-stage-to-direct-path.md);
operating: [install on Kind](../go/snowplow/howto/install.md),
[operating runbook](../go/snowplow/howto/operating.md),
[export](../go/snowplow/howto/export.md),
[health-aggregation](../go/snowplow/howto/health-aggregation.md),
[architecture-module](../go/snowplow/howto/architecture-module.md),
[audit-correlation](../go/snowplow/howto/audit-correlation.md),
[developer-guide](../go/snowplow/howto/developer-guide.md) (superseded by
CONTRIBUTING for the build flow).

**Archive** (`tags: [archive]`) — 38 point-in-time design/RCA/troubleshoot/regression
documents under [go/snowplow/docs/](../go/snowplow/docs/) and the 41-file append-only
engineering ship-log under
[go/snowplow/docs/notes/](../go/snowplow/docs/notes/README.md). Each carries archive
frontmatter with its original date and is preserved verbatim as historical record:
what was true **on that date**, at the SHA/version pinned in its header — **not**
current truth. The code and the deep dives above always win.

**API spec** — [swagger.json](../go/snowplow/docs/swagger.json) /
[swagger.yaml](../go/snowplow/docs/swagger.yaml), served live at `GET /swagger/`.
