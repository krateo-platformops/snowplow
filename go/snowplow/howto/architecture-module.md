---
type: Usage
title: Architecture state (built-in krateo/architecture/v1 jq module)
description: The embedded krateo/architecture/v1 jq module — the Blueprint Composer's architecture-state computation, referenced by name from each chart's <chart>-status RESTAction instead of being copied into it. Interface and versioning rule.
resource: oci://ghcr.io/krateo-platformops/charts/snowplow
tags: [restaction, jq, architecture, blueprint-composer]
timestamp: 2026-10-13T00:00:00Z
---

# Architecture state (built-in `krateo/architecture/v1` jq module)

Snowplow ships a **built-in jq module**, `krateo/architecture/v1`
(`internal/support/jq/modules/krateo/architecture/v1.jq`). It is embedded in the
binary and served by the same layered loader as [`health`](./health-aggregation.md),
so any `RESTAction` filter or widget expression can use it with
`include "krateo/architecture/v1";`.

It holds the computation the Blueprint Composer used to inline into every
chart's `<chart>-status` RESTAction (about 11.5 KB of jq per chart): the
`composition-architecture` projection from the frontend's
`architectureStateJq.ts` and the LIST fold from `projectionCompile.ts`.
A chart now carries only its own data, which is about 250 B (#623).

## Interface

```jq
def arch_step;                       # the `arch` stage filter: this composition's rendered graph, or null
def list_step($step; $apiVersion);   # a LIST stage filter: items reduced to what architecture_state reads,
                                     # each stamped with $apiVersion (LIST items carry none)
def fold_steps($steps);              # $steps: [{ids:[string], step:string}] — which graph node ids each
                                     # LIST stage serves; folds the LISTs into .objs / .objErr / .comp
def architecture_state;              # consumes the post-fold resolve dict as '.'; returns the state
                                     # (architecture, level, state, allReady, waitingOn, progress, nodes, ...)
```

A chart's status RESTAction uses it like this (stage paths are unchanged and
stay in `spec.api[].path`, so `/rbac` derives the same read set):

```yaml
spec:
  api:
    - name: arch
      path: ${ "/api/v1/namespaces/" + ((.compositionNamespace // .namespace) // "") + "/configmaps/" + ((.compositionName // .name) // "") + "-architecture" }
      continueOnError: true
      errorKey: archErr
      filter: include "krateo/architecture/v1"; arch_step
    - name: o0
      path: ${ "/apis/apps/v1/namespaces/" + ((.compositionNamespace // .namespace) // "") + "/deployments" }
      continueOnError: true
      errorKey: o0Err
      filter: include "krateo/architecture/v1"; list_step("o0"; "apps/v1")
  filter: include "krateo/architecture/v1"; fold_steps([{"ids":["deployment"],"step":"o0"}]) | architecture_state
```

Only these four names are exported. The helpers (`truthy`, `condType`,
`readyBy`, `errKind`, ...) are nested inside `architecture_state`, so an
includer's own definitions never collide with them.

The module's output is pinned **byte-identical** to the inlined filters that
frontend 1.8.4 generates. This covers the live `event-site-status` shape and the
edge cases: missing, forbidden and not-found stages, condition-form `readyWhen`
both ready and not ready, and empty LISTs. See
`internal/support/jq/architecture_v1_test.go` and its
`testdata/architecture/reference_filters.json`.

## Versioning rule

The version is part of the module **name**. A chart pins the interface it was
compiled against by the name it includes.

- **Never change `v1` in a way an includer can observe.** That covers the four
  names, their arguments, the input each one reads and the output it returns.
  A RESTAction that includes `krateo/architecture/v1` must keep getting the same
  answer from every snowplow release.
- **An interface or output change ships as a new module**, `krateo/architecture/v2`,
  next to `v1`. The generator moves to `v2`, and charts already published keep `v1`
  until they are regenerated.
- A fix that the byte-parity tests prove leaves every output unchanged
  (a comment, a refactor) may go into `v1`.

A module change reaches the cluster only with a snowplow release, which restarts
the pod and clears the cache. Cached answers are therefore never computed with
a previous module version.
