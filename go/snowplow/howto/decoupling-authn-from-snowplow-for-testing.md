---
type: Decision
title: Decoupling authn from snowplow for testing and operations
description: ADR (2025-10-22) — test snowplow without deploying the authn service by minting Bearer tokens directly with the shared auth library. Superseded in place by ADR 0001.
resource: oci://ghcr.io/krateo-platformops/charts/snowplow
tags: [authn, testing, adr, jwt]
status: superseded-by:../docs/adr/0001-decouple-authn.md
timestamp: 2026-08-06T00:00:00Z
---

# Decoupling `authn` from `snowplow` for Testing and Operations

> Architecture Decision Record, 2025-10-22.
> **This record is superseded**: it is preserved in ADR form as
> [`docs/adr/0001-decouple-authn.md`](../docs/adr/0001-decouple-authn.md),
> which is now the authoritative version. What follows is the original
> decision plus the verified current state.

## The decision (2025-10-22)

Snowplow depends on the `authn` service for authentication and Bearer-token
issuance; standing up `authn` (its setup and CRDs) just to test snowplow was
fragile and slow. The decision: perform a one-time user registration and token
generation with the **shared authentication library**, without deploying
`authn` — originally via the `krateoctl add-user` CLI command.

## Current state (verified against the tree)

The *decoupling* stands and is how snowplow is tested and quick-started today;
the *tool* has changed:

- Snowplow validates tokens entirely in-process: a Krateo access token is a
  plain **HS256 JWT** signed with the `JWT_SIGN_KEY` snowplow is started with
  (`main.go` — `middleware.UserConfig(*signKey, *authnNS)`), carrying
  `username` and `groups` claims. The shape is defined by the shared auth
  library (`github.com/krateo-platformops/plumbing` `jwtutil` —
  `CreateToken` / `Validate`). No `authn` deployment is needed to obtain or
  validate one.
- `krateoctl` is **not published under the `krateo-platformops` org** and is
  not referenced anywhere in this tree. The supported tool-free path is the
  inline mint in [`install.md`](install.md) §4 (a stdlib-only Python HS256
  signer producing the same token the shared library would).

> **Superseded by [#146](https://github.com/krateo-platformops/snowplow/pull/146)
> (2026-08-13):** the two bullets above describe the HS256 / `JWT_SIGN_KEY`
> contract, which no longer exists. snowplow now verifies **RS256** tokens
> against public keys fetched from a JWKS document (authn's
> `<URL_AUTHN>/.well-known/jwks.json` by default, chart `jwt.jwksUrl` to
> override), and holds no key of its own. [`install.md`](install.md) §3–4 now
> generates an RSA keypair, serves its JWKS in-cluster and mints an RS256
> token. The current contract is in
> [ADR 0001](../docs/adr/0001-decouple-authn.md#token-verification-since-146-rs256-via-jwks).

## Consequences (unchanged)

- **Simplified testing:** services that depend on authentication are testable
  independently of `authn`.
- **Reduced operational overhead:** no need to deploy or maintain `authn` in
  local or CI environments.
- **Consistency:** the token shape is the shared library's, so hand-minted
  tokens are compatible with real authentication flows.
- **Admin utility:** the same mechanism bootstraps users for quickstarts.

[authn]: https://github.com/krateo-platformops/authn
