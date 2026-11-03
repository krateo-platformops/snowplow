---
type: Decision
title: "ADR 0001 — Decouple authn from snowplow for testing and operations"
description: >-
  Snowplow can be tested and operated without the authn service: its auth
  contract is an RS256 JWT verified against a JWKS document, plus a per-user
  clientconfig Secret, all of which can be produced by hand. The HS256
  shared-secret contract this record first described was superseded by #146
  (RS256 via authn's JWKS). The original krateoctl vehicle is gone, and one
  best-effort runtime coupling (the prewarm-seed loopback token exchange)
  has since been added.
resource: snowplow
tags:
  - adr
  - authn
  - jwt
  - testing
status: diverged
timestamp: 2026-11-01T00:00:00Z
---

# ADR 0001 — Decouple `authn` from snowplow for testing and operations

- **Status:** Accepted (2025-10-22); the tooling vehicle has diverged — see
  *What holds today* below.
- **Supersedes:** `howto/decoupling-authn-from-snowplow-for-testing.md` — that how-to is
  preserved here in ADR form; this record is now authoritative.
- **Partly superseded (token contract):** the **HS256 / `JWT_SIGN_KEY`** token contract
  recorded under *What holds today* (2026-08-06) was replaced on 2026-08-13 by **RS256
  verification against authn's JWKS**:
  [krateo-platformops/snowplow#146](https://github.com/krateo-platformops/snowplow/pull/146)
  (commit `18a54130312ec4e4111bea5613fc05b937954bc2`). The HS256 text is kept below,
  marked *Superseded*, as the record of what was decided then. The current contract is in
  [*Token verification since #146*](#token-verification-since-146-rs256-via-jwks).

## Context

Snowplow depends on the [`authn`][authn] service for authentication and Bearer-token issuance.
Both run on Kubernetes with bespoke setup and CRDs, which makes isolated testing of snowplow
fragile: standing up `authn` just to exercise snowplow's `/call` path adds setup overhead,
recurring configuration breakage, and slow iteration. Authentication is a hard dependency for
every snowplow request (it carries the `UserInfo` — Username + Groups — that drives all RBAC and
per-user caching), but it should not be a hard dependency for *testing or local operation* of
snowplow.

## Original decision (2025-10-22)

Use the `krateoctl add-user` command to perform a one-time user registration and token
generation using the **shared authentication library** — the same library `authn` uses —
without requiring the `authn` service to be deployed. Because the token is minted from the
shared library, it is compatible with the real authentication flow.

## What holds today

The *architectural* decision — `authn`'s **deployment** is decoupled, its **contract** is not —
survives, but the vehicle and the exact boundary have both moved:

- **`krateoctl` is gone.** It is not published under the `krateo-platformops` org. The token it
  minted is a plain **HS256 JWT** from the shared auth library
  (`github.com/krateo-platformops/plumbing` `jwtutil` — `CreateToken` / `Validate`), so any
  tool can mint it against the `JWT_SIGN_KEY` secret; [`howto/install.md`](../../howto/install.md)
  shows a stdlib-Python inline mint that replaces the CLI.

  > **Superseded by [#146](https://github.com/krateo-platformops/snowplow/pull/146)
  > (2026-08-13):** the token is no longer HS256, and there is no `JWT_SIGN_KEY`. It is an
  > RS256 JWT signed with an RSA private key; `howto/install.md` §3–4 now mints it with
  > `openssl` and publishes the public key as a JWKS. `krateoctl` remains gone.
- **Token validation is stateless.** Snowplow's auth middleware
  (`internal/handlers/middleware/userconfig.go`, a snowplow-local transcription of plumbing's
  `use.UserConfig`) calls `jwtutil.Validate(signingKey, token)` — a pure HS256 check against the
  `jwt-sign-key` secret. No call to the `authn` service is ever made to validate a request.

  > **Superseded by [#146](https://github.com/krateo-platformops/snowplow/pull/146)
  > (2026-08-13):** the middleware calls `jwtutil.ValidateWithKeySource(keys, token)`, an
  > RS256 check against a public key fetched from a JWKS document and cached. Validation is
  > still per-request local (no authn call per request), but the key set *is* fetched over
  > HTTP: on the first validation, after the cache TTL, and when a token names an unknown
  > `kid`. See below.
- **The full `/call` credential is JWT + clientconfig Secret.** After validating the JWT, the
  middleware fetches the per-user `<dns1123(username)>-clientconfig` Secret from the authn
  namespace (cache-first via the in-process Secrets snapshot, apiserver fallback; missing Secret
  → 401). In production `authn` creates that Secret at login; for authn-free operation it must
  be provided by hand alongside the minted JWT. Decoupling the *service* does not remove this
  *data* dependency.
- **One new, best-effort runtime coupling.** The prewarm seed authenticates its nested loopback
  `/call`s by exchanging snowplow's projected (audience-bound) ServiceAccount token at `authn`'s
  `/serviceaccount/login` endpoint (`internal/authn/authn.go`, ported verbatim from the cdc's
  authn client; wired in `main.go` via `URL_AUTHN`). Without a running `authn` this exchange
  fails at runtime and the seed is skipped — the seed is best-effort warmth, not a correctness
  gate, and `/call` serves normally. Additionally, the snowplow chart's prewarm-seed allowlist
  CR requires the `authn-crds` chart (the `serviceaccount.authn.krateo.io` CRD) at install time
  — a CRD dependency, not a service dependency.

## Token verification since #146 (RS256 via JWKS)

[#146](https://github.com/krateo-platformops/snowplow/pull/146) (commit
`18a54130312ec4e4111bea5613fc05b937954bc2`, plumbing `v1.14.0`'s RS256/JWKS `jwtutil`)
replaced the shared HS256 secret. The contract today:

- **snowplow holds no key.** No Secret is mounted, and `JWT_SIGN_KEY` / `--jwt-sign-key` no
  longer exist. `main.go` builds a `jwtutil.JWKSKeySource` for the URL in `--jwks-url` /
  `JWT_JWKS_URL` (chart `jwt.jwksUrl`). When that is empty, the URL is
  `<URL_AUTHN>/.well-known/jwks.json` (`jwtutil.JWKSURL`), and the process exits at startup
  if both are empty.
- **Verification.** `middleware.UserConfig` and `middleware.RefreshAuth` call
  `jwtutil.ValidateWithKeySource`. It accepts only RSA-signed tokens (HS256 is rejected
  before any key lookup) and selects the key by the token's `kid`. A token without a `kid`
  verifies only while the set holds exactly one key.
- **Fetch and rotation.** The set is fetched lazily on the first validation, never at boot,
  and is served from memory for `jwt.cacheTTL` (`5m`). An unknown `kid` triggers a refetch,
  at most once per `jwt.minRefreshInterval` (`30s`), so authn can rotate keys without a
  snowplow redeploy. Each successful fetch replaces the whole set. Per-fetch timeout:
  `jwt.requestTimeout` (`5s`).
- **Missing or unknown key → `503`, not `401`.** If the JWKS is unreachable, malformed or
  empty, or holds no key for the token's `kid`, the result is `jwtutil.ErrKeyUnavailable`,
  which both middlewares answer with `503`. If a refresh fails while the cache still holds
  the `kid`, the cached key keeps serving. Bad signatures and expired tokens get `401`.

**What this changes for the decoupling.** authn's *deployment* is still not needed, but
authn-free operation now needs **an RSA keypair and an HTTP endpoint serving its JWKS** that
snowplow can reach, instead of a shared secret. The tokens must then be signed with the
matching private key. [`howto/install.md`](../../howto/install.md) §3–4 is the reference
recipe: an `openssl` keypair, a JWKS served from a ConfigMap, and an inline RS256 mint.

## Consequences

- **Simplified testing.** Services that depend on authentication can be tested without the
  `authn` service: mint the HS256 JWT from the shared library and create the user's
  clientconfig Secret. *(Superseded by #146: mint an RS256 JWT with a private key whose
  public half is in a JWKS snowplow can fetch — see above.)*
- **Reduced operational overhead.** No `authn` deployment needed in local or CI environments;
  the prewarm seed degrades gracefully in its absence.
- **Consistency.** Tokens are minted from the same shared library `authn` uses, so they behave
  identically to real `authn`-issued tokens (`jwtutil.Validate` is the single verifier).
  *(Since #146 the single verifier is `jwtutil.ValidateWithKeySource`.)*
- **Boundary preserved.** This decouples *deployment* of `authn`, not the *contract*: snowplow
  still consumes the same `UserInfo` shape, so the RBAC and per-user-cache invariants
  (ADR 0002 / ADR 0003) are exercised exactly as in production.

This follows microservice testing best practice — reduce inter-service coupling and lean on
the shared library boundary — while staying aligned with the production authentication
mechanism.

[authn]: https://github.com/krateo-platformops/authn
