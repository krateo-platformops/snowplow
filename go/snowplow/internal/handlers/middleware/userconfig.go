// userconfig.go — Ship D.3 / 0.30.151. Snowplow-local replacement
// for plumbing's `use.UserConfig` middleware, the F-3 closure that
// Ship D.2 / 0.30.143 missed.
//
// PROVENANCE / UPSTREAM DRIFT MONITOR (AC-D3.1 + AC-D3.14).
//   - Upstream module:  github.com/krateo-platformops/plumbing
//   - Upstream version: PinnedPlumbingVersion below, which must equal go.mod's
//     pin; each bump's re-audit is recorded on that constant.
//   - Upstream file:    server/use/userconfig.go
//   - Upstream func:    use.UserConfig(keys jwtutil.KeySource, authnNS string)
//                       func(http.Handler) http.Handler
//   - Upstream lines:   :23-88 at PinnedPlumbingVersion, as every :N below
//
// asymmetric-signing migration: upstream now verifies RS256 against an RSA
// public key instead of an HMAC shared secret, and resolves that key through a
// jwtutil.KeySource keyed on the token's "kid" rather than holding one parsed
// key. In production the source is a jwtutil.JWKSKeySource pointed at authn's
// /.well-known/jwks.json, which fetches and caches the key set — so snowplow no
// longer mounts authn's public key from a Secret at all, and authn can rotate
// its keypair without a snowplow redeploy. The transcription below mirrors
// upstream: every request path that used to call jwtutil.Validate(signingKey,
// ...) now calls jwtutil.ValidateWithKeySource(keys, ...). A key that cannot be
// resolved is reported 503 (our fault, retryable) rather than 401, byte-identical
// to upstream's ErrKeyUnavailable handling.
//
// This file is a VERBATIM transcription of `use.UserConfig`'s control
// flow with ONE intentional behaviour-additive change: the
// `endpoints.FromSecret(...)` call at upstream :66-75 is preceded by
// a cache-first lookup against the Ship D.2 in-process Secrets
// snapshot (`api.FromInformerSecret`). On any cache miss the
// VERBATIM upstream call is then issued with the IDENTICAL arguments;
// error handling on the fallback path is byte-identical to upstream
// (NotFound → 401, anything else → 500). A SECOND intentional change (#994):
// a token whose groups carry krateo:loa:2 reads the step-up Secret
// `<user>-clientconfig-stepup` instead of `<user>-clientconfig`
// (credsecret.For); every other token's Secret name is upstream's. A THIRD
// (#1029, #1076): every 401 this middleware writes keeps upstream's six Status
// fields and message and appends `details.causes[0].reason` from a closed set
// plus a WWW-Authenticate challenge (credential_status.go), so a client can
// tell a pre-dispatch rejection from a relayed upstream 401. Cache hits bypass the
// apiserver GET entirely and are byte-equivalent to what
// `endpoints.FromSecret` would have returned (gated by Ship D.2's
// TestFromInformerSecret_UpstreamFieldParity).
//
// Why this lives in snowplow (not upstream): `project_no_upstream_authority`
// — we cannot patch krateoplatformops/plumbing. Substituting a
// snowplow-local middleware at the same mux mount point is the
// only in-tree way to wrap site #2 of the F-3 call graph (see
// docs/ship-d3-clientconfig-secret-cache-middleware-design.md §1).
//
// Pinned upstream version for drift monitoring (AC-D3.14). Bumping
// `plumbing` in go.mod without re-auditing this file MUST trip
// TestUserConfigMirror_PlumbingVersionPin in userconfig_test.go.
package middleware

import (
	"errors"
	"fmt"
	"net/http"
	"strings"

	xcontext "github.com/krateo-platformops/plumbing/context"
	"github.com/krateo-platformops/plumbing/http/response"
	"github.com/krateo-platformops/plumbing/jwtutil"
	apierrors "k8s.io/apimachinery/pkg/api/errors"

	"github.com/krateo-platformops/snowplow/internal/cache"
	"github.com/krateo-platformops/snowplow/internal/credhttp"
	"github.com/krateo-platformops/snowplow/internal/credsecret"
	"github.com/krateo-platformops/snowplow/internal/resolvers/restactions/api"
	"github.com/krateo-platformops/snowplow/internal/slowcall"
	"github.com/krateo-platformops/snowplow/internal/support/audit"
)

// PinnedPlumbingVersion is the upstream `plumbing` module version this
// middleware was transcribed from. AC-D3.14 — if `go.mod`'s pinned
// version drifts from this constant, `TestUserConfigMirror_PlumbingVersionPin`
// fails and the operator must re-audit upstream
// `server/use/userconfig.go` line-by-line before bumping this string.
//
// v1.14.2 → v1.15.1 (#657, for crdgen): re-audited by `diff -r` of the two
// module trees. server/, endpoints/ and jwtutil/ are byte-identical, and the
// only non-crdgen changes are helm/getter/cache and helm/utils, which
// snowplow does not import.
//
// v1.15.1 → v1.28.1 (#1135): re-audited by `git diff` of the two tags. server/
// is byte-identical, and jwtutil/ changes only the JWKS key source's default
// client (dialguard.Client, the same timeout). endpoints/ adds the
// pass-credentials-on-redirect key, which this file does not read.
//
// v1.28.1 → v1.34.0 (installer#716): re-audited by `git diff` of the two tags
// and `diff -r` of the two module trees. server/use/userconfig.go changes one
// call: the Secret name `fmt.Sprintf("%s-clientconfig",
// kubeutil.MakeDNS1123Compatible(username))` becomes
// `endpoints.SecretName(username)`, the same string (endpoints.go), which
// credsecret.SessionName also builds. jwtutil.ValidateWithKeySource now
// decodes the optional `act` and `principal` claims and refuses a signed token
// whose `act` chain is malformed (ErrTokenInvalid → 401 TokenInvalid here);
// jwt_delegation_claims_plumbing134_test.go pins both answers. The upstream
// line numbers in this file were refreshed to v1.34.0.
//
// v1.34.0 → v1.36.0 (plumbing#63, #107): re-audited by `diff -r` of the two
// module trees. server/use/userconfig.go is byte-identical (only a test is
// added beside it), so the upstream line numbers stand. jwtutil/ reports a
// token whose kid is absent from a key set fetched successfully after the
// token was issued as ErrTokenInvalid AND ErrKeyNotPublished instead of
// ErrKeyUnavailable: upstream's 503 for it becomes a 401, and here
// tokenRejected names it KeyNotPublished
// (predispatch_401_keynotpublished_test.go). The rest of the diff is docs/
// and helm/, which snowplow does not import.
//
// v1.36.0 → v1.52.0 (plumbing#138, the js/wasm dialguard; snowplow#1359):
// re-audited by `diff -r` of the two module trees. server/ and jwtutil/ are
// byte-identical, so this file's upstream line numbers stand. endpoints/
// changes only the write path, Store (now StoreWritten, reporting the Secret
// it wrote), which snowplow never calls; FromSecret and SecretName are
// unchanged. http/request wraps its clients in dialguard.RoundTripper, the
// identity on a server build.
//
// v1.52.0 → v1.54.0 (plumbing#141): re-audited by `diff -r` of the two module
// trees. server/, jwtutil/, endpoints/ and http/request's non-test files are
// byte-identical; the diff is dialguard/ (RoundTripper also sets fetch's
// credentials mode "omit" on js/wasm, still the identity on a server build),
// helm/, which snowplow does not import, and tests.
const PinnedPlumbingVersion = "v1.54.0"

// UserConfig is the snowplow-local cache-aware sibling of plumbing's
// `use.UserConfig`. Signature is byte-identical (same parameters,
// same return type) so the call-site swap at `main.go`'s 7 mount
// points is a single-token replacement (`use.` → `middleware.`).
//
// Flow (verbatim from upstream except where annotated):
//
//   1.  Authorization header presence check — Unauthorized on miss.
//   2.  `Bearer ` prefix validation — Unauthorized on mismatch.
//   3.  `jwtutil.ValidateWithKeySource(keys, token)` — Unauthorized on a bad or
//       expired token, ServiceUnavailable when the key itself is unresolvable.
//
//      (#1076 — each Unauthorized in steps 1-3, and the NotFound in step 5,
//      keeps upstream's six Status fields and message and adds a pre-dispatch
//      cause plus a WWW-Authenticate challenge: credential_status.go.)
//
//   4.  `rest.InClusterConfig()` — InternalError on error.
//
//      (Steps 1-4 are byte-identical to upstream; the early `return`s
//      mean we exit before any cache lookup runs. Cache=off behaviour
//      is preserved trivially: we never reach step 5 on any of these
//      branches.)
//
//   5.  CACHE-FIRST LOOKUP — Ship D.3 change.
//       `api.FromInformerSecret(ctx, authnNS, credsecret.For(user, groups))`:
//       `<user>-clientconfig`, or `<user>-clientconfig-stepup` for a token
//       whose groups carry krateo:loa:2 (#994).
//
//       a. served=true, ferr=nil  → cache HIT. Skip the upstream
//          apiserver GET entirely; the cache's `Endpoint` value is
//          byte-equivalent to what upstream `FromSecret` would have
//          returned (Ship D.2 `TestFromInformerSecret_UpstreamFieldParity`).
//
//       b. served=false, ferr=nil → SOFT MISS (cache=off,
//          pre-readiness, NS mismatch, Secret absent in snapshot).
//          Record the fallthrough counter on the call's scope
//          context, THEN re-issue the verbatim upstream call with
//          IDENTICAL arguments (`endpoints.FromSecret(req.Context() [#1115: carries the page budget],
//          sarc, name, authnNS)`). Error handling on the re-call is
//          byte-identical to upstream :68-75: `apierrors.IsNotFound(err)`
//          → Unauthorized; any other error → InternalError.
//
//       c. served=false, ferr!=nil → CACHE HARD ERROR. The Secret is
//          present in the snapshot but its content is malformed
//          ("missed required attribute for endpoint: server-url"
//          — Ship D.2 verbatim error string). Treat as upstream's
//          non-NotFound branch → InternalError. The error string is
//          byte-identical to what upstream would deliver on the same
//          input (gated by AC-D2.6).
//
//   6-7. `xcontext.BuildContext(ctx, WithAccessToken, WithUserInfo,
//        WithUserConfig(ep))` + `next.ServeHTTP(wri, req.WithContext(ctx))`
//        — verbatim from upstream :77-83. The `ep` value flowing in
//        is byte-equivalent regardless of which branch (5.a, 5.b, 5.c)
//        produced it; the downstream contract is identical.
//
// AC-D3.3 — `cache.RecordApiserverFallthrough(ctx, ReasonSecretGet, "")`
// fires on the SOFT-MISS branch (5.b) BEFORE the upstream
// `endpoints.FromSecret` call. This is the F-3 internal counter the
// 0.30.143 baseline lacked — post-D.3 the counter and apiserver-side
// `secrets,GET` rate have the SAME denominator (each cache miss
// increments both exactly once).
//
// AC-D3.N — `ReasonSecretGet` counter recovers. Post-deploy
// `/debug/vars` will show `snowplow_apiserver_fallthrough_cells["call-*|secret-get"]`
// non-zero on the production workload. This is the empirical proof
// the middleware swap rewired the F-3 path through snowplow's local
// counter-firing wrapper rather than the un-instrumented upstream
// middleware.
//
// AC-D3.10 — `CACHE_ENABLED=false` flag-flip semantics. With caching
// disabled, `api.FromInformerSecret` returns `(zero, false, nil)`
// (the soft-miss path) on every request → the upstream `FromSecret`
// is invoked on every call → apiserver-side rate returns to within
// ±10% of the pre-D.3 baseline. The flag is verified-removable
// (`project_caching_is_provisional`).
func UserConfig(keys jwtutil.KeySource, authnNS string) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		fn := func(wri http.ResponseWriter, req *http.Request) {
			// #1115 — a customer request's network exchanges share the page's
			// 10 s goal budget, from its arrival (pagebudget). Set once, here,
			// where every customer request/response route enters, before the
			// clientconfig read below. A background self-loopback (#903's
			// marker) is not a customer request and carries none.
			req = withCustomerPageBudget(req)
			// #1054 — the auth phase of the request's slow-call record: JWT
			// validation plus the clientconfig lookup (and its apiserver
			// re-read on a miss). It ends before the next handler runs.
			auth := slowcall.Start(req.Context(), slowcall.PhaseAuth)
			authOpen := true
			defer func() {
				if authOpen {
					auth.End()
				}
			}()

			// Upstream :26-29 — a missing key source is a wiring bug, surfaced on every
			// request as an InternalError, before the auth-header check.
			if keys == nil {
				response.InternalError(wri, fmt.Errorf("no JWT key source configured"))
				return
			}

			// Upstream :31-35 — Authorization header presence. #1076 — the
			// 401 keeps upstream's six Status fields and message, and adds
			// the pre-dispatch cause TokenMissing (credential_status.go).
			authHeader := req.Header.Get("Authorization")
			if authHeader == "" {
				tokenMissing(wri, fmt.Errorf("missing authorization header"))
				return
			}

			// Upstream :37-41 — Bearer prefix. #1076 — a header whose scheme
			// is not Bearer presents no bearer token: TokenMissing, the cause
			// RefreshAuth gives a non-Bearer header with no session cookie.
			parts := strings.SplitN(authHeader, " ", 2)
			if len(parts) != 2 || strings.ToLower(parts[0]) != "bearer" {
				tokenMissing(wri, fmt.Errorf("invalid authorization header format"))
				return
			}

			// #1076 — `Bearer ` with an empty token presents no token either:
			// TokenMissing, as RefreshAuth treats an empty bearer. Upstream
			// hands "" to the validator, which answers ErrTokenInvalid; the
			// message (one of the six plumbing fields) stays upstream's.
			if parts[1] == "" {
				tokenMissing(wri, jwtutil.ErrTokenInvalid)
				return
			}

			// Upstream :43-58 — jwtutil.ValidateWithKeySource. Upstream answers
			// the expired and the generic-invalid branch alike with a plain
			// Unauthorized. #1076 — snowplow keeps that 401, its six Status
			// fields and its message, and adds the cause jwtutil's typed error
			// names: ErrKeyNotPublished (a retired signing key) →
			// KeyNotPublished, ErrTokenExpired → TokenExpired, anything else
			// → TokenInvalid (tokenRejected).
			userInfo, err := jwtutil.ValidateWithKeySource(keys, parts[1])
			if err != nil {
				// Upstream — an unresolvable key (JWKS unreachable, or a kid
				// the cached set may predate) is OUR failure, not a bad
				// credential: 503 so the client retries instead of
				// re-authenticating against an authn that is merely down. It
				// carries no cause. A kid absent from a set fetched AFTER the
				// token was issued is not this: since plumbing v1.36.0
				// (plumbing#63) it is ErrKeyNotPublished, which never matches
				// ErrKeyUnavailable, and tokenRejected answers it 401
				// KeyNotPublished.
				if errors.Is(err, jwtutil.ErrKeyUnavailable) {
					response.ServiceUnavailable(wri, err)
					return
				}
				tokenRejected(wri, err)
				return
			}
			// #1379 re-review — the caller is verified from here on: an
			// audited route's one record (audit.OpenRecord) names them,
			// whatever exit the request takes, the credential lookup's below
			// included.
			audit.SetCaller(req.Context(), userInfo)

			// Ship D.3 — cache-first lookup. #994 — the Secret is chosen
			// from the VALIDATED token alone: a token whose groups carry
			// krateo:loa:2 (a step-up login, authn#49) reads
			// `<DNS1123(username)>-clientconfig-stepup`; every other token
			// reads `<DNS1123(username)>-clientconfig`, computed
			// IDENTICALLY to upstream's `endpoints.SecretName(username)`
			// at `userconfig.go:67` (credsecret.SessionName). The name is
			// built from the token's own username, never from a claim or
			// the request. There is NO fallback between the two: a step-up
			// Secret that is neither in the informer nor on the apiserver
			// re-read below is a NotFound, which answers 401 like any
			// missing credential.
			secretName := credsecret.For(userInfo.Username, userInfo.Groups)

			ep, served, ferr := api.FromInformerSecret(req.Context(), authnNS, secretName)
			if !served {
				if ferr != nil {
					// (5.c) — cache HARD ERROR (Ship D.2's verbatim
					// upstream error string for server-url missing).
					// Upstream's equivalent branch is the non-NotFound
					// arm at :73-74 → InternalError.
					audit.FailRoute(req.Context(), http.StatusInternalServerError)
					response.InternalError(wri, ferr)
					return
				}
				// (5.b) — SOFT MISS. Record the fallthrough counter
				// BEFORE the upstream re-call so the per-cell rate
				// matches the apiserver-side denominator exactly
				// (AC-D3.3, AC-D3.4, AC-D3.N).
				cache.RecordApiserverFallthrough(req.Context(), cache.ReasonSecretGet, "")

				// Upstream :60-64 — InClusterConfig. Required for the
				// upstream `FromSecret` re-call below. Deferred from
				// upstream's pre-FromSecret position because:
				//
				//   (a) On cache HIT (the steady state) we don't need
				//       it — the cache supplies the Endpoint and the
				//       SA RC is irrelevant. Deferring avoids paying
				//       its cost on the hot path.
				//   (b) On cache MISS the call order vs upstream is
				//       byte-identical: InClusterConfig immediately
				//       precedes the FromSecret re-call, exactly as
				//       upstream :60-67 has it. The InClusterConfig
				//       failure mode (500 with verbatim "unable to
				//       create in cluster config" wrap) is preserved
				//       and tested by
				//       TestUserConfig_ByteEquivalence_CacheMiss.
				sarc, ierr := credhttp.InClusterConfig()
				if ierr != nil {
					audit.FailRoute(req.Context(), http.StatusInternalServerError)
					response.InternalError(wri, fmt.Errorf("unable to create in cluster config: %w", ierr))
					return
				}

				// Upstream call (`use.UserConfig` :66-67) with the EXACT
				// same arguments, through credhttp.FromSecret: plumbing's
				// endpoints.FromSecret over a hardened copy of sarc (#920:
				// no client-side limiter; #723: no redirect followed). The
				// cache-miss fallback's observable behaviour MUST be
				// byte-identical to upstream — gated by
				// TestUserConfig_ByteEquivalence_CacheMiss.
				// #1115 — on the request's ctx: the re-read is a network exchange of
				// this customer request and runs inside its page budget (a spent
				// budget is never sent: 504 Timeout). Without a budget the call is
				// byte-identical to upstream's.
				ep, ferr = credhttp.FromSecret(req.Context(), sarc, secretName, authnNS)
				if ferr != nil {
					// Upstream :68-75 — NotFound → 401, else → 500.
					// PM load-bearing pin: the IsNotFound distinction
					// MUST be preserved here (cache-miss errors must
					// route 401 vs 500 identically to upstream).
					//
					// #1029 — the NotFound 401 carries a machine-readable
					// cause chosen from the same validated groups that chose
					// the Secret (StepUpRequired + the RFC 9470 challenge for
					// a krateo:loa:2 token, SessionCredentialMissing + the
					// RFC 6750 invalid_token challenge otherwise, #1076). Its
					// kind/apiVersion/status/message/reason/code are
					// byte-identical to upstream's. Any other re-read error
					// stays upstream's 500, with no cause.
					if credhttp.WriteBudgetError(wri, ferr) {
						audit.FailRoute(req.Context(), http.StatusGatewayTimeout)
						return
					}
					if apierrors.IsNotFound(ferr) {
						// The verified caller's own credential is missing:
						// snowplow refuses before anything is sent, and
						// the audited route's record says why.
						audit.DenyRoute(req.Context(), credentialMissingReason(userInfo.Groups))
						credentialMissing(wri, userInfo.Groups, ferr)
						return
					}
					audit.FailRoute(req.Context(), lookupFailureCode(ferr))
					response.InternalError(wri, ferr)
					return
				}
			}

			// Upstream :77-81 — BuildContext with the 3 opts in the same
			// order. `ep` is byte-equivalent regardless of whether (5.a)
			// cache-hit or (5.b) upstream re-call produced it.
			ctx := xcontext.BuildContext(req.Context(),
				xcontext.WithAccessToken(parts[1]),
				xcontext.WithUserInfo(userInfo),
				xcontext.WithUserConfig(ep),
			)

			authOpen = false
			auth.End()

			// Upstream :83 — chain to next handler.
			next.ServeHTTP(wri, req.WithContext(ctx))
		}

		return http.HandlerFunc(fn)
	}
}
