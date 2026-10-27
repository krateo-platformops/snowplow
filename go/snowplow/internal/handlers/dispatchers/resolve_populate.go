// resolve_populate.go — Ship C (0.30.112): the single resolve-and-store
// path for the L1 resolved-output cache.
//
// resolveAndPopulateL1 re-resolves an L1 entry from its own
// ResolvedKeyInputs and writes the fresh bytes back under the canonical
// key. It is the body the runtime refresher's RefreshFunc invokes on a
// dirty-mark (Ship C) and the body Ship F's prewarm will reuse — one
// resolve path, no duplication.
//
// IDENTITY (PM directive, AC-C7): the re-resolve runs under the entry's
// OWN Inputs identity — Username + Groups from the ResolvedKeyInputs.
// A refresh of user U's entry resolves as U, so RBAC narrowing and the
// resolved content stay user-correct. The re-resolve context also carries
// WithL1KeyContext(key) so the resolver re-records dep edges (the inner
// object set may have changed since the original resolve).
//
// SA TRANSPORT (Ship 0.30.113 Part B): a background refresh has no live
// per-user bearer token — the original request's Endpoint is long gone.
// The widget resolver (widgets.Resolve) reads xcontext.UserConfig(ctx)
// directly and fails "user *Endpoint not found in context" if only the
// identity (WithUserInfo) is supplied. With the informer pivot ON
// (#57: implicit-on-cache, i.e. the cache subsystem on) every K8s read
// is informer-served and
// RBAC-narrowed IN-PROCESS from WithUserInfo — never from the user's
// token — so the user's Endpoint is needed ONLY as a transport. We
// therefore supply the snowplow ServiceAccount endpoint + *rest.Config
// as that transport (WithUserConfig + WithInternalEndpoint +
// WithInternalRESTConfig) while keeping WithUserInfo{Username,Groups}
// for per-user correctness. No per-user token is stored. This is the
// EXACT pattern Phase 1 uses (withPhase1SAContext, phase1_walk.go) — the
// load-bearing 0.30.103 SA-CA seam. When saEP/saRC are nil (no SA creds
// — unit test / outside-cluster) the context is identity-only as before.
//
// Per feedback_l1_invalidation_delete_only.md: this path only ever
// Put()s — it never evicts. A refresh that lands after the entry was
// evicted must not resurrect it (see the post-resolve liveness re-check).

package dispatchers

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"

	xcontext "github.com/krateo-platformops/plumbing/context"
	"github.com/krateo-platformops/plumbing/endpoints"
	"github.com/krateo-platformops/plumbing/env"
	"github.com/krateo-platformops/plumbing/jwtutil"
	"github.com/krateo-platformops/snowplow/apis"
	templatesv1 "github.com/krateo-platformops/snowplow/apis/templates/v1"
	"github.com/krateo-platformops/snowplow/internal/cache"
	"github.com/krateo-platformops/snowplow/internal/objects"
	"github.com/krateo-platformops/snowplow/internal/redact"
	"github.com/krateo-platformops/snowplow/internal/resolvers/restactions"
	restactionsapi "github.com/krateo-platformops/snowplow/internal/resolvers/restactions/api"
	"github.com/krateo-platformops/snowplow/internal/resolvers/widgets"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/rest"
)

// resolveOnceFn is the resolve-and-encode seam. It re-fetches the CR
// named by inputs (under ctx's identity) and returns the encoded
// resolver output. Production wires it to resolveOnceProd; tests stub
// it to exercise resolveAndPopulateL1's queue/identity/Put plumbing
// without a live cluster.
//
// A package var rather than a parameter so the refresher's RefreshFunc
// signature (cache.RefreshFunc) is untouched. Swapped only by the
// _test.go shim; production never reassigns it.
var resolveOnceFn = resolveOnceProd

// errSelfObjectGone is the 1.12.5 / #187 (i) sentinel: the refresh
// re-fetch of the entry's OWN object came back with a definite apiserver
// 404. It is wrapped (%w) into the re-fetch error so the existing error
// text — the exact string the 264 refresher.refresh_failed lines on
// krateo-057 carried — is unchanged for anyone grepping logs, while
// resolveAndPopulateL1 can discriminate it with errors.Is.
//
// It says SELF by construction: the only site that wraps it is the
// re-fetch of the CR named by the entry's own Inputs, which runs BEFORE
// the resolver and therefore before any inner call can fail. An inner
// call's NotFound is the dirty-mark class and never produces this.
var errSelfObjectGone = cache.ErrSelfObjectGone

// errRefetchNilObject is a re-fetch that returned neither an object nor a
// status. #630: staged as refetch so it is attributable; the text stays the
// pre-#630 "re-fetch <res>/<name>: nil object".
var errRefetchNilObject = errors.New("nil object")

// resolveAndPopulateL1 is the single resolve-and-store path. It:
//
//  1. computes the canonical L1 key from inputs (must equal the key the
//     entry is filed under — ComputeKey is deterministic);
//  2. builds the re-resolve context: the entry's own Username+Groups
//     identity, the SA transport (saEP/saRC) so the resolver's
//     UserConfig/object-fetch sites have an apiserver client-config,
//     plus WithL1KeyContext(key) so dep edges are re-recorded;
//  3. re-resolves + encodes via the resolveOnce seam;
//  4. re-checks the entry is still live (a DELETE-evict may have raced
//     the refresh) and, if so, Put()s the fresh bytes.
//
// saEP/saRC are the process-singleton snowplow ServiceAccount endpoint +
// *rest.Config — supplied as TRANSPORT only (see the file header). When
// nil (no SA creds) the context is identity-only and a widget re-resolve
// that reads xcontext.UserConfig fails; the refresher's bounded retry
// then drops the key to TTL.
//
// Returns an error on resolve failure so the refresher can retry with
// backoff; returns nil (no-op) for an Inputs the path cannot drive.
func resolveAndPopulateL1(ctx context.Context, inputs cache.ResolvedKeyInputs, saEP *endpoints.Endpoint, saRC *rest.Config) error {
	log := xcontext.Logger(ctx)

	c := cache.ResolvedCache()
	if c == nil {
		// L1 disabled — nothing to populate. Not an error.
		return nil
	}

	key := cache.ComputeKey(inputs)

	// #854 — an incremental list content cell is kept current by the informer
	// deltas themselves (cache.ListCell): a refresh of its own key need not —
	// and must not — re-list and re-Put the whole list. Its Snapshot drains every
	// change up to the refresh's trigger seq; the refresh then ends FRESH (the
	// dirty window closes), with no store write: no generation move, no
	// PublishRefresh, no dep re-record, no rebuild compare (#854 C3). A cell that
	// unanchors here falls through to today's re-dispatch.
	if inputs.CacheEntryClass == cache.CacheEntryClassApistage {
		// #376 — GetNoTouch: the refresher reading its own key's prior entry is
		// an internal read, never a customer serve.
		if prior, ok := c.GetNoTouch(key); ok && prior != nil {
			if lc := prior.ListCell(); lc != nil {
				if _, ok := lc.Snapshot(cache.RefreshTriggerAsOf(ctx)); ok {
					cache.NoteListCellRefreshSkipped()
					cache.NoteRefreshWrite(ctx, key, true)
					return nil
				}
			}
		}
	}

	// #189 — capture the key's generation BEFORE the re-resolve. If a DELETE-
	// eviction lands during the (possibly long) re-resolve, the tail write below
	// is refused via PutIfGen rather than resurrecting the pre-delete body. This
	// REPLACES the racy Get-before-Put re-check (resolve_populate.go's former
	// alive-check — the TOCTOU the issue rejects): capture-then-guarded-write is
	// one atomic store-lock section where Get-then-Put was two.
	gen0 := c.CaptureGen(key)

	// Ship 4a (0.30.198) — capture the prior entry's pin status so a
	// RAFullList refresh RE-PINS rather than demoting a resident cell to
	// transient on a dirty-mark (the prewarm-protection contract:
	// feedback_zero_cold_navigations_hard_requirement). For every other
	// class prePinned stays false and the re-Put is unchanged.
	prePinned := false
	if inputs.CacheEntryClass == cache.CacheEntryClassRAFullList {
		// #376 — GetNoTouch: this is the refresher re-resolve reading the prior
		// entry's pin status; an internal read, not a customer serve, so it must not
		// stamp lastRead/hitTotal (it would feed the self-#316 loop and contaminate
		// the warm_lastread gauge). GetNoTouch returns the entry for prior.Pinned.
		if prior, ok := c.GetNoTouch(key); ok && prior != nil {
			prePinned = prior.Pinned
		}
	}

	// AC-C7: re-resolve under the entry's OWN identity. Ship A.3 /
	// 0.30.179 — the entry is per-COHORT (keyed by BindingSetHash); the
	// refresher re-resolves under the cohort's REPRESENTATIVE tuple
	// (the first writer's Username + Groups recorded on Inputs). Cohort
	// members produce byte-identical resolved output, so the
	// representative is equivalent to any other cohort member at resolve
	// time. A binding mutation that shifts the cohort topology shifts
	// BindingSetHash, MISSes on the next /call, and the seed reseeds
	// under a fresh representative — no stale-identity risk.
	//
	// Ship 1.3 (lever 2) — the IDENTITY-FREE classes (widgetContent /
	// apistage) carry NO representative identity: their key skips the
	// identity fold (ComputeKey, resolved.go:611-612) and their populate
	// sites never set RepresentativeUsername/Groups, so the tuple above
	// would be ("", nil). Re-resolving under that EMPTY identity drives
	// CohortNSACL("") -> no bindings -> permitAll=false -> the apiRef RA's
	// per-namespace stage drops every item -> status.resourcesRefs.items
	// is re-stored EMPTY: the refresher RE-POISONS the cell it is meant to
	// correct (the >3,100-cycle defect, project_prewarm_page_offset_bug).
	//
	// The identity-free cell holds an SA-MAXIMAL SHELL by construction —
	// the F2 walker Put()s it under withPhase1SAContext (phase1_walk.go),
	// and the serve-time gate (gateWidgetEnvelope, widget_content.go)
	// re-derives status.resourcesRefs.items[].allowed PER REQUESTER, so
	// the body that leaves the pod is per-user-narrowed regardless of the
	// shell's identity. The REFRESH must therefore re-resolve under the
	// SAME SA canonical identity the boot walk used, NOT the empty tuple,
	// so it CORRECTS the shell (full resourcesRefs.items) instead of
	// re-poisoning it. Ship 1.1 made the SA's CohortNSACL `*/*`
	// permitAll=true (CohortNSACL ServiceAccount-kind landing), so the
	// canonical `system:serviceaccount:<ns>:<name>` identity yields the
	// full list. The username is derived from the SA token's JWT `sub`
	// claim via phase1SAUsername (no Go literal — feedback_no_special_cases)
	// — the EXACT seam withPhase1SAContext uses. When the SA token is
	// absent (unit test / outside-cluster) we fall back to the
	// representative tuple, the unchanged degraded posture.
	refreshUser := inputs.RepresentativeUsername
	refreshGroups := inputs.RepresentativeGroups
	if isIdentityFreeClass(inputs.CacheEntryClass) {
		if saEP != nil {
			if saUser, ok := phase1SAUsername(saEP.Token); ok {
				refreshUser = saUser
				// SA identity is username-only — the SA's grant lands via
				// its ServiceAccount-kind binding, matched by username in
				// EvaluateRBAC + CohortNSACL. Mirrors withPhase1SAContext,
				// which installs WithUserInfo{Username: saUser} with no
				// Groups.
				refreshGroups = nil
			}
		}
	}
	// #262 privacy — a cell whose representative is a LEARNED class (a real
	// username + its full group set, seeded from authn's clientconfig
	// certificate) is refreshed under that identity; its refresh logs, and the
	// resolve path below through the ctx logger, carry the class only as its
	// sha256 label.
	learnedRep := isLearnedIdentity(refreshUser)
	if learnedRep {
		log = learnedRedactingLogger(log, refreshUser, refreshGroups)
	}
	// #424 R1 — REPRESENTATIVE DRIFT. An identity-bound cell is keyed by the
	// RBAC class of its first writer, recorded here as the representative. The
	// re-resolve below runs under the representative's CURRENT RBAC; if they
	// gained or lost a binding since the cell was minted, their own key has
	// moved but every other member of the old class still derives THIS key, and
	// re-Putting would hand those members the representative's new view.
	// Never refresh under it (checked before paying for the resolve; re-checked
	// after it, TOCTOU, below).
	//
	// #444 — and never leave the cell un-refreshable either (the pre-#444
	// suppress-to-TTL left every remaining member on a stale cell for up to the
	// TTL). Re-pick an identity that is in the key's class NOW — the canonical
	// group representative, else a recent hitter — and refresh under it; the
	// re-Put carries it as the new representative (ReplaceIfGen below keeps the
	// generation guard). If none is in the class, EVICT: a cold, correct refill
	// beats a stale cell.
	repSource := repSourceRecorded
	if drift := identityClassDrift(ctx, &inputs, refreshUser, refreshGroups); drift != "" {
		noteIdentityClassDrift("refresher", drift)
		prior, _ := c.GetNoTouch(key)
		u, g, src, ok := repickRepresentative(ctx, &inputs, prior)
		if !ok {
			evicted := c.EvictUnrefreshable(key, prior)
			if evicted {
				noteRepresentativeRepick("evicted")
			}
			log.Debug("resolveAndPopulateL1: representative left the cell's RBAC class and no in-class replacement exists; evicting",
				slog.String("subsystem", "cache"),
				slog.String("key_hash", key),
				slog.String("handler", inputs.CacheEntryClass),
				slog.String("drift", drift),
				slog.Bool("evicted", evicted),
				slog.String("effect", "the next request re-resolves the cell fresh under its own identity (#444)"),
			)
			return nil
		}
		noteRepresentativeRepick(src)
		refreshUser, refreshGroups, repSource = u, g, src
		inputs.RepresentativeUsername, inputs.RepresentativeGroups = u, g
		// #262 privacy x #444 — the re-resolve now runs (and its resolve path
		// logs) under the replacement. A recent hitter is a live identity like
		// #262's S2 classes, and a learned identity is one by definition: carry
		// either only as its sha256 label. Wrapping the existing logger keeps the
		// drifted learned representative's tokens redacted too; a group
		// representative of a learned class reuses that class's group tokens,
		// which the existing redacting logger (learnedRep) already scrubs.
		if src == repSourceHitter || isLearnedIdentity(u) {
			log = learnedRedactingLogger(log, u, g)
			learnedRep = true
		}
	}
	opts := []xcontext.WithContextFunc{
		xcontext.WithUserInfo(jwtutil.UserInfo{
			Username: refreshUser,
			Groups:   refreshGroups,
		}),
	}
	// Ship 0.30.113 Part B — SA transport. A background refresh has no
	// per-user token; the widget resolver reads xcontext.UserConfig
	// directly. Supply the SA endpoint as the transport so that read
	// succeeds. Under the informer pivot every K8s read is RBAC-narrowed
	// in-process from WithUserInfo above, never from this endpoint's
	// token — so this is transport-only, no per-user-token storage.
	// Mirrors withPhase1SAContext (phase1_walk.go).
	if saEP != nil {
		opts = append(opts, xcontext.WithUserConfig(*saEP))
	}
	if learnedRep {
		opts = append(opts, xcontext.WithLogger(log))
	}
	rctx := xcontext.BuildContext(ctx, opts...)
	// WithInternalEndpoint / WithInternalRESTConfig make cache.ClientConfigFor
	// (internal_client.go) return the pre-built SA *rest.Config verbatim
	// for the objects.Get apiserver fall-through and resourcesrefs.Resolve
	// — the load-bearing 0.30.103 SA-CA seam (the SA's raw-PEM CA cannot
	// survive the base64/cert-only kubeconfig path).
	if saEP != nil {
		rctx = cache.WithInternalEndpoint(rctx, saEP)
	}
	if saRC != nil {
		// #574 — read as the COHORT, not as snowplow. refreshUser/refreshGroups
		// is the representative identity installed via WithUserInfo above, so the
		// transport now matches the identity the cell is keyed to. Returns nil
		// when the apiserver has not granted impersonation (the startup probe) or
		// when there is no cohort identity to impersonate, in which case the
		// pre-#574 SA transport is kept verbatim and rbac.MustRegateSADial still
		// applies — that fallback is what makes a code-before-RBAC rollout safe.
		//
		// #608 — and nil when the apiserver would refuse ANY principal on the dial.
		// The startup probe only answers for `users`; a cohort also carries its
		// groups, which the chart allowlists by name. Dialling a refused group is
		// a certain 403 that leaves the cell stale until TTL (26,462 on 057), so
		// the capability is checked per principal (memoized, invalidated by the
		// RBAC snapshot generation) and a refused cohort takes the SAME re-gated
		// SA branch below — no impersonated-dial marker, so the re-gate and the
		// UAF refilter apply exactly as before #574.
		if imp := cache.PermittedImpersonatingRESTConfig(rctx, saRC, refreshUser, refreshGroups); imp != nil {
			rctx = cache.WithInternalRESTConfig(rctx, imp)
			rctx = cache.WithImpersonatedDial(rctx)
		} else {
			rctx = cache.WithInternalRESTConfig(rctx, saRC)
		}
	}
	// WithL1KeyContext threads the L1 key so the resolver's inner-call
	// recording site re-records dep edges for this refresh.
	rctx = cache.WithL1KeyContext(rctx, key)
	// #890/#893 — a re-resolve whose write leaves key non-resident (refused: the
	// cell was evicted meanwhile) leaves no edges behind (DepTracker.ReleaseUnstored).
	defer cache.Deps().ReleaseUnstored(rctx, key)

	// #598 — collapse the duplicate RA resolves inside one dirty-mark burst.
	// One object event dirties every cell on its coordinate and the refresher
	// re-resolves each independently; on 057 that is 44 widgets sharing the one
	// `incident-detail` RESTAction with identical keyExtras, each paying its own
	// parseListEnvelope. The memo is the existing #130 F4 mechanism, keyed on
	// (RA, identity, RBAC class, extras, page), so a hit is only ever served to a
	// caller that would have computed a byte-identical body. Freshness is the
	// memo's own #411 guard: an entry whose captured deps moved past its Stamp is
	// a MISS, and a refresh is triggered by exactly such a move, so the burst's
	// first cell resolves fresh and its siblings hit that fresh entry.
	// See refresh_resolve_memo.go for why a process-held memo is safe here when
	// F4 refused one for the seed.
	rctx = cache.WithSeedResolveMemo(rctx, refreshResolveMemo())

	// Ship 0.30.120 layer (b) — error-aware Put-gate. Install a
	// stage-error sink on the re-resolve context. The api resolver bumps
	// it whenever it writes dict[call.ErrorKey] (a swallowed,
	// continueOnError'd stage failure — e.g. the 401 from an exportJwt
	// loopback that has no per-user JWT in a background refresh). After
	// the re-resolve we read stageErrSink.Load(): a non-zero value means
	// the fresh bytes were produced under a stage error and MUST NOT
	// overwrite the prior good entry. The sink is request-path-inert:
	// only the refresher installs it, so a cold dispatch is unaffected.
	rctx, stageErrSink := cache.WithStageErrorSink(rctx)
	// #681 — the per-identity forbidden sink: a re-resolve whose ONLY stage
	// errors are per-identity forbidden (cache.DenialOnly) is re-Put like any
	// clean refresh. It obsoletes #677's known-declined mark, whose trigger was
	// exactly that result.
	rctx, forbiddenSink := cache.WithPerIdentityForbiddenSink(rctx)
	// External-no-cache (proposal 2026-06-22) — defense-in-depth. The
	// refresher only ever re-Puts already-cached keys, and external RAs never
	// get cached in the first place (the request-path Put-gates decline them),
	// so the refresher should never re-resolve an external RA. But install +
	// gate the external-touched sink here too, so that IF an external RA ever
	// reached this path it would still decline the re-Put rather than persist
	// stale external data. Additive to the stage-error sink.
	rctx, extTouchedSink := cache.WithExternalTouchedSink(rctx)
	// #398 — sensitive-resource sink: a resolve that dispatched a core
	// v1/secrets read must not be persisted by any resolved-output Put.
	rctx, _ = cache.WithSensitiveTouchedSink(rctx)
	// 1.12.3 A-1 / R-1 — install the UAF-touched sink, third sibling of the two
	// above. The refresher has no CR, so the declaration limb of the gate can
	// only read the HasUAF the original Put carried; the sink gives it a SECOND,
	// derivation-independent signal that works for a cell of ANY class whose
	// re-resolve runs a refilter — including a widgets-class cell, whose stored
	// Inputs never carry HasUAF because the declaration lives on the apiRef'd RA.
	rctx, uafTouchedSink := cache.WithUAFTouchedSink(rctx)
	// #904 — the re-resolve's stage outcomes, kept on a restactions-class entry
	// so an L1 hit answers with the header its cold resolve would have.
	rctx, stageOutcomes := restactionsapi.WithStageOutcomes(rctx)

	encoded, err := resolveOnceFn(rctx, inputs)
	if err != nil {
		// 1.12.5 / #187 (i) — a self-object 404 is NOT evicted here. It is
		// wrapped as cache.ErrSelfObjectGone and RETURNED, so the refresher
		// requeues it; the eviction happens at the DROP point, once the full
		// maxRefreshRequeues budget has re-confirmed the deletion (see
		// refresher.go processNext). The budget is the determinism proof —
		// about 15.5s of backoff at the defaults — so a CRD re-registration
		// window that clears inside it never evicts.
		//
		// An earlier revision evicted on the FIRST definite 404 with a
		// type-exists conjunct as the bound. That conjunct could not hold and
		// was removed — see the comment at the wrap site in resolveOnceProd.
		// The requeue budget is the only bound, and it is sufficient.
		//
		// 1.12.6 C4 (§6.2 row 4) — an entry kind the seam cannot refresh will
		// never become refreshable for THIS entry: SUPPRESS it (refresh-by-
		// traffic-only, cleared by the next real Put) instead of skipping it
		// to TTL forever. Not an error for the refresher: nothing to retry.
		if errors.Is(err, cache.ErrRefreshUnsupported) {
			cache.NoteRefreshDecline(key, "unsupported_kind", true)
			log.Debug("resolveAndPopulateL1: entry kind not refreshable; suppressed until the next real Put",
				slog.String("subsystem", "cache"),
				slog.String("key_hash", key),
				slog.String("handler", inputs.CacheEntryClass),
			)
			return nil
		}
		return fmt.Errorf("resolveAndPopulateL1 %s/%s: %w",
			inputs.CacheEntryClass, inputs.Name, err)
	}
	if encoded == nil {
		// The seam declined to resolve without an error — today the two
		// RAFullList sentinels (no status produced; the empty-full guard while
		// its informer is still syncing). Skip-to-TTL for THIS dequeue, not an
		// error; the next dirty-mark re-resolves it. The permanent "unknown
		// kind" case is the typed ErrRefreshUnsupported above.
		//
		// 1.12.6 C4 (§6.1, architect N6): this must not be a "forget the key
		// and keep the entry" outcome. #547 — it is not: the dequeue's window
		// ends KNOWN-STALE (refresher settleDequeue), so the cell is retried on
		// the next read (demand, under the requester) or the next change. Not a
		// structurally permanent decline, so never suppressed.
		cache.NoteRefreshDecline(key, "empty_full", false)
		return nil
	}

	// #189 — the "was it DELETE-evicted during the refresh?" check is no longer a
	// racy Get-before-Put here; it is folded into the atomic PutIfGen below
	// (capturedGen = gen0), which refuses the write under the same store-lock
	// section that would otherwise resurrect the pre-delete body.

	// Ship 0.30.120 layer (b) — error-aware Put-gate. If the re-resolve
	// observed ANY stage error (a swallowed, continueOnError'd inner-call
	// failure — e.g. the 401 from an exportJwt loopback that the SA-
	// transport refresher cannot satisfy), the fresh bytes are an
	// under-served result: they MUST NOT overwrite the user's prior good
	// entry. Decline the Put and return nil — NOT an error. A deterministic
	// stage failure must not drive AddRateLimited / burn the retry budget;
	// the prior good entry stays and the outer TTL is the safety net.
	//
	// The gate keys on STAGE-ERROR PRESENCE, never on result emptiness — a
	// user who legitimately has 0 compositions produces no stage error
	// (sink == 0) and their empty result IS stored.
	// #681 — a result whose ONLY stage errors are per-identity forbidden is NOT
	// an under-served result: it is exactly what this identity's class reads.
	// It falls through to the re-Put below.
	if stageErrSink.Count() > 0 && !cache.DenialOnly(stageErrSink, forbiddenSink, extTouchedSink, inputs.CacheEntryClass) {
		// #301 observability: surface the FIRST failing stage's name + err
		// inline so a decline can be attributed without a cross-grep
		// against the resolver's error lines (which cost two traces).
		sampleStage, sampleErr := stageErrSink.Sample()
		// #547 — an inner stage that fails under the background identity (a
		// refresher-only 403, an exportJwt loopback) ends the dequeue's window
		// KNOWN-STALE: the cell keeps serving its pre-change body, every hit on
		// it is counted (known_stale_served_total{reason=declined}), and the
		// next read revalidates it under the REQUESTER, whose read is the one
		// known to work. 1.12.6 C4 suppressed the key after K consecutive
		// declines (REFRESH_SUPPRESS_AFTER_DECLINES) and left it frozen; that
		// knob is retired. The #191 WARN storm is bounded instead by logging at
		// WARN once per stale generation: a decline of a cell that is ALREADY
		// known-stale is DEBUG (the counter carries the rate).
		// #903 B2 — a stage error on a resolve that ALSO touched an external
		// endpoint (a write refused in the background is both: a stage error,
		// and its RESTAction declares a branch-E write) can never be re-Put by
		// the refresher (the external gate below would decline it anyway), so
		// it is structurally permanent like the external decline: suppress,
		// don't re-resolve it on every change.
		declineReason, permanent := "stage_error", cache.GenuineExternal(extTouchedSink, forbiddenSink) > 0
		if permanent {
			declineReason = "external_touched"
		}
		suppressed := cache.NoteRefreshDecline(key, declineReason, permanent)
		_, _, alreadyKnownStale := cache.KnownStale(key)
		declineLevel := slog.LevelWarn
		if alreadyKnownStale {
			declineLevel = slog.LevelDebug
		}
		log.Log(rctx, declineLevel, "resolveAndPopulateL1: stage error during refresh; declining to overwrite good entry",
			slog.String("subsystem", "cache"),
			slog.String("key_hash", key),
			slog.String("handler", inputs.CacheEntryClass),
			slog.String("user", refreshLogUser(refreshUser)),
			slog.String("rep_source", repSource),
			slog.Int64("stage_errors", stageErrSink.Count()),
			slog.String("stage_err_stage", sampleStage),
			slog.String("stage_err_sample", redact.ErrorText(sampleErr)),
			slog.Bool("suppressed", suppressed),
			slog.String("effect", "prior entry kept and marked KNOWN-STALE: served and counted, revalidated "+
				"on the next read under the requester, re-enqueued by the next change (#547)"),
		)
		cache.BumpRefresherSkippedStageError()
		return nil
	}

	// External-no-cache (proposal 2026-06-22) — defense-in-depth re-Put gate.
	// If the re-resolve touched a genuine external endpoint, the fresh bytes
	// have no dep edge to invalidate them; decline the re-Put (keep the prior
	// entry; TTL is the outer net) exactly as the stage-error gate does.
	// #681 C2 — an own-credential apiserver 403 is not a genuine external touch.
	if cache.GenuineExternal(extTouchedSink, forbiddenSink) > 0 {
		cache.BumpExternalSkippedPut()
		// 1.12.6 C4 (§6.2 row 7) — structurally permanent: an external RA has
		// no dep edge, so every dirty-mark re-resolve is pure waste. Suppress
		// on first occurrence; the next real Put clears it.
		cache.NoteRefreshDecline(key, "external_touched", true)
		// Not a fault: a designed defense-in-depth decline (external data has no
		// dep edge to invalidate it, so keep the prior entry; TTL is the outer
		// net). Fires on every refresh of an external-touching entry — DEBUG.
		// The BumpExternalSkippedPut counter carries the rate for operators.
		log.Debug("resolveAndPopulateL1: re-resolve touched an external endpoint; declining to overwrite entry",
			slog.String("subsystem", "cache"),
			slog.String("key_hash", key),
			slog.String("handler", inputs.CacheEntryClass),
			slog.String("user", refreshLogUser(refreshUser)),
			slog.String("rep_source", repSource),
			slog.Int64("external_touches", extTouchedSink.Count()),
			slog.String("effect", "prior entry kept; external data has no dep edge — TTL is the outer net"),
		)
		return nil
	}

	// #561 — the refresher leg of the shared gate, now REACHABLE ONLY for an
	// identity-free class. The pre-#561 text here said the refresher "should
	// never be handed" a UAF cell because no Put site could create one; that is
	// no longer true for the identity-bound classes, which cache narrowed bodies
	// again under keys that separate their requesters (#423/#588). For those this
	// gate returns "" and the re-Put proceeds — which is the whole point of
	// #561: a composition-data cell that cannot be refreshed cannot live-refresh,
	// and cannot be subscribed to at all.
	//
	// It still fires for an identity-free cell, where ONE cell is shared by every
	// requester and a narrowed body in it is an unconditional cross-tenant leak
	// no key change short of making the class identity-bound can fix. Same shape
	// as the external-touched re-Put gate above: keep whatever entry exists,
	// decline the write, TTL is the outer net.
	if reason := uafDeclineReason(&inputs, uafTouchedSink); reason != "" {
		// Count under the class the cell actually belongs to, so the widgets
		// carrier's refresh declines do not get buried in the restactions number.
		if isWidgetClass(inputs.CacheEntryClass) {
			declineWidgetUAFPut(&inputs, uafTouchedSink)
		} else {
			declineUAFPut(&inputs, uafTouchedSink)
		}
		// 1.12.6 C4 (§6.2 row 8) — structurally permanent until 1.13.0 folds
		// the UAF scope into the key (#180): suppress on first occurrence.
		cache.NoteRefreshDecline(key, "uaf", true)
		// DEBUG for the same reason as the dispatch site: expected, designed, and
		// potentially per-refresh-cycle frequent. The counter carries the rate.
		log.Debug("resolveAndPopulateL1: re-resolve is userAccessFilter-narrowed; declining to re-Put",
			slog.String("subsystem", "cache"),
			slog.String("key_hash", key),
			slog.String("handler", inputs.CacheEntryClass),
			slog.String("user", refreshLogUser(refreshUser)),
			slog.String("rep_source", repSource),
			slog.String("uaf_reason", reason),
			slog.Int64("uaf_touches", uafTouchedSink.Count()),
			slog.String("effect", "prior entry kept, not refreshed; a UAF body is per-requester-narrowed and the key does not separate co-bound users (1.12.3 A-1)"),
		)
		return nil
	}

	// #398 — a re-resolve that read a sensitive resource is never re-Put
	// (structurally permanent for this cell: suppress on first occurrence).
	if cache.DeclineSensitivePut(rctx) {
		cache.NoteRefreshDecline(key, "sensitive_resource", true)
		log.Debug("resolveAndPopulateL1: re-resolve read a sensitive resource; declining to re-Put",
			slog.String("subsystem", "cache"),
			slog.String("key_hash", key),
		)
		return nil
	}

	// #424 — TOCTOU re-check: an RBAC change on the representative that landed
	// during the re-resolve makes the fresh bytes another class's. Never write
	// them. #444 — nor suppress: return a retryable error, so the refresher
	// requeues and the next attempt re-picks an in-class representative; a spent
	// budget drops at the drop point, leaving the cell KNOWN-STALE (#547).
	if drift := identityClassDrift(ctx, &inputs, refreshUser, refreshGroups); drift != "" {
		noteIdentityClassDrift("refresher", drift)
		return fmt.Errorf("resolveAndPopulateL1 %s/%s (%s): %w",
			inputs.CacheEntryClass, inputs.Name, drift, errRepresentativeDriftedMidRefresh)
	}

	// #561 — carry the UAF fact forward across the re-Put. The refresher builds a
	// FRESH entry with a zero CreatedAt, so a cell that slides forward on every
	// data-plane refresh keeps its cap only if HasUAF survives the round trip.
	// The declaration limb already does (it is stored on the Inputs); the
	// OBSERVED limb does not, so a nested-UAF cell would quietly lose its cap the
	// first time the refresher touched it. Stamp it from this re-resolve's own
	// sink, the same way the dispatch and seed Puts now do.
	if uafTouchedSink.Count() > 0 {
		inputs.HasUAF = true
	}
	entry := &cache.ResolvedEntry{
		RawJSON: encoded,
		Inputs:  &inputs,
		// Ship 4a (0.30.198) — preserve the resident pin on a RAFullList
		// refresh so a dirty-mark re-resolve never demotes a prewarmed
		// expensive cell to the transient LRU. Put honours the pin subject
		// to the resident budget (else demotes — the safe degrade).
		Pinned: prePinned,
		// #118 (d) interim — C-118-6 CRUX: re-stamp the short UAF TTLOverride on
		// the REFRESHER re-Put too. The refresher builds a FRESH entry with zero
		// CreatedAt (Put stamps a new time.Now()), so a hot, data-plane-refreshed
		// UAF cell would slide its CreatedAt forward every refresh and OUTLIVE the
		// cap if the override were stamped only on the first customer Put. inputs
		// is the STORED ResolvedKeyInputs (carrying HasUAF from the original
		// dispatch), so uafTTLOverrideForEntry re-derives the same override here
		// without needing the RESTAction CR. Returns 0 (no override) when the knob
		// is unset or the cell is non-UAF → byte-identical to today.
		TTLOverride: uafTTLOverrideForEntry(&inputs),
		// #904 — a restactions-class cell keeps its resolve's stage outcomes.
		StageOutcomes: storableStageOutcomesFor(inputs.CacheEntryClass, stageOutcomes),
	}
	// Ship #97 (0.30.214) — restore the R3 fast-path on refresher Puts
	// of apistage-class LIST entries. Pre-fix the refresher Put wrote
	// RawJSON only (Items: nil); the apistage read path at apistage.go:487
	// then evaluated `len(entry.Items) > 0 == false` on every content-Get-hit,
	// falling through to gateListEnvelope → parseListEnvelope on the
	// customer request goroutine (45% cum CPU at 0.30.212 production scale
	// per ship-97-prefix-falsifier-2026-05-31). Populating Items here
	// pushes the parse cost ONCE onto the refresher goroutine (per cycle)
	// and lets every subsequent Get-hit short-circuit through
	// gateListItemsWithMemo.
	//
	// GET-by-name (Name != "") and malformed-at-Put envelopes return ok=false
	// and the entry keeps Items=nil — byte-identical fallback to today.
	// #578 — Ship #97's EAGER parse is REMOVED here, and its benefit is kept.
	//
	// WHAT SHIP #97 WAS ACTUALLY FIXING: a refresher Put wrote RawJSON only, so
	// `len(entry.Items) > 0` was false on EVERY subsequent content-Get-hit and
	// each hit re-ran parseListEnvelope on a customer request goroutine (45% cum
	// CPU). The defect was the parse running PER HIT, unbounded in the read count.
	//
	// WHY REMOVING IT DOES NOT REGRESS THAT: the apistage read path now calls
	// ResolvedEntry.EnsureItems, which parses on the FIRST hit and publishes the
	// result on the entry, so every later hit short-circuits exactly as it did
	// with the eager parse. The parse is bounded at ONE per refresh generation
	// either way — #97's guarantee — but it is now only paid for cells somebody
	// reads.
	//
	// WHY THAT MATTERS HERE SPECIFICALLY: this is the REFRESHER's Put. Measured on
	// krateo-057 with the portal idle for 38 minutes, all 3,876 entries had
	// lastRead 2,300-2,500s while the refresher completed 33.7 refreshes/s — so
	// this line parsed envelopes continuously for cells with no reader, and
	// parseListEnvelope was 27.6% of a 30s CPU profile on a pod sitting at 98.75%
	// of its 4-CPU limit with ZERO users (#578).
	//
	// The customer-facing MISS path (apistage.go) still parses eagerly and stores
	// Items, because that requester needs the parsed form to run the content gate
	// before it can be served — there the parse is work, not waste.
	//
	// Nothing about the ENTRY SHAPE changes for readers: Items=nil is the
	// documented "no pre-parse" state that EnsureItems resolves, and an entry
	// written by a pre-#578 binary mid-rollout still carries Items and is honoured
	// ahead of the lazy memo.
	// #189 — GENERATION-GUARDED REPLACE. A refresh only ever REPLACES a live
	// entry; ReplaceIfGen refuses if the key was evicted (before or during the
	// re-resolve) or moved to a newer generation, so a non-resurrecting refresh
	// drops the fresh bytes and does NOT emit the live-refresh signal (L1 did not
	// change). This closes the resurrection race atomically (was the racy
	// alive-check above) AND preserves the "evicted → no signal" contract.
	// #378 — ReplaceIfGenRefresh is ReplaceIfGen plus the age re-mint: inside the
	// lead window [maxAge − L, …) the accepted replace resets BornAt under the SAME
	// key, so a warm cell kept fresh here never reaches the C5 cap under a customer.
	// The only freshMint=true write (TestReMint_SingleSetterAudit).
	//
	// #354 P3 — report the terminal write's outcome to the refresher's dequeue
	// (accepted closes the invalidation→fresh window unless #375 remarked it;
	// refused ends it evicted). A no-op off the refresher.
	// #604 instrument (refresh_rebuild_identity.go): classify this rebuild as
	// a no-op or a real change against the entry it is ABOUT to replace, so the
	// comparison is with what is actually being overwritten rather than with a
	// snapshot read earlier in this function. GetNoTouch, not Get: this is an
	// internal read and must not stamp lastRead/hitTotal (it would feed the
	// self-#316 loop and contaminate warm_lastread). Observation only — nothing
	// below is gated on the verdict.
	rebuildPrior, hadRebuildPrior := c.GetNoTouch(key)
	noteRefreshRebuild(rebuildPrior, hadRebuildPrior, encoded)

	stored := c.ReplaceIfGenRefresh(rctx, key, entry, gen0)
	cache.NoteRefreshWrite(rctx, key, stored)
	// #603 PR-2 / #765 SHADOW audit — when every mark this cell received since
	// its last refresh was a would-avoid, classify this rebuild against the
	// same prior the #612 compare used: identical, or changed (would_change).
	// Observation only.
	cache.NoteNarrowAuditWrite(rctx, key, rebuildPrior, hadRebuildPrior, encoded, stored)
	if stored && stageErrSink.Count() > 0 {
		cache.RecordDenialOnlyPut(cache.DenialOnlyPutSiteRefresher) // #681
	}
	if !stored {
		log.Debug("resolveAndPopulateL1: entry evicted during refresh; not resurrecting (generation moved)",
			slog.String("subsystem", "cache"),
			slog.String("key_hash", key),
		)
		return nil
	}
	// Ship 1 (live-refresh-coherence, option A) — emit the live-refresh
	// signal STRICTLY post-commit, on the refresher path only. This line is
	// reached ONLY after the Put returned and ONLY on a genuine L1 change:
	// the four no-Put success-returns above (cache-off :96-99, declined
	// :214-218, evicted-during-refresh :223-229, stage-error decline
	// :243-260) all return before here, so the signal never fires when L1
	// did not actually change (design §1.1 — coherent by construction).
	// resolveAndPopulateL1 is invoked ONLY from the refresher closure
	// (dispatchers.go:86), never from cold dispatch or the prewarm walker, so
	// this announces dep-change-driven re-resolves only. PublishRefresh is
	// nil-safe and a no-op when the SSE layer is disabled or cache is off.
	cache.PublishRefresh(key)
	log.Debug("resolveAndPopulateL1: re-resolved + stored",
		slog.String("subsystem", "cache"),
		slog.String("key_hash", key),
		slog.String("handler", inputs.CacheEntryClass),
		slog.String("user", refreshLogUser(refreshUser)),
		slog.String("rep_source", repSource),
		slog.Bool("pinned", prePinned),
	)
	return nil
}

// isIdentityFreeClass reports whether the entry class is a SHARED,
// identity-free cache class (cache.IdentityFreeClasses — the single list
// ComputeKey keys by, #449): the widget-content shell and the api-stage
// content cell. Both hold an SA-maximal SHELL that the serve-time gate
// (gateWidgetEnvelope / gateContentEnvelope) narrows per-requester; their
// refresh therefore re-resolves under the SA canonical identity (lever 2)
// rather than the empty representative tuple.
func isIdentityFreeClass(class string) bool {
	return cache.IsIdentityFreeClass(class)
}

// resolveOnceProd is the production resolve-and-encode implementation.
// It re-fetches the dispatch CR named by inputs (objects.Get, under
// ctx's identity) and dispatches the matching resolver, returning the
// encoded output byte-identical to the request-path encode
// (encodeResolvedJSON — same encoder settings as a cold dispatch).
// refresherResolveCtx stamps the refresher's driver origin (see the comment in
// resolveOnceProd for why it is load-bearing). A named builder so the #720
// driver-level arm (nested_bound_driver_ctx_720_test.go) classifies the REAL
// production refresher ctx as background for the nested-resolve gate.
func refresherResolveCtx(ctx context.Context) context.Context {
	return cache.WithBackgroundResolveOrigin(ctx, cache.BackgroundOriginRefresher)
}

func resolveOnceProd(ctx context.Context, inputs cache.ResolvedKeyInputs) ([]byte, error) {
	authnNS := env.String("AUTHN_NAMESPACE", "")

	// Aggregate OOM bound (#720): the refresher is BACKGROUND — its nested-resolve
	// trees are the ones the aggregate admission gate bounds. A customer /call
	// never waits on that gate (owner ruling, "No one waits in queue") but its
	// weight counts, so the refresher's trees are squeezed while customers are
	// active. Mark the ctx so enterNestedResolveUnit classifies this tree as
	// background. Covers BOTH the content-refresh and RA/widget-refresh branches
	// below (this is the single refresher resolve entry).
	//
	// LOAD-BEARING for rbac.MustRegateSADial (#268/#269 Part 2 drift-guard):
	// this is THE stamp that makes the refresher's per-user-representative
	// re-resolve (saCred + a REAL non-SA identity, i.e. !ServesUnnarrowed)
	// satisfy the guard's BackgroundResolve conjunct, so a denied read is
	// re-gated at objects.getFromAPIServer / branch E. Dropping it makes
	// MustRegateSADial go false for the refresher and re-opens the leak. The
	// drift-guard (the WithInternal* producer census + the sa_regate invariant
	// table) enforces this; the step-4 refresher arm trips if it is removed.
	//
	// #563 — the origin form NAMES this driver. It calls WithBackgroundResolve,
	// so the marker every reader above keys on is byte-identical; what it adds
	// is that a resourceRef denial generated under this ctx is counted as the
	// REFRESHER's rather than as an anonymous "background" (the 83%-of-the-log
	// attribution gap). The three origins are censused in
	// background_origin_census_563_test.go.
	ctx = refresherResolveCtx(ctx)

	// Ship F1 (0.30.119): an api-stage entry is a CONTENT-keyed K8s call
	// (gvr, namespace, name-or-empty) — NOT a RESTAction. Its refresh is
	// a single un-gated K8s re-dispatch + re-Put under the same content
	// key; there is no whole-RESTAction re-run, no objects.Get of a CR,
	// no self-hit (a content entry is re-dispatched, never self-Got).
	// resolveContentEntryForRefresh returns the fresh raw envelope, which
	// resolveAndPopulateL1 Puts under the content key.
	if inputs.CacheEntryClass == cache.CacheEntryClassApistage {
		return resolveContentEntryForRefresh(ctx, inputs)
	}

	// restactions / widgets entries identify a CR — re-fetch it, then
	// dispatch the matching resolver.
	ref := templatesv1.ObjectReference{
		Reference: templatesv1.Reference{
			Name:      inputs.Name,
			Namespace: inputs.Namespace,
		},
		APIVersion: schemaGroupVersion(inputs.Group, inputs.Version),
		Resource:   inputs.Resource,
	}
	got := objects.Get(ctx, ref)
	if got.Err != nil {
		// #630 — KEEP THE STATUS. objects.Get reports the apiserver's answer
		// as a plumbing *response.Status (Code + Reason + Message), which is
		// not an error; until #630 it was flattened to a string here, so the
		// refresher's classifier read a 403 on this hop as
		// err_class=unclassified stage=none, retried it through the whole
		// budget, and the drop point evicted it — the one forbidden path that
		// still made a cold navigation after #608/#619. statusErrFromResponse
		// lifts it back into an *apierrors.StatusError (the same repair the
		// Phase-1 seed path uses) and the ResolveStageError names the hop, so
		// it classifies as forbidden/refetch, a 404 as not_found/refetch, and
		// so on. The message text is unchanged ("re-fetch <res>/<name>: <msg>").
		refetchErr := cache.WrapResolveStage(cache.ResolveStageRefetch,
			fmt.Sprintf("re-fetch %s/%s", inputs.Resource, inputs.Name),
			statusErrFromResponse(got.Err))
		//
		// WHAT EACH CLASS DOES WITH IT (the refresher decides, not this site):
		//
		//  - a 403 is NON-RETRYABLE (#608 Q2, cache.IsNonRetryableRefreshError):
		//    the same identity is refused identically at every backoff step, so
		//    it reaches the drop point on its FIRST attempt — and a forbidden
		//    drop NEVER evicts (#619): the cell stays resident and served until
		//    the next dirty mark retries it (#547 makes that KNOWN-STALE).
		//  - a 500, a timeout and a parse failure keep the requeue; they are
		//    retried through the budget and are never a deletion.
		//  - a definite 404 is the only deletion (1.12.5 / #187 (i)): wrapped in
		//    errSelfObjectGone so the drop point EVICTS it once the budget has
		//    re-confirmed it, instead of leaving a deleted CR's body resident.
		//
		// TWO BOUNDS on the 404, both load-bearing:
		//
		//  - ONLY a definite 404. Every other status, a 403 included, is never
		//    a deletion — an apiserver hiccup must never evict a slice of L1
		//    at once.
		//  - NEVER the informer-only miss. Under cache.WithInformerOnlyReads
		//    objects.Get SYNTHESISES a NotFound rather than reaching the
		//    apiserver (objects/get.go informerOnlyMiss), so its 404 means
		//    "not in the indexer", not "deleted from the cluster". That is
		//    precisely the transient a CRD re-registration produces, and it
		//    must stay retryable.
		//
		// Every other 404 here HAS been confirmed by the apiserver: the
		// informer branch falls through to a live GET on a miss.
		//
		// THE TYPE-ABSENT CASE IS NOT BOUND HERE, DELIBERATELY. When a CRD is
		// absent — deleted and recreated, which is what a portal release does —
		// the apiserver returns a GENUINE 404 for every object of that GVR, and
		// this classifies them all as self-gone. A type-exists conjunct
		// (cache.Global().IsRegistered) was tried and REMOVED: objects.Get's own
		// not-servable branch calls EnsureResourceType BEFORE falling through to
		// the apiserver, so the GVR is registered again by the time this line
		// runs, and EnsureResourceType refuses only at GROUP granularity — a
		// single deleted CRD whose group survives passes it. Where it was inert
		// it was worse than absent, because the next reader would trust it as a
		// bound; where it would bind (a whole group gone) the objects really are
		// gone, so it would have blocked a CORRECT eviction and stranded those
		// entries to TTL. See the BCRD arm.
		//
		// The REQUEUE BUDGET is the bound for the 404: nothing is evicted until
		// it has repeated across maxRefreshRequeues (~15.5s of backoff at the
		// defaults), so a CRD re-registration that clears inside it is safe,
		// and a type absent for longer than that has genuinely taken its
		// objects with it.
		if got.Err.Code == http.StatusNotFound &&
			!cache.InformerOnlyReadsFromContext(ctx) {
			return nil, fmt.Errorf("%w: %w", refetchErr, errSelfObjectGone)
		}
		return nil, refetchErr
	}
	if got.Unstructured == nil {
		return nil, cache.WrapResolveStage(cache.ResolveStageRefetch,
			fmt.Sprintf("re-fetch %s/%s", inputs.Resource, inputs.Name), errRefetchNilObject)
	}

	switch inputs.CacheEntryClass {
	case "restactions":
		return resolveRestActionForRefresh(ctx, got, inputs, authnNS)
	case cache.CacheEntryClassRAFullList:
		// Ship 4a (0.30.198) — refresh the page-independent RA full-list
		// cell. The cell holds the RA's STATUS MAP (the resolved result,
		// e.g. {compositionspanels:[...]}) resolved UNPAGINATED — NOT the
		// whole RESTAction CR. resolveRAFullListForRefresh re-resolves the
		// RA at PerPage=0/Page=0 (no `.slice` injected → full sorted set) and
		// returns json.Marshal(ra.Status-map) so the bytes are byte-identical
		// to the apiref serve path's PutRAFullList (which marshals the same
		// map). Using resolveRestActionForRefresh here would store the whole
		// CR — a SHAPE MISMATCH the Go-slice serve could not consume.
		return resolveRAFullListForRefresh(ctx, got, inputs, authnNS)
	case "widgets":
		return resolveWidgetForRefresh(ctx, got, inputs, authnNS)
	case cache.CacheEntryClassWidgetContent:
		// Ship G (0.30.16x) — refresh path for the identity-free widget
		// content layer. The entry's Inputs identify the SAME widget CR
		// the per-user "widgets" entries identify, just under a
		// different key shape (identity-free vs per-user-keyed). The
		// resolver call is identical — widgets.Resolve on the re-fetched
		// CR — so we delegate to the same helper. The fresh bytes are
		// Put back under the identity-free key (ComputeKey reads
		// inputs.CacheEntryClass and skips identity for this class).
		//
		// Stage-error sink semantics + post-resolve liveness check are
		// inherited from resolveAndPopulateL1 (the caller). The Put
		// declines symmetrically when stage errors fire, preserving
		// the AC-G.5 contract.
		return resolveWidgetForRefresh(ctx, got, inputs, authnNS)
	default:
		// Unknown handler kind — 1.12.6 C4 (§6.2 row 4): typed, so the caller
		// SUPPRESSES the key instead of skipping it to TTL forever.
		return nil, fmt.Errorf("refresh of class %q: %w", inputs.CacheEntryClass, cache.ErrRefreshUnsupported)
	}
}

// resolveContentEntryForRefresh re-dispatches the single K8s call an
// api-stage CONTENT entry caches (Ship F1, 0.30.119). The entry's Inputs
// are a content key (gvr, namespace, name-or-empty); the refresh is one
// un-gated K8s re-dispatch — no whole-RESTAction re-run, no self-hit.
// The fresh raw envelope is returned for resolveAndPopulateL1 to Put
// back under the content key. (nil, nil) — the call is not currently
// pivot-servable — is a skip-to-TTL, not an error.
func resolveContentEntryForRefresh(ctx context.Context, inputs cache.ResolvedKeyInputs) ([]byte, error) {
	return restactionsapi.RefreshContentEntry(ctx, inputs)
}

// schemaGroupVersion renders the apiVersion string objects.Get expects.
// Core group ("") renders as just the version.
func schemaGroupVersion(group, version string) string {
	if group == "" {
		return version
	}
	return group + "/" + version
}

// resolveRestActionForRefresh converts the re-fetched CR to a typed
// RESTAction and dispatches restactions.Resolve, returning encoded
// output. Mirrors the restActionHandler.ServeHTTP resolve+encode path.
func resolveRestActionForRefresh(ctx context.Context, got objects.Result, inputs cache.ResolvedKeyInputs, authnNS string) ([]byte, error) {
	scheme := runtime.NewScheme()
	if err := apis.AddToScheme(scheme); err != nil {
		return nil, fmt.Errorf("add apis to scheme: %w", err)
	}
	var cr templatesv1.RESTAction
	if err := runtime.DefaultUnstructuredConverter.FromUnstructured(got.Unstructured.Object, &cr); err != nil {
		return nil, fmt.Errorf("unstructured -> RESTAction: %w", err)
	}

	// Ship 0.30.123 (#155): the Ship 0.30.120 layer-(a) exportJwt
	// skip-to-TTL scan was REMOVED here. Layer (a) existed only because a
	// background refresh had no way to complete an exportJwt /call-loopback
	// stage (no per-user JWT) — so it declined to refresh those RESTActions
	// at all. 0.30.123's in-process nested /call resolves a /call-loopback
	// stage WITHOUT an Authorization header (identity carried on ctx), so
	// the refresher CAN now correctly refresh an exportJwt RESTAction with
	// real, non-empty content. The skip-to-TTL net is obsolete.
	//
	// Layer (b) — the error-aware Put-gate in resolveAndPopulateL1 — STAYS
	// as the general backstop: any stage that still errors (a genuine RBAC
	// denial, an apiserver fault) bumps the stage-error sink and the
	// Put-gate declines to overwrite the good entry.

	res, err := restactions.Resolve(ctx, restactions.ResolveOptions{
		In: &cr,
		// Ship 0.30.230 fix-at-root: SArc threaded from ctx. The
		// refresher's resolveAndPopulateL1 attaches the SA rc via
		// cache.WithInternalRESTConfig (resolve_populate.go:191) before
		// invoking the resolveOnceFn that lands here.
		SArc:    rcFromCtx(ctx),
		AuthnNS: authnNS,
		PerPage: inputs.PerPage,
		Page:    inputs.Page,
		Extras:  inputs.Extras,
	})
	if err != nil {
		return nil, fmt.Errorf("resolve RESTAction: %w", err)
	}
	return encodeResolvedJSON(res)
}

// resolveRAFullListForRefresh re-resolves a RAFullList cell — Ship 4a
// (0.30.198). The cell holds the RA's resolved STATUS MAP (the full sorted
// result) resolved UNPAGINATED. It converts the re-fetched CR to a typed
// RESTAction, resolves it at PerPage=0/Page=0 (no `.slice` → the RA's output
// jq returns the full sorted set), and returns json.Marshal of the Status
// map — byte-identical to the apiref serve path's PutRAFullList (which
// marshals the same map via cache.PutRAFullList). Returns (nil, nil) when the
// resolve produced no Status (skip-to-TTL).
func resolveRAFullListForRefresh(ctx context.Context, got objects.Result, inputs cache.ResolvedKeyInputs, authnNS string) ([]byte, error) {
	scheme := runtime.NewScheme()
	if err := apis.AddToScheme(scheme); err != nil {
		return nil, fmt.Errorf("add apis to scheme: %w", err)
	}
	var cr templatesv1.RESTAction
	if err := runtime.DefaultUnstructuredConverter.FromUnstructured(got.Unstructured.Object, &cr); err != nil {
		return nil, fmt.Errorf("unstructured -> RESTAction (raFullList): %w", err)
	}
	// UNPAGINATED — Inputs already carry PerPage=0/Page=0 (RAFullListKeyInputs),
	// but pass 0/0 explicitly so the contract is local + obvious.
	res, err := restactions.Resolve(ctx, restactions.ResolveOptions{
		In: &cr,
		// Ship 0.30.230 fix-at-root: SArc from ctx — refresher attaches
		// SA rc upstream (resolve_populate.go:191).
		SArc:    rcFromCtx(ctx),
		AuthnNS: authnNS,
		PerPage: 0,
		Page:    0,
		Extras:  inputs.Extras,
	})
	if err != nil {
		return nil, fmt.Errorf("resolve RAFullList: %w", err)
	}
	if res.Status == nil || res.Status.Raw == nil {
		// No status produced — skip-to-TTL (not an error).
		return nil, nil
	}
	// The cell stores the Status MAP (decoded then re-marshaled canonically),
	// matching cache.PutRAFullList(json.Marshal(full)). Round-trip through a
	// map so the bytes are canonical (sorted keys) and byte-identical to the
	// serve-path Put regardless of the jq emitter's key order.
	var full map[string]any
	if err := json.Unmarshal(res.Status.Raw, &full); err != nil {
		return nil, fmt.Errorf("RAFullList status not a JSON object: %w", err)
	}
	// EMPTY-FULL GUARD twin (0.30.208) — symmetric to the apiref serve-path
	// guard. If a refresh re-resolves to an EMPTY full (single array key,
	// length 0), it is INDISTINGUISHABLE from a not-yet-synced /
	// continueOnError-degraded resolve, so refuse to overwrite the existing
	// (possibly good, non-empty) cell with empty. Treat it as skip-to-TTL
	// (return nil,nil — the same no-status sentinel) so the cell is left
	// untouched and re-resolved on the next cycle once the informer is
	// synced. Mechanism-uniform: keyed off "the full is empty", NO
	// resource/name/GVR literal.
	if cache.FullListIsEmpty(full) {
		return nil, nil
	}
	return json.Marshal(full)
}

// resolveWidgetForRefresh dispatches widgets.Resolve on the re-fetched
// CR, returning encoded output. Mirrors the widgetsHandler.ServeHTTP
// resolve+encode path.
func resolveWidgetForRefresh(ctx context.Context, got objects.Result, inputs cache.ResolvedKeyInputs, authnNS string) ([]byte, error) {
	// #547 — through widgetsResolveFn, the SAME seam the customer handler
	// resolves through (dispatch_seams.go; production default widgets.Resolve, so
	// the compiled path is byte-identical). One resolve seam for both callers is
	// what lets a falsifier inject a failure that only the BACKGROUND identity
	// sees (cache.BackgroundResolveFromContext) and drive the real refresher.
	res, err := widgetsResolveFn(ctx, widgets.ResolveOptions{
		In: got.Unstructured,
		// Ship 0.30.230 fix-at-root: RC threaded from ctx. The
		// refresher's resolveAndPopulateL1 (resolve_populate.go:191)
		// attaches the SA rc via cache.WithInternalRESTConfig before
		// invoking the resolveOnceFn that lands here. Without RC set,
		// downstream crdschema.ValidateObjectStatus → cache.GVRFor →
		// discoverPluralInfo 500s with "plurals discovery: nil
		// *rest.Config" (the four-revert root cause).
		RC:      rcFromCtx(ctx),
		AuthnNS: authnNS,
		PerPage: inputs.PerPage,
		Page:    inputs.Page,
		Extras:  inputs.Extras,
	})
	if err != nil {
		return nil, fmt.Errorf("resolve widget: %w", err)
	}
	return encodeResolvedJSON(res)
}
