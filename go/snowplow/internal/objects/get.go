// Package objects is the leaf entry point for fetching a single Kubernetes
// object referenced by a resolver. objects.Get serves the object from the
// in-process informer cache when possible (with RBAC narrowing and cache
// dependency recording) and falls back to a direct apiserver GET otherwise.
// It also hosts the shared /call query-param to ObjectReference decoder used
// by the in-process nested-call loopback.
package objects

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"

	xcontext "github.com/krateo-platformops/plumbing/context"
	"github.com/krateo-platformops/plumbing/http/response"
	templatesv1 "github.com/krateo-platformops/snowplow/apis/templates/v1"
	"github.com/krateo-platformops/snowplow/internal/apipath"
	"github.com/krateo-platformops/snowplow/internal/cache"
	"github.com/krateo-platformops/snowplow/internal/credhttp"
	"github.com/krateo-platformops/snowplow/internal/dynamic"
	"github.com/krateo-platformops/snowplow/internal/rbac"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

const (
	lastAppliedConfigAnnotation = "kubectl.kubernetes.io/last-applied-configuration"
)

type Result struct {
	GVR          schema.GroupVersionResource
	Unstructured *unstructured.Unstructured
	Err          *response.Status
	// RequesterDialled: the read left snowplow under the requester's own
	// credential (getFromAPIServer). It also marked the ExternalTouchedSink on
	// ctx; a caller that installs its sinks only after the read (the dispatcher's
	// fetch of the dispatched CR) marks them from this.
	RequesterDialled bool
}

func Get(ctx context.Context, ref templatesv1.ObjectReference) (res Result) {
	// #1257 / #1414 — THE REFERENCE IS AN APISERVER PATH OR IT IS REFUSED.
	// A reference reaches this read from a request's query, a nested call's
	// path, a cache entry's inputs or CR content (an apiRef, a walked child),
	// and under the requester's credential or snowplow's own. Whatever its
	// source and credential, its group, version and resource each become one
	// segment of the apiserver path (apipath.CheckGVR, the one rule), or the
	// read is refused here with a 400: before the informer, before any
	// discovery, before any dial.
	if st := checkReference(ref); st != nil {
		xcontext.Logger(ctx).ErrorContext(ctx, "reference is not an apiserver path", slog.String("err", st.Message))
		res.Err = st
		return res
	}

	// R5 (0.30.110): resolver-side dep recording. When this Get runs
	// under a cache.WithL1KeyContext ctx, the fetched object becomes a
	// dependency of the L1 entry being populated — record the edge so a
	// later DELETE / ADD / UPDATE of the object invalidates that entry.
	//
	// Gated hard on !cache.Disabled(): with the cache subsystem off
	// there is no DepTracker store to invalidate and no L1 entry to key
	// against; recording would be dead weight. The deferred call reads
	// the FINAL res (after whichever path produced the object) so it
	// covers both the informer-served and apiserver-served branches.
	if !cache.Disabled() {
		defer recordGetDep(ctx, &res)
	}

	log := xcontext.Logger(ctx)

	// Cache routing gate. When the cache subsystem is disabled
	// (CACHE_ENABLED unset/false) there is no informer to serve from;
	// every read goes straight to the apiserver. We do NOT increment the
	// fallthrough counter here — the counters measure the 0.30.96 routed
	// pivot's serve rate, and cache-disabled is "pivot inactive", not a
	// pivot fallthrough.
	if cache.Disabled() {
		// #101: an informer-only ctx (cache.WithInformerOnlyReads — the
		// /refreshes arming path) must NOT reach the apiserver even when the
		// cache subsystem is off: there is no informer to serve from, so the
		// correct informer-only answer is a NotFound-shaped miss, never the
		// endpoint-less-ctx apiserver ERROR. This is the reachable cache-off
		// site (the flag-off branch below is structurally dead — useInformer()
		// folds to !cache.Disabled(), #57).
		if cache.InformerOnlyReadsFromContext(ctx) {
			return informerOnlyMiss(ctx, ref)
		}
		return getFromAPIServer(ctx, ref)
	}

	// 0.30.96 Gap A — routed branch. Serve widget / entry-CR object GETs
	// from the in-process informer cache. #57: implicit-on-cache —
	// useInformer() now folds to !cache.Disabled() (the standalone
	// RESOLVER_USE_INFORMER flag was retired). With cache off the
	// cache.Disabled() short-circuit above already returned, so this
	// block runs only on the cache-on path; the binary is byte-identical
	// to the apiserver path under cache-off (R-FALSE-1 invariant
	// preserved via the single CACHE_ENABLED master gate).
	//
	// Per feedback_no_special_cases.md: the branch is uniform across
	// every GVR — the gate is cache-mode + informer-state predicates,
	// never a per-resource switch.
	if useInformer() {
		// Lazy-start the objects_get.summary goroutine the first time
		// the pivot is exercised (sync.Once-bounded; never started when
		// the flag stays off for the process lifetime).
		startObjectsGetSummary()

		gv, err := schema.ParseGroupVersion(ref.APIVersion)
		if err == nil {
			gvr := gv.WithResource(ref.Resource)
			rw := cache.Global()
			// Passthrough mode has no informers (cache=off diagnostic
			// mode); metadata-only GVRs carry only ObjectMeta — neither
			// can satisfy a full-object resolver read.
			if rw != nil && !rw.IsPassthrough() && !rw.IsMetadataOnly(gvr) {
				// 0.30.97: gate the served path on IsServable
				// (registered AND HasSynced) — the uniform servability
				// predicate also used by the resolver pivot. A
				// not-yet-fully-synced widget GVR must never serve a
				// stale/partial object: its indexer partition can still
				// be draining even after HasSynced has flipped at the
				// start of the processor drain, and a pre-sync miss is
				// indistinguishable from a real NotFound. Anything
				// non-servable falls through to the apiserver.
				if !rw.IsServable(gvr) {
					// Not registered / not yet synced. Fire best-effort
					// lazy registration so a SUBSEQUENT call can serve;
					// EnsureResourceType is idempotent + singleflighted
					// under rw.mu. This call still falls through to the
					// apiserver — pre-sync reads would look identical to
					// a real NotFound.
					_, _ = rw.EnsureResourceTypeFor(ctx, gvr)
					log.Debug("objects.Get: informer not servable; apiserver fallthrough",
						slog.String("gvr", gvr.String()),
						slog.String("ns", ref.Namespace),
						slog.String("name", ref.Name))
				} else if obj, hit := rw.GetObject(ctx, gvr, ref.Namespace, ref.Name); hit && filterGetByRBAC(ctx, gvr, obj) {
					// Cache hit AND the context's user is RBAC-authorized
					// to `get` this object.
					//
					// Tag 0.30.101: filterGetByRBAC is the GET-verb RBAC
					// check — the GET-path sibling of Tag A (0.30.100,
					// the resolver pivot's LIST filter). The informer
					// branch bypasses the per-user `<username>-
					// clientconfig` bearer token that getFromAPIServer
					// reads, so without this check a narrow-RBAC user
					// GETting a known object name in a namespace they
					// have no `get` grant for would receive it.
					// FAIL-CLOSED: a denied GET, a missing identity, or
					// an evaluator error all return false → this branch
					// is skipped → the call falls through to
					// getFromAPIServer, which issues the GET under the
					// user's own token (the apiserver's authoritative
					// 403). See filterGetByRBAC (informer_serve.go).
					//
					// DeepCopy so the strip never mutates the shared
					// informer-store object, then apply the EXACT same
					// field strips getFromAPIServer performs (see
					// stripForServe — byte-equivalence is mandatory per
					// feedback_cache_must_not_constrain_jq.md).
					out := obj.DeepCopy()
					stripForServe(out)
					objectsGetInformerServed.Add(1)
					log.Debug("objects.Get: served from informer",
						slog.String("gvr", gvr.String()),
						slog.String("ns", ref.Namespace),
						slog.String("name", ref.Name))
					res.GVR = gvr
					res.Unstructured = out
					return res
				}
				// Fall through to the apiserver for either:
				//   - GET-miss (informer synced, object absent) — the
				//     caller sees the apiserver NotFound envelope shape;
				//   - GET-hit but RBAC-denied / no-identity / evaluator
				//     error (Tag 0.30.101 filterGetByRBAC) — the
				//     apiserver issues the GET under the user's own
				//     token and returns the authoritative 403.
			}
		}
		// #101: informer-only reads. On an endpoint-less internal route (the
		// /refreshes arming path, cache.WithInformerOnlyReads) the apiserver
		// fallthrough is unreachable-by-design — getFromAPIServer dies at the
		// UserConfig read with a noisy 401-shaped ERROR and the coord is
		// fail-closed-skipped anyway. Return the NotFound-shaped Err the caller
		// already handles, quietly, WITHOUT attempting the doomed apiserver GET.
		// Placed AFTER the informer-serve attempt so a genuine informer HIT still
		// serves; only the fallthrough is short-circuited (serve set unchanged).
		if cache.InformerOnlyReadsFromContext(ctx) {
			return informerOnlyMiss(ctx, ref)
		}
		// Any fall-through inside the routed branch — parse failure,
		// nil/passthrough/metadata-only watcher, not-synced, GET-miss,
		// RBAC-denied GET — is an apiserver-served call under the active
		// pivot.
		objectsGetApiserverFallthrough.Add(1)
		return getFromAPIServer(ctx, ref)
	}

	// Flag off: pivot inactive. Take the apiserver branch unchanged from
	// pre-0.30.96. No counter increment — see the cache.Disabled() note.
	// #101: the informer-only guard for the cache-off case lives at the
	// cache.Disabled() early-return above (the reachable site); this branch is
	// structurally dead (useInformer() == !cache.Disabled(), #57), so no guard
	// is needed here.
	return getFromAPIServer(ctx, ref)
}

// checkReference is nil for a reference whose group, version and resource are
// each one apiserver path segment, else the 400 that refuses it. A group/version
// that does not parse is left to the read, which answers its own 400 for it.
func checkReference(ref templatesv1.ObjectReference) *response.Status {
	gv, err := schema.ParseGroupVersion(ref.APIVersion)
	if err != nil {
		return nil
	}
	if err := apipath.CheckGVR(gv.WithResource(ref.Resource), apipath.Reference); err != nil {
		return response.New(http.StatusBadRequest, err)
	}
	return nil
}

// informerOnlyMiss returns the NotFound-shaped Result an informer-only ctx
// (#101, cache.WithInformerOnlyReads) yields on a routed-branch fallthrough,
// WITHOUT touching the apiserver. It fills res.GVR (best-effort parse) so the
// caller's telemetry keeps the coordinate, and a NotFound-coded res.Err so the
// existing fail-closed skip (subscriptionKeyExtras: got.Err != nil → skip)
// applies unchanged. Logged at Debug only — this is the expected quiet path on
// an endpoint-less route, NOT the "unable to get user endpoint" ERROR it
// replaces.
func informerOnlyMiss(ctx context.Context, ref templatesv1.ObjectReference) (res Result) {
	if gv, err := schema.ParseGroupVersion(ref.APIVersion); err == nil {
		res.GVR = gv.WithResource(ref.Resource)
	}
	xcontext.Logger(ctx).Debug("objects.Get: informer-only ctx, apiserver fallthrough suppressed",
		slog.String("gvr", res.GVR.String()),
		slog.String("ns", ref.Namespace),
		slog.String("name", ref.Name))
	res.Err = response.New(http.StatusNotFound,
		apierrors.NewNotFound(res.GVR.GroupResource(), ref.Name))
	return res
}

func getFromAPIServer(ctx context.Context, ref templatesv1.ObjectReference) (res Result) {
	log := xcontext.Logger(ctx)

	gv, err := schema.ParseGroupVersion(ref.APIVersion)
	if err != nil {
		log.ErrorContext(ctx, "unable to parse group version", slog.Any("reference", ref), slog.Any("err", err))
		res.Err = response.New(http.StatusBadRequest, err)
		return
	}
	res.GVR = gv.WithResource(ref.Resource)
	ep, err := xcontext.UserConfig(ctx)
	if err != nil {
		log.ErrorContext(ctx, "unable to get user endpoint", slog.Any("err", err))
		res.Err = response.New(http.StatusUnauthorized, err)
		return
	}

	// 0.30.103: ClientConfigFor returns the context-injected
	// internal-dispatch *rest.Config when an internal/startup driver
	// (Phase 1's SA-credentialed walk) is in flight, else delegates to
	// the unchanged kubeconfig.NewClientConfig per-user path. This is
	// what makes Phase 1's SA fetch work — the SA's raw-PEM CA cannot
	// survive kubeconfig.NewClientConfig, which base64-decodes it (see
	// cache.WithInternalRESTConfig). A token-only per-user endpoint (the
	// sandbox visitor's) does: plumbing v1.68.0 carries its bearer token
	// (token_only_endpoint_dial_test.go).
	rc, err := cache.ClientConfigFor(ctx, ep)
	if err != nil {
		log.ErrorContext(ctx, "unable to create kubernetes client config", slog.Any("err", err))
		res.Err = response.New(http.StatusInternalServerError, err)
		return
	}

	// Ship D (0.30.141) — F-6: objects.getFromAPIServer is the routed
	// pivot's apiserver fall-through arm. Record BEFORE the upstream
	// client build so a panicking construction still increments the
	// counter (AC-D.3 ordering).
	cache.RecordApiserverFallthrough(ctx, cache.ReasonClientBuild, res.GVR.String())
	// #1115 — the client is this request's: its RESTMapper discovery runs on
	// client-go's own context.TODO(), so the budget is bound to the client.
	cli, err := dynamic.NewClient(credhttp.BindRequestBudget(ctx, rc))
	if err != nil {
		log.ErrorContext(ctx, "unable to create kubernetes dynamic client", slog.Any("err", err))
		res.Err = response.New(http.StatusInternalServerError, err)
		return
	}

	// A read under the requester's own credential returns what the APISERVER
	// allows THIS requester, which the in-process snapshot may not know: on a
	// partial snapshot two identities can share a key while the apiserver
	// allows one and refuses the other. So it marks the resolve as dialled by
	// the requester, exactly as a stage's dial does (resolve.go), and every Put
	// gate declines it. Marked BEFORE the dial, so a failed read still counts.
	// Never a third-party touch: the credential is the requester's.
	//
	// An internal driver's read is not marked here. A plain ServiceAccount read
	// is re-gated below against the same snapshot that made the key, which only
	// lacks bindings, so it cannot carry one identity's view to another. An
	// IMPERSONATED read (the refresher's, #574) is not re-gated (MustRegateSADial
	// skips it) and is not marked either: it can store the representative's view
	// in a shared cell on a partial snapshot. That route is #1233, fixed apart.
	if cache.RequesterCredentialFor(ctx) {
		cache.ExternalTouchedSinkFromContext(ctx).Bump()
		res.RequesterDialled = true
	}
	// #1115 — the request's own ctx, so a customer read carries its page
	// budget to the round trip (credhttp.BudgetTransport); background work has
	// none and is unchanged by it.
	uns, err := cli.Get(ctx, ref.Name, dynamic.Options{
		Namespace: ref.Namespace,
		GVR:       res.GVR,
	})
	if err != nil {
		log.ErrorContext(ctx, "unable to get resource",
			slog.String("name", ref.Name), slog.String("namespace", ref.Namespace),
			slog.String("gvr", res.GVR.String()), slog.Any("err", err))

		res.Err = response.New(http.StatusInternalServerError, err)
		if st, ok := credhttp.BudgetStatus(err); ok {
			res.Err = st // #1115 — 504 Timeout
		} else if apierrors.IsForbidden(err) {
			res.Err = response.New(http.StatusForbidden, err)
		} else if dynamic.IsNotServed(err) {
			res.Err = typeNotInstalled(res.GVR) // #1414 — 424, before the object's 404
		} else if apierrors.IsNotFound(err) {
			res.Err = response.New(http.StatusNotFound, err)
		}

		return
	}

	// Part 2 (#269 site 3): re-gate the SA-credentialed fetch. cli.Get above ran
	// under the ctx *rest.Config, which for the BACKGROUND refresher is the SA
	// config (cache.ClientConfigFor) even though the ctx identity is a
	// per-user-cohort REPRESENTATIVE (non-SA). Without this gate the refresher's
	// re-resolve serves a denied object under the SA and re-populates a per-user L1
	// cell cross-user. mustRegateSADial is FALSE for a live request (no SA cred on
	// ctx post-Part-1 → dials as the user, authoritative 403) and for a genuine
	// SA / identity-free operation (serveUnnarrowed) — so both are unchanged; it is
	// TRUE only for an SA-credentialed dispatch under a real narrowing identity,
	// where we apply the SAME per-user gate the informer branch uses (filterGetByRBAC)
	// and FAIL CLOSED on deny. No per-resource carve-outs (feedback_no_special_cases).
	if rbac.MustRegateSADial(ctx) && !filterGetByRBAC(ctx, res.GVR, uns) {
		// #574 — attribute the refusal to its driver and cell (see
		// rbac/sa_regate_attribution.go).
		log.WarnContext(ctx, "objects.getFromAPIServer: SA-credentialed fetch re-gated and DENIED for the ctx identity; failing closed",
			slog.String("subsystem", "cache"),
			slog.String("gvr", res.GVR.String()),
			slog.String("namespace", ref.Namespace),
			slog.String("name", ref.Name),
			slog.String("sa_regate_origin", rbac.SARegateOrigin(ctx)),
			slog.String("sa_regate_trigger", rbac.SARegateTrigger(ctx)),
			slog.String("sa_regate_scope", rbac.SARegateScope(ctx)),
			slog.String("key_hash", rbac.SARegateKeyHash(ctx)),
		)
		res.Unstructured = nil
		res.Err = response.New(http.StatusForbidden,
			apierrors.NewForbidden(res.GVR.GroupResource(), ref.Name,
				fmt.Errorf("user not authorized to get %s/%s", ref.Namespace, ref.Name)))
		return
	}

	annotations := uns.GetAnnotations()
	if annotations != nil {
		delete(annotations, lastAppliedConfigAnnotation)
		uns.SetAnnotations(annotations)
	}
	uns.SetManagedFields(nil)

	res.Unstructured = uns
	res.Err = nil
	return
}

// ReasonFailedDependency is the Status reason of a 424 answer. Neither plumbing
// nor apimachinery names one for 424.
const ReasonFailedDependency response.StatusReason = "FailedDependency"

// typeNotInstalled is the answer to a read whose type the apiserver does not
// serve (#1414): 424 Failed Dependency, not 404. A 404 says an OBJECT is not
// there: a reader retries it as a cold start and takes it, on a refetch, as a
// confirmed delete. Here nothing was deleted and no retry can help: something
// the read needs is not installed on this cluster.
//
// The message is shown to users as it is, so it is built from the type's
// coordinates alone: no object name or namespace, no identity, none of the
// apiserver's text. One rule for every type.
func typeNotInstalled(gvr schema.GroupVersionResource) *response.Status {
	st := response.New(http.StatusFailedDependency, nil)
	st.Message = fmt.Sprintf("The type %s (%s) is not installed on this cluster.", gvr.Resource, gvr.GroupVersion())
	st.Status = response.StatusFailure
	st.Reason = ReasonFailedDependency
	return st
}

// recordGetDep registers an exact-object dependency edge for a
// successful objects.Get, keyed by the L1 key carried in ctx.
//
// R5 (0.30.110): invoked deferred from Get, only when the cache
// subsystem is enabled (the caller gates on !cache.Disabled()). It is a
// no-op when:
//
//   - ctx carries no L1 key (cache.L1KeyFromContext == "") — the Get is
//     not populating a cacheable L1 entry; recording would be a stray
//     edge. This is the AC-R5 negative case.
//   - the Get did not produce an object (res.Err set or res.Unstructured
//     nil) — there is nothing to depend on.
//
// The recorded tuple is (res.GVR, namespace, name): a DELETE of that
// object self-evicts the L1 entry; an ADD/UPDATE dirty-marks it.
func recordGetDep(ctx context.Context, res *Result) {
	l1Key := cache.L1KeyFromContext(ctx)
	if l1Key == "" {
		return
	}
	if res == nil || res.Err != nil || res.Unstructured == nil {
		return
	}
	cache.Deps().Record(ctx, l1Key, res.GVR, res.Unstructured.GetNamespace(), res.Unstructured.GetName())
}
