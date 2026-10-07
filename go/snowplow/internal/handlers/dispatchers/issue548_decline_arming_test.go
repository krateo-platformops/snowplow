// issue548_decline_arming_test.go — #548 falsifiers: a response that was SERVED
// but deliberately NOT cached must not tell the browser to subscribe.
//
// FILENAME NOTE (do not "tidy" this back): this file was first written as
// issue548_decline_no_arm_test.go and Go silently EXCLUDED it — a file whose
// name ends in _arm carries the implicit `arm` GOARCH build constraint, so on
// every non-arm builder it landed in IgnoredGoFiles and `go test -run
// TestIssue548` reported "no tests to run" while `go vet` stayed green. Nothing
// about these arms may end in a GOOS/GOARCH token.
//
// THE DEFECT (origin/main): the cold tails of both dispatchers stamped
// X-Snowplow-Refresh-Key unconditionally — widgets.go's
// setRefreshKeyHeaderUnlessExternal(..., servedExternalTTL) and restactions.go's
// bare setRefreshKeyHeader(...) — while the only two publishers in the system
// sit strictly inside an ACCEPTED write: publishIfSubscribed on the accepted
// PutIfGen (refresh_publish.go) and the refresher's cache.PublishRefresh, which
// is reached only after ReplaceIfGenRefresh stored and therefore can only ever
// replace a cell that EXISTS. So every Put-DECLINE path armed a subscription to
// a key with no cell and no possible publisher; refreshSse.ts echoes the header
// verbatim, so the viewer subscribes in good faith and waits forever. Per-widget
// live refresh is default-ON and there is no second refresh path, so the page
// is frozen at load.
//
// WHY THE ARM IS SHAPED LIKE THIS — PER BRANCH, NOT PER FIX. The Put-gate chain
// in widgets.go has NINE branches and restactions.go has SEVEN. An arm that
// covered one of them would pass while the rest regressed; #533, #540 and #546
// all shipped exactly that shape. So every branch is driven INDIVIDUALLY through
// the REAL handler (the H1 seams fake only the object fetch, the coarse RBAC
// verdict and the resolve output — the key derivation, the cache handle, the
// whole Put-gate control flow and the header stamp stay production), each with a
// BRANCH-SPECIFIC WITNESS proving the branch under test is the one that fired
// and not a neighbour.
//
// AND THE INVERSE MATTERS MORE. A fix that stopped arming everywhere would
// silently disable live refresh for the entire product, and every pre-existing
// test in this package would still pass. TestIssue548_AcceptedPut_StillArms and
// TestIssue548_L1Hit_StillArms go RED if that happens.
//
// ONE BRANCH IS DELIBERATELY VACUOUS, and it is labelled so rather than counted
// as coverage: `inert` (#443 g) was ALREADY safe before #548 for an unrelated
// reason — dispatchCacheLookupKey returns an empty key for an inert hop, and
// setRefreshKeyHeader no-ops on an empty key. Its arm asserts the end state and
// pins that it stays empty-keyed; it does NOT discriminate the #548 fix.

package dispatchers

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	xcontext "github.com/krateo-platformops/plumbing/context"
	"github.com/krateo-platformops/plumbing/jwtutil"
	templatesv1 "github.com/krateo-platformops/snowplow/apis/templates/v1"
	"github.com/krateo-platformops/snowplow/internal/cache"
	"github.com/krateo-platformops/snowplow/internal/objects"
	"github.com/krateo-platformops/snowplow/internal/resolvers/restactions"
	"github.com/krateo-platformops/snowplow/internal/resolvers/widgets"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/client-go/rest"
)

// a548Unbound is an identity the h1 watcher grants NOTHING, so
// rbac.EvaluateRBAC fail-closes and dispatchCacheLookupKey re-derives a ""
// first-match BindingUID — the #95 fall-through shape.
const a548Unbound = "a548-nobody"

// ---------------------------------------------------------------------------
// Shared assertions
// ---------------------------------------------------------------------------

// a548AssertNoArm is the #548 invariant: the response was served normally AND
// carries NEITHER refresh header, so the browser never arms.
//
// It checks the CLASS header too, not only the key: the two are one contract
// (setRefreshKeyHeader stamps the class only alongside a non-empty key), and a
// class-only stamp would leave the frontend arming decision ambiguous. It also
// asserts the 200 + a non-empty body, because "nothing is armed" must be the
// result of a declined CACHE WRITE and not of a broken serve.
func a548AssertNoArm(t *testing.T, branch string, rec *httptest.ResponseRecorder) {
	t.Helper()
	if rec.Code != 200 {
		t.Fatalf("#548 [%s]: the decline must affect only the cache write — the response must still be 200; got %d body=%s",
			branch, rec.Code, rec.Body.String())
	}
	if len(bytes.TrimSpace(rec.Body.Bytes())) == 0 {
		t.Fatalf("#548 [%s]: the response body must still carry the resolved result", branch)
	}
	if got := rec.Header().Get(refreshKeyHeader); got != "" {
		t.Fatalf("#548 RED [%s]: a response that was NOT cached stamped %s=%q. The browser echoes that header and "+
			"subscribes to it (refreshSse.ts), but nothing stored a cell under that key and neither publisher can "+
			"ever reach it (publishIfSubscribed runs only inside the accepted PutIfGen; the refresher's "+
			"PublishRefresh only after ReplaceIfGenRefresh stored). The viewer waits forever on a dead key, and "+
			"with live refresh default-ON there is no second refresh path to recover.",
			branch, refreshKeyHeader, got)
	}
	if got := rec.Header().Get(refreshClassHeader); got != "" {
		t.Fatalf("#548 RED [%s]: stamped %s=%q with no key — the key and class headers are one contract and must "+
			"appear together or not at all", branch, refreshClassHeader, got)
	}
}

// a548AssertArmed is the INVERSE invariant: a response backed by a stored cell
// MUST still arm, under the exact key and class the subscription decoder
// accepts. This is what goes RED if a #548 fix turns arming off wholesale.
func a548AssertArmed(t *testing.T, branch string, rec *httptest.ResponseRecorder, key, class string) {
	t.Helper()
	if rec.Code != 200 {
		t.Fatalf("#548 inverse [%s]: expected 200; got %d body=%s", branch, rec.Code, rec.Body.String())
	}
	if got := rec.Header().Get(refreshKeyHeader); got != key {
		t.Fatalf("#548 INVERSE RED [%s]: a response backed by a STORED cell must stamp %s=%q; got %q. If this is "+
			"empty, live refresh is off for the whole product: the browser never arms, the refresher's "+
			"PublishRefresh fans out to nobody, and every widget on every page is frozen until a reload.",
			branch, refreshKeyHeader, key, got)
	}
	if got := rec.Header().Get(refreshClassHeader); got != class {
		t.Fatalf("#548 INVERSE RED [%s]: must stamp %s=%q (the class DeriveSubscriptionKey accepts); got %q",
			branch, refreshClassHeader, class, got)
	}
}

// a548WidgetKey derives the REAL per-cohort widgets key for a request shape,
// through the production derivation (including effectiveKeyExtras).
func a548WidgetKey(ctx context.Context, cr *unstructured.Unstructured, extras map[string]any) (string, cacheHandle, *cache.ResolvedKeyInputs) {
	return dispatchCacheLookupKey(ctx, "widgets",
		h1WidgetGVR.Group, h1WidgetGVR.Version, h1WidgetGVR.Resource, h1NS, h1WName,
		-1, -1, effectiveKeyExtras(ctx, cr.Object, extras))
}

// a548WidgetCR is h1WidgetUnstructured plus optional metadata.annotations (the
// external-TTL opt-in lives there).
func a548WidgetCR(t *testing.T, annotations map[string]string) *unstructured.Unstructured {
	t.Helper()
	cr := h1WidgetUnstructured(map[string]any{})
	if len(annotations) == 0 {
		return cr
	}
	ann := map[string]any{}
	for k, v := range annotations {
		ann[k] = v
	}
	if err := unstructured.SetNestedMap(cr.Object, ann, "metadata", "annotations"); err != nil {
		t.Fatalf("setup: set annotations: %v", err)
	}
	return cr
}

// ---------------------------------------------------------------------------
// WIDGETS — every branch of the widgets.go Put-gate chain
// ---------------------------------------------------------------------------

// a548WidgetBranch is one branch of the widgets.go chain and how to reach it.
type a548WidgetBranch struct {
	name string
	// why names the branch in widgets.go whose decline this case drives.
	why string
	// user is the identity on the request ctx (a548Unbound for the #95 shape).
	user string
	// annotations go on the widget CR (external-TTL opt-in).
	annotations map[string]string
	// query is appended to /call (the undeclared-extras shape).
	query string
	// extras is the SAME map the query encodes, so the arm derives the key the
	// handler will derive.
	extras map[string]any
	// headers are set on the request (the inert shape).
	headers map[string]string
	// during runs inside the resolve seam, i.e. after the key mint + generation
	// capture and before the Put gate — the only place a sink bump or a racing
	// eviction is observable by the handler.
	during func(t *testing.T, ctx context.Context, key string)
	// witness returns a branch-specific counter reading; the arm requires it to
	// MOVE, which is what proves this case reached the branch it claims and not
	// a neighbouring one. nil when the branch has no counter (its witness is
	// then the cell-absence assert, which every case makes).
	witness func() uint64
	// emptyKey marks the inert branch: it mints NO key, so it cannot arm for a
	// reason unrelated to #548. Labelled, never counted as #548 coverage.
	emptyKey bool
	// cellStored marks the external-TTL branch: it ACCEPTS a Put (a cell exists)
	// and must STILL not arm, because it deliberately records no dep edge and
	// never publishes (design §6.3).
	cellStored bool
}

func TestIssue548_Widgets_NoDeclineBranchArms(t *testing.T) {
	branches := []a548WidgetBranch{
		{
			name:     "inert",
			why:      "cache.Inert(ctx) — #443 (g) dry-run hop; nothing persisted",
			user:     h1User,
			headers:  map[string]string{cache.InertHeader: "1"},
			emptyKey: true,
		},
		{
			name: "sensitive_secret_read",
			why:  "cache.DeclineSensitivePut(ctx) — #398 the resolve read core v1/secrets",
			user: h1User,
			during: func(t *testing.T, ctx context.Context, _ string) {
				cache.SensitiveTouchedSinkFromContext(ctx).Bump()
			},
			witness: cache.SensitiveSkippedPutForTest,
		},
		{
			name: "uaf_refilter",
			why:  "declineWidgetUAFPut — 1.12.3 A-1/R-1, the hot carrier (66 widgets, 298,064 hits/5d7h)",
			user: h1User,
			during: func(t *testing.T, ctx context.Context, _ string) {
				cache.UAFTouchedSinkFromContext(ctx).Bump()
			},
			witness: cache.WidgetsUAFPutDeclined,
		},
		{
			name: "stage_error",
			why:  "stageErrSink.Count() > 0 — #313 Cache-A partial-with-errors envelope",
			user: h1User,
			during: func(t *testing.T, ctx context.Context, _ string) {
				cache.StageErrorSinkFromContext(ctx).Bump("apistage", "per-item boom")
			},
		},
		{
			name:        "external_ttl_put",
			why:         "the external-TTL opt-in Put — ACCEPTED write that never publishes (design §6.3)",
			user:        h1User,
			annotations: map[string]string{externalCacheTTLAnnotation: "30"},
			during: func(t *testing.T, ctx context.Context, _ string) {
				cache.ExternalTouchedSinkFromContext(ctx).Bump()
			},
			cellStored: true,
		},
		{
			name: "external_touch_no_optin",
			why:  "extTouchedSink.Count() > 0 — external data has no dep edge to invalidate",
			user: h1User,
			during: func(t *testing.T, ctx context.Context, _ string) {
				cache.ExternalTouchedSinkFromContext(ctx).Bump()
			},
			witness: cache.ExternalSkippedPut,
		},
		{
			name:    "undeclared_request_extras",
			why:     "!requestExtrasFullyDeclared — F6 self-quarantine of a cohort-cell polluter",
			user:    h1User,
			query:   `extras={"foo":"bar"}`,
			extras:  map[string]any{"foo": "bar"},
			witness: func() uint64 { return widgetSkippedUndeclaredExtrasPutTotal.Load() },
		},
		{
			name: "empty_binding_uid",
			why:  "the no-branch fall-through: serveFromCacheEligible false (#95 \"\"-BindingUID), key still non-empty",
			user: a548Unbound,
		},
		{
			name: "putifgen_refused",
			why:  "#189 PutIfGen refused — a DELETE-eviction bumped the generation during the resolve",
			user: h1User,
			during: func(t *testing.T, _ context.Context, key string) {
				c := cache.ResolvedCache()
				c.Put(key, &cache.ResolvedEntry{RawJSON: []byte(`{"racer":true}`)})
				c.DeleteForTest(key)
			},
		},
	}

	// Every branch of the production chain must appear above. The chain has nine
	// arms (eight declines + the external-TTL accepted Put that must not arm);
	// a tenth added later without an arm here is the #533/#540/#546 shape.
	if len(branches) != 9 {
		t.Fatalf("#548: the widgets Put-gate chain has 9 arms (widgets.go: inert, sensitive, identity-class drift, "+
			"UAF, stage error, external-TTL Put, external skip, undeclared extras, genuine Put — plus the "+
			"no-branch fall-through) and this table must drive all of them minus drift, which needs the "+
			"grant-mid-resolve watcher and lives in TestIssue548_Widgets_IdentityClassDrift_DoesNotArm; got %d cases",
			len(branches))
	}

	for _, b := range branches {
		t.Run(b.name, func(t *testing.T) {
			h1BuildWatcher(t)
			cache.RegisterUAFPutDeclineMetricsForTest()
			cache.ResetUAFPutDeclineCountersForTest()
			t.Cleanup(cache.ResetUAFPutDeclineCountersForTest)

			reqCtx := xcontext.BuildContext(context.Background(),
				xcontext.WithUserInfo(jwtutil.UserInfo{Username: b.user, Groups: []string{"devs"}}))
			cr := a548WidgetCR(t, b.annotations)

			// The key the HANDLER will derive, from the production derivation.
			// (For the inert case this runs on a non-inert ctx on purpose, to
			// show the branch is reached and that the key is non-trivial —
			// the handler's own ctx IS inert and mints "".)
			key, handle, inputs := a548WidgetKey(reqCtx, cr, b.extras)
			if handle == nil {
				t.Fatalf("PRECONDITION [%s]: expected a live cache handle; the arm would pass vacuously with L1 off", b.name)
			}
			if key == "" {
				t.Fatalf("PRECONDITION [%s]: expected a non-empty derived key", b.name)
			}
			if b.name == "empty_binding_uid" {
				// The whole point of this branch: a "" BindingUID does NOT make
				// the key empty, so pre-#548 it armed a key whose cell this
				// identity must never read.
				if inputs == nil || inputs.BindingUID != "" {
					t.Fatalf("PRECONDITION [%s]: expected a re-derived \"\" BindingUID for an unbound identity; got %+v",
						b.name, inputs)
				}
			} else if inputs == nil || inputs.BindingUID == "" {
				t.Fatalf("PRECONDITION [%s]: expected a non-empty BindingUID, else the Put is declined for the #95 "+
					"reason and this arm would not exercise its own branch; got %+v", b.name, inputs)
			}
			if _, ok := handle.Get(key); ok {
				t.Fatalf("PRECONDITION [%s]: the derived key must be cold before the arm runs", b.name)
			}

			var witnessBefore uint64
			if b.witness != nil {
				witnessBefore = b.witness()
			}
			refusedBefore := cache.ResolvedCache().Stats().PutRefusedGenerationMovedTotal

			restore := installWidgetFakes(t, cr, func() bool { return true },
				func(ctx context.Context, _ widgets.ResolveOptions) (*widgets.Widget, error) {
					if b.during != nil {
						b.during(t, ctx, key)
					}
					return h1WidgetUnstructured(map[string]any{}), nil
				})
			defer restore()

			target := "/call"
			if b.query != "" {
				target += "?" + b.query
			}
			req := httptest.NewRequest("GET", target, nil).WithContext(reqCtx)
			for k, v := range b.headers {
				req.Header.Set(k, v)
			}
			rec := httptest.NewRecorder()
			Widgets().ServeHTTP(rec, req)

			a548AssertNoArm(t, "widgets/"+b.name, rec)

			// The branch-specific witness: proves THIS case reached the branch
			// it names. Without it a case could be silently passing because an
			// earlier branch swallowed it.
			if b.witness != nil {
				if got := b.witness(); got == witnessBefore {
					t.Fatalf("#548 [widgets/%s]: the branch's own decline counter did not move (%d) — this case did "+
						"NOT reach %s, so its no-arm pass proves nothing about that branch",
						b.name, got, b.why)
				}
			}
			if b.name == "putifgen_refused" {
				if got := cache.ResolvedCache().Stats().PutRefusedGenerationMovedTotal; got != refusedBefore+1 {
					t.Fatalf("#548 [widgets/%s]: PutIfGen was not refused (refused_total %d->%d) — this case did not "+
						"reach %s", b.name, refusedBefore, got, b.why)
				}
			}

			// Cell presence: absent for every decline; PRESENT for the
			// external-TTL accepted Put, which must still not arm.
			entry, stored := handle.Get(key)
			switch {
			case b.cellStored:
				if !stored {
					t.Fatalf("#548 [widgets/%s]: expected the external-TTL Put to STORE a cell under %q — without the "+
						"Put this case is not driving %s", b.name, key, b.why)
				}
				if !entry.ExternalTTL {
					t.Fatalf("#548 [widgets/%s]: the stored cell must carry the ExternalTTL marker", b.name)
				}
			case b.emptyKey:
				// Inert: nothing stored and, by construction, no key was minted.
				if stored {
					t.Fatalf("#548 [widgets/%s]: an inert hop must persist nothing; found a cell at %q", b.name, key)
				}
			default:
				if stored {
					t.Fatalf("#548 [widgets/%s]: the branch stored a cell at %q (%q) — this case is NOT on a decline "+
						"path, so its no-arm assertion is testing the wrong thing. %s",
						b.name, key, entry.RawJSON, b.why)
				}
			}
		})
	}
}

// TestIssue548_Widgets_IdentityClassDrift_DoesNotArm is the ninth widgets
// branch — #424's mid-resolve RBAC-class move. It needs a watcher a grant can
// land in DURING the resolve (the h1 watcher is static), so it uses the
// ps/k424 harness that TestKey424_WidgetsPut_TOCTOU_GrantMidResolve established.
// The witness is the #424 drift decline counter for the "widgets" site.
func TestIssue548_Widgets_IdentityClassDrift_DoesNotArm(t *testing.T) {
	a := psArm{name: "configmaps/group", target: psConfigmapsGVR, watchShape: true}
	dyn := psBuildWatcher(t, a, k423WidgetExtras(a)...)
	carolCtx := psUserCtx(a, psCarol)
	cr := h1WidgetUnstructured(map[string]any{})

	key, handle, inputs := a548WidgetKey(carolCtx, cr, nil)
	if handle == nil || key == "" || inputs == nil || inputs.BindingUID == "" {
		t.Fatalf("PRECONDITION: expected a live non-empty-BindingUID widgets key; key=%q handle=%v inputs=%+v",
			key, handle != nil, inputs)
	}

	var granted atomic.Bool
	r1 := setFetchObjectForTest(func(*http.Request) objects.Result {
		return objects.Result{GVR: h1WidgetGVR, Unstructured: cr.DeepCopy()}
	})
	r2 := setWidgetsResolveForTest(func(ctx context.Context, _ widgets.ResolveOptions) (*widgets.Widget, error) {
		// The grant lands DURING the resolve: the body now belongs to the NEW
		// RBAC class while the key still names the OLD one.
		if ui, _ := xcontext.UserInfo(ctx); ui.Username == psCarol && granted.CompareAndSwap(false, true) {
			k424Grant(t, dyn, a, psCarol)
		}
		return h1WidgetUnstructured(map[string]any{}), nil
	})
	defer func() { r2(); r1() }()

	before := driftDeclined424("widgets")
	rec := httptest.NewRecorder()
	h := &widgetsHandler{authnNS: psAuthnNS, saRC: &rest.Config{}}
	h.ServeHTTP(rec, httptest.NewRequest("GET", "/call", nil).WithContext(carolCtx))

	expectDriftCounted424(t, "widgets", before)
	a548AssertNoArm(t, "widgets/identity_class_drift", rec)
	if entry, ok := handle.Get(key); ok {
		t.Fatalf("#548 [widgets/identity_class_drift]: the drift branch stored a cell at %q (%q) — this arm is not "+
			"driving the #424 decline", key, entry.RawJSON)
	}
}

// ---------------------------------------------------------------------------
// RESTACTIONS — every branch of the restactions.go Put-gate chain
// ---------------------------------------------------------------------------

type a548RABranch struct {
	name     string
	why      string
	user     string
	headers  map[string]string
	during   func(t *testing.T, ctx context.Context, key string)
	witness  func() uint64
	emptyKey bool
}

func TestIssue548_RESTActions_NoDeclineBranchArms(t *testing.T) {
	branches := []a548RABranch{
		{
			name:     "inert",
			why:      "cache.Inert(ctx) — #443 (g) dry-run hop; nothing persisted",
			user:     h1User,
			headers:  map[string]string{cache.InertHeader: "1"},
			emptyKey: true,
		},
		{
			name: "sensitive_secret_read",
			why:  "cache.DeclineSensitivePut(ctx) — #398 the resolve read core v1/secrets",
			user: h1User,
			during: func(t *testing.T, ctx context.Context, _ string) {
				cache.SensitiveTouchedSinkFromContext(ctx).Bump()
			},
			witness: cache.SensitiveSkippedPutForTest,
		},
		{
			name: "uaf_narrowed",
			why:  "uafDeclineReason — 1.12.3 A-1 per-requester narrowing is not folded into the key",
			user: h1User,
			during: func(t *testing.T, ctx context.Context, _ string) {
				cache.UAFTouchedSinkFromContext(ctx).Bump()
			},
			witness: cache.RestactionsUAFPutDeclined,
		},
		{
			name: "stage_error",
			why:  "stageErrSink.Count() > 0 — #313 Cache-A partial-with-errors body",
			user: h1User,
			during: func(t *testing.T, ctx context.Context, _ string) {
				cache.StageErrorSinkFromContext(ctx).Bump("apistage", "per-item boom")
			},
		},
		{
			name: "external_touch",
			why:  "extTouchedSink.Count() > 0 — external data has no dep edge to invalidate",
			user: h1User,
			during: func(t *testing.T, ctx context.Context, _ string) {
				cache.ExternalTouchedSinkFromContext(ctx).Bump()
			},
			witness: cache.ExternalSkippedPut,
		},
		{
			name: "empty_binding_uid",
			why:  "the no-branch fall-through: serveFromCacheEligible false (#95 \"\"-BindingUID), key still non-empty",
			user: a548Unbound,
		},
		{
			name: "putifgen_refused",
			why:  "#189 PutIfGen refused — a DELETE-eviction bumped the generation during the resolve",
			user: h1User,
			during: func(t *testing.T, _ context.Context, key string) {
				c := cache.ResolvedCache()
				c.Put(key, &cache.ResolvedEntry{RawJSON: []byte(`{"racer":true}`)})
				c.DeleteForTest(key)
			},
		},
	}

	// restactions.go's chain has seven arms (six declines + the genuine Put);
	// drift is the seventh decline and lives in its own test below, for the same
	// watcher reason as the widgets twin.
	if len(branches) != 7 {
		t.Fatalf("#548: the restactions Put-gate chain arms must ALL be driven here (minus identity-class drift, "+
			"which needs the grant-mid-resolve watcher); got %d cases", len(branches))
	}

	for _, b := range branches {
		t.Run(b.name, func(t *testing.T) {
			h1BuildWatcher(t)
			cache.RegisterUAFPutDeclineMetricsForTest()
			cache.ResetUAFPutDeclineCountersForTest()
			t.Cleanup(cache.ResetUAFPutDeclineCountersForTest)

			reqCtx := xcontext.BuildContext(context.Background(),
				xcontext.WithUserInfo(jwtutil.UserInfo{Username: b.user, Groups: []string{"devs"}}))

			key, handle, inputs := dispatchCacheLookupKey(reqCtx, "restactions",
				h1RAGVR.Group, h1RAGVR.Version, h1RAGVR.Resource, h1NS, h1RAName, -1, -1, nil)
			if handle == nil {
				t.Fatalf("PRECONDITION [%s]: expected a live cache handle", b.name)
			}
			if key == "" {
				t.Fatalf("PRECONDITION [%s]: expected a non-empty derived key", b.name)
			}
			if b.name == "empty_binding_uid" {
				if inputs == nil || inputs.BindingUID != "" {
					t.Fatalf("PRECONDITION [%s]: expected a re-derived \"\" BindingUID; got %+v", b.name, inputs)
				}
			} else if inputs == nil || inputs.BindingUID == "" {
				t.Fatalf("PRECONDITION [%s]: expected a non-empty BindingUID; got %+v", b.name, inputs)
			}
			if _, ok := handle.Get(key); ok {
				t.Fatalf("PRECONDITION [%s]: the derived key must be cold before the arm runs", b.name)
			}

			var witnessBefore uint64
			if b.witness != nil {
				witnessBefore = b.witness()
			}
			refusedBefore := cache.ResolvedCache().Stats().PutRefusedGenerationMovedTotal

			resolved := &templatesv1.RESTAction{}
			resolved.SetName(h1RAName)
			resolved.SetNamespace(h1NS)
			restore := installRAFakes(t, h1RAUnstructured(), func() bool { return true },
				func(ctx context.Context, _ restactions.ResolveOptions) (*templatesv1.RESTAction, error) {
					if b.during != nil {
						b.during(t, ctx, key)
					}
					return resolved, nil
				})
			defer restore()

			req := httptest.NewRequest("GET", "/call", nil).WithContext(reqCtx)
			for k, v := range b.headers {
				req.Header.Set(k, v)
			}
			rec := httptest.NewRecorder()
			RESTAction().ServeHTTP(rec, req)

			a548AssertNoArm(t, "restactions/"+b.name, rec)

			if b.witness != nil {
				if got := b.witness(); got == witnessBefore {
					t.Fatalf("#548 [restactions/%s]: the branch's own decline counter did not move (%d) — this case "+
						"did NOT reach %s, so its no-arm pass proves nothing about that branch", b.name, got, b.why)
				}
			}
			if b.name == "putifgen_refused" {
				if got := cache.ResolvedCache().Stats().PutRefusedGenerationMovedTotal; got != refusedBefore+1 {
					t.Fatalf("#548 [restactions/%s]: PutIfGen was not refused (refused_total %d->%d) — this case did "+
						"not reach %s", b.name, refusedBefore, got, b.why)
				}
			}
			if entry, ok := handle.Get(key); ok {
				t.Fatalf("#548 [restactions/%s]: the branch stored a cell at %q (%q) — this case is NOT on a decline "+
					"path. %s", b.name, key, entry.RawJSON, b.why)
			}
		})
	}
}

// TestIssue548_RESTActions_IdentityClassDrift_DoesNotArm — the restactions twin
// of the widgets drift arm.
func TestIssue548_RESTActions_IdentityClassDrift_DoesNotArm(t *testing.T) {
	a := psArm{name: "configmaps/group", target: psConfigmapsGVR, watchShape: true}
	dyn := psBuildWatcher(t, a)
	carolCtx := psUserCtx(a, psCarol)

	key, inputs := k423RAKey(t, carolCtx)
	if key == "" || inputs == nil || inputs.BindingUID == "" {
		t.Fatalf("PRECONDITION: expected a live non-empty-BindingUID restactions key; key=%q inputs=%+v", key, inputs)
	}
	c := cache.ResolvedCache()
	if c == nil {
		t.Fatal("PRECONDITION: expected a live resolved cache")
	}

	var granted atomic.Bool
	cr := psRACR(a)
	r1 := setFetchObjectForTest(func(*http.Request) objects.Result {
		return objects.Result{GVR: h1RAGVR, Unstructured: cr.DeepCopy()}
	})
	r2 := setRestactionsResolveForTest(func(ctx context.Context, opts restactions.ResolveOptions) (*templatesv1.RESTAction, error) {
		if ui, _ := xcontext.UserInfo(ctx); ui.Username == psCarol && granted.CompareAndSwap(false, true) {
			k424Grant(t, dyn, a, psCarol)
		}
		return opts.In.DeepCopy(), nil
	})
	defer func() { r2(); r1() }()

	before := driftDeclined424("restactions")
	rec := httptest.NewRecorder()
	h := &restActionHandler{authnNS: psAuthnNS, saRC: &rest.Config{}}
	h.ServeHTTP(rec, httptest.NewRequest("GET", "/call", nil).WithContext(carolCtx))

	expectDriftCounted424(t, "restactions", before)
	a548AssertNoArm(t, "restactions/identity_class_drift", rec)
	if entry, ok := c.Get(key); ok {
		t.Fatalf("#548 [restactions/identity_class_drift]: the drift branch stored a cell at %q (%q)", key, entry.RawJSON)
	}
}

// ---------------------------------------------------------------------------
// THE INVERSE — arming must still happen where a cell exists
// ---------------------------------------------------------------------------

// TestIssue548_AcceptedPut_StillArms is the arm that goes RED if a #548 fix
// stops arming everywhere. A clean cold dispatch — no sink bumped, no drift, a
// bound identity, an accepted PutIfGen — MUST stamp the key the browser needs,
// for BOTH classes. Each half asserts the cell was actually stored first, so
// "armed" is never asserted about a response that had nothing to arm for.
func TestIssue548_AcceptedPut_StillArms(t *testing.T) {
	t.Run("widgets", func(t *testing.T) {
		h1BuildWatcher(t)
		reqCtx := h1ReqCtx(h1User)
		cr := h1WidgetUnstructured(map[string]any{})
		key, handle, _ := a548WidgetKey(reqCtx, cr, nil)
		if handle == nil || key == "" {
			t.Fatalf("PRECONDITION: live widgets key/handle expected")
		}

		restore := installWidgetFakes(t, cr, func() bool { return true },
			func(context.Context, widgets.ResolveOptions) (*widgets.Widget, error) {
				return h1WidgetUnstructured(map[string]any{}), nil
			})
		defer restore()

		rec := httptest.NewRecorder()
		Widgets().ServeHTTP(rec, httptest.NewRequest("GET", "/call", nil).WithContext(reqCtx))

		if _, ok := handle.Get(key); !ok {
			t.Fatalf("PRECONDITION: the clean cold dispatch must have Put a cell under %q — without the Put this arm "+
				"cannot distinguish 'armed correctly' from 'armed a dead key'", key)
		}
		a548AssertArmed(t, "widgets/accepted_put", rec, key, "widgets")
	})

	t.Run("restactions", func(t *testing.T) {
		h1BuildWatcher(t)
		reqCtx := h1ReqCtx(h1User)
		key, handle, _ := dispatchCacheLookupKey(reqCtx, "restactions",
			h1RAGVR.Group, h1RAGVR.Version, h1RAGVR.Resource, h1NS, h1RAName, -1, -1, nil)
		if handle == nil || key == "" {
			t.Fatalf("PRECONDITION: live restactions key/handle expected")
		}

		resolved := &templatesv1.RESTAction{}
		resolved.SetName(h1RAName)
		resolved.SetNamespace(h1NS)
		restore := installRAFakes(t, h1RAUnstructured(), func() bool { return true },
			func(context.Context, restactions.ResolveOptions) (*templatesv1.RESTAction, error) {
				return resolved, nil
			})
		defer restore()

		rec := httptest.NewRecorder()
		RESTAction().ServeHTTP(rec, httptest.NewRequest("GET", "/call", nil).WithContext(reqCtx))

		if _, ok := handle.Get(key); !ok {
			t.Fatalf("PRECONDITION: the clean cold dispatch must have Put a cell under %q", key)
		}
		a548AssertArmed(t, "restactions/accepted_put", rec, key, "restactions")
	})
}

// TestIssue548_L1Hit_StillArms pins the OTHER armable shape #548 must not
// touch: a warm HIT serves an EXISTING cell, which the refresher can publish to
// on the next dep-change, so it must keep arming. The #548 change is confined to
// the two cold tails; this is the regression guard for that confinement.
func TestIssue548_L1Hit_StillArms(t *testing.T) {
	t.Run("widgets", func(t *testing.T) {
		h1BuildWatcher(t)
		reqCtx := h1ReqCtx(h1User)
		cr := h1WidgetUnstructured(map[string]any{})
		key, handle, inputs := a548WidgetKey(reqCtx, cr, nil)
		if handle == nil || key == "" || inputs == nil {
			t.Fatalf("PRECONDITION: live widgets key/handle expected")
		}
		warm := []byte(`{"warm":"widget-cell"}` + "\n")
		handle.Put(key, &cache.ResolvedEntry{RawJSON: warm, Inputs: inputs})

		restore := installWidgetFakes(t, cr, func() bool { return true },
			func(context.Context, widgets.ResolveOptions) (*widgets.Widget, error) {
				t.Fatalf("the second request must be an L1 HIT (no resolve)")
				return nil, nil
			})
		defer restore()

		rec := httptest.NewRecorder()
		Widgets().ServeHTTP(rec, httptest.NewRequest("GET", "/call", nil).WithContext(reqCtx))
		if !bytes.Equal(rec.Body.Bytes(), warm) {
			t.Fatalf("PRECONDITION: expected the warm cell served verbatim; got %q", rec.Body.Bytes())
		}
		a548AssertArmed(t, "widgets/l1_hit", rec, key, "widgets")
	})

	t.Run("restactions", func(t *testing.T) {
		h1BuildWatcher(t)
		reqCtx := h1ReqCtx(h1User)
		key, handle, inputs := dispatchCacheLookupKey(reqCtx, "restactions",
			h1RAGVR.Group, h1RAGVR.Version, h1RAGVR.Resource, h1NS, h1RAName, -1, -1, nil)
		if handle == nil || key == "" || inputs == nil {
			t.Fatalf("PRECONDITION: live restactions key/handle expected")
		}
		warm := []byte(`{"warm":"ra-cell"}` + "\n")
		handle.Put(key, &cache.ResolvedEntry{RawJSON: warm, Inputs: inputs})

		restore := installRAFakes(t, h1RAUnstructured(), func() bool { return true },
			func(context.Context, restactions.ResolveOptions) (*templatesv1.RESTAction, error) {
				t.Fatalf("the second request must be an L1 HIT (no resolve)")
				return nil, nil
			})
		defer restore()

		rec := httptest.NewRecorder()
		RESTAction().ServeHTTP(rec, httptest.NewRequest("GET", "/call", nil).WithContext(reqCtx))
		if !bytes.Equal(rec.Body.Bytes(), warm) {
			t.Fatalf("PRECONDITION: expected the warm cell served verbatim; got %q", rec.Body.Bytes())
		}
		a548AssertArmed(t, "restactions/l1_hit", rec, key, "restactions")
	})
}

// TestIssue548_ArmingHelperPolarity pins the helper's own contract, so the two
// call sites' `armable` argument cannot be read as the OPPOSITE of what it
// means (the polarity trap: setRefreshKeyHeaderUnlessExternal's flag SUPPRESSES
// and setRefreshKeyHeaderIfArmable's flag ENABLES, and they sit side by side in
// helpers.go).
func TestIssue548_ArmingHelperPolarity(t *testing.T) {
	const key, class = "k1", "widgets"

	armed := httptest.NewRecorder()
	setRefreshKeyHeaderIfArmable(armed, key, class, true)
	if got := armed.Header().Get(refreshKeyHeader); got != key {
		t.Fatalf("armable=true must stamp %s=%q; got %q", refreshKeyHeader, key, got)
	}
	if got := armed.Header().Get(refreshClassHeader); got != class {
		t.Fatalf("armable=true must stamp %s=%q; got %q", refreshClassHeader, class, got)
	}

	// armable=true is byte-identical to the plain helper (so an armed response is
	// unchanged from pre-#548).
	plain := httptest.NewRecorder()
	setRefreshKeyHeader(plain, key, class)
	if armed.Header().Get(refreshKeyHeader) != plain.Header().Get(refreshKeyHeader) ||
		armed.Header().Get(refreshClassHeader) != plain.Header().Get(refreshClassHeader) {
		t.Fatalf("armable=true must be byte-identical to setRefreshKeyHeader; armable=%v plain=%v",
			armed.Header(), plain.Header())
	}

	notArmed := httptest.NewRecorder()
	setRefreshKeyHeaderIfArmable(notArmed, key, class, false)
	if got := notArmed.Header().Get(refreshKeyHeader); got != "" {
		t.Fatalf("armable=false must stamp NOTHING; got %s=%q", refreshKeyHeader, got)
	}
	if got := notArmed.Header().Get(refreshClassHeader); got != "" {
		t.Fatalf("armable=false must stamp no class either; got %s=%q", refreshClassHeader, got)
	}

	// An empty key never arms, whatever the flag says (the pre-existing no-op the
	// inert branch relies on).
	empty := httptest.NewRecorder()
	setRefreshKeyHeaderIfArmable(empty, "", class, true)
	if got := empty.Header().Get(refreshKeyHeader); got != "" {
		t.Fatalf("an empty key must never stamp a header; got %q", got)
	}
}
