package widgets

import (
	"context"
	"errors"
	"net/http"
	"testing"

	xcontext "github.com/krateo-platformops/plumbing/context"
	"github.com/krateo-platformops/plumbing/endpoints"
	xenv "github.com/krateo-platformops/plumbing/env"
	"github.com/krateo-platformops/snowplow/internal/cache"
	crdschema "github.com/krateo-platformops/snowplow/internal/resolvers/crds/schema"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/rest"
)

// hermeticCtx builds a context the resolver's resourcesRefs stage can run
// against WITHOUT a cluster: it carries a UserConfig endpoint (so
// resourcesrefs.Resolve's xcontext.UserConfig succeeds) and an internal
// *rest.Config (so cache.ClientConfigFor returns it directly and never dials
// kubeconfig). TestMode keeps UserConfig from rewriting the endpoint's
// ServerURL to the in-cluster address. The GET-verb resourceRefs the tests use
// (v1/namespaces) resolve their Kind from the builtin scheme (no discovery hop)
// and build a /call path string only — no network I/O.
func hermeticCtx(t *testing.T) context.Context {
	t.Helper()
	prev := xenv.TestMode()
	xenv.SetTestMode(true)
	t.Cleanup(func() { xenv.SetTestMode(prev) })

	ctx := xcontext.BuildContext(context.Background(),
		xcontext.WithUserConfig(endpoints.Endpoint{ServerURL: "https://test.invalid"}))
	ctx = cache.WithInternalRESTConfig(ctx, &rest.Config{Host: "https://test.invalid"})
	return ctx
}

// resolve_orchestration_test.go — M20 (widgets.Resolve orchestration) + H2
// (resolveWidgetData #69 fail-soft). These are HERMETIC unit tests: neither the
// apiRef RESTAction fetch (apiref.Resolve → apiserver) nor the CRD-status
// validation (crdschema.ValidateObjectStatus → cache.GVRFor discovery) can run
// without a cluster, so the two boundary hops are swapped through the
// package-level function-variable seams (resolveApiRefFn / validateObjectStatusFn,
// resolve.go) whose defaults are the real implementations. Every stub is
// restored in a defer so package state never leaks across tests. All other
// stages (mergeExtras, injectSlice, resolveWidgetData, resolveResourceRefs) run
// their REAL code against the stubbed ds — so these tests exercise the genuine
// orchestration and ordering of Resolve, not a re-implementation.

// stubApiRef swaps resolveApiRefFn to return (ds, err) and restores the real
// implementation when the test ends. The returned ds is what the rest of
// Resolve (injectSlice → mergeExtras → resolveWidgetData → resolveResourceRefs)
// evaluates against.
func stubApiRef(t *testing.T, ds map[string]any, err error) {
	t.Helper()
	orig := resolveApiRefFn
	resolveApiRefFn = func(_ context.Context, _ ResolveOptions) (map[string]any, error) {
		if ds == nil && err == nil {
			return map[string]any{}, nil
		}
		return ds, err
	}
	t.Cleanup(func() { resolveApiRefFn = orig })
}

// stubValidate swaps validateObjectStatusFn to return err and restores the real
// implementation when the test ends.
func stubValidate(t *testing.T, err error) {
	t.Helper()
	orig := validateObjectStatusFn
	validateObjectStatusFn = func(_ context.Context, _ *rest.Config, _ map[string]any) error {
		return err
	}
	t.Cleanup(func() { validateObjectStatusFn = orig })
}

// baseWidget builds a minimal apiRef-less Button widget with a static
// widgetData block. widgetDataTemplate / resourcesRefsTemplate are added by the
// individual tests.
func baseWidget() *unstructured.Unstructured {
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "widgets.templates.krateo.io/v1beta1",
		"kind":       "Button",
		"metadata":   map[string]any{"name": "w", "namespace": "demo-system"},
		"spec": map[string]any{
			"widgetData": map[string]any{
				"label": "static-base",
			},
		},
	}}
}

// ---------------------------------------------------------------------------
// M20(a): an apiRef error sets status.error and Resolve returns that same error.
// ---------------------------------------------------------------------------

func TestResolve_ApiRefError_SetsStatusErrorAndReturns(t *testing.T) {
	sentinel := errors.New("apiref boom: forbidden")
	// apiRef fails BEFORE ds is produced.
	stubApiRef(t, nil, sentinel)
	// validate must NOT be reached; if it is, make that observable by failing.
	validateReached := false
	orig := validateObjectStatusFn
	validateObjectStatusFn = func(_ context.Context, _ *rest.Config, _ map[string]any) error {
		validateReached = true
		return nil
	}
	t.Cleanup(func() { validateObjectStatusFn = orig })

	w := baseWidget()
	got, err := Resolve(hermeticCtx(t), ResolveOptions{In: w})

	require.Error(t, err, "Resolve must propagate the apiRef error")
	assert.ErrorIs(t, err, sentinel, "the returned error must be the apiRef error unchanged")
	assert.Same(t, w, got, "Resolve returns opts.In on the apiRef-error path")

	// status.error must carry the apiRef error message.
	statusErr, ok, e := unstructured.NestedString(w.Object, "status", "error")
	require.NoError(t, e)
	require.True(t, ok, "status.error must be set on the apiRef-error path")
	assert.Equal(t, sentinel.Error(), statusErr,
		"status.error must equal the apiRef error message")

	// Short-circuit: validate + widgetData must NOT have run.
	assert.False(t, validateReached, "validate must not be reached after an apiRef error")
	_, wdSet, _ := unstructured.NestedMap(w.Object, "status", "widgetData")
	assert.False(t, wdSet, "status.widgetData must NOT be written after an apiRef error")
}

// ---------------------------------------------------------------------------
// M20(b): a validate error is wrapped as an *apierrors.StatusError with HTTP 400
// (BadRequest), and status.error carries the raw validate message.
// ---------------------------------------------------------------------------

func TestResolve_ValidateError_WrapsAs400StatusError(t *testing.T) {
	// apiRef succeeds with an empty ds; widgetDataTemplate absent ⇒ static
	// widgetData is kept; resourcesRefsTemplate absent ⇒ no refs.
	stubApiRef(t, map[string]any{}, nil)
	validateMsg := "status.widgetData.label: Invalid value: must be a string"
	// #608 — only a GENUINE schema-validation failure (crdschema marks it
	// InvalidStatusError) is a 400; TestResolve_ValidateHopError_KeepsItsStatus
	// pins that every other validator error keeps its own status.
	stubValidate(t, &crdschema.InvalidStatusError{Err: errors.New(validateMsg)})

	w := baseWidget()
	got, err := Resolve(hermeticCtx(t), ResolveOptions{In: w})

	require.Error(t, err, "a validate error must fail Resolve")
	assert.Same(t, w, got, "Resolve returns opts.In on the validate-error path")

	// The error must be recoverable as an *apierrors.StatusError with 400.
	var se *apierrors.StatusError
	require.True(t, errors.As(err, &se),
		"validate error must be wrapped as *apierrors.StatusError (the dispatcher's errors.As contract)")
	assert.Equal(t, int32(http.StatusBadRequest), se.ErrStatus.Code,
		"the wrapped StatusError code must be 400 BadRequest")
	assert.Equal(t, metav1.StatusReasonBadRequest, se.ErrStatus.Reason,
		"the wrapped StatusError reason must be BadRequest")
	assert.Equal(t, validateMsg, se.ErrStatus.Message,
		"the wrapped StatusError message must carry the raw validate message")

	// status.error carries the raw validate message (set before the wrap).
	statusErr, ok, e := unstructured.NestedString(w.Object, "status", "error")
	require.NoError(t, e)
	require.True(t, ok, "status.error must be set on the validate-error path")
	assert.Equal(t, validateMsg, statusErr)

	// status.widgetData was still written before validation ran (the static block).
	wd, ok, e := unstructured.NestedMap(w.Object, "status", "widgetData")
	require.NoError(t, e)
	require.True(t, ok, "status.widgetData must be written before validation runs")
	assert.Equal(t, "static-base", wd["label"])
}

// TestResolve_ValidateHopError_KeepsItsStatus — #608. A 403 from the
// validator's apiserver hop (on 057: the discovery GET /api in front of the CRD
// GET, whose message client-go reports as the literal "unknown") must NOT be
// relabelled a 400. It keeps Code 403 through the chain, and the error names
// the stage and the widget.
func TestResolve_ValidateHopError_KeepsItsStatus(t *testing.T) {
	stubApiRef(t, map[string]any{}, nil)
	forbidden := apierrors.NewGenericServerResponse(http.StatusForbidden, "get",
		schema.GroupResource{}, "", "unknown", 0, false)
	stubValidate(t, forbidden)

	w := baseWidget()
	_, err := Resolve(hermeticCtx(t), ResolveOptions{In: w})
	require.Error(t, err)

	var se *apierrors.StatusError
	require.True(t, errors.As(err, &se))
	assert.Equal(t, int32(http.StatusForbidden), se.ErrStatus.Code, "a validator-hop 403 must stay a 403")
	assert.True(t, apierrors.IsForbidden(err))
	assert.Equal(t, cache.ResolveStageValidate, cache.ResolveStageOf(err))
	assert.Contains(t, err.Error(), "validate status "+w.GroupVersionKind().String())
}

// TestResolve_ValidateOK_ReturnsNilNoStatusError — the happy path: validate
// passes ⇒ no error, no status.error, status.widgetData present. Guards that
// the 400 wrap is truly gated on the validate error (not always emitted).
func TestResolve_ValidateOK_ReturnsNilNoStatusError(t *testing.T) {
	stubApiRef(t, map[string]any{}, nil)
	stubValidate(t, nil)

	w := baseWidget()
	got, err := Resolve(hermeticCtx(t), ResolveOptions{In: w})

	require.NoError(t, err, "validate-OK path must not error")
	assert.Same(t, w, got)
	_, hasErr, _ := unstructured.NestedString(w.Object, "status", "error")
	assert.False(t, hasErr, "status.error must NOT be set on the success path")
	wd, ok, _ := unstructured.NestedMap(w.Object, "status", "widgetData")
	require.True(t, ok)
	assert.Equal(t, "static-base", wd["label"])
}

// ---------------------------------------------------------------------------
// M20(c): resourcesRefsTemplateExtras is referenceable in the resourcesRefsTemplate
// jq but NOT in the widgetDataTemplate jq — the scope isolation that the
// mergeExtras(GetResourcesRefsExtras) fold ORDER guarantees (it runs AFTER
// resolveWidgetData, BEFORE resolveResourceRefs).
//
// RED arm: if that fold were moved BEFORE resolveWidgetData, the rrt-only key
// would leak into the widgetData. TestResolve_RrtExtras_LeaksWhenFoldedEarly
// proves the arm discriminates by simulating that wrong ordering.
// ---------------------------------------------------------------------------

// scopeIsolationWidget mirrors extras_integration_test.go's inlineStaticWidget:
// widgetDataTemplate reads `.rrtNs` (must be ABSENT), resourcesRefsTemplate
// reads it (must be PRESENT), and the sibling resourcesRefsTemplateExtras block
// is the sole source of rrtNs.
func scopeIsolationWidget() *unstructured.Unstructured {
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "widgets.templates.krateo.io/v1beta1",
		"kind":       "Button",
		"metadata":   map[string]any{"name": "w", "namespace": "demo-system"},
		"spec": map[string]any{
			"widgetData": map[string]any{"label": "base"},
			"widgetDataTemplate": []any{
				map[string]any{"forPath": "label", "expression": `${ "ns=" + (.rrtNs // "ABSENT") }`},
			},
			"resourcesRefsTemplate": []any{
				map[string]any{
					"template": map[string]any{
						"id": `${ "from-rrtNs-" + .rrtNs }`, "apiVersion": "v1",
						"resource": "namespaces", "namespace": `${ .rrtNs }`, "verb": "GET",
					},
				},
			},
			"resourcesRefsTemplateExtras": map[string]any{
				"rrtNs": "inline-team",
			},
		},
	}}
}

func TestResolve_RrtExtras_ScopedToResourcesRefsOnly(t *testing.T) {
	// apiRef-less: ds starts empty. No request extras.
	stubApiRef(t, map[string]any{}, nil)
	stubValidate(t, nil) // don't gate the assertion on cluster schema.

	w := scopeIsolationWidget()
	_, err := Resolve(hermeticCtx(t), ResolveOptions{In: w})
	require.NoError(t, err)

	// Scope isolation: the widgetDataTemplate ran BEFORE the rrt-extras fold, so
	// .rrtNs was absent → label == "ns=ABSENT".
	wd, ok, e := unstructured.NestedMap(w.Object, "status", "widgetData")
	require.NoError(t, e)
	require.True(t, ok)
	assert.Equal(t, "ns=ABSENT", wd["label"],
		"widgetDataTemplate MUST NOT see the resourcesRefsTemplateExtras-only key rrtNs (scope isolation)")

	// The resourcesRefsTemplate jq DID see rrtNs → one ref with the derived id.
	items, ok, e := unstructured.NestedSlice(w.Object, "status", "resourcesRefs", "items")
	require.NoError(t, e)
	require.True(t, ok, "status.resourcesRefs.items must be present")
	require.Len(t, items, 1)
	m, _ := items[0].(map[string]any)
	require.NotNil(t, m)
	assert.Equal(t, "from-rrtNs-inline-team", m["id"],
		"resourcesRefsTemplate jq MUST see the inline rrtNs (derived id proves it reached the rrt jq)")
}

// TestResolve_RrtExtras_LeaksWhenFoldedEarly is the RED-arm falsifier for the
// scope-isolation ordering. It reproduces the WRONG ordering the plan warns
// against — "move mergeExtras(GetResourcesRefsExtras) before resolveWidgetData"
// — by folding the rrt-extras into ds BEFORE running the widgetDataTemplate.
// With that wrong ordering the rrt-only key rrtNs LEAKS into the widgetData
// (label == "ns=inline-team", not "ns=ABSENT"). This proves the assertion in
// TestResolve_RrtExtras_ScopedToResourcesRefsOnly is discriminating: the real
// Resolve's fold ORDER is what keeps rrtNs out of widgetData.
func TestResolve_RrtExtras_LeaksWhenFoldedEarly(t *testing.T) {
	w := scopeIsolationWidget()

	// WRONG ordering (the RED impl): fold the rrt-extras into ds FIRST …
	ds := map[string]any{}
	mergeExtras(ds, GetResourcesRefsExtras(w.Object))
	// … THEN run the widgetDataTemplate against that ds.
	widgetData, err := resolveWidgetData(context.Background(), w, ds)
	require.NoError(t, err)

	assert.Equal(t, "ns=inline-team", widgetData["label"],
		"RED PROOF: with the rrt-extras folded BEFORE resolveWidgetData, the rrt-only key rrtNs LEAKS "+
			"into widgetData — this is exactly the leak the real Resolve's fold ORDER prevents")
}

// ---------------------------------------------------------------------------
// H2 [SEC]: resolveWidgetData #69 fail-soft. A malformed spec.widgetDataTemplate
// (a read error from GetWidgetDataTemplate) MUST fail-soft to the STATIC
// GetWidgetData src — it must NEVER invoke widgetdatatemplate.Resolve nor build
// an aggregate from ds (the SA-maximal cross-namespace data source). If it did,
// the SA-maximal aggregate would land in the shared identity-free widgetContent
// cell → cross-user leak (task #69).
// ---------------------------------------------------------------------------

// malformedWDTWidget makes spec.widgetDataTemplate a STRING (not a []any), so
// GetWidgetDataTemplate → maps.NestedSliceNoCopy returns an accessor error, and
// resolveWidgetData must fail-soft. The static widgetData carries `label` (a
// STATIC key that MUST survive).
func malformedWDTWidget() *unstructured.Unstructured {
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "widgets.templates.krateo.io/v1beta1",
		"kind":       "Button",
		"metadata":   map[string]any{"name": "w", "namespace": "demo-system"},
		"spec": map[string]any{
			"widgetData": map[string]any{
				"label": "static-only",
			},
			// MALFORMED — a string where a []any is required.
			"widgetDataTemplate": "this-is-not-a-slice",
		},
	}}
}

func TestResolveWidgetData_MalformedTemplate_FailsSoftToStatic(t *testing.T) {
	w := malformedWDTWidget()
	// ds carries an "aggregate" key that models the SA-maximal cross-ns data
	// source. It MUST NOT appear in the returned widgetData.
	ds := map[string]any{
		"aggregate": []any{
			map[string]any{"ns": "team-a", "secret": "cross-ns-1"},
			map[string]any{"ns": "team-b", "secret": "cross-ns-2"},
		},
	}

	got, err := resolveWidgetData(context.Background(), w, ds)

	require.NoError(t, err, "a malformed widgetDataTemplate must fail-soft (nil error), not hard-fail")
	// Fail-soft returns the STATIC src verbatim.
	assert.Equal(t, "static-only", got["label"], "the static widgetData src must be returned unchanged")
	// The SA-maximal aggregate key must NOT have leaked into the returned map.
	_, leaked := got["aggregate"]
	assert.False(t, leaked,
		"SEC/#69: the cross-ns aggregate from ds MUST NOT appear in the fail-soft widgetData "+
			"(else the SA-maximal aggregate lands in the shared identity-free cell — the cross-user leak)")
}

// TestResolveWidgetData_WellFormedTemplate_BuildsFromDs — negative control:
// with a WELL-FORMED widgetDataTemplate, resolveWidgetData DOES run the jq
// against ds and folds the derived value into the static src. Guards that the
// fail-soft above is truly gated on the read error, not always short-circuiting.
func TestResolveWidgetData_WellFormedTemplate_BuildsFromDs(t *testing.T) {
	w := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "widgets.templates.krateo.io/v1beta1",
		"kind":       "Button",
		"metadata":   map[string]any{"name": "w", "namespace": "demo-system"},
		"spec": map[string]any{
			"widgetData": map[string]any{"label": "static-only"},
			"widgetDataTemplate": []any{
				map[string]any{"forPath": "label", "expression": `${ .tenant }`},
			},
		},
	}}
	ds := map[string]any{"tenant": "acme"}

	got, err := resolveWidgetData(context.Background(), w, ds)
	require.NoError(t, err)
	assert.Equal(t, "acme", got["label"],
		"a well-formed widgetDataTemplate must run against ds and overwrite the static value")
}

// TestResolveWidgetData_MalformedTemplate_RED_WrongImplLeaks is the RED-arm
// proof for H2. It runs a SHADOW wrong-impl that, on the SAME read error,
// proceeds to build the widgetData from ds (the aggregate) instead of failing
// soft — and asserts the aggregate DOES leak. This demonstrates the assertion
// in TestResolveWidgetData_MalformedTemplate_FailsSoftToStatic is discriminating:
// a plausible wrong impl (build-from-ds on read error) is caught by it.
func TestResolveWidgetData_MalformedTemplate_RED_WrongImplLeaks(t *testing.T) {
	w := malformedWDTWidget()
	ds := map[string]any{
		"aggregate": []any{map[string]any{"ns": "team-a", "secret": "cross-ns-1"}},
	}

	got := resolveWidgetDataWrongAggregate(w, ds)

	_, leaked := got["aggregate"]
	assert.True(t, leaked,
		"RED PROOF: the wrong impl (build aggregate from ds on a widgetDataTemplate read error) "+
			"leaks the SA-maximal aggregate — exactly what the real fail-soft prevents")
}

// resolveWidgetDataWrongAggregate is the PLAUSIBLE-WRONG shadow impl referenced
// by the RED-arm test: on a GetWidgetDataTemplate read error it builds the
// widgetData by folding ds into the static src (an aggregate build) instead of
// failing soft. It never touches prod behaviour — it exists only to prove the
// H2 fail-soft assertion discriminates.
func resolveWidgetDataWrongAggregate(obj *Widget, ds map[string]any) map[string]any {
	src := GetWidgetData(obj.Object)
	if _, err := GetWidgetDataTemplate(obj.Object); err != nil {
		// WRONG: aggregate ds into the widgetData instead of returning static src.
		for k, v := range ds {
			if _, present := src[k]; !present {
				src[k] = v
			}
		}
		return src
	}
	return src
}
