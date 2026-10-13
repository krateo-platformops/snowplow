// Package schema validates a custom resource's status against its CRD's
// OpenAPI schema. ValidateObjectStatus fetches (and per-GVR memoises) the
// CRD's structural schema, compiles it once, and checks the object's
// widgetData/status payload, failing closed when the schema is absent. The
// memo is invalidated on CRD lifecycle changes via a generation fence.
package schema

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"

	"github.com/krateo-platformops/snowplow/internal/cache"
	"github.com/krateo-platformops/snowplow/internal/dynamic"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	runtimeschema "k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/rest"
)

const (
	widgetDataKey = "widgetData"
)

// crdGVR is the hardcoded apiextensions.k8s.io/v1 CRD GroupVersionResource.
// Stable across all clusters; pinning here avoids both a discovery hop
// and a string-build allocation on every widget /call.
var crdGVR = runtimeschema.GroupVersionResource{
	Group:    "apiextensions.k8s.io",
	Version:  "v1",
	Resource: "customresourcedefinitions",
}

// InvalidStatusError marks a GENUINE schema-validation failure: the object's
// status.widgetData does not conform to its CRD's schema. It is the only
// validator outcome widgets.Resolve reports as a 400 BadRequest; every other
// error (a 403/404/5xx from the apiserver hops in front of it, a CRD whose
// schema cannot be compiled) keeps its own identity (#608). Error() is the
// inner text verbatim, so the customer-facing message is unchanged.
type InvalidStatusError struct{ Err error }

func (e *InvalidStatusError) Error() string { return e.Err.Error() }
func (e *InvalidStatusError) Unwrap() error { return e.Err }

// schemaRESTConfig picks the transport for the validator's apiserver hops.
//
// #608 — THE CRD SCHEMA IS A SYSTEM READ. It is identity-free cluster metadata,
// the same for every user, so it is always read as snowplow: the process SA
// *rest.Config main.go installs once at startup (cache.SetProcessSARestConfig).
// The caller's rc is NOT consulted when that is set — whatever a caller puts on
// ctx or in ResolveOptions.RC, a system read can never be routed through an
// identity transport. That is the defect this closes: since #574 the refresher
// passed its cohort-IMPERSONATED config here, 057 refused the impersonation on
// the discovery hop in front of the CRD GET (403, reported by client-go as the
// literal "unknown"), and ~50K refreshes failed.
//
// The caller's rc is used ONLY when no process SA rc is installed (cache off,
// out-of-cluster, unit tests) — the unchanged pre-#608 fallback.
func schemaRESTConfig(callerRC *rest.Config) *rest.Config {
	if sa := cache.ProcessSARestConfig(); sa != nil {
		return sa
	}
	return callerRC
}

func ValidateObjectStatus(ctx context.Context, rc *rest.Config, obj map[string]any) error {
	gv := dynamic.GroupVersion(obj)
	gvk := gv.WithKind(dynamic.GetKind(obj))
	rc = schemaRESTConfig(rc)

	// Ship 2 (production-aim cleanup 2026-06-01) — resolve the
	// composition GVR through cache.GVRFor. The builtin scheme arm
	// covers apiextensions.k8s.io/v1 itself; the permanent plurals
	// store covers the composition's own CRD-backed GVK after one
	// discovery hop per process lifetime (already counted by
	// ReasonPluralsDiscoveryHop inside PluralFor). Replaces Ship D's
	// ReasonRestmapperResourceFor + the dynamic.ResourceFor cold-
	// restmapper build per /call (the helper was deleted in Ship 2).
	if cache.IsResolverGVRHit(gvk) {
		cache.RecordResolverPluralsHit(ctx, gvk.String())
	} else {
		cache.RecordResolverPluralsMiss(ctx, gvk.String())
	}
	gvr, err := cache.GVRFor(ctx, gvk, rc)
	if err != nil {
		return fmt.Errorf("plural for %s: %w", gvk.String(), err)
	}

	widgetData, ok, err := unstructured.NestedMap(obj, "status", widgetDataKey)
	if err != nil {
		return err
	}
	if !ok {
		name := dynamic.GetName(obj)
		return &apierrors.StatusError{
			ErrStatus: metav1.Status{
				Status: metav1.StatusFailure,
				Code:   http.StatusNotFound,
				Reason: metav1.StatusReasonNotFound,
				Details: &metav1.StatusDetails{
					Group: gvr.Group,
					Kind:  gvr.Resource,
					Name:  name,
				},
				Message: fmt.Sprintf("status.widgetData not found in %s %q", gvr.String(), name),
			}}
	}

	// Task #323 (#318-R2 Commit 2-B) — per-GVR compiled-schema memo. On a
	// hit, skip the CRD GET + extractOpenAPISchemaFromCRD +
	// buildValidationFromSchemaData recompute (TRACED 0.91% of the 0.30.258
	// drain profile — they are a pure function of (CRD bytes, version),
	// invariant per GVR across a window) and go straight to the per-object
	// validateCustomResource below. The memo is reset on any CRD lifecycle
	// event via the EXISTING 0.30.233 bridge (InvalidateCRDSchemaMemo wired
	// in main.go) AND fenced by a generation counter so an inflight miss-fill
	// (this call) cannot re-install a schema compiled from pre-reset bytes —
	// so a hit cannot serve a stale schema for a changed GVR even under a
	// concurrent CRD install (architect A1; schema_cache.go header).
	// Placed AFTER the status.widgetData-absent NotFound guard above so a
	// memo hit NEVER bypasses the fail-closed NotFound path (never-change-
	// output: a widgetData-absent object returns NotFound on hit AND miss).
	if crv, hit := lookupCRDSchema(gvr); hit {
		return invalidStatus(validateCustomResource(crv, widgetData))
	}

	// MISS — snapshot the memo generation BEFORE the CRD GET below. If a CRD
	// lifecycle reset (InvalidateCRDSchemaMemo) lands during the GET+compile
	// window, the generation moves and storeCRDSchema drops the (potentially
	// stale-bytes) install, so the next call recompiles from fresh bytes
	// (architect A1 generation fence).
	gen := currentSchemaGen()

	// Ship 0.30.231 (2026-06-01) — inlined CRD GET. The deleted
	// internal/resolvers/crds.Get helper wrapped the same two-line
	// dynamic.NewClient + Get call below; inlining removes the
	// indirection. Earlier (Ship 2) attempted to also skip the
	// mapper build via dynamic.WithSkipMapper, but the contract was
	// unsafe — resourceInterfaceFor (client.go:146) calls
	// uc.mapper.RESTMapping unconditionally; the opts.GVR/opts.GVK
	// branch at line 138 only chooses the source GVK, it does not
	// skip the mapper. WithSkipMapper has been removed.
	//
	// Task #322 (#318-R2) Commit 1 — the per-call dynamic.NewClient
	// here built a FRESH memCacheClient + DeferredDiscoveryRESTMapper
	// every child GET, so the first GVR-only KindFor re-downloaded the
	// full API surface per call (TRACED 2.13% of the 0.30.258 drain
	// profile). SharedSADiscoveryClient returns a process-singleton
	// whose mapper is built ONCE and reused warm; the discovery
	// download is amortised to one boot download. rc here is ALWAYS the
	// SA rest.Config (opts.RC = widgets.go:228 r.saRC on the customer
	// /call path; phase1_walk_pagination_jobs.go:433/:579 saRC on the
	// drain) — the singleton's identity invariant. This is the cached-
	// mapper correction, NOT the WithSkipMapper revival: the mapper is
	// always non-nil and populated (cached_client.go header).
	//
	// FALLBACK — if the singleton errors (nil rc / startup race),
	// fall back to the per-call dynamic.NewClient so the path is never
	// worse than today (project_cache_off_is_transparent_fallback).
	cli, err := dynamic.SharedSADiscoveryClient(rc)
	if err != nil {
		// Fall back LOUDLY: a systemically broken singleton would
		// otherwise silently evaporate the caching win back to
		// per-call discovery downloads.
		slog.Warn("schema: SA discovery singleton unavailable; "+
			"falling back to per-call client",
			slog.String("error", err.Error()))
		cli, err = dynamic.NewClient(rc)
		if err != nil {
			return err
		}
	}
	crdName := fmt.Sprintf("%s.%s", gvr.Resource, gvr.Group)
	crdObj, err := cli.Get(ctx, crdName, dynamic.Options{
		GVR:       crdGVR,
		Namespace: "",
	})
	if err != nil {
		return fmt.Errorf("crd schema %s: %w", crdName, err)
	}
	var crd map[string]any
	if crdObj != nil {
		crd = crdObj.UnstructuredContent()
	} else {
		crd = map[string]any{}
	}

	// Miss — compile the CRV (the 0.91% extract + v1->internal build) and
	// memoise it per GVR. compileCRDSchemaFn is the package-private seam the
	// RED/GREEN falsifier counts (per-call recompile N -> 1 with the memo);
	// it is initialised once to extractOpenAPISchemaFromCRD and only
	// reassigned by test code (mirrors discoveryClientForConfigFn,
	// dynamic/cached_client.go:176).
	crv, err := compileCRDSchemaFn(crd, gvr.Version)
	if err != nil {
		return fmt.Errorf("compile crd schema %s: %w", crdName, err)
	}
	storeCRDSchema(gvr, crv, gen)

	return invalidStatus(validateCustomResource(crv, widgetData))
}

// invalidStatus marks a validateCustomResource failure as the genuine schema
// violation it is; nil stays nil.
func invalidStatus(err error) error {
	if err == nil {
		return nil
	}
	return &InvalidStatusError{Err: err}
}
