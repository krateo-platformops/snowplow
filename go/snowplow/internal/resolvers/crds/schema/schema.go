// Package schema validates a custom resource's status against its CRD's
// OpenAPI schema. ValidateObjectStatus fetches (and per-GVR memoises) the
// CRD's structural schema, compiles it once, and checks the object's
// widgetData/status payload, failing closed when the schema is absent. The
// memo is invalidated on CRD lifecycle changes via a generation fence.
package schema

import (
	"context"
	"fmt"
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

	// MISS — #1318: the memo is shared server state, so it is refilled by ONE
	// shared, detached fill (schema_fill.go), never under this request's page
	// budget: a request whose budget is spent still gets the schema, and the
	// memo refills whatever the request that happened to miss. The fill
	// snapshots the memo generation BEFORE its CRD GET, so a CRD lifecycle reset
	// during the GET+compile window drops its store (architect A1 fence) and
	// the next call recompiles from fresh bytes.
	crv, err := fillCRDSchema(ctx, rc, gvr)
	if err != nil {
		return err
	}

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
