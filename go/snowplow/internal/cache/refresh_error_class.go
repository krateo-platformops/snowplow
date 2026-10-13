// refresh_error_class.go — #608: a refresh failure is never `unknown` again.
//
// THE DEFECT THIS NAMES. On krateo-057 50,189 refreshes failed with the text
// `resolveAndPopulateL1 widgets/<name>: resolve widget: unknown`. The inner
// error was a 403 Forbidden on the discovery request in front of the CRD GET
// (client-go cannot decode a Status body on .Raw(), so its message is the
// literal "unknown"), and widgets.Resolve then relabelled it as a 400. Nothing
// in the log said which hop failed, on which object, or what kind of failure
// it was, so the cause took a day of tracing.
//
// Two pieces close it:
//
//   - ResolveStageError: every failing stage of a widget resolve wraps its
//     error with a fixed prefix naming the stage and the object, and keeps
//     the chain intact (%w semantics), so errors.As still reaches the real
//     *apierrors.StatusError underneath.
//   - ClassifyRefreshError: a CLOSED set of failure classes computed from the
//     apimachinery reason/code, plus the stage. The refresher stamps both on
//     refresher.refresh_failed and counts them, so a regression of this shape
//     reads as `forbidden` at `validate` the moment it happens.
package cache

import (
	"context"
	"errors"
	"net"
	"sync/atomic"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/client-go/discovery"
)

// Resolve stages of a widget resolve, in pipeline order. Closed set.
const (
	ResolveStageApiRef        = "apiref"
	ResolveStageWidgetData    = "widget_data"
	ResolveStageResourcesRefs = "resources_refs"
	ResolveStageValidate      = "validate"
	// ResolveStageRefetch is the refresher's re-fetch of the cell's OWN CR
	// (objects.Get in resolveOnceProd), before any resolver runs (#630).
	ResolveStageRefetch = "refetch"
	// ResolveStageNone is the stage of an error that did not come out of a
	// staged hop (an encode failure, a decline, a TOCTOU drift).
	ResolveStageNone = "none"
)

// refreshStages is the closed stage set, in counter-index order.
var refreshStages = [...]string{
	ResolveStageNone, ResolveStageApiRef, ResolveStageWidgetData,
	ResolveStageResourcesRefs, ResolveStageValidate, ResolveStageRefetch,
}

// Refresh failure classes. Closed set — a new class is a code change, never a
// free-form string, so the counter's cardinality is bounded by construction.
const (
	RefreshErrForbidden     = "forbidden"
	RefreshErrUnauthorized  = "unauthorized"
	RefreshErrNotFound      = "not_found"
	RefreshErrTimeout       = "timeout"
	RefreshErrUnavailable   = "unavailable"
	RefreshErrSchemaInvalid = "schema_invalid"
	RefreshErrDiscovery     = "discovery"
	RefreshErrJQ            = "jq"
	RefreshErrUnclassified  = "unclassified"
)

// refreshClasses is the closed class set, in counter-index order.
var refreshClasses = [...]string{
	RefreshErrForbidden, RefreshErrUnauthorized, RefreshErrNotFound,
	RefreshErrTimeout, RefreshErrUnavailable, RefreshErrSchemaInvalid,
	RefreshErrDiscovery, RefreshErrJQ, RefreshErrUnclassified,
}

// ResolveStageError carries the failing stage of a resolve and a fixed prefix
// naming it and the object, wrapping the original error.
type ResolveStageError struct {
	Stage  string
	Prefix string
	Err    error
}

func (e *ResolveStageError) Error() string { return e.Prefix + ": " + e.Err.Error() }
func (e *ResolveStageError) Unwrap() error { return e.Err }

// WrapResolveStage wraps err with its stage and prefix; nil stays nil.
func WrapResolveStage(stage, prefix string, err error) error {
	if err == nil {
		return nil
	}
	return &ResolveStageError{Stage: stage, Prefix: prefix, Err: err}
}

// ResolveStageOf returns the innermost-wrapping stage of err, or
// ResolveStageNone.
func ResolveStageOf(err error) string {
	var se *ResolveStageError
	if errors.As(err, &se) {
		return se.Stage
	}
	return ResolveStageNone
}

// ClassifyRefreshError maps a refresh error onto the closed class set and its
// resolve stage. Order matters: a context deadline that wraps an apierror is a
// timeout; a typed apiserver status beats any heuristic below it.
func ClassifyRefreshError(err error) (class, stage string) {
	stage = ResolveStageOf(err)
	if err == nil {
		return RefreshErrUnclassified, stage
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return RefreshErrTimeout, stage
	}
	switch {
	case apierrors.IsForbidden(err):
		return RefreshErrForbidden, stage
	case apierrors.IsUnauthorized(err):
		return RefreshErrUnauthorized, stage
	case apierrors.IsNotFound(err):
		return RefreshErrNotFound, stage
	case apierrors.IsTimeout(err), apierrors.IsServerTimeout(err):
		return RefreshErrTimeout, stage
	case apierrors.IsServiceUnavailable(err), apierrors.IsInternalError(err),
		apierrors.IsTooManyRequests(err):
		return RefreshErrUnavailable, stage
	case apierrors.IsBadRequest(err) && stage == ResolveStageValidate:
		// widgets.Resolve emits a 400 at validate ONLY for a genuine
		// schema-validation failure (#608); every other validate error keeps
		// its own status.
		return RefreshErrSchemaInvalid, stage
	}
	if discovery.IsGroupDiscoveryFailedError(err) || meta.IsNoMatchError(err) {
		return RefreshErrDiscovery, stage
	}
	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		return RefreshErrTimeout, stage
	}
	if stage == ResolveStageWidgetData {
		// The widgetData stage is a pure jq evaluation over the apiRef
		// result; a non-apiserver error there is the template.
		return RefreshErrJQ, stage
	}
	return RefreshErrUnclassified, stage
}

// IsNonRetryableRefreshError reports a refresh failure that cannot clear
// inside the requeue budget, so the refresher sends it to the drop point on
// the first attempt (#608 Q2 ruling). A 403 is the apiserver's authorization
// decision for this identity: the same request is refused again at every
// backoff step (on 057, 89% of a burst's failures were retries that could not
// succeed). The drop point does NOT evict it: one 403 is not a determinism
// proof, and an eviction is a cold navigation (#547 ruling). The entry stays
// resident and served; the next dirty mark retries it.
func IsNonRetryableRefreshError(err error) bool {
	return err != nil && apierrors.IsForbidden(err)
}

// refreshFailedByClass counts refresh failures per (class, stage). Fixed-size,
// lock-free, bounded cardinality.
var refreshFailedByClass [len(refreshClasses)][len(refreshStages)]atomic.Uint64

func classIndex(class string) int {
	for i, c := range refreshClasses {
		if c == class {
			return i
		}
	}
	return len(refreshClasses) - 1 // unclassified
}

func stageIndex(stage string) int {
	for i, s := range refreshStages {
		if s == stage {
			return i
		}
	}
	return 0 // none
}

func recordRefreshFailedClass(class, stage string) {
	refreshFailedByClass[classIndex(class)][stageIndex(stage)].Add(1)
}

// RefreshFailedClassCount is one non-zero (class, stage) cell.
type RefreshFailedClassCount struct {
	Class string
	Stage string
	N     uint64
}

// RefreshFailedByClassSnapshot returns every non-zero (class, stage) count,
// for the expvar and OTLP publishers.
func RefreshFailedByClassSnapshot() []RefreshFailedClassCount {
	var out []RefreshFailedClassCount
	for ci, c := range refreshClasses {
		for si, s := range refreshStages {
			if n := refreshFailedByClass[ci][si].Load(); n > 0 {
				out = append(out, RefreshFailedClassCount{Class: c, Stage: s, N: n})
			}
		}
	}
	return out
}

// ResetRefreshFailedByClassForTest zeroes the per-class counters. TEST-ONLY.
func ResetRefreshFailedByClassForTest() {
	for ci := range refreshFailedByClass {
		for si := range refreshFailedByClass[ci] {
			refreshFailedByClass[ci][si].Store(0)
		}
	}
}
