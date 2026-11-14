// phase1_seed_classify.go — #158 (P9-B): seedCohort error classification.
//
// THE DEFECT THIS SHIPS AGAINST (architect design Part 1, TRACED at HEAD
// a1ad6fa). Before this ship, every non-nil seedCohort return bumped the
// single undifferentiated counter pipBindingSetSeedFailuresTotal and
// logged a hardcoded "narrow RBAC cohorts … are expected to fail" effect
// string (phase1_pip_seed.go:500-512). But seedCohort only ever returns
// non-nil for an OPERATIONAL failure (per-cohort ctx timeout / parent
// cancel at :692/:736, or a recovered panic) — a per-target RBAC deny is
// CONTAINED per-target (:694-712 restactions, :738-756 widgets do
// `continue`) and the cohort ends "partial"→nil. So the one counter that
// fired was firing ONLY for operational failures yet describing them as
// "expected RBAC" — a self-concealing mislabel.
//
// THE FIX. Split the verdict into an explicit taxonomy and classify at
// BOTH call sites (legacy runPIPSeed AND engine seedScopeYielding):
//   - seedFailRBACDeny  → Info log + snowplow_phase1_seed_rbac_deny_total;
//     NO re-enqueue (a genuine deny will deny again — nothing to retry).
//   - seedFailOperational → Warn log + snowplow_phase1_seed_operational_fail_total
//     + RE-ENQUEUE (a transient apiserver/ctx failure may succeed on retry).
// The back-compat grand total pipBindingSetSeedFailuresTotal is kept as the
// SUM of the two new counters (see phase1_pip_metrics.go) so existing
// dashboards do not break.
//
// FAIL-LOUD DEFAULT (design §1.3 rule 5). An UNCLASSIFIED error defaults
// to seedFailOperational — it is surfaced (Warn) + retried, never silently
// dropped as "expected." This is the inversion of the prior default-silent
// posture and is the crux of the fix.
//
// PRIOR ART (feedback_check_k8s_clientgo_prior_art, design §1.2).
// k8s.io/apimachinery/pkg/api/errors already provides the classification
// predicates — apierrors.IsForbidden/IsUnauthorized (RBAC deny),
// IsServiceUnavailable/IsServerTimeout/IsTimeout/IsInternalError
// (operational), and the APIStatus interface for code extraction (the repo
// already uses apierrors.IsForbidden at objects/get.go:210). No custom
// error library is added — only a thin snowplow-side classifier + an
// ~8-LOC helper that repairs the restaction-path string-flattening so the
// 403 code survives to the call site.

package dispatchers

import (
	"context"
	"errors"
	"github.com/krateo-platformops/snowplow/internal/statusdetails"

	"github.com/krateo-platformops/plumbing/http/response"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// seedFailClass is the classification verdict for a seedCohort /
// seedOneRestaction / seedOneWidget error. It carries the call site's
// branch decision (log level + counter + re-enqueue) without re-parsing
// strings.
type seedFailClass int

const (
	// seedFailNone — err == nil.
	seedFailNone seedFailClass = iota

	// seedFailRBACDeny — EXPECTED: a 403/401 from the apiserver (a narrow
	// cohort that genuinely cannot read the seed target). Logged Info; NOT
	// re-enqueued (it would deny again).
	seedFailRBACDeny

	// seedFailOperational — UNEXPECTED: ctx timeout/cancel, transport
	// error, 5xx, server-timeout, snapshot-not-ready, panic, OR any
	// unclassified error (fail-loud default). Logged Warn; re-enqueued.
	seedFailOperational
)

// classifySeedErr decides the verdict for a seed error, in the order the
// design (§1.3) specifies. The ordering matters: ctx cancellation is
// checked BEFORE the apierrors predicates because a deadline-exceeded that
// wrapped a downstream apierror must classify as operational (we want it
// retried), and the apierrors predicates thread through %w via errors.As
// so a ctx error that ALSO embeds a StatusError would otherwise misroute.
func classifySeedErr(err error) seedFailClass {
	// 1. nil → none.
	if err == nil {
		return seedFailNone
	}

	// 2. ctx timeout / parent cancel → operational. seedCohort's :692/:736
	//    returns wrap ctx.Err() with %w, so errors.Is threads through.
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
		return seedFailOperational
	}

	// 3. 403 / 401 → RBAC deny (expected). Covers the widget path's
	//    %w-wrapped *apierrors.StatusError and the restaction path's
	//    statusErrFromResponse-repaired error. apierrors.IsForbidden/
	//    IsUnauthorized use errors.As internally so wrapping is honoured.
	if apierrors.IsForbidden(err) || apierrors.IsUnauthorized(err) {
		return seedFailRBACDeny
	}

	// 4. 5xx / snapshot-not-ready / server-timeout → operational.
	if apierrors.IsServiceUnavailable(err) ||
		apierrors.IsServerTimeout(err) ||
		apierrors.IsTimeout(err) ||
		apierrors.IsInternalError(err) {
		return seedFailOperational
	}

	// 5. default → operational (FAIL-LOUD). An unclassified error is
	//    surfaced + retried, never silently dropped as "expected." This is
	//    the inversion of the prior default-silent posture (design §1.3).
	return seedFailOperational
}

// statusErrFromResponse repairs the restaction-path string-flattening
// (seedOneRestaction's old `fmt.Errorf(… got.Err.Message)` at
// phase1_pip_seed.go:828) by lifting the plumbing *response.Status back
// into a typed *apierrors.StatusError that carries the HTTP Code AND
// Reason. classifySeedErr can then see 403 vs 500 instead of an opaque
// string.
//
// Code AND Reason are both copied because apierrors' predicates match by
// reason FIRST, then fall back to code-only matching only when the reason
// is unknown (errors.go:reasonAndCodeForError + knownReasons). plumbing's
// response.StatusReason string constants are byte-identical to metav1's
// (e.g. "Forbidden", "InternalError", "ServiceUnavailable" — TRACED:
// plumbing/http/response/status.go vs apimachinery meta/v1/types.go), so
// copying the reason verbatim makes the reason-match path fire; copying
// the code makes the code-only fallback fire for any reason plumbing
// leaves empty. Either way the verdict is correct.
//
// Nil-safe: a nil *response.Status returns nil (caller treats as success).
func statusErrFromResponse(s *response.Status) error {
	if s == nil {
		return nil
	}
	st := metav1.Status{
		Status:  metav1.StatusFailure,
		Code:    int32(s.Code),
		Reason:  metav1.StatusReason(s.Reason),
		Message: s.Message,
	}
	// #1429 — a read that found its TYPE not installed says which type in its
	// details; that, and only that, travels with the error (statusdetails).
	if gvr, ok := statusdetails.MissingType(s); ok {
		st.Details = statusdetails.TypeNotInstalledMeta(gvr)
	}
	return &apierrors.StatusError{ErrStatus: st}
}
