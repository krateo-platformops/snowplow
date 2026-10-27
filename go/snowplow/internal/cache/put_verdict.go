// put_verdict.go — THE Put-gate verdict (#912 re-review, shared-verdict form).
//
// Every site that stores a resolve's output in L1, or marks a key for the
// background (the seed and walk declined-external marks, the refresher's
// suppression), decides from what the resolve recorded on its Put-gate sinks:
// a declared write (#903 F1), stage errors and per-identity forbiddens (#681),
// genuine external touches. Before this file each site combined those signals
// by hand, and "declares a write" was folded into the external count — so the
// refresher logged a write RESTAction as external_touched, the external-skip
// counter counted it, and a site that only checked the count could not tell a
// write from an external read (#915's external-TTL branch had to special-case
// it). PutGateVerdict is the one place they are combined; each input keeps its
// own reason.
//
// It covers the sink-derived signals only. The checks that are not a property
// of what the resolve recorded stay at their sites: the inert (dry-run) skip,
// the sensitive-read decline, the #424 class-drift re-check and the UAF decline
// (whose rule depends on the cell's key class, not on the sinks alone).
//
// scripts/check_resolved_entry_put_sites.sh enforces that every function that
// builds a ResolvedEntry reads this verdict, or carries a reasoned waiver.

package cache

import "context"

// PutDecision is the verdict's closed set of outcomes.
type PutDecision uint8

const (
	// PutStore — nothing the resolve recorded forbids storing it.
	PutStore PutDecision = iota
	// PutDeclineTransient — do not store this result, but a later resolve of the
	// same inputs may be storable (a transient stage error): retry it.
	PutDeclineTransient
	// PutDeclinePermanent — no resolve of these inputs is ever storable (it
	// declares a write, or reads an endpoint no dep edge can invalidate): the
	// background marks it and stops re-resolving it.
	PutDeclinePermanent
)

// PutReason is the verdict's closed set of decline reasons.
type PutReason string

const (
	PutReasonNone PutReason = ""
	// PutReasonDeclaredWrite — the RESTAction declares a write-verb stage
	// (#903 F1): every /call must run it, so it is never stored, whether or not
	// this resolve sent the write.
	PutReasonDeclaredWrite PutReason = "declared_write"
	// PutReasonStageError — a stage error that is not per-identity forbidden
	// (#681 DenialOnly false): an under-served result.
	PutReasonStageError PutReason = "stage_error"
	// PutReasonExternal — a genuine external touch (#681 C2): no dep edge can
	// invalidate the result.
	PutReasonExternal PutReason = "external_touched"
)

// PutVerdict is the combined Put-gate decision for one resolve.
type PutVerdict struct {
	Decision PutDecision
	Reason   PutReason
	// DenialOnly — stored although stage errors were recorded, because every one
	// was a per-identity forbidden (#681): the site counts it.
	DenialOnly bool
}

// Store reports whether the verdict allows the Put.
func (v PutVerdict) Store() bool { return v.Decision == PutStore }

// Permanent reports whether the decline is structurally permanent.
func (v PutVerdict) Permanent() bool { return v.Decision == PutDeclinePermanent }

// PutGateVerdict combines the Put-gate sinks of one resolve. class is the
// cell's entry class (the #681 DenialOnly rule refuses denial-only bodies for
// identity-free classes). Precedence, first match wins:
//
//  1. a declared write                     → permanent, declared_write
//  2. a stage error that is not denial-only → stage_error; permanent when the
//     resolve also touched a genuine external endpoint (no re-resolve can ever
//     be stored, #903 B2), else transient
//  3. a genuine external touch             → permanent, external_touched
//  4. otherwise                            → store (DenialOnly when stage errors
//     were recorded and every one was a per-identity forbidden)
//
// nil sinks read as empty.
func PutGateVerdict(g PutGateSinks, class string) PutVerdict {
	external := GenuineExternal(g.External, g.Forbidden)
	switch {
	case g.External.DeclaredWrite():
		return PutVerdict{Decision: PutDeclinePermanent, Reason: PutReasonDeclaredWrite}
	case g.Stage.Count() > 0 && !DenialOnly(g.Stage, g.Forbidden, g.External, class):
		d := PutDeclineTransient
		if external > 0 {
			d = PutDeclinePermanent
		}
		return PutVerdict{Decision: d, Reason: PutReasonStageError}
	case external > 0:
		return PutVerdict{Decision: PutDeclinePermanent, Reason: PutReasonExternal}
	}
	return PutVerdict{Decision: PutStore, DenialOnly: g.Stage.Count() > 0}
}

// PutGateSinksFromContext returns the Put-gate sinks ctx carries (nil fields
// for the ones it does not).
func PutGateSinksFromContext(ctx context.Context) PutGateSinks {
	return PutGateSinks{
		Stage:     StageErrorSinkFromContext(ctx),
		Forbidden: PerIdentityForbiddenSinkFromContext(ctx),
		External:  ExternalTouchedSinkFromContext(ctx),
	}
}
