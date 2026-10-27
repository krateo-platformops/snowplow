// put_verdict.go — THE Put-gate verdict (#912 re-review, shared-verdict form).
//
// Every site that stores a resolve's output in L1, or marks a key for the
// background (the seed and walk declined-external marks, the refresher's
// suppression), decides from what the resolve recorded on its Put-gate sinks:
// a declared write (#903 F1), stage errors and per-identity forbiddens (#681),
// genuine external touches. PutGateVerdict is the one place they are combined;
// each input keeps its own reason.
//
// THE TYPE ENFORCES IT. A verdict cannot be forged outside this package (its
// fields are unexported, and the zero value admits nothing), and every store
// method a resolve output is written through takes one and refuses to store
// unless it allows the Put (put_gated.go). A site therefore never decides
// "store" from a decline reason it did not recognise: the reason only selects
// the log line and the metric. Mark sites read Permanent().
//
// It covers the sink-derived signals only. The checks that are not a property
// of what the resolve recorded stay at their sites: the inert (dry-run) skip,
// the sensitive-read decline, the #424 class-drift re-check and the UAF decline
// (whose rule depends on the cell's key class, not on the sinks alone).
//
// scripts/check_resolved_entry_put_sites.sh enforces that the resolve-output
// packages write only through the verdict-gated methods.

package cache

import "context"

// PutDecision is the verdict's closed set of outcomes.
type PutDecision uint8

const (
	// putUndecided is the zero value: a verdict nobody computed. It admits
	// nothing and is neither a transient nor a permanent decline.
	putUndecided PutDecision = iota
	// PutStore — nothing the resolve recorded forbids storing it.
	PutStore
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

// PutVerdict is the combined Put-gate decision for one resolve. Only
// PutGateVerdict (and WithExternalTTL on its result) produce one.
type PutVerdict struct {
	decision    PutDecision
	reason      PutReason
	denialOnly  bool
	externalTTL bool
}

// Store reports whether the verdict allows the Put.
func (v PutVerdict) Store() bool { return v.decision == PutStore }

// Permanent reports whether the decline is structurally permanent.
func (v PutVerdict) Permanent() bool { return v.decision == PutDeclinePermanent }

// Reason is the decline reason (PutReasonNone when the verdict stores, except
// an external-TTL store, which keeps PutReasonExternal). It selects a log line
// and a metric; it never decides whether to store.
func (v PutVerdict) Reason() PutReason { return v.reason }

// DenialOnly reports a store although stage errors were recorded, because every
// one was a per-identity forbidden (#681): the site counts it.
func (v PutVerdict) DenialOnly() bool { return v.denialOnly }

// ExternalTTL reports a store that the widget's external-cache opt-in allowed:
// the cell must carry the bounded external TTL and no dep edges.
func (v PutVerdict) ExternalTTL() bool { return v.externalTTL }

// WithExternalTTL applies a widget's opt-in external-TTL cache
// (krateo.io/external-cache-ttl-seconds): a verdict whose ONLY reason to decline
// is a genuine external touch becomes a store with ExternalTTL. Every other
// verdict — a declared write (#915), a stage error, a store — is returned
// unchanged.
func (v PutVerdict) WithExternalTTL(optedIn bool) PutVerdict {
	if optedIn && v.reason == PutReasonExternal && v.decision == PutDeclinePermanent {
		return PutVerdict{decision: PutStore, reason: PutReasonExternal, externalTTL: true}
	}
	return v
}

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
		return PutVerdict{decision: PutDeclinePermanent, reason: PutReasonDeclaredWrite}
	case g.Stage.Count() > 0 && !DenialOnly(g.Stage, g.Forbidden, g.External, class):
		d := PutDeclineTransient
		if external > 0 {
			d = PutDeclinePermanent
		}
		return PutVerdict{decision: d, reason: PutReasonStageError}
	case external > 0:
		return PutVerdict{decision: PutDeclinePermanent, reason: PutReasonExternal}
	}
	return PutVerdict{decision: PutStore, denialOnly: g.Stage.Count() > 0}
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
