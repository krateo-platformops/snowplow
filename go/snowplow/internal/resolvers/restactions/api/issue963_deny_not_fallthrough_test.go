package api

// issue963_deny_not_fallthrough_test.go — #961 x #988 (rebase review RV-DC on
// #963): a LIST the requester may not make is answered in process with the
// per-identity Forbidden. It is NOT an apiserver fall-through: the ctx
// dispatchViaInformer returns carries no fall-through reason, and no
// fall-through cell moves. Otherwise every refused LIST would count as a LIST
// sent to the apiserver (informer-fallthrough-rbac-deny) on the meter the bench
// and #988's attribution read, for a call that never dials.
//
// Positive control: in the same fall-through scope a write verb (which never
// pivots) does stamp a reason and moves the meter by exactly one, so the zero
// asserted for the deny is not an inert meter.
//
// Adopted from the #963 rebase review (comment 6522294279): GREEN on the
// rebased head, RED under DENYCNT (the informer deny returns
// RecordApiserverFallthrough(ctx, ReasonInformerRBACDeny, gvr)).

import (
	"net/http"
	"testing"

	"github.com/krateo-platformops/snowplow/internal/cache"
)

// r963FallthroughTotal sums every apiserver fall-through cell.
func r963FallthroughTotal() uint64 {
	cells, _ := cache.FallthroughCellsSnapshot()
	var n uint64
	for _, c := range cells {
		n += c.Count
	}
	return n
}

func TestR963_DenyIsNotAFallthrough(t *testing.T) {
	newRBACNarrowWatcher(t)
	ctx := cache.WithFallthroughScope(ctxWithUser(narrowUser), cache.ScopeCallRestactions)

	// The namespace-only tenant's cluster-wide LIST: refused (#961).
	before := r963FallthroughTotal()
	raw, served, retCtx, denied := dispatchViaInformer(ctx,
		buildCall(http.MethodGet, "/apis/templates.krateo.io/v1/restactions"))
	if served || raw != nil || !isRegateDenied(denied) {
		t.Fatalf("setup: want the per-identity Forbidden, got served=%v bytes=%d err=%v", served, len(raw), denied)
	}
	if r := cache.FallthroughReasonOf(retCtx); r != "" {
		t.Errorf("a deny returned a ctx stamped with fall-through reason %q; a deny is not a fall-through", r)
	}
	if got := r963FallthroughTotal() - before; got != 0 {
		t.Errorf("a deny moved the apiserver fall-through meter by %d; it must move none (nothing is dialed)", got)
	}

	// Positive control: the same scope counts a real fall-through.
	before = r963FallthroughTotal()
	_, served, fctx, denied := dispatchViaInformer(ctx,
		buildCall(http.MethodPost, "/apis/templates.krateo.io/v1/namespaces/x/restactions"))
	if served || denied != nil || cache.FallthroughReasonOf(fctx) == "" || r963FallthroughTotal()-before != 1 {
		t.Fatalf("positive control: a write fall-through served=%v denied=%v reason=%q moved=%d, want a stamped reason and exactly 1",
			served, denied, cache.FallthroughReasonOf(fctx), r963FallthroughTotal()-before)
	}
}
