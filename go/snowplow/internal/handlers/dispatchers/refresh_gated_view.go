// refresh_gated_view.go — #1328: a tab armed on a GATED key is told when an
// RBAC change alters what it is served, though the stored bytes did not move.
//
// THE GAP. A widgetContent shell is identity-free: one cell for every user,
// narrowed per requester at serve time (gateWidgetEnvelope re-derives each
// action's `allowed` with rbac.UserCan). So what a tab is SERVED depends on two
// inputs: the shell's bytes and the requester's RBAC. A change of the bytes is
// announced by the write that made it (cache/put_outcome.go). A change of the
// RBAC is not a write, and nothing announced it: the key folds no identity, so
// #819 never rotates it (refresh_rotation.go Derive tracks identity-bound
// keys only); the #258 RBAC-shift re-seed re-mints identity-bound keys only
// (prewarm_rbac_shift.go); and no RBAC event dirty-marks a shell (its dep
// edges are the widget, its apiRef and its action targets, never a binding or
// a role). On main the tab healed only incidentally, when some unrelated
// refresh of the shell re-Put identical bytes and the generation-gated publish
// announced them; #1321 withholds exactly those frames.
//
// THE MECHANISM. The frame decision for a gated key follows what THIS
// subscriber is served (the #1324 served digest, servedDigestNow), not the
// stored bytes. The connection keeps, per armed gated key, the digest its tab
// holds or will hold after the frames already written: seeded at arm, and
// re-read after every frame written on the key (the tab's refetch reads at
// least that view). When cache.IdentityClassesMayHaveMoved fires (every RBAC
// snapshot publish, rbac_snapshot.go) and THIS identity's class moved, each
// gated key's served digest is recomputed for this identity; a key whose
// digest differs gets one frame, at once, on this connection only.
//
// WHY THE CLASS TEST IS EXACT. rbac.IdentityClassOf is the binding-set digest
// plus the per-subject sub-generation: equal digests give equal verdicts for
// every RBAC check (#423), and the sub-generation moves on every grant, revoke
// or rules edit touching the subject (#118 (c), #435). Every input of the gate
// besides the shell's bytes is a UserCan verdict for this identity, so a
// publish that leaves the class unchanged cannot change what it is served.
// The test is the same one the identity-bound keys are built on, and it keeps
// a burst of unrelated RBAC events (per-composition RBAC) from re-gating every
// open tab's shells.
//
// GENERAL. Nothing names a class: a key is tracked when its class is
// identity-free (one cell narrowed per requester), and the digest is the one
// the /call hit of that class would write for this identity.
package dispatchers

import (
	"sort"

	"github.com/krateo-platformops/snowplow/internal/cache"
	"github.com/krateo-platformops/snowplow/internal/rbac"
)

type gatedArm struct {
	class string
	// served is the digest of what this connection's tab is served from the
	// key, as of its last frame (or the arm); "" when the cell is gone or the
	// gate refuses.
	served string
}

// trackGated records an armed identity-free (serve-time-gated) key.
func (t *SubscriptionRotationTracker) trackGated(key, class string) {
	if t.gated == nil {
		t.gated = map[string]*gatedArm{}
	}
	t.gated[key] = &gatedArm{class: class}
}

// servedNow is the digest of what this identity's /call hit of class would
// write from key's cell now ("" when there is none). Reads only (GetNoTouch).
func (t *SubscriptionRotationTracker) servedNow(key, class string) string {
	store := cache.ResolvedCache()
	if store == nil {
		return ""
	}
	//serve-probe: #1328 gated-view check — digests what /call would serve now, serves no body
	e, ok := store.GetNoTouch(key)
	if !ok || e == nil {
		return ""
	}
	return servedDigestNow(t.ctx, class, e)
}

// ArmGated seeds every armed gated key with what its tab is served now, after
// reading the identity's class: a change after the class read moves the class
// again, and GatedMoved re-checks. Call once, at arm, before the #1324
// catch-up check (a key the catch-up announces is re-read when its frame is
// written).
func (t *SubscriptionRotationTracker) ArmGated() {
	if len(t.gated) == 0 {
		return
	}
	t.gatedClass = rbac.IdentityClassOf(t.ui)
	for k, g := range t.gated {
		g.served = t.servedNow(k, g.class)
	}
}

// NoteFrameWritten re-reads what the tab will be served after a frame on key
// was written: its refetch reads at least this view. A no-op for a key that is
// not gated.
func (t *SubscriptionRotationTracker) NoteFrameWritten(key string) {
	if g, ok := t.gated[key]; ok {
		g.served = t.servedNow(key, g.class)
	}
}

// GatedMoved returns, sorted, the armed gated keys whose served view for this
// identity is no longer the one its tab holds, and records the new view (one
// frame per change). Nil when this identity's class did not move.
func (t *SubscriptionRotationTracker) GatedMoved() []string {
	if len(t.gated) == 0 {
		return nil
	}
	cur := rbac.IdentityClassOf(t.ui)
	if cur == t.gatedClass {
		return nil
	}
	t.gatedClass = cur
	var out []string
	for k, g := range t.gated {
		if d := t.servedNow(k, g.class); d != g.served {
			g.served = d
			out = append(out, k)
		}
	}
	sort.Strings(out)
	return out
}
