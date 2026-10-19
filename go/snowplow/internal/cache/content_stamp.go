// content_stamp.go — #765 option A: the content stamp.
//
// THE COST IT REMOVES (#606 run 8, architect-623 on #765). Every refresher
// re-resolve whose trigger set held a content cell's GVR discarded that shared,
// identity-free cell and re-filled it (apistageContentServe's R1 content-shield,
// forceContentMiss). N dependent cells refreshed after ONE change therefore meant
// up to N re-fills of the same list — at 50K objects ~3.4 s each, about 2.3 of
// the ~3.2 busy cores in run 8's no-load window.
//
// THE RULE. The R1 shield exists so that a refreshed cell is never derived from
// a snapshot OLDER than the change that triggered its refresh. A content cell
// therefore carries the dep-event seq its read started at (ResolvedEntry.
// ContentAsOf — the fill flight's #375 content-sink startSeq), and a refresher
// re-resolve carries the dep-event seq at its dequeue (RefreshTriggerAsOf). The
// cell is re-filled only when it might be older than a trigger it consumed;
// otherwise it is reused (ContentStampCovers).
//
// WHY THE DEQUEUE SEQ BOUNDS EVERY CONSUMED TRIGGER. A trigger GVR reaches the
// key's trigger set only through a dirty-mark (OnObjectEvent, the type-level
// handlers) or a #375 remark, and every one of them advances depEventSeq BEFORE
// the fan-out that merges the trigger (R2, Store-before-mark). The dequeue reads
// depEventSeq AFTER its LoadAndDelete of the set, so every trigger it consumed
// came from an event whose seq is ≤ that value. And R1 — the bump runs only in
// the post-indexer handler — means a read that started at seq ≥ s sees event s.
//
// Events AFTER the dequeue are not this refresh's obligation: they re-mark the
// key (another dequeue), and the outer resolve's #375 Put-check remarks a Put
// that raced them. Nothing about that changes here.
package cache

import (
	"context"
	"sync/atomic"

	"k8s.io/apimachinery/pkg/runtime/schema"
)

type ctxKeyRefreshTriggerAsOfType struct{}

var ctxKeyRefreshTriggerAsOf = ctxKeyRefreshTriggerAsOfType{}

// withRefreshTriggerAsOf stamps the dequeue's dep-event seq on a refresher
// re-resolve ctx (#765). Set ONLY by processNext, right after the trigger set
// was consumed.
func withRefreshTriggerAsOf(ctx context.Context, seq uint64) context.Context {
	if ctx == nil {
		return ctx
	}
	return context.WithValue(ctx, ctxKeyRefreshTriggerAsOf, seq)
}

// RefreshTriggerAsOf returns the dep-event seq this refresher re-resolve must be
// at least as fresh as (#765): every trigger in its set came from an event at or
// before it. A ctx the refresher's dequeue did not stamp (a test, any other
// caller of WithRefreshTriggerGVR[s]) gets the CURRENT seq — later, so stricter:
// it can only cause a re-fill the stamp would have avoided, never a reuse it
// would have refused.
func RefreshTriggerAsOf(ctx context.Context) uint64 {
	if ctx != nil {
		if v, ok := ctx.Value(ctxKeyRefreshTriggerAsOf).(uint64); ok {
			return v
		}
	}
	return depEventSeq.Load()
}

// ContentStampCovers reports whether a content cell whose read started at dep-event
// seq asOf may serve a refresher re-resolve that must be fresh as of need, for the
// cell's own coordinate (gvr, namespace, name; name "" = the LIST). True iff asOf is
// known (non-zero) and either
//
//	(a) asOf >= need — the read started after every trigger the refresh consumed
//	    was indexed; or
//	(b) the coordinate saw NO event since asOf — its bucket (or, cold, its GVR
//	    floor: the #375 fix-ii test the Put-check and the #710 join rule use) has
//	    not moved, and no type-level event on the GVR happened since asOf (a
//	    listOnly type event leaves a GET cell's bucket untouched).
//
// asOf == 0 means "unknown" (a cell no fill leader stored): never covered, the
// pre-#765 forced re-fill. Uniform over every GVR — no resource special case.
func ContentStampCovers(gvr schema.GroupVersionResource, namespace, name string, asOf, need uint64) bool {
	if asOf == 0 {
		return false
	}
	if asOf >= need {
		return true
	}
	if ContentCoordinateMovedSince(gvr, namespace, name, asOf) {
		return false
	}
	return Deps().gvrLastTypeBumpSeq(gvr) <= asOf
}

// gvrLastTypeBumpSeq returns the seq of the last type-level event on gvr, 0 when
// none (#765).
func (d *DepTracker) gvrLastTypeBumpSeq(gvr schema.GroupVersionResource) uint64 {
	if d == nil {
		return 0
	}
	if v, ok := d.gvrLastTypeBump.Load(gvr); ok {
		return v.(*atomic.Uint64).Load()
	}
	return 0
}
