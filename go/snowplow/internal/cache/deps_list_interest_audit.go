// deps_list_interest_audit.go — the never-skip-a-real-change audit of the
// #603 / #765 shadow (design §4, "Shadow (PR-2)").
//
// The shadow decides on every list_dep mark whether enforcement WOULD skip
// it, and marks it anyway. So every would-avoid mark still produces a refresh.
// When EVERY mark a cell received since its last accepted refresh Put was a
// would-avoid, enforcement would have produced no refresh at all and kept the
// resident bytes. The audit compares the refresh's rebuilt bytes with the bytes
// it replaces:
//
//	identical  — the skip would have been right;
//	changed    — WOULD_CHANGE: the skip would have left the cell stale. The
//	             owner's gate requires this to be zero;
//	no_prior   — nothing to compare (the cell was empty);
//	unaudited  — the refresh never reached an accepted Put (failed, declined,
//	             refused): its snapshot is restored, so the NEXT comparison is
//	             never made against bytes older than a kept mark.
//
// The four are published together, so "changed = 0" always reads as "N
// audited, 0 changed" and never as health by absence.
//
// MECHANICS. Per key, since the last accepted refresh Put: avoided (some mark
// was a would-avoid) and tainted (some enqueue was anything else). OnObjectEvent
// brackets the enqueue of an avoided mark with a one-shot token; the
// refresher's enqueue funnel (refresher.enqueue / enqueueClusterList, which
// every mark, remark, retry and explicit enqueue goes through) consumes the
// token, or taints. The refresher takes the pair at dequeue and carries it on
// the re-resolve ctx; the terminal write (dispatchers resolve_populate.go,
// beside the #612 compare) reports the bytes. Any race between the token and a
// concurrent other enqueue of the same key resolves to tainted, never to a
// spurious avoid-only.
//
// RESIDUAL NON-DETERMINISM (the #612 caveat) reads as changed. That can only
// fail the gate, never pass it falsely. A changed verdict logs the key hash and
// the two sha256 digests and sizes, never a body (debug-surface rule).
package cache

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"log/slog"
	"sync"
	"sync/atomic"
)

// Audit verdicts (closed enum, OTLP attribute).
const (
	AuditVerdictIdentical = "identical"
	AuditVerdictChanged   = "changed"
	AuditVerdictNoPrior   = "no_prior"
	AuditVerdictUnaudited = "unaudited"
)

type narrowAudit struct {
	cells sync.Map // l1Key -> *narrowAuditCell

	identical atomic.Uint64
	changed   atomic.Uint64
	noPrior   atomic.Uint64
	unaudited atomic.Uint64
}

type narrowAuditCell struct {
	mu      sync.Mutex
	token   int
	avoided bool
	tainted bool
}

// narrowAuditSnap is a key's pair, taken at dequeue.
type narrowAuditSnap struct {
	avoided, tainted bool
}

func (s narrowAuditSnap) avoidOnly() bool { return s.avoided && !s.tainted }

func (a *narrowAudit) cell(key string) *narrowAuditCell {
	c, _ := a.cells.LoadOrStore(key, &narrowAuditCell{})
	return c.(*narrowAuditCell)
}

// tokenBegin / tokenEnd bracket the enqueue of a would-avoid mark.
func (a *narrowAudit) tokenBegin(key string) {
	c := a.cell(key)
	c.mu.Lock()
	c.token++
	c.mu.Unlock()
}

func (a *narrowAudit) tokenEnd(key string) {
	if v, ok := a.cells.Load(key); ok {
		c := v.(*narrowAuditCell)
		c.mu.Lock()
		if c.token > 0 {
			c.token--
		}
		c.mu.Unlock()
	}
}

// onEnqueue is called by the refresher's enqueue funnel for every enqueue.
func (a *narrowAudit) onEnqueue(key string) {
	c := a.cell(key)
	c.mu.Lock()
	if c.token > 0 {
		c.token--
		c.avoided = true
	} else {
		c.tainted = true
	}
	c.mu.Unlock()
}

// take removes and returns the key's pair (the refresher's dequeue).
func (a *narrowAudit) take(key string) narrowAuditSnap {
	v, ok := a.cells.Load(key)
	if !ok {
		return narrowAuditSnap{}
	}
	c := v.(*narrowAuditCell)
	c.mu.Lock()
	s := narrowAuditSnap{avoided: c.avoided, tainted: c.tainted}
	c.avoided, c.tainted = false, false
	c.mu.Unlock()
	// The cell is NOT deleted here: an enqueue racing this take must land on
	// the same cell, never on a deleted one (a lost taint). Cells leave with
	// the key (RemoveL1Key → forget), so they are bounded by the store.
	return s
}

// restore merges a snapshot back (its refresh did not reach an accepted Put).
func (a *narrowAudit) restore(key string, s narrowAuditSnap) {
	if !s.avoided && !s.tainted {
		return
	}
	c := a.cell(key)
	c.mu.Lock()
	c.avoided = c.avoided || s.avoided
	c.tainted = c.tainted || s.tainted
	c.mu.Unlock()
}

func (a *narrowAudit) forget(key string) { a.cells.Delete(key) }

// The audit state lives on the REFRESHER (refresher.narrowAudit): the refresher
// owns the enqueue funnel and the dequeue, and its singleton is an atomic
// pointer, so the DepTracker side reaches it through refresherPeek without
// touching the refresher's lifetime. No refresher (cache off, a test with a
// recorder hook) = no audit, and every hook below is a no-op.

// narrowAuditTokenBegin / narrowAuditTokenEnd bracket the enqueue of a
// would-avoid mark (OnObjectEvent).
func narrowAuditTokenBegin(key string) {
	if r := refresherPeek(); r != nil {
		r.narrowAudit.tokenBegin(key)
	}
}

func narrowAuditTokenEnd(key string) {
	if r := refresherPeek(); r != nil {
		r.narrowAudit.tokenEnd(key)
	}
}

// narrowAuditForget drops key's pair (the key left the store).
func narrowAuditForget(key string) {
	if r := refresherPeek(); r != nil {
		r.narrowAudit.forget(key)
	}
}

// onEnqueueAudit is the refresher funnel's hook.
func (r *refresher) onEnqueueAudit(key string) {
	if !listNarrowShadowOn() || key == "" {
		return
	}
	r.narrowAudit.onEnqueue(key)
}

// --- the dequeue token ----------------------------------------------------------

type ctxKeyNarrowAuditType struct{}

var ctxKeyNarrowAudit = ctxKeyNarrowAuditType{}

// narrowAuditToken rides the refresher's re-resolve ctx for ONE dequeue.
type narrowAuditToken struct {
	key       string
	snap      narrowAuditSnap
	audit     *narrowAudit
	accepted  atomic.Bool
	verdicted atomic.Bool
}

// narrowAuditDequeue takes the key's pair and installs the token on ctx.
func (r *refresher) narrowAuditDequeue(ctx context.Context, key string) (context.Context, *narrowAuditToken) {
	if !listNarrowShadowOn() {
		return ctx, nil
	}
	t := &narrowAuditToken{key: key, snap: r.narrowAudit.take(key), audit: &r.narrowAudit}
	return context.WithValue(ctx, ctxKeyNarrowAudit, t), t
}

// narrowAuditSettle closes a dequeue: no accepted Put restores the snapshot
// (and counts an avoid-only refresh as unaudited); a missing entry drops it
// (the next body is a from-scratch resolve).
func narrowAuditSettle(t *narrowAuditToken, hadEntry bool) {
	if t == nil {
		return
	}
	a := t.audit
	if t.accepted.Load() {
		if t.snap.avoidOnly() && !t.verdicted.Load() {
			a.unaudited.Add(1)
		}
		return
	}
	if t.snap.avoidOnly() {
		a.unaudited.Add(1)
	}
	if hadEntry {
		a.restore(t.key, t.snap)
		return
	}
	// No resident entry: the next body is a from-scratch resolve, so the marks
	// before it are irrelevant to any later comparison.
	a.forget(t.key)
}

// NoteNarrowAuditWrite reports the refresher terminal write of key: prior is
// the entry being replaced (read just before), encoded the rebuilt bytes,
// stored the gen-guarded replace's result. A no-op off the refresher and for
// any key but the dequeued one.
func NoteNarrowAuditWrite(ctx context.Context, key string, prior *ResolvedEntry, hadPrior bool, encoded []byte, stored bool) {
	if ctx == nil {
		return
	}
	t, _ := ctx.Value(ctxKeyNarrowAudit).(*narrowAuditToken)
	if t == nil || t.key != key || !stored {
		return
	}
	t.accepted.Store(true)
	if !t.snap.avoidOnly() || !t.verdicted.CompareAndSwap(false, true) {
		return
	}
	a := t.audit
	switch {
	case !hadPrior || prior == nil || len(prior.RawJSON) == 0 || len(encoded) == 0:
		a.noPrior.Add(1)
	case bytes.Equal(prior.RawJSON, encoded):
		a.identical.Add(1)
	default:
		a.changed.Add(1)
		ps := sha256.Sum256(prior.RawJSON)
		ns := sha256.Sum256(encoded)
		slog.Warn("deps.list_narrow.would_change",
			slog.String("subsystem", "cache"),
			slog.String("key_hash", key),
			slog.String("prior_sha256", hex.EncodeToString(ps[:])),
			slog.String("rebuilt_sha256", hex.EncodeToString(ns[:])),
			slog.Int("prior_bytes", len(prior.RawJSON)),
			slog.Int("rebuilt_bytes", len(encoded)),
			slog.String("effect", "SHADOW only (#603/#765): every mark this cell received since its last refresh "+
				"was a would-avoid, and its refresh changed the bytes. Enforcement would have left it stale. "+
				"Nothing was skipped; the enforcement gate requires zero of these."),
		)
	}
}

// WithNarrowAuditAvoidOnlyForTest installs on ctx the dequeue token of an
// AVOID-ONLY refresh of key, as processNext would (counted on the refresher
// singleton). Cross-package test seam for the dispatchers wiring arm (the
// terminal write must report to the audit); production code MUST NOT call it.
func WithNarrowAuditAvoidOnlyForTest(ctx context.Context, key string) context.Context {
	r := refresherSingleton()
	t := &narrowAuditToken{key: key, snap: narrowAuditSnap{avoided: true}, audit: &r.narrowAudit}
	return context.WithValue(ctx, ctxKeyNarrowAudit, t)
}

// ListNarrowAuditSnapshot returns the audit verdict counts.
func ListNarrowAuditSnapshot() map[string]uint64 {
	a := &narrowAudit{}
	if r := refresherPeek(); r != nil {
		a = &r.narrowAudit
	}
	return map[string]uint64{
		AuditVerdictIdentical: a.identical.Load(),
		AuditVerdictChanged:   a.changed.Load(),
		AuditVerdictNoPrior:   a.noPrior.Load(),
		AuditVerdictUnaudited: a.unaudited.Load(),
	}
}
