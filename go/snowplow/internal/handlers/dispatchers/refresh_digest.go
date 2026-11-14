// refresh_digest.go — #1324: the X-Snowplow-Refresh-Digest header on an armed
// /call response, and the arm-time check that tells a /refreshes connection
// which of its keys the tab is BEHIND on (cache/content_digest.go has the
// why; docs/howto-frontend-refresh-digest-catch-up.md is the contract).
//
// WHICH BYTES. The digest names exactly the bytes the response wrote (and,
// #1319, the stored X-Snowplow-Stage-Outcomes it answered with when a stage
// did not succeed: cache.AnswerDigest), and the arm-time check recomputes
// exactly the answer the same /call would write NOW for the subscriber's
// identity. Every transform between the stored L1 cell
// and the served body, by armed class:
//
//   - restactions (hit and cold) and widgets (hit and cold): none. The body is
//     the stored cell verbatim (writeResolvedJSON(entry.RawJSON) on a hit,
//     writeResolvedJSON(encoded) after PutIfGen stored `encoded` on a cold
//     fill). Pagination (perPage/page) and the request extras are folded
//     into the KEY, never sliced at serve time, and the key folds the
//     requester's RBAC class (BindingUID + SubjectBindingSet), so every
//     identity that shares one of these keys is served the same bytes. The
//     digest is the entry's memoised ContentDigest (one SHA-256 per entry).
//   - widgetContent (the identity-free shell, hit only — the cold path never
//     stamps a widgetContent key): gateWidgetEnvelope rewrites every
//     status.resourcesRefs.items[].allowed under THIS requester (rbac.UserCan),
//     so two identities sharing the key are served DIFFERENT bytes. The digest
//     is of the gated bytes the response wrote, and the arm runs the same
//     gateWidgetEnvelope under the subscriber's identity and digests that:
//     the same function on the same inputs, so equal digests mean the tab
//     holds exactly what /call would answer now. Digesting the shell instead
//     would let one user's verdict change hide behind another's unchanged
//     shell (or trigger the other's catch-up).
//
// No other class is armable (DeriveSubscriptionKey's allowlist), and the
// gzip middleware compresses after the handler: the digest is of the JSON the
// handler wrote, before transport encoding.
package dispatchers

import (
	"context"
	"net/http"
	"sort"

	xcontext "github.com/krateo-platformops/plumbing/context"
	"github.com/krateo-platformops/snowplow/internal/cache"
)

// RefreshDigestHeader carries the deterministic digest of the exact bytes an
// armed /call response wrote (#1324). Stamped only alongside
// X-Snowplow-Refresh-Key; MUST be in CORS ExposedHeaders (main.go).
const RefreshDigestHeader = "X-Snowplow-Refresh-Digest"

// stampRefreshDigest sets X-Snowplow-Refresh-Digest to digest() — only when
// this response stamped X-Snowplow-Refresh-Key (so the two headers are always
// consistent, and an unarmed response pays no hash). Call it right after the
// setRefreshKeyHeader* call, before writeResolvedJSON.
func stampRefreshDigest(wri http.ResponseWriter, digest func() string) {
	if wri.Header().Get(refreshKeyHeader) == "" {
		return
	}
	if d := digest(); d != "" {
		wri.Header().Set(RefreshDigestHeader, d)
	}
}

// HeldDigests is what one /refreshes arm carried for one key: the class it
// was armed under and every non-empty `held` digest its coordinates sent (two
// widgets of one tab can share a key).
type HeldDigests struct {
	Class   string
	Digests []string
}

// BehindKeys returns, sorted, the keys in held whose cell is gone or whose
// current served digest for ctx's identity differs from ANY digest the tab
// holds for it (#1324). Each such key gets one catch-up frame; every other
// key gets none. A key with no held digest is never in held (no catch-up:
// today's behaviour). Reads only (GetNoTouch: no warmth, no hit accounting).
func BehindKeys(ctx context.Context, held map[string]HeldDigests) []string {
	if len(held) == 0 {
		return nil
	}
	store := cache.ResolvedCache()
	var behind []string
	for key, h := range held {
		cur := ""
		if store != nil {
			//serve-probe: #1324 arm catch-up — digests what /call would serve now, serves no body
			if e, ok := store.GetNoTouch(key); ok && e != nil {
				cur = servedDigestNow(ctx, h.Class, e)
			}
		}
		for _, d := range h.Digests {
			if cur == "" || d != cur {
				behind = append(behind, key)
				break
			}
		}
	}
	sort.Strings(behind)
	return behind
}

// servedDigestNow is the digest of the bytes the /call hit of class would
// write for ctx's identity from entry (see the file comment). "" when that
// hit would not serve the cell (the widgetContent gate fails closed): the tab
// is behind.
//
// #1430: the widgetContent digest is computed once per (answer, identity,
// published RBAC snapshot) and reused by every caller: the arm (ArmGated),
// the catch-up check (BehindKeys), a written frame (NoteFrameWritten) and a
// class move (GatedMoved). The /call hit that stamps the header fills it with
// the digest of the bytes it wrote (noteServedView); it never reads it.
func servedDigestNow(ctx context.Context, class string, e *cache.ResolvedEntry) string {
	if class == cache.CacheEntryClassWidgetContent {
		ui, err := xcontext.UserInfo(ctx)
		if err != nil {
			return "" // the gate refuses a request with no identity
		}
		// #1430 — one gate per (the cell's answer, this identity, the published
		// RBAC snapshot): every input the gate reads (cache/served_view_memo.go).
		return e.ServedViewDigest(cache.ServedViewIdentityOf(ui.Username, ui.Groups), func() (string, bool) {
			return gatedAnswerDigest(ctx, e)
		})
	}
	return e.ContentDigest()
}

// gatedAnswerDigest runs the serve-time gate over e for ctx's identity and
// digests the answer its /call hit would write: the uncached computation
// servedDigestNow memoises. False when that hit would not serve the cell.
func gatedAnswerDigest(ctx context.Context, e *cache.ResolvedEntry) (string, bool) {
	gated, served := gateWidgetEnvelope(ctx, e.Raw())
	if !served {
		return "", false
	}
	return cache.AnswerDigest(gated, e.StageOutcomes), true
}

// noteServedView stores the digest a widgetContent /call hit stamped for ctx's
// identity (#1430), so that identity's next /refreshes arm reuses it. before
// is cache.ServedViewRBACNow() read BEFORE the hit's gate ran.
func noteServedView(ctx context.Context, e *cache.ResolvedEntry, before cache.ServedViewRBAC, digest string) {
	ui, err := xcontext.UserInfo(ctx)
	if err != nil {
		return
	}
	e.NoteServedView(cache.ServedViewIdentityOf(ui.Username, ui.Groups), before, digest)
}
