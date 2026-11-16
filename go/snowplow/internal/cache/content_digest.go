// content_digest.go — #1324: a deterministic digest of the bytes a /call
// answered with, so a /refreshes arm can tell whether the tab is behind.
//
// THE GAP. snowplow keeps no replay: a refresh frame published while a tab's
// key is not armed on any stream reaches nobody. That happens between a tab's
// /call and its arm, and — because the SPA aborts and re-opens its single
// stream on every widget mount and unmount (refreshSse.ts scheduleReconnect,
// deliberately without re-validation) — for EVERY armed key across every
// navigation. Before #1321 an identical republish about every 60 s quietly
// healed such a tab; after it nothing does until the next real change.
//
// THE CONTRACT (docs/howto-frontend-refresh-digest-catch-up.md):
//
//   - every armed /call response stamps X-Snowplow-Refresh-Digest: the
//     digest of exactly the bytes it wrote (next to X-Snowplow-Refresh-Key);
//   - the tab echoes it back per coordinate as `held` when it arms;
//   - on subscribe snowplow computes, for that subscriber, the digest of the
//     bytes the same /call would write NOW and offers exactly one refresh
//     frame for every key where it differs or whose cell is gone. An absent
//     or empty `held` means no catch-up (today's behaviour).
//
// THE DIGEST. "v1." + the first 16 bytes of SHA-256 over the body, hex. It is
// a function of the bytes only, so it survives restarts and replicas (a
// per-process counter could coincide after a restart and hide a change). The
// prefix names the algorithm: a client compares it as an opaque string, and a
// future change of algorithm makes every held digest differ once (one frame
// per key, the safe direction), never match by accident.
//
// THE ANSWER, NOT ONLY THE BODY (#1319). The portal renders a widget's
// X-Snowplow-Stage-Outcomes (an exception-only notice), so what a tab holds is
// the body AND the stage outcomes it was answered with. AnswerDigest folds the
// stored outcomes in when a stage did not succeed (the body, a 0x00 byte —
// which JSON never carries unescaped — and the outcomes); an answer whose
// stages all succeeded keeps the body-only digest, byte-identical to #1324.
package cache

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
)

// contentDigestPrefix versions the digest algorithm (see the file comment).
const contentDigestPrefix = "v1."

// ContentDigest is the deterministic digest of b (#1324): "v1." + hex of the
// first 16 bytes of SHA-256(b).
func ContentDigest(b []byte) string {
	sum := sha256.Sum256(b)
	return contentDigestPrefix + hex.EncodeToString(sum[:16])
}

// AnswerDigest (#1319) is the digest of an answer: ContentDigest(body) when
// stageOutcomes is "" (every stage succeeded), else the digest of the body
// followed by a 0x00 byte and the stored stage outcomes. Same "v1." prefix: a
// complete answer's digest is unchanged, and an incomplete one differs from
// every complete one.
func AnswerDigest(body []byte, stageOutcomes string) string {
	if stageOutcomes == "" {
		return ContentDigest(body)
	}
	h := sha256.New()
	h.Write(body)
	h.Write([]byte{0})
	h.Write([]byte(stageOutcomes))
	return contentDigestPrefix + hex.EncodeToString(h.Sum(nil)[:16])
}

// SameAnswer (#1319) reports whether two entries answer a /call identically:
// the same body and the same stored stage outcomes. It is the one "identical"
// every compare reads (the publish gate's writeOutcome, the refresher's and
// #406's sameBytesPrior), so the frame rule and the digest agree.
func SameAnswer(a, b *ResolvedEntry) bool {
	return a != nil && b != nil && a.StageOutcomes == b.StageOutcomes && a.RawLen() == b.RawLen() && bytes.Equal(a.Raw(), b.Raw())
}

// ContentDigest is the memoised AnswerDigest of e (its body, Raw(), and its
// StageOutcomes): computed once per entry, on first use, then read with one
// atomic load. Entries are immutable once stored, so every racer computes the
// same value and whichever publishes first wins. "" for a nil entry.
func (e *ResolvedEntry) ContentDigest() string {
	if e == nil {
		return ""
	}
	if d := e.digest.Load(); d != nil {
		return *d
	}
	d := AnswerDigest(e.Raw(), e.StageOutcomes)
	e.digest.CompareAndSwap(nil, &d)
	return *e.digest.Load()
}
