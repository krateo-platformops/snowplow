// resolved_key_version_golden_test.go — coverage-audit L3.
//
// resolvedKeyVersion (resolved.go) is the salt folded FIRST into every
// ComputeKey hash so a key-schema change forces a clean rolling-restart break:
// no pre-bump entry ever serves as a post-bump hit. This test pins two things:
//
//  1. resolvedKeyVersion == "v8" — the current schema generation (#435: the raFullList RBACSubGen fold; v7 was #423, the
//     SubjectBindingSet fold; v6 was #118 (c)-v2).
//     A silent bump here without a matching golden update fails loud, forcing a
//     deliberate decision (matches the compile-once golden discipline).
//
//  2. A GOLDEN DIGEST anchor for a fixed ResolvedKeyInputs. The golden was
//     captured from the real ComputeKey; if the key ENCODING (field order,
//     terminators, version fold) drifts, the digest changes and this reds.
//
// RED PROOF: dropping the version prefix from the key encoding leaves the
// digest UNCHANGED for a same-version corpus — the version salt's whole job is
// to rotate the space across bumps. The shadow computeKeyWithoutVersionPrefix
// produces a DIFFERENT digest than the real (version-prefixed) golden, proving
// the version fold materially contributes to the hash; a real impl that dropped
// the prefix would match the shadow's digest, not the golden.
package cache

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"strconv"
	"testing"
)

// goldenKeyInputs is the fixed corpus the golden digest anchors. Every field is
// populated (incl RBACSubGen and Extras) so the golden exercises the full
// encoding path. DO NOT change this without re-capturing the golden.
func goldenKeyInputs() ResolvedKeyInputs {
	return ResolvedKeyInputs{
		CacheEntryClass: CacheEntryClassRestactions,
		Group:           "g", Version: "v", Resource: "r",
		Namespace: "ns", Name: "n",
		BindingUID:        "C:uid-fixed",
		SubjectBindingSet: "sbs-fixed",
		PerPage:           20, Page: 1,
		RBACSubGen: 7,
		Extras:     map[string]any{"a": "1", "z": float64(2)},
	}
}

// goldenResolvedKeyDigest is the SHA-256 hex ComputeKey produces for
// goldenKeyInputs() under resolvedKeyVersion "v8". Captured empirically from the
// real implementation. A drift in the key encoding (or an unannounced version
// bump) changes this and reds the test.
// gitleaks:allow — this is a SHA-256 test golden (ComputeKey output), not a credential.
const goldenResolvedKeyDigest = "c302990dc7748f6111df7715604cedfebcee5f629ea2cad8a4c3c2a123f462af" //gitleaks:allow

// TestResolvedKeyVersion_ConstantAnchor is L3 part 1: the version const is "v8".
func TestResolvedKeyVersion_ConstantAnchor(t *testing.T) {
	if resolvedKeyVersion != "v8" {
		t.Fatalf("resolvedKeyVersion = %q, want %q — a key-schema bump must be a "+
			"DELIBERATE change; update this anchor and the golden digest together",
			resolvedKeyVersion, "v8")
	}
}

// TestResolvedKeyVersion_GoldenDigest is L3 part 2: the golden digest anchor.
func TestResolvedKeyVersion_GoldenDigest(t *testing.T) {
	got := ComputeKey(goldenKeyInputs())
	if got != goldenResolvedKeyDigest {
		t.Fatalf("ComputeKey golden drift: got %q, want %q — the key ENCODING changed "+
			"(field order / terminators / version fold). If intentional, bump "+
			"resolvedKeyVersion and re-capture the golden.", got, goldenResolvedKeyDigest)
	}
}

// --- RED-arm proof ----------------------------------------------------------

// computeKeyWithoutVersionPrefix is the L3 WRONG impl: it re-encodes the SAME
// key material as the real ComputeKey (identical field order + terminators) but
// OMITS the leading version-prefix write. This is COMPUTED at runtime (not a
// copied constant), so it stays faithful even if the encoding evolves — as long
// as the only intended difference is the missing version fold.
func computeKeyWithoutVersionPrefix(in ResolvedKeyInputs) string {
	h := sha256.New()
	// NOTE: the version prefix write is DELIBERATELY OMITTED here — that is the
	// whole defect this shadow models.
	h.Write([]byte(in.CacheEntryClass))
	h.Write([]byte{0})
	h.Write([]byte(in.Group))
	h.Write([]byte{0})
	h.Write([]byte(in.Version))
	h.Write([]byte{0})
	h.Write([]byte(in.Resource))
	h.Write([]byte{0})
	h.Write([]byte(in.Namespace))
	h.Write([]byte{0})
	h.Write([]byte(in.Name))
	h.Write([]byte{0})
	if in.CacheEntryClass != CacheEntryClassWidgetContent {
		h.Write([]byte(in.BindingUID))
		h.Write([]byte{0xff})
		h.Write([]byte(in.SubjectBindingSet))
		h.Write([]byte{0xfd})
		var subgen [8]byte
		binary.LittleEndian.PutUint64(subgen[:], in.RBACSubGen)
		h.Write(subgen[:])
		h.Write([]byte{0xfe})
	}
	h.Write([]byte(strconv.Itoa(in.PerPage)))
	h.Write([]byte{0})
	h.Write([]byte(strconv.Itoa(in.Page)))
	h.Write([]byte{0})
	if in.Stage != "" {
		h.Write([]byte{0x01})
		h.Write([]byte(in.Stage))
		h.Write([]byte{0})
	}
	if len(in.Extras) > 0 {
		if buf, err := canonicaliseExtras(in.Extras); err == nil {
			h.Write(buf)
		}
	}
	h.Write([]byte{0})
	return hex.EncodeToString(h.Sum(nil))
}

// TestResolvedKeyVersion_DropPrefix_RedArm proves the L3 golden discriminates
// against a version-prefix-dropping impl: the no-version-prefix re-encoding
// produces a DIFFERENT digest than the version-prefixed golden. If the version
// fold were a no-op, the two would be equal and the golden could not detect a
// dropped-prefix regression.
func TestResolvedKeyVersion_DropPrefix_RedArm(t *testing.T) {
	in := goldenKeyInputs()
	real := ComputeKey(in)
	shadow := computeKeyWithoutVersionPrefix(in)

	// Sanity: the real key matches the pinned golden (guards against the shadow
	// accidentally equalling a drifted golden).
	if real != goldenResolvedKeyDigest {
		t.Fatalf("live ComputeKey no longer matches the version-prefixed golden anchor: %q", real)
	}
	// The RED proof: dropping the version prefix MUST change the digest.
	if shadow == real {
		t.Fatalf("RED-arm sanity: the no-version-prefix re-encoding MUST differ from the " +
			"version-prefixed golden; if equal, dropping the version fold would be " +
			"undetectable and L3 would not discriminate")
	}
}
