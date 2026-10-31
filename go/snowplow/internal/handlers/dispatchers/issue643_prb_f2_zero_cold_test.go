package dispatchers

// issue643_prb_f2_zero_cold_test.go — #643 PR-B F2: the zero-cold differential
// (I-1). The declined-external skip must change NO Put.
//
// Each arm runs the same pass sequence (boot, gvr-discovered ×2, keepwarm) on a
// fresh harness twice — once as shipped, once with the consult forced off
// through its one seam (declinedExternalSkipFn) — and compares the resulting L1:
// the key set AND the sha256 of every body must be identical. The fixture
// carries W_int: a widget whose RESTAction DECLARES the external endpointRef
// (apiref.DeclaresExternalEndpoint == true) but never dials it — the external
// stage iterates over no items — while its other stage reads this cluster with
// no endpointRef. It touches no external endpoint and IS cacheable. It must be
// Put in both runs. (Before #1017 W_int was "an external endpointRef on an
// apiserver path": that stage was answered from this cluster, the #1017 defect.
// A named endpointRef is now always sent to its endpoint, so a declared-but-not-
// dialled external stage is the shape that keeps I-5's distinction.)
//
// F2-m (mutation): the consult is replaced by the #594 predicate
// (DeclaresExternalEndpoint) — the criterion I-5 forbids. The differential must
// then FAIL, and specifically lose W_int. If it did not, F2 could not see the
// defect it exists for.
//
// Arms are RUN on a fast endpoint and on a slow one (every external fetch
// abandoned at the boot-readiness bound).
//
// Not t.Parallel(): reassigns package vars.

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"sort"
	"strings"
	"testing"
	"time"

	templatesv1 "github.com/krateo-platformops/snowplow/apis/templates/v1"
	"github.com/krateo-platformops/snowplow/internal/cache"
	"github.com/krateo-platformops/snowplow/internal/redact"
	"github.com/krateo-platformops/snowplow/internal/resolvers/widgets/apiref"
)

// h643WInt is W_int: it declares the external endpointRef (stage s1) but
// dials it for no item, and reads this cluster with stage s0 (no endpointRef).
var h643WInt = h643Extra{
	widget: "wint-w", ra: "wint-ra", external: false,
	api: []any{
		h643OwnStage("s0", "/apis/"+h643ThingsGVR.Group+"/"+h643ThingsGVR.Version+"/namespaces/"+h643NS+"/"+h643ThingsGVR.Resource),
		h643Iterate(h643ExtStage("s1", "/external/${ .id }"), "[]"),
	},
}

// h643L1Snapshot is every L1 key with the sha256 of its body.
func h643L1Snapshot() map[string]string {
	store := cache.ResolvedCache()
	out := map[string]string{}
	for _, k := range store.KeysForTest() {
		e, ok := store.GetNoTouch(k)
		if !ok || e == nil {
			continue
		}
		sum := sha256.Sum256(e.RawJSON)
		out[k] = hex.EncodeToString(sum[:])
	}
	return out
}

// h643WidgetKeyFor returns the L1 key of the widget cell for wName, or "".
func h643WidgetKeysFor(wName string) []string {
	store := cache.ResolvedCache()
	var out []string
	for _, k := range store.KeysForTest() {
		e, ok := store.GetNoTouch(k)
		if ok && e != nil && e.Inputs != nil && e.Inputs.Resource == h643WidgetGVR.Resource && e.Inputs.Name == wName {
			out = append(out, k)
		}
	}
	sort.Strings(out)
	return out
}

type h643F2Run struct {
	l1      map[string]string
	wint    []string
	skipped uint64
}

// h643F2Sequence runs the pass sequence on a fresh harness. consult replaces
// the skip consult when non-nil.
// Each run is its own subtest: the harness holds engineLatchTestMu until the
// subtest's cleanup, so two runs cannot share one test's lifetime.
func h643F2Sequence(t *testing.T, slow bool, consult func(context.Context, string, string, string, redact.Label) bool) h643F2Run {
	t.Helper()
	var out h643F2Run
	name := "skip-as-shipped"
	if consult != nil {
		name = "consult-replaced"
	}
	if !t.Run(name, func(t *testing.T) { out = h643F2SequenceRun(t, slow, consult) }) {
		t.FailNow()
	}
	return out
}

func h643F2SequenceRun(t *testing.T, slow bool, consult func(context.Context, string, string, string, redact.Label) bool) h643F2Run {
	t.Helper()
	o := h643Opts{cohorts: h643K, external: h643M, internal: h643N, things: 2, extra: []h643Extra{h643WInt}}
	if slow {
		o.slow = h643M
	}
	env := h643Setup(t, o)
	if slow {
		env.ep.stall = 5 * time.Second
	}
	if got := h643Cohorts(); got != h643K {
		t.Fatalf("fixture precondition: %d cohorts, want %d", got, h643K)
	}
	h643Budget(t, nil)
	prev := declinedExternalSkipFn
	if consult != nil {
		declinedExternalSkipFn = consult
	}
	defer func() { declinedExternalSkipFn = prev }()

	skip0 := h643SkipSnap()
	for _, s := range []prewarmScope{
		{kind: scopeKindBoot},
		{kind: scopeKindGVRDiscovered, gvr: h643WidgetGVR},
		{kind: scopeKindGVRDiscovered, gvr: h643RAGVR},
		{kind: scopeKindKeepwarm},
	} {
		env.pass(s)
	}
	var skipped uint64
	for _, n := range h643SkipDelta(skip0) {
		skipped += n
	}
	return h643F2Run{l1: h643L1Snapshot(), wint: h643WidgetKeysFor(h643WInt.widget), skipped: skipped}
}

func h643L1Diff(a, b map[string]string) []string {
	var d []string
	for k, v := range a {
		if w, ok := b[k]; !ok {
			d = append(d, "only-skip-on:"+k)
		} else if w != v {
			d = append(d, "body-differs:"+k)
		}
	}
	for k := range b {
		if _, ok := a[k]; !ok {
			d = append(d, "only-skip-off:"+k)
		}
	}
	sort.Strings(d)
	return d
}

func h643ForcedOff(context.Context, string, string, string, redact.Label) bool { return false }

func h643F2Differential(t *testing.T, slow bool) {
	t.Setenv("RESOLVER_SEED_EXTERNAL_FETCH_TIMEOUT_MS", "200")
	on := h643F2Sequence(t, slow, nil)
	off := h643F2Sequence(t, slow, h643ForcedOff)

	if on.skipped == 0 {
		t.Fatalf("precondition: the skip never fired in the skip-on run — the differential would be vacuous")
	}
	if off.skipped != 0 {
		t.Fatalf("precondition: the forced-off run still skipped %d units — the seam did not reach the consult", off.skipped)
	}
	if len(on.wint) != h643K || len(off.wint) != h643K {
		t.Fatalf("W_int (declares the external endpointRef, dials it for no item; cacheable) must be Put for all %d cohorts in BOTH runs: skip-on=%d skip-off=%d", h643K, len(on.wint), len(off.wint))
	}
	if d := h643L1Diff(on.l1, off.l1); len(d) != 0 {
		t.Fatalf("I-1 VIOLATED: skip on vs off produced different L1 (%d keys vs %d):\n%s", len(on.l1), len(off.l1), strings.Join(d, "\n"))
	}
}

func TestIssue643_F2_ZeroColdDifferential_Fast(t *testing.T) { h643F2Differential(t, false) }

func TestIssue643_F2_ZeroColdDifferential_Slow(t *testing.T) { h643F2Differential(t, true) }

// TestIssue643_F2m_DeclaresExternalEndpointAsTheCriterionFailsF2 — the mutation
// arm. The consult answers with apiref.DeclaresExternalEndpoint (the #594
// ordering predicate) instead of an observed external touch; F2's differential
// must catch it, by losing W_int.
func TestIssue643_F2m_DeclaresExternalEndpointAsTheCriterionFailsF2(t *testing.T) {
	t.Setenv("RESOLVER_SEED_EXTERNAL_FETCH_TIMEOUT_MS", "200")
	raOf := map[string]string{h643NS + "/" + h643WInt.widget: h643WInt.ra}
	for i := 0; i < h643M; i++ {
		raOf[h643NS+"/ext-w"+itoaLine(i)] = "ext-ra" + itoaLine(i)
	}
	for i := 0; i < h643N; i++ {
		raOf[h643NS+"/int-w"+itoaLine(i)] = "int-ra" + itoaLine(i)
	}
	mutant := func(ctx context.Context, key, class, target string, _ redact.Label) bool {
		ra, ok := raOf[target]
		if class != "widgets" || !ok {
			return false
		}
		return apiref.DeclaresExternalEndpoint(ctx, templatesv1.ObjectReference{
			Reference:  templatesv1.Reference{Name: ra, Namespace: h643NS},
			APIVersion: h643RAGVR.Group + "/" + h643RAGVR.Version,
			Resource:   h643RAGVR.Resource,
		})
	}
	mut := h643F2Sequence(t, false, mutant)
	off := h643F2Sequence(t, false, h643ForcedOff)
	d := h643L1Diff(mut.l1, off.l1)
	if len(d) == 0 {
		t.Fatal("F2-m: with DeclaresExternalEndpoint as the skip criterion the differential found NO difference — F2 cannot see the I-5 defect it exists for")
	}
	if len(mut.wint) != 0 {
		t.Fatalf("F2-m: the mutant should leave W_int cold (it declares an external endpoint but never dials it), yet %d W_int cells were Put", len(mut.wint))
	}
	t.Logf("F2-m RED as required: %d L1 differences, W_int Put by the mutant: %d of %d", len(d), len(mut.wint), h643K)
}
