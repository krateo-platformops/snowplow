// apistage_list_overserve_byteparity_test.go — #58 serve-time per-identity
// RBAC BYTE-PARITY hardening (L-CONTENT-BENCH / #58 gap, the test-blindspot
// analysis "highest-value net-new" residual).
//
// WHY THIS EXISTS ON TOP OF apistage_list_overserve_falsifier_test.go: the
// existing arm (TestFalsifier58_NoUAF_NarrowTenantGetsOnlyOwnNamespaces)
// collects the served items' namespaces into a Go SET and compares set
// membership. Per feedback #121 (name-set-only byte-parity masked a real
// serve defect), a set assertion is WEAKER than the customer contract: the
// customer receives BYTES, and the RBAC decision rides a per-ITEM field
// (metadata.namespace). This hardening MARSHALS the served envelope and
// asserts, per item over the RBAC-load-bearing namespace field, that ONLY
// the granted namespaces appear in the serialized bytes — the exact shape
// the analysis prescribes ("assert per-item over the namespace field,
// marshal-and-compare, NOT a name-set").
//
// REAL SERVE PATH: it drives apistageContentServe (the production hit-path
// entry) which, for a no-UAF LIST content-hit, routes to gateListItems →
// filterListByRBAC (the serve-time re-filter). No reconstruction.
//
// DISCRIMINATING RED ARM (TestFalsifier58_ByteParity_RED_NeuteredRefilterLeaks):
// serveParsedListEnvelope is the PRODUCTION no-filter pass-through the #58 fix
// deliberately does NOT use for a no-UAF LIST (the pre-#58 over-serve
// regression path — apistage.go:687). Feeding the SAME SA-maximal cell through
// it under the narrow identity models "the serve-time re-filter was neutered":
// the RED arm asserts the ungranted-namespace items DO leak into the bytes, so
// the byte-parity assertion is proven to DISCRIMINATE (it fails when the
// filter is absent, passes only when present). This is a RED-arm-actually-runs
// proof, not a claim.
//
// Hermetic: reuses the F1 dynamicfake watcher + the real in-process RBAC
// indexer + the seedOverserveCell SA-maximal 4-namespace fixture.

package api

import (
	"encoding/json"
	"net/http"
	"sort"
	"testing"

	xcontext "github.com/krateo-platformops/plumbing/context"
	"github.com/krateo-platformops/plumbing/jwtutil"
	"github.com/krateo-platformops/plumbing/ptr"
	httpcall "github.com/krateo-platformops/plumbing/http/request"
	"github.com/krateo-platformops/snowplow/internal/cache"

	"context"
)

// namespacesInServedBytes MARSHALS the served envelope value to JSON, then
// re-decodes and extracts the RBAC-load-bearing metadata.namespace of every
// item — asserting over the SERIALIZED BYTES the customer receives, not the
// in-memory value (feedback #121: byte-parity marshals bytes, not a name-set).
// Returns the sorted distinct namespace list actually present in the bytes.
func namespacesInServedBytes(t *testing.T, served any) []string {
	t.Helper()
	raw, err := json.Marshal(served)
	if err != nil {
		t.Fatalf("marshal served envelope: %v", err)
	}
	var env struct {
		Items []struct {
			Metadata struct {
				Namespace string `json:"namespace"`
			} `json:"metadata"`
		} `json:"items"`
	}
	if err := json.Unmarshal(raw, &env); err != nil {
		t.Fatalf("unmarshal served bytes: %v", err)
	}
	seen := map[string]bool{}
	for _, it := range env.Items {
		if it.Metadata.Namespace != "" {
			seen[it.Metadata.Namespace] = true
		}
	}
	out := make([]string, 0, len(seen))
	for ns := range seen {
		out = append(out, ns)
	}
	sort.Strings(out)
	return out
}

// TestFalsifier58_ByteParity_NarrowTenant_OnlyGrantedNamespacesInBytes is the
// hardened GREEN arm: the narrow tenant's served BYTES contain ONLY its
// granted namespaces (asserted per-item over metadata.namespace in the
// marshalled envelope), through the REAL serve path (apistageContentServe →
// gateListItems → filterListByRBAC).
func TestFalsifier58_ByteParity_NarrowTenant_OnlyGrantedNamespacesInBytes(t *testing.T) {
	_ = newF1Watcher(t)
	seedOverserveCell(t) // SA-maximal cell across f1AllNamespaces (4 ns)

	store := cache.ResolvedCache()
	ctx := xcontext.BuildContext(context.Background(), xcontext.WithUserInfo(jwtutil.UserInfo{Username: f1NarrowUser}))
	call := httpcall.RequestOptions{RequestInfo: httpcall.RequestInfo{
		Path: "/apis/widgets.krateo.io/v1/widgets", Verb: ptr.To(http.MethodGet),
	}}

	v, served, ok, _, _ := apistageContentServe(ctx, store, call, false /*no-UAF*/)
	if !ok || !served {
		t.Fatalf("byte-parity: expected a served (gated) LIST; ok=%v served=%v", ok, served)
	}

	gotNss := namespacesInServedBytes(t, v)
	want := append([]string{}, f1NarrowNamespaces...) // team-a, team-b
	sort.Strings(want)
	if !equalStrs(gotNss, want) {
		t.Fatalf("#58 BYTE-PARITY (no-UAF): served BYTES carry items in namespaces %v, want ONLY %v — "+
			"the RBAC-load-bearing metadata.namespace field leaked ungranted namespaces into the "+
			"serialized envelope (over-serve). Asserted over marshalled bytes per feedback #121, "+
			"NOT a name-set.", gotNss, want)
	}

	// Explicit: NONE of the ungranted namespaces (team-c, team-d) may appear.
	granted := map[string]bool{}
	for _, ns := range f1NarrowNamespaces {
		granted[ns] = true
	}
	for _, ns := range gotNss {
		if !granted[ns] {
			t.Fatalf("#58 BYTE-PARITY: ungranted namespace %q present in served bytes — cross-tenant overserve", ns)
		}
	}
}

// TestFalsifier58_ByteParity_RED_NeuteredRefilterLeaks is the DISCRIMINATING
// RED arm: it feeds the SAME SA-maximal cell through the PRODUCTION no-filter
// pass-through (serveParsedListEnvelope — the pre-#58 over-serve path) under
// the narrow identity, and asserts the ungranted namespaces DO leak into the
// bytes. This proves the byte-parity assertion above actually discriminates:
// with the serve-time re-filter neutered (bypassed), the exact assertion the
// GREEN arm makes MUST fail. If this RED arm ever stops leaking, the
// byte-parity check is no longer a live guard and this test flags it.
func TestFalsifier58_ByteParity_RED_NeuteredRefilterLeaks(t *testing.T) {
	_ = newF1Watcher(t)
	seedOverserveCell(t)

	// Re-derive the SAME cell's pre-parsed envelope the serve path holds, then
	// route it through the NO-FILTER production pass-through (the neutered
	// serve-time re-filter). We reconstruct `parsed` from the stored entry's
	// pre-parsed Items — exactly what apistageContentServe hands to the gate on
	// a pre-parsed hit — so the RED arm exercises the real pass-through fn on
	// the real cell, not a hand-built envelope.
	store := cache.ResolvedCache()
	key := cache.ComputeKey(contentKeyInputs(f1WidgetsGVR, "", ""))
	entry, hit := store.Get(key)
	if !hit || entry == nil || len(entry.Items) == 0 {
		t.Fatalf("RED arm setup: expected the seeded pre-parsed cell; hit=%v", hit)
	}
	parsed := parsedListEnvelope{
		items:      entry.Items,
		apiVersion: entry.ItemsAPIVersion,
		kind:       entry.ItemsKind,
	}

	// serveParsedListEnvelope applies NO RBAC filter (the pre-#58 over-serve
	// path). Identity is irrelevant to it — that's the point.
	v, served := serveParsedListEnvelope(parsed)
	if !served {
		t.Fatalf("RED arm: serveParsedListEnvelope returned served=false on a populated cell")
	}

	gotNss := namespacesInServedBytes(t, v)
	// The neutered path MUST leak the full SA-maximal set (all 4 ns), including
	// the ungranted team-c / team-d — proving the byte-parity assertion
	// discriminates a present re-filter from an absent one.
	wantAll := append([]string{}, f1AllNamespaces...)
	sort.Strings(wantAll)
	if !equalStrs(gotNss, wantAll) {
		t.Fatalf("RED arm did NOT reproduce the leak: neutered serve carried %v, expected the full "+
			"SA-maximal set %v. If the no-filter pass-through no longer over-serves, the byte-parity "+
			"GREEN arm is no longer a discriminating guard — investigate.", gotNss, wantAll)
	}
	// Assert the ungranted namespaces are specifically present (the leak).
	leaked := map[string]bool{}
	for _, ns := range gotNss {
		leaked[ns] = true
	}
	for _, ungranted := range []string{"team-c", "team-d"} {
		if !leaked[ungranted] {
			t.Fatalf("RED arm: expected ungranted namespace %q to leak through the neutered re-filter; it did not", ungranted)
		}
	}
}
