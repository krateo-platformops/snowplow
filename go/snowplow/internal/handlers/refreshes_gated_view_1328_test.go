// refreshes_gated_view_1328_test.go — #1328: an RBAC change that alters what a
// tab is SERVED from a gated (identity-free, serve-time-gated) widgetContent
// key, while the shell's stored bytes stay the same, reaches that tab as one
// frame. The REAL chain: RefreshAuth → Refreshes → the RBAC informer → the
// snapshot publish (#819's class signal) → the per-identity served-digest
// check → the bytes on the wire.
//
//	G1  userA's tab is armed on a shell whose DELETE action it may not take;
//	    userA is granted delete on panels. The shell is not re-Put (its bytes
//	    cannot change: the gate overwrites `allowed` per user), and no dep
//	    event marks it. Exactly one frame reaches the tab. RED on ba71c780:
//	    none — the only frame a shell key ever got was a Put's, and #1321
//	    withholds the identical Puts that incidentally announced it on main.
//	G2  a later class move of userA that changes nothing it is served (a
//	    configmaps grant) → no frame: the frame tracks the served view, not
//	    the RBAC event.
//	G3  a grant to ANOTHER identity changes nobody's view on this stream → no
//	    frame (and #819's N2 still holds: an identity-free key never rotates).

package handlers

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	xcontext "github.com/krateo-platformops/plumbing/context"
	"github.com/krateo-platformops/plumbing/jwtutil"

	"github.com/krateo-platformops/snowplow/internal/cache"
	"github.com/krateo-platformops/snowplow/internal/rbac"
)

// canDeleteTarget1328 is the verdict the gate re-derives for the shell's one
// action, for user (in group devs, as every token of these arms).
func canDeleteTarget1328(user string) bool {
	ctx := xcontext.BuildContext(context.Background(),
		xcontext.WithUserInfo(jwtutil.UserInfo{Username: user, Groups: []string{"devs"}}))
	return rbac.UserCan(ctx, rbac.UserCanOptions{Verb: "delete", GroupResource: panelGVR.GroupResource(),
		Namespace: evictTestNS, Name: "target"})
}

// shell1328 is a widgetContent shell with one DELETE action on panel target
// (in evictTestNS). The stored `allowed` is the SA's; the gate re-derives it
// per requester.
func shell1328() []byte {
	raw, _ := json.Marshal(map[string]any{
		"apiVersion": panelGVR.Group + "/" + panelGVR.Version, "kind": "Panel",
		"metadata": map[string]any{"name": "g1328", "namespace": evictTestNS},
		"status": map[string]any{"resourcesRefs": map[string]any{"items": []any{map[string]any{
			"id": "delete-target", "verb": "DELETE", "allowed": true,
			"path": "/call?resource=" + panelGVR.Resource + "&apiVersion=" + panelGVR.Group + "/" + panelGVR.Version +
				"&namespace=" + evictTestNS + "&name=target",
		}}}},
	})
	return raw
}

func crb1328(name, user, role string) map[string]any {
	return map[string]any{"apiVersion": "rbac.authorization.k8s.io/v1", "kind": "ClusterRoleBinding",
		"metadata": map[string]any{"name": name, "uid": "uid-" + name},
		"subjects": []any{map[string]any{"kind": "User", "name": user}},
		"roleRef":  map[string]any{"apiGroup": "rbac.authorization.k8s.io", "kind": "ClusterRole", "name": role}}
}

func cr1328(name, group, resource, verb string) map[string]any {
	return map[string]any{"apiVersion": "rbac.authorization.k8s.io/v1", "kind": "ClusterRole",
		"metadata": map[string]any{"name": name},
		"rules":    []any{map[string]any{"apiGroups": []any{group}, "resources": []any{resource}, "verbs": []any{verb}}}}
}

func TestRefreshes1328_G1_AGrantThatChangesTheGatedViewIsOneFrame_G2G3_NothingElse(t *testing.T) {
	const name = "g1328"
	dyn := setup819(t, name)
	cache.BuildBindingsByGVRIndex(nil)
	var keys []string
	store := evictionWiring(t, &keys)
	k := expectedKey(t, name) // widgetContent: identity-free, gated per requester
	keys = append(keys, k)
	store.PutResolved(fixtureStoreVerdict, k, &cache.ResolvedEntry{RawJSON: shell1328(), Inputs: &cache.ResolvedKeyInputs{
		CacheEntryClass: cache.CacheEntryClassWidgetContent,
		Group:           panelGVR.Group, Version: panelGVR.Version, Resource: panelGVR.Resource,
		Namespace: evictTestNS, Name: name,
	}})
	cell, _ := store.GetNoTouch(k)

	base := refreshServer(t)
	frames, cancel := openArmedStream(t, base, []string{name}, []string{k})
	defer cancel()
	if got := drain1324(frames, 300*time.Millisecond); len(got) != 0 {
		t.Fatalf("PRE: frames %v on arm", got)
	}
	if canDeleteTarget1328("userA") {
		t.Fatal("PRE: userA may already delete the target")
	}
	before := cache.RefreshBroadcasterStatsSnapshot()

	// G1 — userA may now delete panels: the gate serves allowed=true to it.
	create819(t, dyn, cr819GVR, cr1328("panel-deleter-1328", panelGVR.Group, panelGVR.Resource, "delete"))
	create819(t, dyn, crb819GVR, crb1328("usera-deletes-1328", "userA", "panel-deleter-1328"))
	awaitPublished(t, "userA's delete grant", hasCRB("usera-deletes-1328"))
	if !canDeleteTarget1328("userA") || canDeleteTarget1328("userB") {
		t.Fatal("PRE: the grant did not change exactly userA's verdict — the gated view would not move")
	}
	if got := drain1324(frames, 1500*time.Millisecond); len(got) != 1 || got[0] != k {
		t.Fatalf("G1 RED: frames %v after an RBAC change that altered what userA is served from %q, want exactly one — the "+
			"shell's bytes did not move, so no Put announces it, and the tab keeps showing the action as forbidden (#1328)", got, k)
	}
	if now, _ := store.GetNoTouch(k); now != cell {
		t.Fatal("PRE: the shell was re-Put — G1 must hold with the stored bytes untouched")
	}
	if d := cache.RefreshBroadcasterStatsSnapshot().GatedViewMoved - before.GatedViewMoved; d != 1 {
		t.Fatalf("G1: gated_view_moved Δ=%d, want 1", d)
	}

	// G2 — userA's class moves again, but nothing it is served changes.
	create819(t, dyn, cr819GVR, cr1328("cm-reader-1328", "", "configmaps", "get"))
	create819(t, dyn, crb819GVR, crb1328("usera-cm-1328", "userA", "cm-reader-1328"))
	awaitPublished(t, "userA's configmaps grant", hasCRB("usera-cm-1328"))
	if got := drain1324(frames, 700*time.Millisecond); len(got) != 0 {
		t.Fatalf("G2 RED: frames %v for a class move that changed nothing userA is served", got)
	}

	// G3 — another identity's grant changes nothing on this stream.
	create819(t, dyn, crb819GVR, crb1328("userb-deletes-1328", "userB", "panel-deleter-1328"))
	awaitPublished(t, "userB's grant", hasCRB("userb-deletes-1328"))
	if got := drain1324(frames, 700*time.Millisecond); len(got) != 0 {
		t.Fatalf("G3 RED: frames %v on userA's stream for userB's grant", got)
	}
}

// G4 — after a frame is written for a gated key, the connection re-reads what
// the tab will be served (its refetch reads at least that view): a key that
// was gone at arm and is then filled and announced gets no second frame at the
// next class move that changes nothing it is served.
func TestRefreshes1328_G4_AWrittenFrameUpdatesWhatTheTabHolds(t *testing.T) {
	const name = "g1328-fill"
	dyn := setup819(t, name)
	cache.BuildBindingsByGVRIndex(nil)
	var keys []string
	store := evictionWiring(t, &keys)
	k := expectedKey(t, name)
	keys = append(keys, k)

	base := refreshServer(t)
	frames, cancel := openArmedStream(t, base, []string{name}, []string{k}) // no cell yet
	defer cancel()
	store.PutResolved(fixtureStoreVerdict, k, &cache.ResolvedEntry{RawJSON: shell1328(), Inputs: &cache.ResolvedKeyInputs{
		CacheEntryClass: cache.CacheEntryClassWidgetContent,
		Group:           panelGVR.Group, Version: panelGVR.Version, Resource: panelGVR.Resource,
		Namespace: evictTestNS, Name: name,
	}})
	cache.PublishRefresh(k) // the fill's announcement
	if got := drain1324(frames, 500*time.Millisecond); len(got) != 1 {
		t.Fatalf("PRE: frames %v for the fill, want 1", got)
	}
	create819(t, dyn, cr819GVR, cr1328("cm-reader-1328-g4", "", "configmaps", "get"))
	create819(t, dyn, crb819GVR, crb1328("usera-cm-1328-g4", "userA", "cm-reader-1328-g4"))
	awaitPublished(t, "userA's configmaps grant", hasCRB("usera-cm-1328-g4"))
	if got := drain1324(frames, 700*time.Millisecond); len(got) != 0 {
		t.Fatalf("G4 RED: frames %v at a class move that changed nothing userA is served — the fill's frame was written, "+
			"so the tab holds the current view", got)
	}
}
