package dispatchers

// issue1324_refresh_digest_test.go — #1324, the /call half and the per-identity
// arm check.
//
//	D1  every armed /call response (restactions cold and hit, widgets hit, the
//	    widgetContent shell hit) stamps X-Snowplow-Refresh-Digest equal to
//	    cache.ContentDigest of exactly the body it wrote, and an unarmed
//	    response stamps none. For the shell hit that is the GATED body, which
//	    differs from the stored cell.
//	D2  two identities share one widgetContent key; an RBAC change moves only
//	    one's `allowed` verdict. Only that one is behind. A digest of the
//	    stored shell would see neither (the shell did not change).
//	D3  a shell change confined to the SA-evaluated `allowed` flags (which the
//	    gate overwrites) changes no served bytes: nobody is behind.
//	D4  a gone cell is behind; no held digest means no check at all.

import (
	"context"
	"encoding/json"
	"go/ast"
	"go/parser"
	"go/token"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	xcontext "github.com/krateo-platformops/plumbing/context"
	"github.com/krateo-platformops/plumbing/jwtutil"
	"github.com/krateo-platformops/snowplow/internal/cache"
	"github.com/krateo-platformops/snowplow/internal/rbac"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

func d1324AssertDigestIsBody(t *testing.T, site string, rec *httptest.ResponseRecorder) {
	t.Helper()
	key, digest := rec.Header().Get(refreshKeyHeader), rec.Header().Get(RefreshDigestHeader)
	if key == "" {
		t.Fatalf("PRE (%s): the response is not armed (no %s)", site, refreshKeyHeader)
	}
	if want := cache.ContentDigest(rec.Body.Bytes()); digest != want {
		t.Fatalf("D1 RED (%s): %s = %q, want %q — the digest must name exactly the bytes this response wrote", site,
			RefreshDigestHeader, digest, want)
	}
}

func TestIssue1324_D1_EveryArmedResponseDigestsTheBytesItWrote(t *testing.T) {
	t.Setenv("REFRESH_SSE_ENABLED", "true")
	widgetGV := k547WidgetGVR.Group + "/" + k547WidgetGVR.Version
	raGV := k547RAGVR.Group + "/" + k547RAGVR.Version

	t.Run("restactions_cold_then_hit", func(t *testing.T) {
		h := newK547Harness(t, k547DenyBackground)
		cold := k1134Get(h, RESTAction(), raGV, k547RAGVR.Resource, k547RAName, 0, k547Changed)
		d1324AssertDigestIsBody(t, "restactions cold", cold)
		hit := k1134Get(h, RESTAction(), raGV, k547RAGVR.Resource, k547RAName, 0, k547Changed)
		d1324AssertDigestIsBody(t, "restactions hit", hit)
		if hit.Header().Get(RefreshDigestHeader) != cold.Header().Get(RefreshDigestHeader) {
			t.Fatal("D1: the hit of the cell the cold fill stored names other bytes")
		}
	})

	t.Run("widgets_hit", func(t *testing.T) {
		h := newK547Harness(t, k547DenyBackground)
		h.warm()
		d1324AssertDigestIsBody(t, "widgets hit", k1134Get(h, Widgets(), widgetGV, k547WidgetGVR.Resource, k547WName, 0, k547Changed))
	})

	t.Run("widgetcontent_shell_hit", func(t *testing.T) {
		h := newK547Harness(t, k547DenyBackground)
		ctx := k547ReqCtx(0)
		contentKey, handle, inputs := dispatchWidgetContentKey(ctx,
			k547WidgetGVR.Group, k547WidgetGVR.Version, k547WidgetGVR.Resource, k547WNS, k547WName, -1, -1,
			effectiveKeyExtras(ctx, h.widgetU.Object, k547Extras(k547Changed)))
		if handle == nil || contentKey == "" {
			t.Fatalf("PRE: no widgetContent key")
		}
		shell := k547Widget()
		shell.Object["status"] = map[string]any{"resourcesRefs": map[string]any{"items": []any{map[string]any{
			"id": "del", "verb": "DELETE", "allowed": true,
			"path": "/call?resource=" + k547WidgetGVR.Resource + "&apiVersion=" + widgetGV + "&namespace=" + k547WNS + "&name=" + k547WName,
		}}}}
		raw, _ := json.Marshal(shell.Object)
		putFixture(handle, contentKey, &cache.ResolvedEntry{RawJSON: raw, Inputs: inputs})
		rec := k1134Get(h, Widgets(), widgetGV, k547WidgetGVR.Resource, k547WName, 0, k547Changed)
		if rec.Header().Get(refreshClassHeader) != cache.CacheEntryClassWidgetContent {
			t.Fatalf("PRE: not a widgetContent shell hit (class %q)", rec.Header().Get(refreshClassHeader))
		}
		d1324AssertDigestIsBody(t, "widgetContent shell hit", rec)
		if rec.Header().Get(RefreshDigestHeader) == cache.ContentDigest(raw) {
			t.Fatal("D1 RED: the shell hit digested the stored shell, not the gated bytes it wrote")
		}
	})

	t.Run("unarmed_response_stamps_none", func(t *testing.T) {
		rec := httptest.NewRecorder()
		setRefreshKeyHeaderIfArmable(rec, "k", "widgets", false)
		called := false
		stampRefreshDigest(rec, func() string { called = true; return "v1.x" })
		if rec.Header().Get(RefreshDigestHeader) != "" || called {
			t.Fatalf("D1 RED: an unarmed response stamped a digest (%q) or paid its hash (%v)",
				rec.Header().Get(RefreshDigestHeader), called)
		}
	})
}

// d1324Shell is a widgetContent shell with one DELETE item on a panel.
func d1324Shell(saAllowed bool) []byte {
	return fakeEnvelope([]map[string]any{{
		"id": "delete-panel", "verb": "DELETE", "allowed": saAllowed,
		"path": "/call?resource=" + h1WidgetGVR.Resource + "&apiVersion=" + h1WidgetGVR.Group + "/" + h1WidgetGVR.Version +
			"&namespace=" + h1NS + "&name=target",
	}})
}

func d1324Ctx(user string) context.Context {
	return xcontext.BuildContext(context.Background(), xcontext.WithUserInfo(jwtutil.UserInfo{Username: user, Groups: []string{"team-1324"}}))
}

func d1324Held(t *testing.T, ctx context.Context, shell []byte) string {
	t.Helper()
	gated, served := gateWidgetEnvelope(ctx, shell)
	if !served {
		t.Fatal("PRE: the gate did not serve")
	}
	return cache.ContentDigest(gated)
}

func TestIssue1324_D2_TwoIdentitiesOneKeyOnlyTheBehindOneCatchesUp(t *testing.T) {
	// The RBAC change below moves process-wide RBAC state (the sub-gen and the
	// bindings index); reset it after, as the #819 arms do, so no later test
	// inherits it.
	resetRBAC := func() {
		cache.ResetBindingsByGVRIndexForTest()
		cache.ResetRBACSubGenForTest()
	}
	resetRBAC()
	t.Cleanup(resetRBAC)
	dyn := h1BuildWatcherWithRA(t)
	store := cache.ResolvedCache()
	const key = "wc-1324-shared"
	shell := d1324Shell(true)
	store.PutResolved(fixtureStoreVerdict, key, &cache.ResolvedEntry{RawJSON: shell,
		Inputs: &cache.ResolvedKeyInputs{CacheEntryClass: cache.CacheEntryClassWidgetContent, Name: "shared"}})
	alice, bob := d1324Ctx("alice-1324"), d1324Ctx("bob-1324")
	canDelete := func(ctx context.Context) bool {
		return rbac.UserCan(ctx, rbac.UserCanOptions{Verb: "delete",
			GroupResource: h1WidgetGVR.GroupResource(), Namespace: h1NS, Name: "target"})
	}
	if canDelete(alice) || canDelete(bob) {
		t.Fatal("PRE: an identity can already delete the target")
	}
	// Both tabs were served the shell gated under their identity: the same
	// bytes today (neither may delete).
	heldA, heldB := d1324Held(t, alice, shell), d1324Held(t, bob, shell)
	if heldA != heldB {
		t.Fatal("PRE: the two identities were served different bytes before the RBAC change")
	}

	// Alice is granted delete; the shell does not change.
	rbacGVR := func(r string) schema.GroupVersionResource {
		return schema.GroupVersionResource{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: r}
	}
	mk := func(gvr schema.GroupVersionResource, obj map[string]any) {
		if _, err := dyn.Resource(gvr).Create(context.Background(), &unstructured.Unstructured{Object: obj}, metav1.CreateOptions{}); err != nil {
			t.Fatal(err)
		}
	}
	mk(rbacGVR("clusterroles"), map[string]any{"apiVersion": "rbac.authorization.k8s.io/v1", "kind": "ClusterRole",
		"metadata": map[string]any{"name": "panel-deleter-1324"},
		"rules": []any{map[string]any{"apiGroups": []any{h1WidgetGVR.Group}, "resources": []any{h1WidgetGVR.Resource},
			"verbs": []any{"delete"}}}})
	mk(rbacGVR("clusterrolebindings"), map[string]any{"apiVersion": "rbac.authorization.k8s.io/v1", "kind": "ClusterRoleBinding",
		"metadata": map[string]any{"name": "alice-deletes-1324", "uid": "uid-alice-deletes-1324"},
		"subjects": []any{map[string]any{"kind": "User", "name": "alice-1324"}},
		"roleRef":  map[string]any{"apiGroup": "rbac.authorization.k8s.io", "kind": "ClusterRole", "name": "panel-deleter-1324"}})
	deadline := time.Now().Add(10 * time.Second)
	for !canDelete(alice) {
		if time.Now().After(deadline) {
			t.Fatal("PRE: the grant never reached a published RBAC snapshot")
		}
		time.Sleep(5 * time.Millisecond)
	}
	if canDelete(bob) {
		t.Fatal("PRE: the grant reached bob")
	}

	held := func(d string) map[string]HeldDigests {
		return map[string]HeldDigests{key: {Class: cache.CacheEntryClassWidgetContent, Digests: []string{d}}}
	}
	if got := BehindKeys(alice, held(heldA)); len(got) != 1 || got[0] != key {
		t.Fatalf("D2 RED: alice's tab is behind (her /call would now show delete allowed) but BehindKeys=%v — a digest of the "+
			"shared shell cannot see a per-identity verdict change", got)
	}
	if got := BehindKeys(bob, held(heldB)); len(got) != 0 {
		t.Fatalf("D2 RED: bob's served bytes did not change but BehindKeys=%v", got)
	}
}

func TestIssue1324_D3_AShellChangeTheGateOverwritesMovesNobody(t *testing.T) {
	h1BuildWatcherWithRA(t)
	store := cache.ResolvedCache()
	const key = "wc-1324-sa-flag"
	in := &cache.ResolvedKeyInputs{CacheEntryClass: cache.CacheEntryClassWidgetContent, Name: "sa-flag"}
	store.PutResolved(fixtureStoreVerdict, key, &cache.ResolvedEntry{RawJSON: d1324Shell(true), Inputs: in})
	ctx := d1324Ctx("carol-1324")
	held := d1324Held(t, ctx, d1324Shell(true))
	// Only the SA-evaluated flag in the stored shell moves.
	store.PutResolved(fixtureStoreVerdict, key, &cache.ResolvedEntry{RawJSON: d1324Shell(false), Inputs: in})
	if got := BehindKeys(ctx, map[string]HeldDigests{key: {Class: cache.CacheEntryClassWidgetContent, Digests: []string{held}}}); len(got) != 0 {
		t.Fatalf("D3 RED: BehindKeys=%v for a shell change the gate overwrites — the tab's served bytes did not move", got)
	}
}

func TestIssue1324_D4_AGoneCellIsBehind_NoHeldNoCheck(t *testing.T) {
	h1BuildWatcherWithRA(t)
	ctx := d1324Ctx("dave-1324")
	if got := BehindKeys(ctx, map[string]HeldDigests{"gone-1324": {Class: "widgets", Digests: []string{"v1.00"}}}); len(got) != 1 {
		t.Fatalf("D4 RED: a gone cell is not behind (%v)", got)
	}
	if got := BehindKeys(ctx, nil); got != nil {
		t.Fatalf("D4: no held digests produced %v", got)
	}
}

// TestIssue1324_D5_EveryRefreshKeyStampSiteStampsTheDigest — the census: a
// production function that stamps X-Snowplow-Refresh-Key (any
// setRefreshKeyHeader* call outside the helpers themselves) stamps the digest
// too, as many times. A new armed serve site without a digest would leave its
// tab unable to catch up; it fails here instead.
func TestIssue1324_D5_EveryRefreshKeyStampSiteStampsTheDigest(t *testing.T) {
	fset := token.NewFileSet()
	files, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatal(err)
	}
	stampers := map[string]bool{"setRefreshKeyHeader": true, "setRefreshKeyHeaderUnlessExternal": true, "setRefreshKeyHeaderIfArmable": true}
	sites := 0
	for _, fn := range files {
		if strings.HasSuffix(fn, "_test.go") {
			continue
		}
		f, err := parser.ParseFile(fset, fn, nil, parser.SkipObjectResolution)
		if err != nil {
			t.Fatal(err)
		}
		for _, d := range f.Decls {
			fd, ok := d.(*ast.FuncDecl)
			if !ok || fd.Body == nil || stampers[fd.Name.Name] {
				continue
			}
			keys, digests := 0, 0
			ast.Inspect(fd.Body, func(n ast.Node) bool {
				if c, ok := n.(*ast.CallExpr); ok {
					if id, ok := c.Fun.(*ast.Ident); ok {
						switch {
						case stampers[id.Name]:
							keys++
						case id.Name == "stampRefreshDigest":
							digests++
						}
					}
				}
				return true
			})
			sites += keys
			if keys != digests {
				t.Errorf("D5 RED: %s %s stamps the refresh key %d time(s) and the digest %d time(s)", fn, fd.Name.Name, keys, digests)
			}
		}
	}
	if sites < 5 {
		t.Fatalf("D5: found %d refresh-key stamp sites, want the five serve sites — the scan is not looking at the package", sites)
	}
}
