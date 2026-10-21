// refreshes_summary_test.go — #101 ARM-3: the per-subscription INFO summary
// content arm.
//
// The #101 fix REPLACES the 378-per-nav "unable to get user endpoint" ERROR
// storm with ONE INFO line per /refreshes connection carrying {requested,
// armed, skipped_informer_miss, skipped_other}. This arm drives the REAL
// validateSubscription (via a capturing logger on the request ctx) over a MIXED
// fixture — one armable widgetContent coord (CR seeded) + one informer-miss
// coord (CR absent) — and asserts the emitted summary's counts match the
// fixture exactly. So an unarmed subscription is diagnosable at INFO from a
// single line, with no per-coord noise.
//
// Hermetic: reuses seedAuthTestWidget's watcher shape + a JSON-capturing slog
// handler on the request ctx (xcontext.WithLogger). NO apiserver, NO cluster.

package handlers

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"log/slog"
	"net/http"
	"testing"
	"time"

	xcontext "github.com/krateo-platformops/plumbing/context"
	"github.com/krateo-platformops/plumbing/jwtutil"
	"github.com/krateo-platformops/snowplow/internal/cache"
	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	dynamicfake "k8s.io/client-go/dynamic/fake"
)

// seedSummaryWidget wires cache.Global() with ONE panel CR ("armed-panel") that
// the armable coord references, plus an RBAC binding granting the devs group
// get/list panels — so the armable coord's subscriptionKeyExtras objects.Get
// succeeds while a DIFFERENT (absent) coord informer-misses. Mirrors
// seedAuthTestWidget but with a single named CR the summary fixture points at.
func seedSummaryWidget(t *testing.T) {
	t.Helper()
	t.Setenv("CACHE_ENABLED", "true")
	t.Setenv("RESOLVED_CACHE_ENABLED", "true")

	panelGVR := schema.GroupVersionResource{Group: "widgets.templates.krateo.io", Version: "v1beta1", Resource: "panels"}
	crbGVR := schema.GroupVersionResource{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "clusterrolebindings"}
	crGVR := schema.GroupVersionResource{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "clusterroles"}
	scheme := runtime.NewScheme()
	_ = rbacv1.AddToScheme(scheme)
	listKinds := map[schema.GroupVersionResource]string{
		crbGVR: "ClusterRoleBindingList",
		crGVR:  "ClusterRoleList",
		{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "rolebindings"}: "RoleBindingList",
		{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "roles"}:        "RoleList",
		panelGVR: "PanelList",
	}
	rule := []rbacv1.PolicyRule{{Verbs: []string{"get", "list"}, APIGroups: []string{"widgets.templates.krateo.io"}, Resources: []string{"panels"}}}
	seed := []runtime.Object{
		&rbacv1.ClusterRole{ObjectMeta: metav1.ObjectMeta{Name: "panel-reader"}, Rules: rule},
		&rbacv1.ClusterRoleBinding{
			ObjectMeta: metav1.ObjectMeta{Name: "devs-bind", UID: types.UID("uid-devs")},
			Subjects:   []rbacv1.Subject{{Kind: "Group", Name: "devs"}},
			RoleRef:    rbacv1.RoleRef{APIGroup: "rbac.authorization.k8s.io", Kind: "ClusterRole", Name: "panel-reader"},
		},
		&unstructured.Unstructured{Object: map[string]any{
			"apiVersion": "widgets.templates.krateo.io/v1beta1",
			"kind":       "Panel",
			"metadata":   map[string]any{"name": "armed-panel", "namespace": "krateo-system"},
			"spec":       map[string]any{},
		}},
	}

	wctx, wcancel := context.WithCancel(context.Background())
	dyn := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(scheme, listKinds, seed...)
	rw, err := cache.NewResourceWatcher(wctx, dyn)
	if err != nil {
		wcancel()
		t.Fatalf("NewResourceWatcher: %v", err)
	}
	syncCtx, syncCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer syncCancel()
	if err := rw.WaitForCacheSync(syncCtx, 5*time.Second); err != nil {
		rw.Stop()
		wcancel()
		t.Fatalf("WaitForCacheSync: %v", err)
	}
	_, _ = rw.EnsureResourceType(panelGVR)
	_ = rw.WaitForCacheSync(syncCtx, 5*time.Second)
	cache.RebuildRBACSnapshotForTest(rw)
	prev := cache.Global()
	cache.SetGlobal(rw)
	t.Cleanup(func() {
		rw.Stop()
		wcancel()
		cache.SetGlobal(prev)
		cache.PublishRBACSnapshotForTest(nil)
	})
}

// widgetContentCoord builds one widgetContent ?sub= entry for a panel name.
func widgetContentCoord(name string) map[string]any {
	return map[string]any{
		"class":     "widgetContent",
		"group":     "widgets.templates.krateo.io",
		"version":   "v1beta1",
		"resource":  "panels",
		"namespace": "krateo-system",
		"name":      name,
		"perPage":   5,
		"page":      1,
	}
}

// summaryLine is the subset of the refreshes.subscription.summary JSON the arm
// asserts on.
type summaryLine struct {
	Msg                 string `json:"msg"`
	Requested           int    `json:"requested"`
	Armed               int    `json:"armed"`
	SkippedInformerMiss int    `json:"skipped_informer_miss"`
	SkippedOther        int    `json:"skipped_other"`
}

// TestRefreshes_SubscriptionSummary_CountsMatchFixture is #101 ARM-3. A mixed
// two-coord subscription — armed-panel (CR seeded → arms) + missing-panel (CR
// absent → informer-miss skip) — produces exactly {requested:2, armed:1,
// skipped_informer_miss:1, skipped_other:0}, and the armed set the handler
// streams still contains the one good key (remaining coords still armed).
func TestRefreshes_SubscriptionSummary_CountsMatchFixture(t *testing.T) {
	seedSummaryWidget(t)

	// Capture the summary line: install a JSON slog handler on the request ctx
	// (validateSubscription reads xcontext.Logger(req.Context())).
	buf := &bytes.Buffer{}
	logger := slog.New(slog.NewJSONHandler(buf, &slog.HandlerOptions{Level: slog.LevelInfo}))

	body := []map[string]any{
		widgetContentCoord("armed-panel"),   // CR present → arms
		widgetContentCoord("missing-panel"), // CR absent → informer-miss skip
	}
	raw, _ := json.Marshal(body)
	sub := base64.StdEncoding.EncodeToString(raw)

	ctx := xcontext.BuildContext(context.Background(),
		xcontext.WithLogger(logger),
		xcontext.WithUserInfo(jwtutil.UserInfo{Username: "userA", Groups: []string{"devs"}}),
	)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "/refreshes?sub="+sub, nil)
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}

	armed, _, derr := validateSubscription(req)
	if derr != nil {
		t.Fatalf("validateSubscription: unexpected error %v", derr)
	}
	// One coord armed (armed-panel), one skipped (missing-panel).
	if len(armed) != 1 {
		t.Fatalf("armed set: want 1 (armed-panel), got %d (%v)", len(armed), armed)
	}

	// Find + assert the summary line.
	sum := findSummaryLine(t, buf)
	if sum.Requested != 2 {
		t.Fatalf("summary requested: want 2; got %d", sum.Requested)
	}
	if sum.Armed != 1 {
		t.Fatalf("summary armed: want 1; got %d", sum.Armed)
	}
	if sum.SkippedInformerMiss != 1 {
		t.Fatalf("summary skipped_informer_miss: want 1 (missing-panel); got %d — the diagnostic count must reflect the informer-miss coord",
			sum.SkippedInformerMiss)
	}
	if sum.SkippedOther != 0 {
		t.Fatalf("summary skipped_other: want 0; got %d (informer-miss mis-bucketed?)", sum.SkippedOther)
	}
}

// TestRefreshes_SubscriptionSummary_AllInformerMiss — a subscription whose every
// coord informer-misses arms 0 and reports skipped_informer_miss == requested,
// the diagnostic that answers "admin's dashboard armed nothing — why?" from ONE
// INFO line (the #101 storm's replacement).
func TestRefreshes_SubscriptionSummary_AllInformerMiss(t *testing.T) {
	seedSummaryWidget(t)

	buf := &bytes.Buffer{}
	logger := slog.New(slog.NewJSONHandler(buf, &slog.HandlerOptions{Level: slog.LevelInfo}))

	body := []map[string]any{
		widgetContentCoord("gone-1"),
		widgetContentCoord("gone-2"),
		widgetContentCoord("gone-3"),
	}
	raw, _ := json.Marshal(body)
	sub := base64.StdEncoding.EncodeToString(raw)

	ctx := xcontext.BuildContext(context.Background(),
		xcontext.WithLogger(logger),
		xcontext.WithUserInfo(jwtutil.UserInfo{Username: "userA", Groups: []string{"devs"}}),
	)
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, "/refreshes?sub="+sub, nil)

	armed, _, derr := validateSubscription(req)
	if derr != nil {
		t.Fatalf("validateSubscription: unexpected error %v", derr)
	}
	if len(armed) != 0 {
		t.Fatalf("armed set: want 0 (all coords informer-miss); got %d", len(armed))
	}
	sum := findSummaryLine(t, buf)
	if sum.Requested != 3 || sum.Armed != 0 || sum.SkippedInformerMiss != 3 || sum.SkippedOther != 0 {
		t.Fatalf("all-miss summary: want {requested:3, armed:0, skipped_informer_miss:3, skipped_other:0}; got %+v", sum)
	}
}

// findSummaryLine scans the captured JSON log lines for the single
// refreshes.subscription.summary entry.
func findSummaryLine(t *testing.T, buf *bytes.Buffer) summaryLine {
	t.Helper()
	for _, line := range bytes.Split(buf.Bytes(), []byte("\n")) {
		if len(bytes.TrimSpace(line)) == 0 {
			continue
		}
		var s summaryLine
		if err := json.Unmarshal(line, &s); err != nil {
			continue
		}
		if s.Msg == "refreshes.subscription.summary" {
			return s
		}
	}
	t.Fatalf("no refreshes.subscription.summary line emitted; captured log:\n%s", buf.String())
	return summaryLine{}
}
