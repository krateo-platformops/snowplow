package dispatchers

// sa_regate_attribution_574_test.go — #574 arms after the owner RE-RULING
// (2026-10-13): do NOT exempt SubjectAccessReviews from the SA re-gate;
// instead attribute every re-gate refusal to its driver and cell.
//
// WHY THESE ARMS LIVE IN dispatchers: internal/rbac's TestMain exits 0 without
// running anything unless RBAC_TEST_ALLOW_DESTRUCTIVE=1, so a test there would
// report "ok" having executed NOTHING (see sa_regate_impersonation_574_test.go).
// This package runs untagged and drives the REAL refresher entry point
// (resolveAndPopulateL1 → resolveOnceProd → restactions.Resolve → branch E).
//
//   B1  a background SAR stays re-gated, and the WARN names origin=refresher,
//       trigger=none and the cell's key_hash.
//   B1t the same on a triggered (dirty-mark) dispatch: trigger=dirty-mark.
//   B2  a background NON-SAR SA dial stays re-gated and is attributed.
//   B3  a platform-template SA fetch (objects.getFromAPIServer) stays denied, and
//       its DENIED WARN names origin and key_hash.
//   B4  attribution across drivers: keepwarm seed (origin=cohort-seed,
//       scope=keepwarm), live (origin=serve), key_hash never raw.
//   A1b MEASUREMENT pinned by the ruling: with the gate stood down, the SAR
//       result still does NOT re-Put the cell (external-touched decline).

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	xcontext "github.com/krateo-platformops/plumbing/context"
	"github.com/krateo-platformops/plumbing/endpoints"
	"github.com/krateo-platformops/plumbing/jwtutil"
	"github.com/krateo-platformops/snowplow/internal/cache"
	"github.com/krateo-platformops/snowplow/internal/rbac"

	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	"k8s.io/client-go/rest"
)

// ── harness ────────────────────────────────────────────────────────────────

const (
	sar574SAToken   = "sa-token-574"
	sar574UserToken = "user-token-574"
	sar574Marker    = "SAR574-DECISION-MARKER"
	sar574Rep       = "cohort-rep-574"
	sar574SARPath   = "/apis/authorization.k8s.io/v1/subjectaccessreviews"
	sar574SSARPath  = "/apis/authorization.k8s.io/v1/selfsubjectaccessreviews"
	sar574PodPath   = "/api/v1/namespaces/ns1/pods/p1/status"
)

type sar574Req struct{ method, path, auth string }

// sar574Server is a hermetic apiserver stand-in. It records every request and
// answers the access-review POSTs, a pod subresource, a platform-template RA,
// the discovery documents client-go needs, and a per-user clientconfig Secret.
type sar574Server struct {
	srv  *httptest.Server
	mu   sync.Mutex
	reqs []sar574Req
	objs map[string]any
}

func newSAR574Server(t *testing.T, objs map[string]any) *sar574Server {
	t.Helper()
	s := &sar574Server{objs: objs}
	s.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		s.mu.Lock()
		s.reqs = append(s.reqs, sar574Req{r.Method, r.URL.Path, r.Header.Get("Authorization")})
		s.mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		if doc, ok := discoveryDoc574(r.URL.Path); ok {
			_, _ = io.WriteString(w, doc)
			return
		}
		switch {
		case r.Method == http.MethodPost && (r.URL.Path == sar574SARPath || r.URL.Path == sar574SSARPath):
			body, _ := io.ReadAll(r.Body)
			var in map[string]any
			_ = json.Unmarshal(body, &in)
			in["status"] = map[string]any{"allowed": true, "reason": sar574Marker}
			w.WriteHeader(http.StatusCreated)
			_ = json.NewEncoder(w).Encode(in)
			return
		}
		if v, ok := s.objs[r.URL.Path]; ok {
			_ = json.NewEncoder(w).Encode(v)
			return
		}
		w.WriteHeader(http.StatusNotFound)
		_, _ = io.WriteString(w, `{"kind":"Status","apiVersion":"v1","status":"Failure","code":404}`)
	}))
	t.Cleanup(s.srv.Close)
	return s
}

func (s *sar574Server) saw(method, path string) (bool, string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, r := range s.reqs {
		if r.method == method && r.path == path {
			return true, r.auth
		}
	}
	return false, ""
}

func (s *sar574Server) dump() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	var b strings.Builder
	for _, r := range s.reqs {
		fmt.Fprintf(&b, "\n  %s %s", r.method, r.path)
	}
	if b.Len() == 0 {
		return " (no requests)"
	}
	return b.String()
}

func discoveryDoc574(path string) (string, bool) {
	switch path {
	case "/api":
		return `{"kind":"APIVersions","versions":["v1"],"serverAddressByClientCIDRs":[]}`, true
	case "/api/v1":
		return `{"kind":"APIResourceList","apiVersion":"v1","groupVersion":"v1","resources":[` +
			`{"name":"pods","singularName":"pod","namespaced":true,"kind":"Pod","verbs":["get","list"]},` +
			`{"name":"secrets","singularName":"secret","namespaced":true,"kind":"Secret","verbs":["get","list"]}]}`, true
	case "/apis":
		return `{"kind":"APIGroupList","apiVersion":"v1","groups":[` +
			`{"name":"templates.krateo.io","versions":[{"groupVersion":"templates.krateo.io/v1","version":"v1"}],` +
			`"preferredVersion":{"groupVersion":"templates.krateo.io/v1","version":"v1"}}]}`, true
	case "/apis/templates.krateo.io/v1":
		return `{"kind":"APIResourceList","apiVersion":"v1","groupVersion":"templates.krateo.io/v1","resources":[` +
			`{"name":"restactions","singularName":"restaction","namespaced":true,"kind":"RestAction","verbs":["get","list"]}]}`, true
	}
	return "", false
}

var sar574RAGVR = schema.GroupVersionResource{Group: "templates.krateo.io", Version: "v1", Resource: "restactions"}

// sar574RA builds a RESTAction with ONE api stage.
func sar574RA(ns, name string, stage map[string]any) *unstructured.Unstructured {
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "templates.krateo.io/v1",
		"kind":       "RESTAction",
		"metadata":   map[string]any{"namespace": ns, "name": name},
		"spec":       map[string]any{"api": []any{stage}, "filter": "." + stage["name"].(string)},
	}}
}

func sar574ReviewStage(path string) map[string]any {
	return map[string]any{
		"name": "review",
		"path": path,
		"verb": "POST",
		// The subject named in the body is NOT the cohort, which is the point:
		// the decision is about "subject-x", whoever sends it.
		"payload":         `{"apiVersion":"authorization.k8s.io/v1","kind":"SubjectAccessReview","spec":{"user":"subject-x","resourceAttributes":{"verb":"list","resource":"pods","namespace":"ns1"}}}`,
		"headers":         []any{"Content-Type: application/json"},
		"continueOnError": true,
		"errorKey":        "reviewError",
		"filter":          ".review",
	}
}

func sar574PodStage() map[string]any {
	return map[string]any{"name": "podstatus", "path": sar574PodPath, "verb": "GET", "filter": ".podstatus"}
}

// sar574Watcher publishes a synced cache=on watcher. It HOLDS ras (so the
// refresher's dispatch-CR fetch is informer-served when the cohort may get
// restactions in ns1) and grants the cohort representative:
//   - get restactions in ns1 (always);
//   - the extra cluster rules in extra (e.g. create subjectaccessreviews).
func sar574Watcher(t *testing.T, ras []*unstructured.Unstructured, holdRAs bool, extra []rbacv1.PolicyRule) {
	t.Helper()
	t.Setenv("CACHE_ENABLED", "true")
	t.Setenv("RESOLVED_CACHE_ENABLED", "true")
	cache.ResetResolvedCacheForTest()
	cache.ResetDepsForTest()
	t.Cleanup(func() {
		cache.ResetResolvedCacheForTest()
		cache.ResetDepsForTest()
	})

	scheme := runtime.NewScheme()
	_ = rbacv1.AddToScheme(scheme)
	listKinds := map[schema.GroupVersionResource]string{
		sar574RAGVR: "RestActionList",
		{Group: "", Version: "v1", Resource: "pods"}: "PodList",
		// The resolver lazy-registers inner-call GVRs for dep tracking; the fake
		// dynamic client panics on any unregistered LIST, so the review kinds are
		// declared here. (The real apiserver does not serve LIST for them.)
		{Group: "authorization.k8s.io", Version: "v1", Resource: "subjectaccessreviews"}:     "SubjectAccessReviewList",
		{Group: "authorization.k8s.io", Version: "v1", Resource: "selfsubjectaccessreviews"}: "SelfSubjectAccessReviewList",
		{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "roles"}:               "RoleList",
		{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "rolebindings"}:        "RoleBindingList",
		{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "clusterroles"}:        "ClusterRoleList",
		{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "clusterrolebindings"}: "ClusterRoleBindingList",
	}
	rbacRef := func(kind, name string) rbacv1.RoleRef {
		return rbacv1.RoleRef{APIGroup: "rbac.authorization.k8s.io", Kind: kind, Name: name}
	}
	repSubject := []rbacv1.Subject{{Kind: rbacv1.UserKind, APIGroup: "rbac.authorization.k8s.io", Name: sar574Rep}}
	seed := []runtime.Object{
		&rbacv1.ClusterRole{
			TypeMeta:   metav1.TypeMeta{APIVersion: "rbac.authorization.k8s.io/v1", Kind: "ClusterRole"},
			ObjectMeta: metav1.ObjectMeta{Name: "sar574-ra-getter"},
			Rules:      []rbacv1.PolicyRule{{APIGroups: []string{"templates.krateo.io"}, Resources: []string{"restactions"}, Verbs: []string{"get"}}},
		},
		&rbacv1.RoleBinding{
			TypeMeta:   metav1.TypeMeta{APIVersion: "rbac.authorization.k8s.io/v1", Kind: "RoleBinding"},
			ObjectMeta: metav1.ObjectMeta{Namespace: "ns1", Name: "sar574-ra-getter"},
			Subjects:   repSubject,
			RoleRef:    rbacRef("ClusterRole", "sar574-ra-getter"),
		},
	}
	if len(extra) > 0 {
		seed = append(seed,
			&rbacv1.ClusterRole{
				TypeMeta:   metav1.TypeMeta{APIVersion: "rbac.authorization.k8s.io/v1", Kind: "ClusterRole"},
				ObjectMeta: metav1.ObjectMeta{Name: "sar574-extra"},
				Rules:      extra,
			},
			&rbacv1.ClusterRoleBinding{
				TypeMeta:   metav1.TypeMeta{APIVersion: "rbac.authorization.k8s.io/v1", Kind: "ClusterRoleBinding"},
				ObjectMeta: metav1.ObjectMeta{Name: "sar574-extra"},
				Subjects:   repSubject,
				RoleRef:    rbacRef("ClusterRole", "sar574-extra"),
			})
	}
	if holdRAs {
		for _, ra := range ras {
			seed = append(seed, ra)
		}
	}
	dyn := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(scheme, listKinds, seed...)
	rw, err := cache.NewResourceWatcher(context.Background(), dyn)
	if err != nil {
		t.Fatalf("NewResourceWatcher: %v", err)
	}
	t.Cleanup(rw.Stop)
	if holdRAs {
		added, syncCh := rw.EnsureResourceType(sar574RAGVR)
		if !added {
			t.Fatalf("EnsureResourceType(restactions): want added=true")
		}
		select {
		case <-syncCh:
		case <-time.After(5 * time.Second):
			t.Fatalf("restactions informer did not sync")
		}
	}
	sctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := rw.WaitForCacheSync(sctx, 5*time.Second); err != nil {
		t.Fatalf("WaitForCacheSync: %v", err)
	}
	cache.RebuildRBACSnapshotForTest(rw)
	cache.SetGlobal(rw)
	t.Cleanup(func() {
		cache.SetGlobal(nil)
		cache.PublishRBACSnapshotForTest(nil)
	})
}

var sar574CreateSAR = rbacv1.PolicyRule{APIGroups: []string{"authorization.k8s.io"}, Resources: []string{"subjectaccessreviews"}, Verbs: []string{"create"}}

// ── log capture ────────────────────────────────────────────────────────────

type sar574LogBuf struct {
	mu     sync.Mutex
	buf    bytes.Buffer
	logger *slog.Logger
}

// ctx returns parent carrying the capturing logger. xcontext.Logger has its
// own stderr fallback (not slog.Default), so the logger must ride the ctx.
func (l *sar574LogBuf) ctx(parent context.Context) context.Context {
	return xcontext.BuildContext(parent, xcontext.WithLogger(l.logger))
}

func (l *sar574LogBuf) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.buf.Write(p)
}

// lines returns every JSON log record whose msg is msg.
func (l *sar574LogBuf) lines(t *testing.T, msg string) []map[string]any {
	t.Helper()
	l.mu.Lock()
	defer l.mu.Unlock()
	var out []map[string]any
	for _, ln := range strings.Split(l.buf.String(), "\n") {
		if ln == "" {
			continue
		}
		var rec map[string]any
		if json.Unmarshal([]byte(ln), &rec) != nil {
			continue
		}
		if rec["msg"] == msg {
			out = append(out, rec)
		}
	}
	return out
}

// sar574CaptureLogs captures JSON log records into a buffer for the duration of
// the test: slog.Default and, via lb.ctx, the ctx logger.
func sar574CaptureLogs(t *testing.T) *sar574LogBuf {
	t.Helper()
	lb := &sar574LogBuf{}
	lb.logger = slog.New(slog.NewJSONHandler(lb, &slog.HandlerOptions{Level: slog.LevelDebug}))
	prev := slog.Default()
	slog.SetDefault(lb.logger)
	t.Cleanup(func() { slog.SetDefault(prev) })
	return lb
}

const (
	sar574BranchEMsg = "branch-E SA-endpoint dial re-gated and failed closed"
	sar574Site3Msg   = "objects.getFromAPIServer: SA-credentialed fetch re-gated and DENIED for the ctx identity; failing closed"
)

// ── refresher driver ───────────────────────────────────────────────────────

// sar574Outcome is what one refresher re-resolve did.
type sar574Outcome struct {
	key           string
	dialed        bool
	dialAuth      string
	extSkipDelta  uint64
	stageErrDelta uint64
	cellHasMarker bool
}

// sar574Refresh drives the REAL refresher entry point for an already-resident
// restactions cell keyed to the cohort representative, then reports what
// happened on the wire, on the decline counters and in the cell. ctx lets an arm
// add what the refresher's processNext adds before dispatch (e.g. trigger GVRs).
func sar574Refresh(t *testing.T, ctx context.Context, srv *sar574Server, ns, name, wantMethod, wantPath string) sar574Outcome {
	t.Helper()
	inputs := cache.ResolvedKeyInputs{
		CacheEntryClass:        "restactions",
		Group:                  "templates.krateo.io",
		Version:                "v1",
		Resource:               "restactions",
		Namespace:              ns,
		Name:                   name,
		BindingUID:             "uid-sar574-" + name,
		RepresentativeUsername: sar574Rep,
		RepresentativeGroups:   []string{"tenant-a"},
	}
	// Mint the cell under the representative's CURRENT identity class, exactly
	// as a real Put would. Without this the refresher's #444 drift check sees a
	// different class, evicts the cell and returns before resolving anything,
	// and every "not dialed" assertion would pass VACUOUSLY (#617).
	cls := rbac.IdentityClassOf(jwtutil.UserInfo{Username: sar574Rep, Groups: inputs.RepresentativeGroups})
	inputs.SubjectBindingSet, inputs.RBACSubGen = cls.SubjectBindingSet, cls.RBACSubGen
	key := cache.ComputeKey(inputs)
	store := cache.ResolvedCache()
	if store == nil {
		t.Fatal("resolved cache disabled: the arm cannot observe the cell")
	}
	store.Put(key, &cache.ResolvedEntry{RawJSON: []byte(`{"stale":"prior"}`), Inputs: &inputs})

	ext0 := cache.ExternalSkippedPut()
	se0 := cache.RefresherSkippedStageError()

	saEP := &endpoints.Endpoint{ServerURL: srv.srv.URL, Token: sar574SAToken}
	saRC := &rest.Config{Host: srv.srv.URL, BearerToken: sar574SAToken}
	err := resolveAndPopulateL1(ctx, inputs, saEP, saRC)

	out := sar574Outcome{key: key}
	out.dialed, out.dialAuth = srv.saw(wantMethod, wantPath)
	out.extSkipDelta = cache.ExternalSkippedPut() - ext0
	out.stageErrDelta = cache.RefresherSkippedStageError() - se0
	if e, ok := store.Get(key); ok {
		out.cellHasMarker = strings.Contains(string(e.RawJSON), sar574Marker)
	}
	t.Logf("refresh %s/%s err=%v dialed(%s %s)=%v external_skip_delta=%d stage_err_skip_delta=%d cell_has_decision=%v wire:%s",
		ns, name, err, wantMethod, wantPath, out.dialed, out.extSkipDelta, out.stageErrDelta, out.cellHasMarker, srv.dump())
	return out
}

// sar574RequireRegateFired fails a "re-gated" arm that passed only because
// nothing ran: the re-gate must have recorded a stage error that declined the
// re-Put, so the refresher demonstrably reached branch E.
func sar574RequireRegateFired(t *testing.T, arm string, got sar574Outcome) {
	t.Helper()
	if got.stageErrDelta == 0 {
		t.Fatalf("%s precondition: the refresher did not decline on a stage error (stage_err_skip_delta=0), so the "+
			"re-gate never demonstrably fired and the \"not dialed\" result is vacuous", arm)
	}
}

func sar574Attr(rec map[string]any, k string) string {
	v, _ := rec[k].(string)
	return v
}

// ── B1: a background SAR is re-gated (owner ruling) and the WARN says who ────

// The owner ruled 2026-10-13 (#574) NOT to exempt SubjectAccessReviews: the
// exemption alone changes nothing a user sees (A1b). The SAR stays re-gated, and
// the refusal WARN now names its driver and its cell. Trigger-less dispatch:
// the dequeue carried no trigger GVRs.
func TestIssue574SAR_B1_BackgroundSARRegated_WarnNamesRefresherTriggerlessAndCell(t *testing.T) {
	ra := sar574RA("ns1", "sar574-review", sar574ReviewStage(sar574SARPath))
	sar574Watcher(t, []*unstructured.Unstructured{ra}, true, []rbacv1.PolicyRule{sar574CreateSAR})
	srv := newSAR574Server(t, nil)
	logs := sar574CaptureLogs(t)

	got := sar574Refresh(t, logs.ctx(context.Background()), srv, "ns1", "sar574-review", http.MethodPost, sar574SARPath)

	if got.dialed {
		t.Fatalf("B1: the background SAR was dialed; the owner ruling keeps it re-gated.\n  wire:%s", srv.dump())
	}
	sar574RequireRegateFired(t, "B1", got)
	recs := logs.lines(t, sar574BranchEMsg)
	if len(recs) == 0 {
		t.Fatal("B1: the branch-E re-gate WARN was not emitted")
	}
	rec := recs[0]
	t.Logf("B1 WARN attrs: origin=%q trigger=%q scope=%q key_hash=%q",
		sar574Attr(rec, "sa_regate_origin"), sar574Attr(rec, "sa_regate_trigger"),
		sar574Attr(rec, "sa_regate_scope"), sar574Attr(rec, "key_hash"))
	if o := sar574Attr(rec, "sa_regate_origin"); o != cache.BackgroundOriginRefresher {
		t.Errorf("B1: sa_regate_origin = %q, want %q", o, cache.BackgroundOriginRefresher)
	}
	if tr := sar574Attr(rec, "sa_regate_trigger"); tr != "none" {
		t.Errorf("B1: sa_regate_trigger = %q, want \"none\" (no trigger GVRs on a trigger-less dispatch)", tr)
	}
	if kh := sar574Attr(rec, "key_hash"); kh != got.key {
		t.Errorf("B1: key_hash = %q, want the cell's ComputeKey %q (joinable with the refresher's own key_hash lines)", kh, got.key)
	}
}

// B1t: the same refusal on a dirty-mark (triggered) dispatch says so. The
// trigger GVR set is what the refresher's processNext puts on the ctx before
// dispatch (refresher.go, WithRefreshTriggerGVRs); the arm installs it the same way.
func TestIssue574SAR_B1t_TriggeredDispatch_WarnSaysDirtyMark(t *testing.T) {
	ra := sar574RA("ns1", "sar574-review-trig", sar574ReviewStage(sar574SARPath))
	sar574Watcher(t, []*unstructured.Unstructured{ra}, true, []rbacv1.PolicyRule{sar574CreateSAR})
	srv := newSAR574Server(t, nil)
	logs := sar574CaptureLogs(t)

	ctx := cache.WithRefreshTriggerGVRs(context.Background(),
		[]schema.GroupVersionResource{{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "rolebindings"}})
	got := sar574Refresh(t, logs.ctx(ctx), srv, "ns1", "sar574-review-trig", http.MethodPost, sar574SARPath)
	sar574RequireRegateFired(t, "B1t", got)

	recs := logs.lines(t, sar574BranchEMsg)
	if len(recs) == 0 {
		t.Fatal("B1t: the branch-E re-gate WARN was not emitted")
	}
	if tr := sar574Attr(recs[0], "sa_regate_trigger"); tr != "dirty-mark" {
		t.Errorf("B1t: sa_regate_trigger = %q, want \"dirty-mark\"", tr)
	}
}

// ── B2: a background NON-SAR SA dial stays re-gated, and is attributed ──────

func TestIssue574SAR_B2_BackgroundNonSARDial_StillRegated(t *testing.T) {
	ra := sar574RA("ns1", "sar574-pod", sar574PodStage())
	sar574Watcher(t, []*unstructured.Unstructured{ra}, true, []rbacv1.PolicyRule{sar574CreateSAR})
	srv := newSAR574Server(t, map[string]any{
		sar574PodPath: map[string]any{"apiVersion": "v1", "kind": "Pod", "status": map[string]any{"phase": sar574Marker}},
	})
	logs := sar574CaptureLogs(t)

	got := sar574Refresh(t, logs.ctx(context.Background()), srv, "ns1", "sar574-pod", http.MethodGet, sar574PodPath)

	if got.dialed {
		t.Fatalf("B2: a background NON-SAR SA-endpoint dial (pod subresource) was dialed.\n  wire:%s", srv.dump())
	}
	sar574RequireRegateFired(t, "B2", got)
	if recs := logs.lines(t, sar574BranchEMsg); len(recs) == 0 || sar574Attr(recs[0], "sa_regate_origin") != cache.BackgroundOriginRefresher {
		t.Errorf("B2: the re-gate WARN must carry sa_regate_origin=refresher; got %v", recs)
	}
}

// ── B3: a platform-template SA fetch is still denied, and is attributed ─────

func TestIssue574SAR_B3_PlatformTemplateSAFetch_StillDenied_WarnNamesCell(t *testing.T) {
	const tplPath = "/apis/templates.krateo.io/v1/namespaces/krateo-system/restactions/sar574-platform"
	served := sar574RA("krateo-system", "sar574-platform", sar574ReviewStage(sar574SARPath)).Object
	// Not held by the informer and not granted → objects.getFromAPIServer → re-gate.
	sar574Watcher(t, nil, false, []rbacv1.PolicyRule{sar574CreateSAR})
	srv := newSAR574Server(t, map[string]any{tplPath: served})
	logs := sar574CaptureLogs(t)

	got := sar574Refresh(t, logs.ctx(context.Background()), srv, "krateo-system", "sar574-platform", http.MethodPost, sar574SARPath)

	if ok, _ := srv.saw(http.MethodGet, tplPath); !ok {
		t.Fatalf("B3 precondition: the refresher never fetched the platform template, so the arm proves nothing.\n  wire:%s", srv.dump())
	}
	if got.cellHasMarker || got.dialed {
		t.Fatalf("B3: a platform template the cohort may not read was fetched with the SA and resolved "+
			"(cell_has_decision=%v sar_dialed=%v). objects.getFromAPIServer must stay re-gated.\n  wire:%s",
			got.cellHasMarker, got.dialed, srv.dump())
	}
	recs := logs.lines(t, sar574Site3Msg)
	if len(recs) == 0 {
		t.Fatal("B3: the objects.getFromAPIServer re-gate DENIED WARN was not emitted")
	}
	rec := recs[0]
	if o := sar574Attr(rec, "sa_regate_origin"); o != cache.BackgroundOriginRefresher {
		t.Errorf("B3: sa_regate_origin = %q, want %q", o, cache.BackgroundOriginRefresher)
	}
	if kh := sar574Attr(rec, "key_hash"); kh != got.key {
		t.Errorf("B3: key_hash = %q, want the cell's ComputeKey %q", kh, got.key)
	}
}

// ── B4: seed + prewarm scope, live, and key_hash hygiene (attribute unit) ────

func TestIssue574SAR_B4_AttributionAcrossDrivers(t *testing.T) {
	attrs := func(ctx context.Context) map[string]string {
		return map[string]string{
			"sa_regate_origin":  rbac.SARegateOrigin(ctx),
			"sa_regate_trigger": rbac.SARegateTrigger(ctx),
			"sa_regate_scope":   rbac.SARegateScope(ctx),
			"key_hash":          rbac.SARegateKeyHash(ctx),
		}
	}

	// Keep-warm sweep → the cohort seed. The scope is stamped by the engine's
	// scope handler and must survive the REAL seed ctx builder.
	seedCtx := withCohortSeedContext(
		cache.WithPrewarmScope(context.Background(), string(scopeKindKeepwarm)),
		seedTarget{Username: sar574Rep, Groups: []string{"tenant-a"}},
		endpoints.Endpoint{ServerURL: "https://kubernetes.default.svc", Token: sar574SAToken}, nil)
	if got := attrs(seedCtx); got["sa_regate_origin"] != cache.BackgroundOriginCohortSeed ||
		got["sa_regate_scope"] != string(scopeKindKeepwarm) || got["sa_regate_trigger"] != "" {
		t.Errorf("B4 keepwarm seed: got %v, want origin=cohort-seed scope=keepwarm and no trigger", got)
	}

	// Live customer request: origin serve, nothing else.
	live := xcontext.BuildContext(context.Background(),
		xcontext.WithUserInfo(jwtutil.UserInfo{Username: "live-574", Groups: []string{"tenant-a"}}))
	if got := attrs(live); got["sa_regate_origin"] != cache.RefDenialOriginServe ||
		got["sa_regate_trigger"] != "" || got["sa_regate_scope"] != "" || got["key_hash"] != "" {
		t.Errorf("B4 live: got %v, want sa_regate_origin=serve and nothing else", got)
	}

	// key_hash is never a raw non-digest key.
	raw := "snowplow:restactions:ns1/some-object:alice"
	got := attrs(cache.WithL1KeyContext(context.Background(), raw))
	if kh := got["key_hash"]; kh == "" || strings.Contains(kh, "alice") || strings.Contains(kh, "some-object") {
		t.Errorf("B4: a non-digest L1 key must be logged only as a redact label; got key_hash=%q", kh)
	}
}

// ── A1b: MEASUREMENT pinned by the owner re-ruling (#574, 2026-10-13) ────────
//
// Question: if the SA re-gate let a background SubjectAccessReview through,
// would the result reach the cell (so a permission change propagates)?
// Measured answer: NO. This is why the owner withdrew the SAR exemption.
//
// The gate is made to stand down WITHOUT any exemption code, through the one
// production path that already does it: impersonation available
// (cache.SetImpersonationAvailable(true) → resolve_populate stamps
// WithImpersonatedDial → MustRegateSADial returns false). The SAR is then
// dialed at branch E. Branch E bumps the external-touched sink, and
// resolveAndPopulateL1 declines the re-Put on it. The cell keeps its prior body.
//
// This also exhibits #615. The dial carries the plain SA bearer: the endpoint
// transport does not impersonate even though the gate trusted the marker.
//
// If this arm goes red because the cell now carries the decision, a branch-E
// result became cacheable. Before accepting that, review (i) keying: the SAR
// body can name a subject taken from request extras; (ii) invalidation: a SAR
// result has no dep edge, so a RoleBinding edit must dirty it; and (iii) #615.
func TestIssue574SAR_A1b_GateStoodDown_SARResultStillNotRePut(t *testing.T) {
	ra := sar574RA("ns1", "sar574-review-put", sar574ReviewStage(sar574SARPath))
	sar574Watcher(t, []*unstructured.Unstructured{ra}, true, []rbacv1.PolicyRule{sar574CreateSAR})
	srv := newSAR574Server(t, nil)
	cache.SetImpersonationAvailable(true)
	// #608 (PR #620): impersonation is used only for a cohort the apiserver
	// would accept, as answered by the capability reviewer that main.go
	// installs alongside SetImpersonationAvailable. Without one, the check
	// errors and the refresher keeps the re-gated SA transport, so the gate
	// would NOT stand down and this arm would measure nothing. The reviewer
	// permits exactly this cohort (the representative user and its one group).
	cache.SetImpersonationReviewer(func(_ context.Context, resource, _, name string) (bool, error) {
		switch resource {
		case cache.ImpersonateResourceUsers:
			return name == sar574Rep, nil
		case cache.ImpersonateResourceGroups:
			return name == "tenant-a", nil
		}
		return false, nil
	})
	t.Cleanup(func() {
		cache.SetImpersonationAvailable(false)
		cache.ResetImpersonationCapabilityForTest()
	})

	got := sar574Refresh(t, context.Background(), srv, "ns1", "sar574-review-put", http.MethodPost, sar574SARPath)

	if !got.dialed {
		t.Fatalf("A1b precondition: with the gate stood down the SAR must be dialed.\n  wire:%s", srv.dump())
	}
	if got.cellHasMarker {
		t.Fatalf("A1b: a branch-E SAR decision now RE-PUTS the cell. That was not part of the #574 ruling; " +
			"review keying, invalidation and #615 before accepting it (see this arm's comment).")
	}
	if got.extSkipDelta == 0 {
		t.Fatalf("A1b: expected the re-Put to be declined by the external-touched gate (external_skip_delta>0); "+
			"got external_skip_delta=%d stage_err_skip_delta=%d", got.extSkipDelta, got.stageErrDelta)
	}
	t.Logf("A1b MEASURED: SAR dialed=true (auth is SA bearer: %v, #615), cell re-Put=false, decline=external_touched (delta %d)",
		got.dialAuth == "Bearer "+sar574SAToken, got.extSkipDelta)
}
