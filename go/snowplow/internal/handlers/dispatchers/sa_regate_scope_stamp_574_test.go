package dispatchers

// sa_regate_scope_stamp_574_test.go — #574 (PR #621 gate condition): the REAL
// makeBootScopeHandler stamps the prewarm scope itself, and the stamp survives
// to the seed primitive's ctx, where a branch-E / objects.get re-gate WARN
// reads it as sa_regate_scope.
//
// The cache.WithPrewarmScope call in makeBootScopeHandler is the ONLY
// production line that makes sa_regate_scope non-empty. B4 in
// sa_regate_attribution_574_test.go stamps the scope by hand, so it stays green
// with that line deleted; these arms call the handler with an UNSTAMPED ctx
// and never call cache.WithPrewarmScope themselves.
//
//   S1  walk-core scopes (boot, keepwarm, gvr-discovered): every seed ctx has
//       origin=cohort-seed, scope=<kind>, trigger empty, background=true.
//   S2  rbac-shift via the engine queue: scope=rbac-shift, origin=cohort-seed.
//
// Originally written by reviewer-621; adopted with repo naming.

import (
	"context"
	"sync"
	"testing"

	templatesv1 "github.com/krateo-platformops/snowplow/apis/templates/v1"
	"github.com/krateo-platformops/snowplow/internal/cache"
	"github.com/krateo-platformops/snowplow/internal/rbac"
	"github.com/krateo-platformops/snowplow/internal/redact"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

type scopeStamp574Seen struct {
	mu   sync.Mutex
	recs []map[string]string
}

func (s *scopeStamp574Seen) rec(ctx context.Context) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.recs = append(s.recs, map[string]string{
		"origin":  rbac.SARegateOrigin(ctx),
		"scope":   rbac.SARegateScope(ctx),
		"trigger": rbac.SARegateTrigger(ctx),
		"bg":      scopeStamp574Bool(cache.BackgroundResolveFromContext(ctx)),
	})
}

func scopeStamp574Bool(b bool) string {
	if b {
		return "true"
	}
	return "false"
}

func installScopeStamp574Recorder(t *testing.T, seen *scopeStamp574Seen) {
	prevW, prevR := seedOneWidgetFn, seedOneRestactionFn
	seedOneWidgetFn = func(ctx context.Context, _ navWidgetEntry, _ string, _ seedScopeMode) error {
		seen.rec(ctx)
		return nil
	}
	seedOneRestactionFn = func(ctx context.Context, _ redact.Label, _ templatesv1.ObjectReference, _ string, _ seedScopeMode) error {
		seen.rec(ctx)
		return nil
	}
	t.Cleanup(func() { seedOneWidgetFn, seedOneRestactionFn = prevW, prevR })
}

// Walk-core scopes: boot, keepwarm, gvr-discovered all go through
// rePrewarmBootScoped → withCohortSeedContext → seed primitive.
func TestIssue574Scope_S1_BootScopeHandlerStampsScope_WalkCore(t *testing.T) {
	engineLatchTestMu.Lock()
	defer engineLatchTestMu.Unlock()
	zeroCustomerInFlight()
	quietLoggingE(t)
	_, rw := s258BuildWatcher(t)

	w1 := eID{name: "w1", group: true, collapsed: 200}
	h := newNavWidgetHarvester()
	widgetGVR := eWidgetGVR("flexes")
	eHarvestWidget(h, "dashboard-flex", widgetGVR)

	prevEnum := enumeratePrewarmTargetsForGVRFn
	enumeratePrewarmTargetsForGVRFn = func(g schema.GroupVersionResource, _ string) []cache.PrewarmTarget {
		if g == widgetGVR {
			return eIdentityTargets(g, w1)
		}
		return nil
	}
	t.Cleanup(func() { enumeratePrewarmTargetsForGVRFn = prevEnum })

	deps := rePrewarmDeps{
		rw:        rw,
		harvester: newContentPrewarmHarvester(),
		navHarv:   h,
		authnNS:   "authn-ns",
		lister:    func(context.Context) ([]navigationRoot, error) { return nil, nil },
	}
	for _, kind := range []prewarmScopeKind{scopeKindBoot, scopeKindKeepwarm, scopeKindGVRDiscovered} {
		t.Run(string(kind), func(t *testing.T) {
			seen := &scopeStamp574Seen{}
			installScopeStamp574Recorder(t, seen)
			s := prewarmScope{kind: kind}
			if kind == scopeKindGVRDiscovered {
				s.gvr = widgetGVR
			}
			// The ctx the engine worker hands the handler carries NO scope stamp.
			if err := makeBootScopeHandler(deps)(context.Background(), s); err != nil {
				t.Fatalf("handler(%s): %v", kind, err)
			}
			if len(seen.recs) == 0 {
				t.Fatalf("%s: no seed primitive ran — arm is vacuous", kind)
			}
			for _, r := range seen.recs {
				t.Logf("%s seed ctx attrs: %v", kind, r)
				if r["scope"] != string(kind) {
					t.Errorf("%s: sa_regate_scope=%q want %q", kind, r["scope"], kind)
				}
				if r["origin"] != cache.BackgroundOriginCohortSeed {
					t.Errorf("%s: sa_regate_origin=%q want cohort-seed", kind, r["origin"])
				}
				if r["trigger"] != "" {
					t.Errorf("%s: sa_regate_trigger=%q want empty", kind, r["trigger"])
				}
				if r["bg"] != "true" {
					t.Errorf("%s: seed ctx not background — MustRegateSADial could never fire here", kind)
				}
			}
		})
	}
}

// rbac-shift: drives the engine queue + real scope handler (s258 fixture).
func TestIssue574Scope_S2_BootScopeHandlerStampsScope_RBACShift(t *testing.T) {
	s258BuildWatcher(t)
	cache.ResetRBACShiftHooksForTest()
	t.Cleanup(cache.ResetRBACShiftHooksForTest)
	e := prewarmEngineSingleton()
	e.rbacShift.Drain()
	for e.queue.Len() > 0 {
		it, _ := e.queue.Get()
		e.queue.Forget(it)
		e.queue.Done(it)
	}
	registerEngineRBACShiftHook(e)

	navHarv := newNavWidgetHarvester()
	w := reseedWidgetEntry()
	navHarv.harvestNavWidget(w.W, w.GVR, w.Pagination)
	deps := rePrewarmDeps{harvester: newContentPrewarmHarvester(), navHarv: navHarv, authnNS: h1NS}

	seen := &scopeStamp574Seen{}
	installScopeStamp574Recorder(t, seen)
	cache.NotifyRBACShiftForTest([]cache.RotatedSubject{{Kind: "User", Name: a1Alice}})
	item, _ := e.queue.Get()
	defer e.queue.Done(item)
	if item.kind != scopeKindRBACShift {
		t.Fatalf("queued kind=%q want rbac-shift", item.kind)
	}
	if err := makeBootScopeHandler(deps)(context.Background(), item); err != nil {
		t.Fatal(err)
	}
	if len(seen.recs) == 0 {
		t.Fatal("rbac-shift: no reseed ran — arm is vacuous")
	}
	for _, r := range seen.recs {
		t.Logf("rbac-shift seed ctx attrs: %v", r)
		if r["scope"] != string(scopeKindRBACShift) || r["origin"] != cache.BackgroundOriginCohortSeed {
			t.Errorf("rbac-shift: got %v want scope=rbac-shift origin=cohort-seed", r)
		}
	}
}
