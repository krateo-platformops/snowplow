// controller_health_509_selector_test.go — #509 falsifier arms.
//
// DEFECT. podRestartCounts derived its Pod-LIST selector from
// Deployment.Spec.Selector.MatchLabels alone
// (labels.SelectorFromSet), dropping MatchExpressions. apimachinery
// treats a nil/empty Set as Everything() (pkg/labels/selector.go:
// 943-986), so a Deployment whose spec.selector is expressed only
// through matchExpressions — valid, and satisfying the required-field
// constraint — yielded an EMPTY selector string, i.e. a LIST of every
// pod in the namespace, whose restart counts were then attributed to
// that one controller.
//
// These arms drive the REAL boundary: the rebuild walk discovers the
// controller from the webhook configuration, reads the Deployment out
// of the informer indexer, and issues the Pod LIST through the fake
// clientset — which applies the label selector exactly as the
// apiserver would (client-go gentype/fake.go:162-180). The assertion
// is on the published snapshot's PodRestartCount, not on an internal
// selector value.
//
// Each arm asserts BOTH directions: the foreign pods are excluded AND
// the pods that do belong are still counted.
package cache

import (
	"context"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	clienttesting "k8s.io/client-go/testing"
)

// mkDeploymentWithSelector builds a Deployment with an arbitrary
// selector shape (mkDeployment hardcodes matchLabels{app: name},
// which is exactly the shape that hides this defect).
func mkDeploymentWithSelector(ns, name string, sel *metav1.LabelSelector) *appsv1.Deployment {
	return &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: name},
		Spec:       appsv1.DeploymentSpec{Selector: sel},
	}
}

// mkPodLabelled builds a pod with an explicit label set (mkPod always
// stamps app=<deployment>).
func mkPodLabelled(ns, podName string, labelSet map[string]string, restartCount int32) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: ns,
			Name:      podName,
			Labels:    labelSet,
		},
		Status: corev1.PodStatus{
			ContainerStatuses: []corev1.ContainerStatus{
				{Name: "sentinel", RestartCount: restartCount},
			},
		},
	}
}

// startControllerHealth509 wires the subsystem over the given objects
// and runs one synchronous rebuild, returning the entry published for
// ns/name.
func startControllerHealth509(t *testing.T, ns, name string, objs ...runtime.Object) (*fake.Clientset, ControllerHealthEntry) {
	t.Helper()
	cli := fake.NewSimpleClientset(objs...)
	restore := SetControllerHealthClientForTest(cli)
	t.Cleanup(restore)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	t.Cleanup(cancel)
	if err := StartControllerHealthInformer(ctx, nil, []string{ns}); err != nil {
		t.Fatalf("StartControllerHealthInformer: %v", err)
	}
	t.Cleanup(func() { resetControllerHealthForTest(t) })

	RebuildControllerHealthSnapshotForTest()
	snap := ControllerHealthSnapshotLoad()
	if snap == nil {
		t.Fatalf("nil snapshot")
	}
	key := ns + "/" + name
	e, ok := snap.Controllers[key]
	if !ok {
		t.Fatalf("controllers map missing key %q; got %+v", key, snap.Controllers)
	}
	return cli, e
}

// ─────────────────────────────────────────────────────────────────────
// #509 ARM 1 — a matchExpressions-only selector must not select the
// whole namespace.
// ─────────────────────────────────────────────────────────────────────

func TestControllerHealth_509_MatchExpressionsOnlySelector_ExcludesForeignPods(t *testing.T) {
	t.Setenv("CACHE_ENABLED", "true")
	resetControllerHealthForTest(t)

	const (
		ns   = "krateo-system"
		name = "ctrl-expr"
	)

	// A selector expressed ONLY through matchExpressions. Valid, and
	// it satisfies "selector is a required, non-empty field" —
	// MatchLabels is legitimately absent.
	dep := mkDeploymentWithSelector(ns, name, &metav1.LabelSelector{
		MatchExpressions: []metav1.LabelSelectorRequirement{{
			Key:      "app",
			Operator: metav1.LabelSelectorOpIn,
			Values:   []string{name},
		}},
	})
	ep := mkEndpoints(ns, name, 1) // Conjunct B healthy — isolate Conjunct A
	mwc := mkMWC("cfg-expr", "mutate.expr", ns, name, "Fail")

	// Pods that DO belong: 3 + 4 = 7.
	mine1 := mkPodLabelled(ns, name+"-aaa", map[string]string{"app": name}, 3)
	mine2 := mkPodLabelled(ns, name+"-bbb", map[string]string{"app": name}, 4)
	// Pods in the SAME namespace that do NOT belong: a differently
	// labelled workload and an unlabelled one. 11 + 13 = 24.
	foreign1 := mkPodLabelled(ns, "unrelated-deploy-xyz", map[string]string{"app": "something-else"}, 11)
	foreign2 := mkPodLabelled(ns, "bare-pod", nil, 13)

	_, e := startControllerHealth509(t, ns, name, dep, ep, mwc, mine1, mine2, foreign1, foreign2)

	const wantOwn = 7
	const bugTotal = wantOwn + 24 // what labels.Everything() would report

	if e.PodRestartCount == bugTotal {
		t.Fatalf("PodRestartCount=%d — the foreign pods in %q were attributed to %s/%s; "+
			"the Pod LIST selector degraded to labels.Everything() because MatchExpressions "+
			"was dropped (#509)", e.PodRestartCount, ns, ns, name)
	}
	// Both directions in one assertion: foreign pods excluded (not 31,
	// and not any other over-count) AND the two pods that do belong
	// still counted (not 0, which a "report nothing" over-correction
	// would produce).
	if e.PodRestartCount != wantOwn {
		t.Fatalf("PodRestartCount=%d; want %d (= the 3 + 4 restarts of the two pods matching "+
			"the matchExpressions selector, and nothing else)", e.PodRestartCount, wantOwn)
	}
	if e.EndpointReadyCount != 1 {
		t.Errorf("EndpointReadyCount=%d; want 1 (Conjunct B must stay healthy so this arm "+
			"isolates Conjunct A)", e.EndpointReadyCount)
	}
}

// ─────────────────────────────────────────────────────────────────────
// #509 ARM 2 — MatchLabels AND MatchExpressions are ANDed. Asserts the
// fold in both directions: the expression narrows the matchLabels set,
// and matchLabels still narrows on its own.
// ─────────────────────────────────────────────────────────────────────

func TestControllerHealth_509_MatchLabelsAndExpressionsAreFolded(t *testing.T) {
	t.Setenv("CACHE_ENABLED", "true")
	resetControllerHealthForTest(t)

	const (
		ns   = "krateo-system"
		name = "ctrl-fold"
	)

	dep := mkDeploymentWithSelector(ns, name, &metav1.LabelSelector{
		MatchLabels: map[string]string{"app": name},
		MatchExpressions: []metav1.LabelSelectorRequirement{{
			Key:      "tier",
			Operator: metav1.LabelSelectorOpIn,
			Values:   []string{"control"},
		}},
	})
	ep := mkEndpoints(ns, name, 1)
	mwc := mkMWC("cfg-fold", "mutate.fold", ns, name, "Fail")

	// Matches BOTH clauses → counted.
	owned := mkPodLabelled(ns, name+"-ctl", map[string]string{"app": name, "tier": "control"}, 5)
	// Matches matchLabels but FAILS the expression → must be excluded.
	// This is the pod the old code counted.
	wrongTier := mkPodLabelled(ns, name+"-data", map[string]string{"app": name, "tier": "data"}, 9)
	// Matches the expression but FAILS matchLabels → must be excluded.
	// This is the direction that must STILL work after the change.
	wrongApp := mkPodLabelled(ns, "other-ctl", map[string]string{"app": "other", "tier": "control"}, 17)

	_, e := startControllerHealth509(t, ns, name, dep, ep, mwc, owned, wrongTier, wrongApp)

	if e.PodRestartCount == 5+9 {
		t.Fatalf("PodRestartCount=%d — the tier=data pod was counted, so MatchExpressions was "+
			"dropped from the selector (#509)", e.PodRestartCount)
	}
	if e.PodRestartCount == 5+17 || e.PodRestartCount == 5+9+17 {
		t.Fatalf("PodRestartCount=%d — a pod from a different workload (app=other) was counted, "+
			"so MatchLabels stopped narrowing", e.PodRestartCount)
	}
	if e.PodRestartCount != 5 {
		t.Fatalf("PodRestartCount=%d; want 5 (only the pod satisfying app=%s AND tier in "+
			"(control))", e.PodRestartCount, name)
	}
}

// ─────────────────────────────────────────────────────────────────────
// #509 ARM 3 — a selector that would stringify to "" reports NOTHING
// and says so at Warn. "" on the wire is "every pod in the namespace",
// and labels.Nothing().String() is also "" (selector.go:104), so the
// string form cannot carry Nothing — the guard has to be here.
// ─────────────────────────────────────────────────────────────────────

func TestControllerHealth_509_EmptySelectorReportsNothingAndWarns(t *testing.T) {
	t.Setenv("CACHE_ENABLED", "true")

	cases := []struct {
		label string
		sel   *metav1.LabelSelector
	}{
		{"empty-selector", &metav1.LabelSelector{}}, // → labels.Everything()
		{"nil-selector", nil},                       // → labels.Nothing()
	}
	for _, tc := range cases {
		t.Run(tc.label, func(t *testing.T) {
			resetControllerHealthForTest(t)
			const (
				ns   = "krateo-system"
				name = "ctrl-empty"
			)
			log := capturePodSelectorWarns(t)

			dep := mkDeploymentWithSelector(ns, name, tc.sel)
			ep := mkEndpoints(ns, name, 1)
			mwc := mkMWC("cfg-empty", "mutate.empty", ns, name, "Fail")
			foreign1 := mkPodLabelled(ns, "unrelated-a", map[string]string{"app": "a"}, 31)
			foreign2 := mkPodLabelled(ns, "unrelated-b", nil, 37)

			_, e := startControllerHealth509(t, ns, name, dep, ep, mwc, foreign1, foreign2)

			if e.PodRestartCount != 0 {
				t.Fatalf("PodRestartCount=%d; want 0 — an empty selector must report nothing, "+
					"not every pod in the namespace (#509)", e.PodRestartCount)
			}
			if n := log.count("cache.controller_health.pod_selector_empty"); n == 0 {
				t.Fatalf("no cache.controller_health.pod_selector_empty Warn record; a selector "+
					"that reports nothing must be VISIBLE, otherwise a silent 0 reads as health. "+
					"records seen: %v", log.messages())
			}
		})
	}
}

// ─────────────────────────────────────────────────────────────────────
// #509 ARM 4 — a truncated page is logged at Warn, so a PARTIAL
// restart total cannot read as a healthy one. The other direction is
// asserted too: an untruncated page must NOT warn.
// ─────────────────────────────────────────────────────────────────────

func TestControllerHealth_509_TruncatedPodPageIsWarned(t *testing.T) {
	t.Setenv("CACHE_ENABLED", "true")

	const (
		ns   = "krateo-system"
		name = "ctrl-trunc"
	)
	dep := mkDeploymentWithSelector(ns, name, &metav1.LabelSelector{
		MatchExpressions: []metav1.LabelSelectorRequirement{{
			Key:      "app",
			Operator: metav1.LabelSelectorOpIn,
			Values:   []string{name},
		}},
	})
	ep := mkEndpoints(ns, name, 1)
	mwc := mkMWC("cfg-trunc", "mutate.trunc", ns, name, "Fail")
	mine := mkPodLabelled(ns, name+"-aaa", map[string]string{"app": name}, 2)

	// The fake clientset ignores ListOptions.Limit, so the truncated
	// page is injected with a reactor: the apiserver signals a
	// truncated page by returning a non-empty metadata.continue.
	// gentype/fake.go:168 copies the returned ListMeta through, so
	// the token reaches the code under test while the label filter
	// still applies.
	t.Run("continue-token-present", func(t *testing.T) {
		resetControllerHealthForTest(t)
		log := capturePodSelectorWarns(t)

		cli := fake.NewSimpleClientset(dep, ep, mwc, mine)
		cli.PrependReactor("list", "pods", func(a clienttesting.Action) (bool, runtime.Object, error) {
			return true, &corev1.PodList{
				ListMeta: metav1.ListMeta{Continue: "truncated-page-token"},
				Items:    []corev1.Pod{*mine},
			}, nil
		})
		restore := SetControllerHealthClientForTest(cli)
		t.Cleanup(restore)

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		t.Cleanup(cancel)
		if err := StartControllerHealthInformer(ctx, nil, []string{ns}); err != nil {
			t.Fatalf("StartControllerHealthInformer: %v", err)
		}
		t.Cleanup(func() { resetControllerHealthForTest(t) })
		RebuildControllerHealthSnapshotForTest()

		e := ControllerHealthSnapshotLoad().Controllers[ns+"/"+name]
		// The page that DID arrive is still counted — the warn is a
		// visibility measure, not a bail-out.
		if e.PodRestartCount != 2 {
			t.Fatalf("PodRestartCount=%d; want 2 — the pods on the page that arrived must still "+
				"be counted", e.PodRestartCount)
		}
		if n := log.count("cache.controller_health.pod_list_truncated"); n == 0 {
			t.Fatalf("no cache.controller_health.pod_list_truncated Warn record for a page "+
				"carrying metadata.continue; a partial restart total would read as a complete "+
				"one. records seen: %v", log.messages())
		}
	})

	t.Run("no-continue-token-does-not-warn", func(t *testing.T) {
		resetControllerHealthForTest(t)
		log := capturePodSelectorWarns(t)

		_, e := startControllerHealth509(t, ns, name, dep, ep, mwc, mine)
		if e.PodRestartCount != 2 {
			t.Fatalf("PodRestartCount=%d; want 2", e.PodRestartCount)
		}
		if n := log.count("cache.controller_health.pod_list_truncated"); n != 0 {
			t.Fatalf("%d truncation Warn record(s) on a complete page — the warn must fire only "+
				"when metadata.continue is set, or every rebuild cries wolf", n)
		}
		if n := log.count("cache.controller_health.pod_selector_empty"); n != 0 {
			t.Fatalf("%d pod_selector_empty Warn record(s) for a matchExpressions-only selector — "+
				"the guard must not swallow a legitimate selector", n)
		}
	})
}

// --- the Warn capture -------------------------------------------------

// podSelectorWarnLog collects the controller-health Pod-LIST records so
// an arm can assert that a reading which reports NOTHING, or reports a
// PARTIAL total, is visible rather than silent.
type podSelectorWarnLog struct {
	mu   sync.Mutex
	msgs []string
}

func (l *podSelectorWarnLog) add(m string) {
	l.mu.Lock()
	l.msgs = append(l.msgs, m)
	l.mu.Unlock()
}

func (l *podSelectorWarnLog) count(m string) int {
	l.mu.Lock()
	defer l.mu.Unlock()
	n := 0
	for _, got := range l.msgs {
		if got == m {
			n++
		}
	}
	return n
}

func (l *podSelectorWarnLog) messages() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]string(nil), l.msgs...)
}

type podSelectorWarnHandler struct {
	slog.Handler
	log *podSelectorWarnLog
}

func (h *podSelectorWarnHandler) Handle(ctx context.Context, r slog.Record) error {
	// Narrow to this code path's own records: the slog default is
	// process-global, so other packages' goroutines share this stream.
	if strings.HasPrefix(r.Message, "cache.controller_health.pod_") {
		h.log.add(r.Message)
	}
	return nil
}

func (h *podSelectorWarnHandler) Enabled(context.Context, slog.Level) bool { return true }

func capturePodSelectorWarns(t *testing.T) *podSelectorWarnLog {
	t.Helper()
	prev := slog.Default()
	log := &podSelectorWarnLog{}
	slog.SetDefault(slog.New(&podSelectorWarnHandler{Handler: prev.Handler(), log: log}))
	t.Cleanup(func() { slog.SetDefault(prev) })
	return log
}
