//go:build accessdomainmeasure

// shadow_parity_corpus_test.go — the OFFLINE HALVES of the v7 Step 2C
// falsifiers, run over the 057 staged corpus dumps (restactions.json,
// widgets-all.json). Gated behind the `accessdomainmeasure` build tag (same as
// access_domain_measurement_test.go, whose loaders — loadRAs / loadWidgets /
// corpusResolver / meta / rawList / raName — this file reuses), so it never runs
// in the default hermetic suite and reads no absolute path there.
//
// Run:
//   go test -tags accessdomainmeasure -v \
//     -run 'TestFD4Offline|TestFProjOffline' ./internal/handlers/dispatchers/
//
// Dump paths default to the 057 staged dumps; override with
//   SNOWPLOW_RA_DUMP / SNOWPLOW_WIDGET_DUMP.
//
// Contacts no cluster, no helm, no network.

package dispatchers

import (
	"os"
	"sort"
	"strings"
	"testing"

	"github.com/krateo-platformops/snowplow/internal/rbac"
	rbacv1 "k8s.io/api/rbac/v1"
)

func corpusPaths() (raPath, wPath string) {
	raPath = os.Getenv("SNOWPLOW_RA_DUMP")
	if raPath == "" {
		raPath = defaultRADump
	}
	wPath = os.Getenv("SNOWPLOW_WIDGET_DUMP")
	if wPath == "" {
		wPath = defaultWidgetDump
	}
	return
}

// ───────────────────────── F-D4-offline ─────────────────────────
// D-completeness at the source: enumerate every api-step across all 91 RAs and
// assert no INTERNAL apiserver step (no endpointRef, no UAF) is silently
// classified external. A step whose RAW path carries a literal "/apis/" or
// "/api/" anchor but that classifyStepPath returns non-apiserver is a HARD GAP
// (the #265 templated-prefix blind spot at the deriver). Steps classified
// external with NO apiserver anchor at all are reported as informational
// (candidate fully-variable prefix, undetectable offline; genuinely external
// otherwise). Reports the full categorization for the shadow-parity acceptance.
func TestFD4Offline_DomainCompletenessOverCorpus(t *testing.T) {
	raPath, _ := corpusPaths()
	ras, _ := loadRAs(t, raPath)

	var (
		totalSteps     int
		uafSteps       int
		endpointExt    int
		apiserverRead  int
		apiserverWrite int // escapes (non-read apiserver)
		emptyPath      int // no path, no endpointRef, no UAF → no apiserver call
		externalNoAnch int // classified external, NO apiserver anchor in raw path
		hardGaps       []string
		externalPaths  []string
	)

	hasAPIServerAnchor := func(p string) bool {
		return strings.Contains(p, "/apis/") || strings.Contains(p, "/api/")
	}

	for i, ra := range ras {
		name := raName(ras, i)
		if ra == nil {
			continue
		}
		for _, step := range ra.Spec.API {
			if step == nil {
				continue
			}
			totalSteps++
			switch {
			case step.UserAccessFilter != nil:
				uafSteps++
			case step.EndpointRef != nil:
				// External endpoint dispatch — no in-process apiserver RBAC check.
				endpointExt++
			default:
				pi := classifyStepPath(step.Path)
				switch {
				case pi.apiserver:
					httpVerb := "GET"
					if step.Verb != nil && *step.Verb != "" {
						httpVerb = strings.ToUpper(*step.Verb)
					}
					if httpVerb == "GET" || httpVerb == "HEAD" {
						apiserverRead++
					} else {
						apiserverWrite++
					}
				case step.Path == "":
					emptyPath++
				case hasAPIServerAnchor(step.Path):
					// Internal step, literal apiserver anchor, yet classified
					// external → the deriver under-approximates. HARD GAP.
					hardGaps = append(hardGaps, name+" / step="+step.Name+" path="+step.Path)
				default:
					externalNoAnch++
					externalPaths = append(externalPaths, name+" / step="+step.Name+" path="+truncate(step.Path, 80))
				}
			}
		}
	}

	sort.Strings(externalPaths)
	t.Logf("================= F-D4-OFFLINE: D-COMPLETENESS OVER 057 CORPUS =================")
	t.Logf("RESTActions:                      %d", len(ras))
	t.Logf("api-steps enumerated (total):     %d", totalSteps)
	t.Logf("  UAF stanzas (→ nsset/wild):     %d", uafSteps)
	t.Logf("  endpointRef (external-by-design): %d", endpointExt)
	t.Logf("  apiserver READ (→ class):       %d", apiserverRead)
	t.Logf("  apiserver non-read (→ escape):  %d", apiserverWrite)
	t.Logf("  empty path (no apiserver call): %d", emptyPath)
	t.Logf("  external, NO apiserver anchor:  %d (informational — genuinely external or fully-variable prefix)", externalNoAnch)
	t.Logf("  HARD GAPS (anchor present, classified external): %d", len(hardGaps))
	covered := uafSteps + apiserverRead + apiserverWrite
	t.Logf("apiserver-touching steps COVERED by a class/escape: %d", covered)
	if len(externalPaths) > 0 {
		t.Logf("--- external (no-anchor) step paths, for manual review: ---")
		for _, p := range externalPaths {
			t.Logf("    %s", p)
		}
	}
	if len(hardGaps) > 0 {
		t.Errorf("F-D4-offline FAILED: %d internal apiserver step(s) silently classified external:", len(hardGaps))
		for _, g := range hardGaps {
			t.Errorf("    GAP: %s", g)
		}
	}
	t.Logf("================================================================================")
}

// ───────────────────────── F-proj offline half ─────────────────────────
// Two-identity discrimination over the corpus: for every cell (RA + widget)
// whose D contains a name-ambiguous class (a resourceNames grant could make it
// name-ambiguous), assert the LEAK-CLOSURE invariant — the cell is NOT shareable
// (so two identities with different name-scoped access can NEVER collapse to one
// trusted shared digest). It also drives the concrete two-identity case on the
// live `agents` get-UAF cell: two profiles differing ONLY in a resourceNames
// grant, asserting `!Shareable(D) OR digest1 != digest2` (never: same digest
// while access differs).
func TestFProjOffline_TwoIdentityDiscrimination(t *testing.T) {
	raPath, wPath := corpusPaths()
	ras, raByKey := loadRAs(t, raPath)
	widgets := loadWidgets(t, wPath)
	res := corpusResolver{ras: raByKey, widgets: map[string]map[string]any{}}

	type cell struct {
		kind string
		name string
		d    AccessDomain
	}
	var cells []cell
	for i, ra := range ras {
		cells = append(cells, cell{"RA", raName(ras, i), DeriveRESTActionAccessDomain(ra, res)})
	}
	for _, w := range widgets {
		cells = append(cells, cell{"widget", widgetName(w), DeriveWidgetAccessDomain(w, res)})
	}

	var (
		shareableCells    int
		nameAmbigCells    int
		escapeCells       int
		nameAmbigClasses  int
		foundAgentsGetUAF bool
	)

	for _, c := range cells {
		reasons := NotShareableReasons(c.d)
		nameAmbigClasses += reasons.NameAmbiguous
		sh := Shareable(c.d)
		if sh {
			shareableCells++
		}
		if reasons.Escape > 0 {
			escapeCells++
		}
		if reasons.NameAmbiguous > 0 {
			nameAmbigCells++
			// LEAK-CLOSURE INVARIANT: any cell with a name-ambiguous class is
			// structurally non-shareable — the projected-P analogue of the leak
			// arm (never same digest while name-scoped access differs).
			if sh {
				t.Errorf("F-proj FAILED: %s %q has a name-ambiguous class yet Shareable==true", c.kind, c.name)
			}
		}
		// Locate the live agents get-UAF class (design §2.4 concreteness).
		for _, cl := range c.d.Classes {
			if cl.Verb == "get" && cl.Group == "kagent.dev" && cl.Resource == "agents" &&
				cl.Kind == ClassNamespaceSet {
				foundAgentsGetUAF = true
			}
		}
	}

	t.Logf("================= F-PROJ OFFLINE: TWO-IDENTITY DISCRIMINATION =================")
	t.Logf("cells examined (RA + widget):          %d", len(cells))
	t.Logf("shareable cells:                       %d", shareableCells)
	t.Logf("non-shareable via name-ambiguous class: %d (name-ambiguous classes: %d)", nameAmbigCells, nameAmbigClasses)
	t.Logf("non-shareable via escape:              %d", escapeCells)
	t.Logf("live `get kagent.dev/agents` name-free class present: %v", foundAgentsGetUAF)

	// Concrete two-identity case: build the agents get-UAF cell's D directly and
	// two profiles differing ONLY in a resourceNames grant on (get,kagent.dev,
	// agents). The disjunction `!Shareable(D) OR digest1 != digest2` must hold.
	agentsD := domain([]AccessClass{
		{Kind: ClassNamespaceSet, Verb: "get", Group: "kagent.dev", Resource: "agents"},
	}, nil)
	r1 := &rbac.RequesterProfile{NamespacedRules: map[string][]rbacv1.PolicyRule{
		"team-a": {{Verbs: []string{"get"}, APIGroups: []string{"kagent.dev"}, Resources: []string{"agents"}, ResourceNames: []string{"agent-x"}}},
	}}
	r2 := &rbac.RequesterProfile{NamespacedRules: map[string][]rbacv1.PolicyRule{
		"team-a": {{Verbs: []string{"get"}, APIGroups: []string{"kagent.dev"}, Resources: []string{"agents"}, ResourceNames: []string{"agent-y"}}},
	}}
	dg1, _ := ComputeProjectionDigest(r1, agentsD)
	dg2, _ := ComputeProjectionDigest(r2, agentsD)
	sh := Shareable(agentsD)
	t.Logf("agents get-UAF cell: Shareable=%v digest1==digest2=%v", sh, dg1 == dg2)
	if sh && dg1 == dg2 {
		t.Errorf("F-proj FAILED: same digest while name-scoped access differs AND cell marked shareable (leak)")
	}
	if sh {
		t.Errorf("F-proj FAILED: agents get-UAF cell must be non-shareable (name-ambiguous)")
	}
	t.Logf("================================================================================")
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "…"
}

func widgetName(w map[string]any) string {
	if md, ok := w["metadata"].(map[string]any); ok {
		if n, ok := md["name"].(string); ok {
			return n
		}
	}
	return "<widget>"
}
