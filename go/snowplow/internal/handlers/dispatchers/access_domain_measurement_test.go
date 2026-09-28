//go:build accessdomainmeasure

// access_domain_measurement_test.go — the DARK MEASUREMENT for the v7 Step-2a
// access-domain deriver D. It is NOT a unit test and NOT part of the normal
// suite: it is gated behind the `accessdomainmeasure` build tag and reads the
// staged live-cluster dumps of every RESTAction + Widget, runs the pure deriver
// over all of them, and reports the numbers that inform the v7 owner decisions.
//
// Run:
//   go test -tags accessdomainmeasure -run TestAccessDomainMeasurement -v \
//     ./internal/handlers/dispatchers/
//
// Dump paths default to the 057 staged dumps; override with
//   SNOWPLOW_RA_DUMP / SNOWPLOW_WIDGET_DUMP.
//
// It contacts no cluster, no helm, no network — it reads two JSON files and runs
// the identity-free deriver in-process.

package dispatchers

import (
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"testing"

	templatesv1 "github.com/krateo-platformops/snowplow/apis/templates/v1"
)

const (
	defaultRADump     = "/private/tmp/claude-501/-Users-diegobraga-krateo-snowplow-cache-snowplow--claude-worktrees-gate-118cv2/35e88d50-8fa5-4a2a-9780-d3b7ef3f70cf/scratchpad/restactions.json"
	defaultWidgetDump = "/private/tmp/claude-501/-Users-diegobraga-krateo-snowplow-cache-snowplow--claude-worktrees-gate-118cv2/35e88d50-8fa5-4a2a-9780-d3b7ef3f70cf/scratchpad/widgets-all.json"
)

// corpusResolver resolves apiRef / resolve:true targets from the staged dumps.
type corpusResolver struct {
	ras     map[string]*templatesv1.RESTAction
	widgets map[string]map[string]any
}

func (r corpusResolver) RESTActionByRef(resource, ns, name string) (*templatesv1.RESTAction, bool) {
	ra, ok := r.ras[resource+"/"+ns+"/"+name]
	// apiRef default resource is "restactions"; some refs omit it and default in
	// GetApiRef, so also try the restactions key when the exact key misses.
	if !ok && resource != "restactions" {
		ra, ok = r.ras["restactions/"+ns+"/"+name]
	}
	return ra, ok
}
func (r corpusResolver) WidgetByRef(resource, ns, name string) (map[string]any, bool) {
	w, ok := r.widgets[resource+"/"+ns+"/"+name]
	return w, ok
}

type rawList struct {
	Items []json.RawMessage `json:"items"`
}

func meta(raw json.RawMessage) (ns, name string) {
	var m struct {
		Metadata struct {
			Namespace string `json:"namespace"`
			Name      string `json:"name"`
		} `json:"metadata"`
	}
	_ = json.Unmarshal(raw, &m)
	return m.Metadata.Namespace, m.Metadata.Name
}

func loadRAs(t *testing.T, path string) ([]*templatesv1.RESTAction, map[string]*templatesv1.RESTAction) {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read RA dump: %v", err)
	}
	var l rawList
	if err := json.Unmarshal(data, &l); err != nil {
		t.Fatalf("unmarshal RA dump: %v", err)
	}
	var ras []*templatesv1.RESTAction
	byKey := map[string]*templatesv1.RESTAction{}
	for _, raw := range l.Items {
		ra := &templatesv1.RESTAction{}
		if err := json.Unmarshal(raw, ra); err != nil {
			t.Fatalf("unmarshal RA: %v", err)
		}
		ns, name := meta(raw)
		ras = append(ras, ra)
		byKey["restactions/"+ns+"/"+name] = ra
	}
	return ras, byKey
}

func loadWidgets(t *testing.T, path string) []map[string]any {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read widget dump: %v", err)
	}
	var l rawList
	if err := json.Unmarshal(data, &l); err != nil {
		t.Fatalf("unmarshal widget dump: %v", err)
	}
	var ws []map[string]any
	for _, raw := range l.Items {
		var w map[string]any
		if err := json.Unmarshal(raw, &w); err != nil {
			t.Fatalf("unmarshal widget: %v", err)
		}
		ws = append(ws, w)
	}
	return ws
}

type kindTally struct{ exact, nsset, wild, escapes int }

func tally(d AccessDomain, k *kindTally) {
	for _, c := range d.Classes {
		switch c.Kind {
		case ClassExact:
			k.exact++
		case ClassNamespaceSet:
			k.nsset++
		case ClassWildcard:
			k.wild++
		}
	}
	k.escapes += len(d.Escapes)
}

func TestAccessDomainMeasurement(t *testing.T) {
	raPath := os.Getenv("SNOWPLOW_RA_DUMP")
	if raPath == "" {
		raPath = defaultRADump
	}
	wPath := os.Getenv("SNOWPLOW_WIDGET_DUMP")
	if wPath == "" {
		wPath = defaultWidgetDump
	}

	ras, raByKey := loadRAs(t, raPath)
	widgets := loadWidgets(t, wPath)
	// No resolve:true widget targets exist in the corpus, so the widget resolver
	// stays empty; apiRef targets are RESTActions, resolved via raByKey.
	res := corpusResolver{ras: raByKey, widgets: map[string]map[string]any{}}

	// ---- RA-level (the ~58/91 headline) ----
	var raNonEmptyClasses, raWithEscape, raFullyEmpty int
	var raTally kindTally
	var emptyRAs []string
	for i, ra := range ras {
		d := DeriveRESTActionAccessDomain(ra, res)
		tally(d, &raTally)
		if d.HasClasses() {
			raNonEmptyClasses++
		}
		if len(d.Escapes) > 0 {
			raWithEscape++
		}
		if d.IsEmpty() {
			raFullyEmpty++
			// recover name for the report
			emptyRAs = append(emptyRAs, raName(ras, i))
		}
	}

	t.Logf("=================== ACCESS-DOMAIN DARK MEASUREMENT (057 dumps) ===================")
	t.Logf("RA dump:     %s", raPath)
	t.Logf("Widget dump: %s", wPath)
	t.Logf("")
	t.Logf("--- RESTActions: %d total ---", len(ras))
	t.Logf("RAs with a NON-EMPTY D (>=1 access class):      %d / %d  (design expects ~58/91)", raNonEmptyClasses, len(ras))
	t.Logf("RAs carrying an escape marker:                  %d", raWithEscape)
	t.Logf("RAs with a fully-empty D (no class, no escape): %d", raFullyEmpty)
	t.Logf("RA class-kind distribution (summed over all RA domains):")
	t.Logf("    exact:         %d", raTally.exact)
	t.Logf("    namespace-set: %d", raTally.nsset)
	t.Logf("    wildcard:      %d", raTally.wild)
	t.Logf("    escapes:       %d", raTally.escapes)
	sort.Strings(emptyRAs)
	t.Logf("RAs with fully-empty D (external/discovery/opaque only): %v", emptyRAs)

	// ---- Widget-level (apiRef + resourcesRefs + D(RA)) ----
	var wNonEmpty, wWithEscape, wEmpty int
	var wTally kindTally
	for _, w := range widgets {
		d := DeriveWidgetAccessDomain(w, res)
		tally(d, &wTally)
		if d.HasClasses() {
			wNonEmpty++
		}
		if len(d.Escapes) > 0 {
			wWithEscape++
		}
		if d.IsEmpty() {
			wEmpty++
		}
	}
	t.Logf("")
	t.Logf("--- Widgets: %d total (apiRef + resourcesRefs + D(referenced RA)) ---", len(widgets))
	t.Logf("Widgets with a NON-EMPTY D:            %d / %d", wNonEmpty, len(widgets))
	t.Logf("Widgets carrying an escape marker:     %d", wWithEscape)
	t.Logf("Widgets with a fully-empty D:          %d", wEmpty)
	t.Logf("Widget class-kind distribution (summed over all widget domains):")
	t.Logf("    exact:         %d", wTally.exact)
	t.Logf("    namespace-set: %d", wTally.nsset)
	t.Logf("    wildcard:      %d", wTally.wild)
	t.Logf("    escapes:       %d", wTally.escapes)
	t.Logf("================================================================================")
}

// raName recovers a RESTAction's name by re-reading the dump order. It is only
// used for the human-readable empty-RA list; the deriver itself never needs it.
func raName(ras []*templatesv1.RESTAction, i int) string {
	// RESTAction embeds ObjectMeta, so the name is on the typed object.
	if i >= 0 && i < len(ras) && ras[i] != nil {
		if ras[i].Name != "" {
			return ras[i].Name
		}
	}
	return fmt.Sprintf("<ra#%d>", i)
}
