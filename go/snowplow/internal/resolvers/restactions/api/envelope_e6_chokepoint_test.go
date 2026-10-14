package api

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"sync"
	"testing"

	"github.com/krateo-platformops/snowplow/internal/listenvelope"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// The E6 behavioural chokepoint (#674, reviewer-603's gate on #676). Every
// Kubernetes LIST envelope reaches a stage through jsonHandlerCore. In this
// package's test binary stageEnvelopeCheck is installed for EVERY test, so
// each dispatch test (informer, content hit, cluster-list collapse, branch C
// informer and live, the integration resolves) checks the envelope it
// served. A violation fails the whole run in TestMain (e6ChokepointExit),
// even when it happened under a recover() or in a goroutine.
//
// This closes the AST scan's blind spots (envelope_e6_test.go): a key held
// in a constant (B1), a key split across string literals (B3), and an
// envelope built by a converter or list type from another package (B4).

var e6Choke struct {
	mu         sync.Mutex
	checked    int
	violations []string
}

func init() { stageEnvelopeCheck = e6ChokepointCheck }

func e6ChokepointCheck(opts jsonHandlerOptions, tmp any) {
	m, ok := tmp.(map[string]any)
	if !ok {
		return
	}
	if _, isList := m[listenvelope.Items]; !isList {
		return // not a list envelope (a create's response object, a Status)
	}
	allowed := map[string]bool{}
	for _, k := range listenvelope.Keys() {
		allowed[k] = true
	}
	var extra []string
	for k := range m {
		if !allowed[k] {
			extra = append(extra, k)
		}
	}
	sort.Strings(extra)
	e6Choke.mu.Lock()
	defer e6Choke.mu.Unlock()
	e6Choke.checked++
	if len(extra) > 0 {
		v := fmt.Sprintf("stage %q (RESTAction %s/%s): the list envelope carries %v beyond E6 %v",
			opts.key, opts.raNamespace, opts.raName, extra, listenvelope.Keys())
		e6Choke.violations = append(e6Choke.violations, v)
		fmt.Fprintln(os.Stderr, "E6 CHOKEPOINT VIOLATION:", v)
	}
}

// e6ChokepointSnapshot returns the checks and violations so far.
func e6ChokepointSnapshot() (int, []string) {
	e6Choke.mu.Lock()
	defer e6Choke.mu.Unlock()
	return e6Choke.checked, append([]string(nil), e6Choke.violations...)
}

// e6ChokepointExit is TestMain's last word: it reports the chokepoint and
// fails the run on any violation.
func e6ChokepointExit(code int) int {
	checked, violations := e6ChokepointSnapshot()
	fmt.Fprintf(os.Stderr, "E6 chokepoint (#674): %d Kubernetes list envelopes checked at jsonHandlerCore, %d violations\n",
		checked, len(violations))
	if len(violations) > 0 {
		for _, v := range violations {
			fmt.Fprintln(os.Stderr, "  ", v)
		}
		return 1
	}
	return code
}

// TestE6_ChokepointCatchesAFourthKey: the chokepoint itself. An E6 envelope
// passes. A live-List envelope (list metadata) is NORMALISED to E6 before the
// stage sees it (#676 C2(a)), so it passes too. A key the normalisation does
// not recognise reaches the check and is a violation. The violation it plants
// is removed again, so the run's TestMain verdict is not affected by it.
func TestE6_ChokepointCatchesAFourthKey(t *testing.T) {
	gvr := schema.GroupVersionResource{Group: "g", Version: "v1", Resource: "things"}
	opts := jsonHandlerOptions{key: "s", out: map[string]any{}, dict: map[string]any{}, k8sList: true, listGVR: gvr}
	before, vBefore := e6ChokepointSnapshot()
	if err := jsonHandlerCore(context.Background(), opts, listenvelope.For("g", "v1", "things", []any{})); err != nil {
		t.Fatal(err)
	}
	after, vAfter := e6ChokepointSnapshot()
	if after != before+1 || len(vAfter) != len(vBefore) {
		t.Fatalf("an E6 envelope: checked %d -> %d, violations %d -> %d; want one check, no violation",
			before, after, len(vBefore), len(vAfter))
	}

	// A live apiserver List (the fall-through): normalised, so no violation,
	// and the stage receives exactly E6.
	live := map[string]any{"apiVersion": "g/v1", "kind": "ThingList",
		"metadata": map[string]any{"resourceVersion": "7", "continue": ""}, "items": []any{}}
	opts.out = map[string]any{}
	if err := jsonHandlerCore(context.Background(), opts, live); err != nil {
		t.Fatal(err)
	}
	if _, v := e6ChokepointSnapshot(); len(v) != len(vAfter) {
		t.Fatalf("a live List was not normalised before the stage: %v", v[len(vAfter):])
	}
	got, _ := json.Marshal(opts.out["s"])
	want, _ := json.Marshal(listenvelope.For("g", "v1", "things", []any{}))
	if string(got) != string(want) {
		t.Fatalf("the stage received %s, want the E6 envelope %s", got, want)
	}

	bad := listenvelope.For("g", "v1", "things", []any{})
	bad["foo"] = "bar" // not a Kubernetes List key: left in place, caught
	if err := jsonHandlerCore(context.Background(), opts, bad); err != nil {
		t.Fatal(err)
	}
	_, vBad := e6ChokepointSnapshot()
	if len(vBad) != len(vAfter)+1 {
		t.Fatalf("a list envelope with an extra key was not caught by the chokepoint (violations %d -> %d)", len(vAfter), len(vBad))
	}
	t.Logf("caught: %s", vBad[len(vBad)-1])
	e6Choke.mu.Lock()
	e6Choke.violations = e6Choke.violations[:len(vAfter)]
	e6Choke.mu.Unlock()

	// A non-LIST call is neither normalised nor checked.
	n0, _ := e6ChokepointSnapshot()
	opts.k8sList = false
	if err := jsonHandlerCore(context.Background(), opts, bad); err != nil {
		t.Fatal(err)
	}
	if n, v := e6ChokepointSnapshot(); n != n0 || len(v) != len(vAfter) {
		t.Fatalf("a non-LIST call was checked: checked %d -> %d, violations %d", n0, n, len(v))
	}
}
