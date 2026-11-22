package dispatchers

// issue1569_refresher_failure_log_test.go — #1569: on 057 (1.12.74) every
// refresh of a resident denial-only cell logged its per-identity denies at
// WARN again (~0.5 lines/s), while the #711/#1476 rule is "the first failure of
// a cell's residency at its own level, every unchanged repeat at DEBUG".
//
// Driven through the REAL refresher pool -> resolveAndPopulateL1, the #681
// harness (i677Setup): a resident "restactions" cell whose stage the
// representative may not read.

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"strings"
	"sync"
	"testing"

	xcontext "github.com/krateo-platformops/plumbing/context"
	"github.com/krateo-platformops/plumbing/endpoints"
	"github.com/krateo-platformops/snowplow/internal/cache"
	"github.com/krateo-platformops/snowplow/internal/objects"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/client-go/rest"
)

// i1569Log captures every slog record as JSON, safely across the refresher's
// goroutines.
type i1569Log struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (l *i1569Log) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.buf.Write(p)
}

// failures returns the levels of the "api call response failure" lines for
// stage `name`, in order, and resets the capture.
func (l *i1569Log) failures(name string) []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	var out []string
	sc := bufio.NewScanner(bytes.NewReader(l.buf.Bytes()))
	sc.Buffer(make([]byte, 1<<20), 1<<24)
	for sc.Scan() {
		var m map[string]any
		if json.Unmarshal(sc.Bytes(), &m) != nil {
			continue
		}
		if m["msg"] == "api call response failure" && m["name"] == name {
			out = append(out, strings.ToUpper(m["level"].(string)))
		}
	}
	l.buf.Reset()
	return out
}

func i1569Capture(t *testing.T) *i1569Log {
	t.Helper()
	l := &i1569Log{}
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(l, &slog.HandlerOptions{Level: slog.LevelDebug})))
	t.Cleanup(func() { slog.SetDefault(prev) })
	return l
}

// R1 — the internal-dispatch (branch C) site: three refreshes of one resident
// denial-only cell. The first logs its deny at WARN, the two unchanged repeats
// at DEBUG, and the cell stays resident throughout.
func TestIssue1569_RefresherRepeatsOfAResidentDenialOnlyCell_AreDebug_InternalDispatch(t *testing.T) {
	logs := i1569Capture(t)
	e := i677Setup(t, i677DefaultRA())
	// The same real pool and handler, its resolve logging through the process
	// logger (the refresher's ctx is the pool's; the resolver logs through the
	// logger on it).
	cache.RegisterRefreshFunc("restactions", func(ctx context.Context, _ string, in cache.ResolvedKeyInputs) error {
		return resolveAndPopulateL1(xcontext.BuildContext(ctx, xcontext.WithLogger(slog.Default())), in, e.saEP, e.saRC)
	})
	var got [][]string
	for i := 0; i < 3; i++ {
		e.cycle(t)
		if _, ok := e.store.GetNoTouch(e.key); !ok {
			t.Fatalf("refresh %d: the denial-only cell is not resident (the arm needs a stored cell)", i+1)
		}
		got = append(got, logs.failures("catalog"))
	}
	for i, lv := range got {
		if len(lv) != 1 {
			t.Fatalf("refresh %d: %d deny lines for the denied stage, want exactly 1 (all rounds: %v)", i+1, len(lv), got)
		}
	}
	if got[0][0] != "WARN" {
		t.Fatalf("the first deny of the cell's residency logged at %s, want WARN (rounds: %v)", got[0][0], got)
	}
	for i := 1; i < 3; i++ {
		if got[i][0] != "DEBUG" {
			t.Fatalf("#1569 RED: refresh %d of a RESIDENT denial-only cell logged its unchanged deny at %s, "+
				"want DEBUG (rounds: %v)", i+1, got[i][0], got)
		}
	}
}

// i1569Line is one captured "api call response failure" line.
type i1569Line struct{ name, level, dispatch string }

// lines returns every failure line captured since the last call, and resets.
func (l *i1569Log) lines() []i1569Line {
	l.mu.Lock()
	defer l.mu.Unlock()
	var out []i1569Line
	sc := bufio.NewScanner(bytes.NewReader(l.buf.Bytes()))
	sc.Buffer(make([]byte, 1<<20), 1<<24)
	for sc.Scan() {
		var m map[string]any
		if json.Unmarshal(sc.Bytes(), &m) != nil || m["msg"] != "api call response failure" {
			continue
		}
		d, _ := m["dispatch"].(string)
		n, _ := m["name"].(string)
		out = append(out, i1569Line{name: n, level: strings.ToUpper(m["level"].(string)), dispatch: d})
	}
	l.buf.Reset()
	return out
}

// i1569WidgetEnv is the #1319 widget harness (a widget whose apiRef
// RESTAction has stages identity A may not read) with a SECOND widget on the
// same RESTAction, as dashboard-inbox is shared by seven widgets on 057.
type i1569WidgetEnv struct {
	*w1319Env
	w2 *unstructured.Unstructured
	k1 cache.ResolvedKeyInputs
	k2 cache.ResolvedKeyInputs
}

// The RESTAction: sa a GET A may not read, sb a GET A may read, sc a LIST A
// may not read.
func i1569WidgetSetup(t *testing.T) *i1569WidgetEnv {
	t.Helper()
	e := &i1569WidgetEnv{w1319Env: w1319Setup(t,
		"/api/v1/namespaces/x/configmaps/y", "/api/v1/namespaces/pub/configmaps/p", "/api/v1/namespaces/x/configmaps")}
	resetRefreshMemoForTest()
	t.Cleanup(resetRefreshMemoForTest)
	// W1's cell, stored by A's cold /call (a denial-only result is stored, #681).
	e.call(t, e.ctx())
	c1, ok := e.cell(t, e.ctx())
	if !ok || c1.Inputs == nil {
		t.Fatal("SETUP: A's denial-only widget result must be stored (#681)")
	}
	e.k1 = *c1.Inputs
	// W2: the same widget shape under another name, its cell resident.
	e.w2 = e.widget.DeepCopy()
	e.w2.SetName(h1WName + "-two")
	e.k2 = e.k1
	e.k2.Name = e.w2.GetName()
	cache.ResolvedCache().PutResolved(fixtureStoreVerdict, cache.ComputeKey(e.k2),
		&cache.ResolvedEntry{RawJSON: []byte(`{"prior":true}`), Inputs: &e.k2})
	return e
}

// refreshCell re-resolves one widget cell the way the refresher does
// (resolveAndPopulateL1, whose ctx carries the process-wide RA-resolve memo).
func (e *i1569WidgetEnv) refreshCell(t *testing.T, w *unstructured.Unstructured, in cache.ResolvedKeyInputs) {
	t.Helper()
	restore := setResolveOnceForTest(func(rctx context.Context, in cache.ResolvedKeyInputs) ([]byte, error) {
		return resolveWidgetForRefresh(rctx, objects.Result{GVR: h1WidgetGVR, Unstructured: w.DeepCopy()}, in, c681AuthnNS)
	})
	defer restore()
	ep := &endpoints.Endpoint{ServerURL: e.user.URL, Token: c681A.token()}
	// The process logger, as the refresher's own lines carry it.
	ctx := xcontext.BuildContext(context.Background(), xcontext.WithLogger(slog.Default()))
	if err := resolveAndPopulateL1(ctx, in, ep, &rest.Config{Host: e.user.URL, BearerToken: c681A.token()}); err != nil {
		t.Fatalf("resolveAndPopulateL1: %v", err)
	}
	if _, ok := cache.ResolvedCache().GetNoTouch(cache.ComputeKey(in)); !ok {
		t.Fatalf("SETUP: the refreshed cell %s is not resident", in.Name)
	}
}

// pass is one dirty-mark burst: both cells re-resolved, the first producing
// the RESTAction and the second served from the memo. The previous burst's memo
// entry is gone (its deps moved, which is what dirtied the cells). It returns
// the failure lines of the whole burst.
func (e *i1569WidgetEnv) pass(t *testing.T, logs *i1569Log, w1First bool) []i1569Line {
	t.Helper()
	resetRefreshMemoForTest()
	logs.lines()
	if w1First {
		e.refreshCell(t, e.widget, e.k1)
		e.refreshCell(t, e.w2, e.k2)
	} else {
		e.refreshCell(t, e.w2, e.k2)
		e.refreshCell(t, e.widget, e.k1)
	}
	return logs.lines()
}

// R2 — the shape 057 runs: several widget cells on one RESTAction, re-resolved
// together by the refresher, whichever is dequeued first producing the
// RESTAction and the others served from the memo. Each cell's unchanged deny
// logs at its level once in the cell's residency, then DEBUG.
func TestIssue1569_RefresherRepeatsAcrossMemoSiblings_AreDebug(t *testing.T) {
	e := i1569WidgetSetup(t)
	// After the setup: the #1319 harness installs its own default logger.
	logs := i1569Capture(t)
	// Four bursts, the producer alternating W1, W2, W1, W2. Burst 2 is W2's
	// first produce in its residency: W2 has not logged its denies yet.
	var passes [][]i1569Line
	for i, w1First := range []bool{true, false, true, false} {
		p := e.pass(t, logs, w1First)
		if len(p) == 0 {
			t.Fatalf("burst %d logged no failure line: the producing resolve did not run", i+1)
		}
		passes = append(passes, p)
	}
	t.Logf("bursts: %v", passes)
	for _, l := range passes[1] {
		if l.level != "WARN" {
			t.Fatalf("burst 2 (W2's first produce): stage %s via %s logged at %s, want WARN — the first deny of "+
				"W2's cell logs at its own level (bursts %v)", l.name, l.dispatch, l.level, passes)
		}
	}
	for i := 2; i < 4; i++ {
		for _, l := range passes[i] {
			if l.level != "DEBUG" {
				t.Fatalf("#1569 RED: burst %d re-resolved a RESIDENT cell whose deny is unchanged, and stage %s via %s "+
					"logged at %s, want DEBUG (bursts %v)", i+1, l.name, l.dispatch, l.level, passes)
			}
		}
	}
}
