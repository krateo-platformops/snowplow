// resolve_background_denial_563_test.go — #563: a resourceRef RBAC denial must be
// logged and counted by WHO is resolving, not by whether one particular marker
// happens to be on the ctx.
//
// THE DEFECT. #214 de-WARNed the denial for the PREWARM walk only. The refresher
// marks its re-resolve WithBackgroundResolve and never WithPrewarmPath, so its
// denials kept WARNing: 17,578 of 21,283 lines (83%) in 161 min on 057, flat at
// ~109/min, all write verbs. The seed and the engine boot are in the same
// position.
//
// THE TWO FAILURE DIRECTIONS, both armed below, because a red count is not
// coverage when a defect can be fixed wrongly:
//   - UNDER-reaching (the defect): a background driver still WARNs      → arms
//     refresher / cohort-seed / prewarm-engine-boot / background-unattributed.
//   - OVER-reaching (the tempting wrong fix): swapping the prewarm predicate for
//     the background one instead of ORing them re-WARNs the content prewarm,
//     which stamps WithPrewarmPath and NOT WithBackgroundResolve
//     (phase1_content_prewarm.go:251) → arm content-prewarm. That arm is GREEN on
//     origin/main and goes RED on a swap, which is the only thing that catches it.
//
// Plus the attribution the counter owes: the three background producers must land
// in THREE DISTINCT cells (a {background, serve} map passes every log-level arm
// above and still cannot tell the refresher from the seed), and the surviving
// serve WARN must name the requester as a redact label.
//
// Hermetic, same harness shape as #214's: the real resolveOne over a builtin GVR
// (configmaps → cache.KindForGVR builtin arm, no discovery hop), denied through
// rbac.UserCan with no RBAC snapshot resident. Only the denial record is read;
// every other log line is ignored.

package resourcesrefs

import (
	"context"
	"log/slog"
	"reflect"
	"strings"
	"sync"
	"testing"

	xcontext "github.com/krateo-platformops/plumbing/context"
	"github.com/krateo-platformops/plumbing/jwtutil"
	templatesv1 "github.com/krateo-platformops/snowplow/apis/templates/v1"
	"github.com/krateo-platformops/snowplow/internal/cache"
	"github.com/krateo-platformops/snowplow/internal/redact"
	"k8s.io/client-go/rest"
)

const (
	denial563Msg  = "resource ref action not allowed"
	denial563User = "u-563-requester"
)

type denial563Rec struct {
	level slog.Level
	attrs map[string]string
}

type denial563Handler struct {
	mu   *sync.Mutex
	recs *[]denial563Rec
}

func (h denial563Handler) Enabled(context.Context, slog.Level) bool { return true } // capture Debug too
func (h denial563Handler) Handle(_ context.Context, r slog.Record) error {
	if r.Message != denial563Msg {
		return nil
	}
	attrs := map[string]string{}
	r.Attrs(func(a slog.Attr) bool {
		attrs[a.Key] = a.Value.String()
		return true
	})
	h.mu.Lock()
	defer h.mu.Unlock()
	*h.recs = append(*h.recs, denial563Rec{level: r.Level, attrs: attrs})
	return nil
}
func (h denial563Handler) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h denial563Handler) WithGroup(string) slog.Handler      { return h }

// denial563Ctx builds a resolve ctx carrying a REAL identity (so the WARN has
// something to redact and the clear name has somewhere to leak from) and applies
// the driver's marker(s).
func denial563Ctx(wrap func(context.Context) context.Context) (context.Context, *[]denial563Rec) {
	recs := &[]denial563Rec{}
	ctx := xcontext.BuildContext(context.Background(),
		xcontext.WithLogger(slog.New(denial563Handler{mu: &sync.Mutex{}, recs: recs})),
		xcontext.WithUserInfo(jwtutil.UserInfo{Username: denial563User}),
	)
	if wrap != nil {
		ctx = wrap(ctx)
	}
	return ctx, recs
}

func denial563Ref() *templatesv1.ResourceRef {
	// A create (write) ref on a builtin GVR — the production population: the
	// background walks resolve create/publish refs the identity does not hold.
	return &templatesv1.ResourceRef{
		ID: "r563", APIVersion: "v1", Resource: "configmaps",
		Name: "cm563", Namespace: "ns563", Verb: "POST",
	}
}

func TestIssue563_RefDenialIsLoggedAndCountedByItsDriver(t *testing.T) {
	t.Setenv("CACHE_ENABLED", "true") // rbac.UserCan → EvaluateRBAC path; no snapshot → deny, no network

	drivers := []struct {
		name       string
		wrap       func(context.Context) context.Context
		wantLevel  slog.Level
		wantOrigin string
	}{
		{
			// The one denial that may be a real access problem. STILL WARNs.
			name: "serve", wrap: nil,
			wantLevel: slog.LevelWarn, wantOrigin: cache.RefDenialOriginServe,
		},
		{
			// #214's population — the SA content prewarm stamps WithPrewarmPath
			// and NOT WithBackgroundResolve. GREEN before this change; RED if the
			// prewarm arm of the predicate is swapped away instead of added to.
			name: "content-prewarm", wrap: cache.WithPrewarmPath,
			wantLevel: slog.LevelDebug, wantOrigin: cache.RefDenialOriginPrewarmPath,
		},
		{
			// The 83%. RED before this change (WARN).
			name: "refresher",
			wrap: func(ctx context.Context) context.Context {
				return cache.WithBackgroundResolveOrigin(ctx, cache.BackgroundOriginRefresher)
			},
			wantLevel: slog.LevelDebug, wantOrigin: cache.BackgroundOriginRefresher,
		},
		{
			// As production layers it: the cohort seed stamps BOTH markers
			// (phase1_pip_seed.go:710 background origin, :1606 prewarm path).
			// It must attribute to the SEED — if the prewarm fallback won, the
			// seed would hide inside the prewarm cell.
			name: "cohort-seed",
			wrap: func(ctx context.Context) context.Context {
				return cache.WithPrewarmPath(
					cache.WithBackgroundResolveOrigin(ctx, cache.BackgroundOriginCohortSeed))
			},
			wantLevel: slog.LevelDebug, wantOrigin: cache.BackgroundOriginCohortSeed,
		},
		{
			name: "prewarm-engine-boot",
			wrap: func(ctx context.Context) context.Context {
				return cache.WithBackgroundResolveOrigin(ctx, cache.BackgroundOriginPrewarmEngineBoot)
			},
			wantLevel: slog.LevelDebug, wantOrigin: cache.BackgroundOriginPrewarmEngineBoot,
		},
		{
			// A background ctx with no origin: de-WARNed like any other
			// background driver, but counted in the cell that says the
			// attribution has a hole.
			name: "background-unattributed", wrap: cache.WithBackgroundResolve,
			wantLevel: slog.LevelDebug, wantOrigin: cache.RefDenialOriginBackgroundUnattributed,
		},
	}

	var serveOut []templatesv1.ResourceRefResult
	for _, d := range drivers {
		t.Run(d.name, func(t *testing.T) {
			before := cache.RefDeniedByOriginSnapshot()
			ctx, recs := denial563Ctx(d.wrap)
			out, err := resolveOne(ctx, &rest.Config{}, denial563Ref())
			if err != nil {
				t.Fatalf("resolveOne errored: %v", err)
			}

			// PREMISE (non-vacuity): the ref was actually DENIED. Without this
			// every assertion below would pass on a resolve that never denied.
			if len(out) != 1 || out[0].Allowed {
				t.Fatalf("premise: want exactly one DENIED resource-ref result, got %+v", out)
			}
			if len(*recs) != 1 {
				t.Fatalf("premise: want exactly one %q record, got %d", denial563Msg, len(*recs))
			}
			rec := (*recs)[0]

			// (1) LOG LEVEL — a background driver is not a customer problem.
			if rec.level != d.wantLevel {
				t.Errorf("#563: the %s denial logged at %v, want %v "+
					"(every non-customer driver Debugs; only a serve-path denial WARNs)",
					d.name, rec.level, d.wantLevel)
			}

			// (2) COUNTER CELL — exactly this origin moved, and nothing else.
			// This is what a {background, serve} map cannot do: it would pass
			// every level assertion above and still merge refresher with seed.
			after := cache.RefDeniedByOriginSnapshot()
			for origin, now := range after {
				want := before[origin]
				if origin == d.wantOrigin {
					want++
				}
				if now != want {
					t.Errorf("#563: the %s denial moved cell %q by %d, want %d "+
						"(each driver owns its own cell; before=%v after=%v)",
						d.name, origin, now-before[origin], want-before[origin], before, after)
				}
			}
			if _, ok := after[d.wantOrigin]; !ok {
				t.Errorf("#563: cell %q is not published at all — a cell that is absent "+
					"cannot be read as zero", d.wantOrigin)
			}

			// (3) IDENTITY on the surviving WARN — redact label, never clear.
			if d.wantLevel == slog.LevelWarn {
				got, ok := rec.attrs["user"]
				if !ok {
					t.Errorf("#563: the serve-path WARN carries no user attr (%v) — "+
						"a real access problem that does not name the requester is unactionable",
						rec.attrs)
				}
				if want := redact.User(denial563User); got != want {
					t.Errorf("#563: serve WARN user=%q, want the redact label %q", got, want)
				}
				for k, v := range rec.attrs {
					if strings.Contains(v, denial563User) {
						t.Errorf("#453: attr %q carries the username in clear (%q)", k, v)
					}
				}
			} else if got := rec.attrs["origin"]; got != d.wantOrigin {
				t.Errorf("#563: the %s Debug line carries origin=%q, want %q — the log "+
					"must say which driver generated it, not just that it was not a customer",
					d.name, got, d.wantOrigin)
			}

			// (4) BYTE PARITY (load-bearing, inherited from #214): this is a
			// log+counter change only. The emitted result must be identical for
			// every driver.
			if d.name == "serve" {
				serveOut = out
			} else if !reflect.DeepEqual(serveOut, out) {
				t.Errorf("#563: byte-parity broken — %s output != serve output.\nserve=%+v\n%s=%+v",
					d.name, serveOut, d.name, out)
			}
		})
	}
}

// The #214 counter keeps its exact published meaning: snowplow_prewarm_ref_denied_total
// still counts prewarm-path denials and nothing else, so a dashboard built on it
// reads the same after the breakdown lands.
func TestIssue563_PrewarmScalarStillTracksThePrewarmCellOnly(t *testing.T) {
	t.Setenv("CACHE_ENABLED", "true")

	before := cache.PrewarmRefDeniedTotal()
	ctx, _ := denial563Ctx(cache.WithPrewarmPath)
	if out, err := resolveOne(ctx, &rest.Config{}, denial563Ref()); err != nil || len(out) != 1 || out[0].Allowed {
		t.Fatalf("premise: want one denied result, got %+v (err=%v)", out, err)
	}
	if got := cache.PrewarmRefDeniedTotal(); got != before+1 {
		t.Fatalf("#214: a prewarm-path denial must still bump prewarm_ref_denied_total (%d -> %d)", before, got)
	}

	// A REFRESHER denial must NOT land in #214's prewarm key — that key means
	// "the prewarm walk", and widening it silently would misreport the prewarm
	// walk's cost by the refresher's volume.
	before = cache.PrewarmRefDeniedTotal()
	ctx, _ = denial563Ctx(func(c context.Context) context.Context {
		return cache.WithBackgroundResolveOrigin(c, cache.BackgroundOriginRefresher)
	})
	if out, err := resolveOne(ctx, &rest.Config{}, denial563Ref()); err != nil || len(out) != 1 || out[0].Allowed {
		t.Fatalf("premise: want one denied result, got %+v (err=%v)", out, err)
	}
	if got := cache.PrewarmRefDeniedTotal(); got != before {
		t.Fatalf("#563: a refresher denial moved prewarm_ref_denied_total (%d -> %d) — "+
			"that key is the prewarm walk's, the breakdown is where the refresher belongs", before, got)
	}
}
