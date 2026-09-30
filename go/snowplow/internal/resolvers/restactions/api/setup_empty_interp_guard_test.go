//go:build unit
// +build unit

package api

import (
	"context"
	"io"
	"log/slog"
	"testing"

	"github.com/krateo-platformops/plumbing/ptr"
	templates "github.com/krateo-platformops/snowplow/apis/templates/v1"
)

// setup_empty_interp_guard_test.go — #288 falsifier for the malformed-name
// empty-interpolation DIAL GUARD.
//
// DEFECT (#288): createRequestOption renders an api-call path via raw evalJQ
// (setup.go:69) with NO validity guard, so a path whose interpolated name or
// namespace segment collapsed (empty) or is DNS-1123-invalid (e.g. a trailing
// '-') is appended to the plan and DIALED — ~600 noise-404s / 20 min on 057.
//
// These arms drive the REAL plan builder createRequestOptions (the choke point
// that appends every call). The guard's contract: a malformed single-object
// apiserver path is SKIPPED (not appended → not dialed), C-3 style; valid paths,
// legit cluster-scoped paths, LIST paths, and external paths are untouched.
//
// The arm set discriminates BOTH ways:
//   - malformed (trailing-dash name, collapsed empty namespace) → SKIPPED;
//   - valid namespaced, valid cluster-scoped (ns legitimately absent), and the
//     valid item of a mixed iterator → STILL DIALED (guard must not over-reject).
//
// RED (unfixed code): the malformed paths ARE appended (dialed) → the skip
// asserts FAIL. GREEN (post-fix): they are skipped, valids still dial. Able-to-fail.

func guard288Log() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// TestEmptyInterpGuard_MalformedPathsNotDialed — the two production shapes from
// #288: shape 1 = a NON-empty but DNS-1123-invalid name (trailing '-'); shape 2
// = a COLLAPSED empty namespace segment (`/namespaces//`). Neither may be dialed.
// A plain empty-segment check would miss shape 1, so the guard must be DNS-1123
// validity.
func TestEmptyInterpGuard_MalformedPathsNotDialed(t *testing.T) {
	log := guard288Log()
	cases := []struct {
		name     string
		path     string
		dict     map[string]any
		rendered string // the malformed path unfixed code would dial
	}{
		{
			name:     "shape1_trailing_dash_name",
			path:     `${ "/api/v1/namespaces/team-a/configmaps/" + (.name) }`,
			dict:     map[string]any{"name": "review-"},
			rendered: "/api/v1/namespaces/team-a/configmaps/review-",
		},
		{
			name:     "shape2_empty_namespace",
			path:     `${ "/api/v1/namespaces/" + (.ns) + "/configmaps/architecture" }`,
			dict:     map[string]any{"ns": ""},
			rendered: "/api/v1/namespaces//configmaps/architecture",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// Fixture sanity: the interpolation really produces the malformed path
			// (guards against fixture drift — we are exercising the real render).
			if got := evalJQ(tc.path, tc.dict); got != tc.rendered {
				t.Fatalf("fixture drift: rendered %q, want %q", got, tc.rendered)
			}
			all := createRequestOptions(context.Background(), log,
				&templates.API{Name: "s", Path: tc.path}, tc.dict)
			if len(all) != 0 {
				t.Fatalf("#288: malformed interpolated path %q was appended to the plan "+
					"(%d call(s)) — it would be DIALED (noise-404). The guard must SKIP it "+
					"(0 calls).", all[0].Path, len(all))
			}
		})
	}
}

// TestEmptyInterpGuard_ValidPathStillDialed — the guard must NOT over-skip: a
// well-formed single-object NAMESPACED path is still dialed. GREEN on both
// unfixed and fixed code (the discriminating control).
func TestEmptyInterpGuard_ValidPathStillDialed(t *testing.T) {
	log := guard288Log()
	const want = "/api/v1/namespaces/team-a/configmaps/ok-name"
	all := createRequestOptions(context.Background(), log,
		&templates.API{Name: "v", Path: `${ "/api/v1/namespaces/team-a/configmaps/ok-name" }`}, nil)
	if len(all) != 1 || all[0].Path != want {
		t.Fatalf("valid namespaced path must be dialed exactly once; got %d call(s) %+v", len(all), all)
	}
}

// TestEmptyInterpGuard_ClusterScopedPath_NotOverRejected — a legit CLUSTER-SCOPED
// path has NO "/namespaces/" segment, so its parsed ns=="" is CORRECT, not a
// collapse. The guard MUST dial it — proving it keys off the "/namespaces//"
// collapse, NOT merely ns=="" (which would over-reject every cluster-scoped
// call). This is the third, both-ways-discriminating arm (TL review ask).
func TestEmptyInterpGuard_ClusterScopedPath_NotOverRejected(t *testing.T) {
	log := guard288Log()
	const want = "/api/v1/nodes/node-1"
	all := createRequestOptions(context.Background(), log,
		&templates.API{Name: "cs", Path: `${ "/api/v1/nodes/" + (.name) }`},
		map[string]any{"name": "node-1"})
	if len(all) != 1 || all[0].Path != want {
		t.Fatalf("legit cluster-scoped path (ns legitimately absent, no /namespaces/) must be "+
			"dialed — the guard must not over-reject ns==\"\"; got %d call(s) %+v", len(all), all)
	}
}

// TestEmptyInterpGuard_Iterator_SkipsOnlyMalformedItems — the guard is per-call,
// so an iterator stage keeps its VALID items and drops only the malformed one
// (the arm must discriminate — valid survives, invalid is skipped). RED (unfixed):
// both items dialed (len 2). GREEN: only the valid item (len 1).
func TestEmptyInterpGuard_Iterator_SkipsOnlyMalformedItems(t *testing.T) {
	log := guard288Log()
	dict := map[string]any{"names": []any{"ok-one", "bad-"}}
	all := createRequestOptions(context.Background(), log, &templates.API{
		Name: "it",
		Path: `${ "/api/v1/namespaces/team-a/configmaps/" + (.) }`,
		DependsOn: &templates.Dependency{
			Name:     "names",
			Iterator: ptr.To(".names"),
		},
	}, dict)
	if len(all) != 1 {
		t.Fatalf("iterator: exactly the 1 VALID item must be dialed; got %d call(s) %+v "+
			"(unfixed dials the malformed 'bad-' item too)", len(all), all)
	}
	if all[0].Path != "/api/v1/namespaces/team-a/configmaps/ok-one" {
		t.Fatalf("iterator: the surviving call must be the valid item; got %q", all[0].Path)
	}
}
