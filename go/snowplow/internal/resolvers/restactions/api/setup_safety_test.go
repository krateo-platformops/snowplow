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

// setup_safety_test.go — #288 arch-1217 blocker + verb-aware empty-name safety
// (b) control arms, table-driven over the REAL createRequestOptions plan builder.
// Each row discriminates: a collapsed mutating dial is SKIPPED + counted, while a
// legit nameless op (collection DELETE without trailing slash, POST create, GET
// list, namespace-by-name GET) STILL DIALS + does not bump the counter.
func safetyLog() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

func TestEmptyInterpGuard_VerbAwareEmptyName(t *testing.T) {
	cases := []struct {
		name       string
		path       string
		verb       string
		dict       map[string]any
		wantDialed bool
	}{
		// collapsed by-name DELETE (name==\"\" leaves a trailing slash) → SKIP.
		{"collapsed_delete_skipped", "${ \"/api/v1/namespaces/team-a/configmaps/\" + (.name) }", "DELETE", map[string]any{"name": ""}, false},
		// genuine collection DELETE (no name interp, no trailing slash) → DIAL (discriminator).
		{"collection_delete_dialed", "${ \"/api/v1/namespaces/team-a/configmaps\" }", "DELETE", nil, true},
		// collapsed by-name PUT (no nameless update exists) → SKIP.
		{"collapsed_put_skipped", "${ \"/api/v1/namespaces/team-a/configmaps/\" + (.name) }", "PUT", map[string]any{"name": ""}, false},
		// nameless POST — collection create (SAR-shaped) → DIAL.
		{"nameless_post_dialed", "${ \"/apis/authorization.k8s.io/v1/subjectaccessreviews\" }", "POST", nil, true},
		// namespaces-by-name GET → DIAL (blocker control: ns==\"\" but no /namespaces// collapse).
		{"namespaces_by_name_get_dialed", "${ \"/api/v1/namespaces/\" + (.ns) }", "GET", map[string]any{"ns": "kube-system"}, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			log := safetyLog()
			before := MalformedDialSkippedTotal()
			all := createRequestOptions(context.Background(), log,
				&templates.API{Name: "x", Path: tc.path, Verb: ptr.To(tc.verb)}, tc.dict)
			if tc.wantDialed {
				if len(all) != 1 {
					t.Fatalf("%s: must DIAL (len 1); got %d %+v", tc.name, len(all), all)
				}
				if got := MalformedDialSkippedTotal() - before; got != 0 {
					t.Fatalf("%s: a dial must NOT bump the skip counter; delta=%d", tc.name, got)
				}
			} else {
				if len(all) != 0 {
					t.Fatalf("%s: must SKIP (len 0); got %d, dialed %q", tc.name, len(all), all[0].Path)
				}
				if got := MalformedDialSkippedTotal() - before; got != 1 {
					t.Fatalf("%s: a skip must bump the counter by 1; delta=%d", tc.name, got)
				}
			}
		})
	}
}
