package api

// issue904_review_test.go — the independent review's #904 arms, committed with
// assertions (the review ran them as probes).
//
//   R1  a READ that depends on a failed write still runs (reads unchanged), and
//       a write that depends on that (succeeded) read IS sent: a skip does not
//       propagate through a read that ran and succeeded.
//   R2  a write that depends on a continueOnError read that 404s is not sent,
//       and the header carries no identity, URL, path or upstream message.
//   R5  a dependency whose iterator errors (the stage could not be built)
//       FAILED: its dependent write is skipped.
//   R6  (F3) a resolve that aborts before running any stage (a dependsOn naming
//       no stage; a dependsOn cycle) sends nothing and its header is NOT []:
//       a resolve-level Aborted row and every declared stage NotRun.
//   R7  a dry run with a continueOnError read that 404s: the dependent write is
//       Skipped (what a real run would do), not StageNotExecuted.
//   R8  the as-written semantics of the recommended delete lifecycle when the
//       SERVER READ fails: detachVolumes (continueOnError, an iterator over
//       the failed read) has zero items and SUCCEEDS, so delete (dependsOn
//       detachVolumes) IS sent. continueOnError on a write means "send me even
//       if what I depend on did not succeed" — an author who wants the chain to
//       stop at a failed read must not set it on the stage that reads.
//
// Mutation (logic-only): abortedResolve not marking the abort → R6 RED.

import (
	"net/http"
	"strings"
	"testing"

	"github.com/krateo-platformops/plumbing/ptr"
	templates "github.com/krateo-platformops/snowplow/apis/templates/v1"
)

func TestIssue904_R1_ReadAfterFailedWriteRuns_WriteAfterThatReadIsSent(t *testing.T) {
	u := u904NewUpstream(t, u904ConflictRoutes())
	stages := []*templates.API{
		u904Stage("server", http.MethodGet, "/cloudservers/s1"),
		u904After(u904Stage("detach", http.MethodPost, "/cloudservers/s1/detach"), "server"),
		u904After(u904Stage("check", http.MethodGet, "/cloudservers/s1/status"), "detach"),
		u904After(u904Stage("remove", http.MethodDelete, "/cloudservers/s1"), "check"),
	}
	res := u904Resolve(t, u, stages, false)
	if n := u.n(http.MethodGet, "/cloudservers/s1/status"); n != 1 {
		t.Fatalf("a read after a failed write must still run, got %d", n)
	}
	if n := u.n(http.MethodDelete, "/cloudservers/s1"); n != 1 {
		t.Fatalf("a write depending on a read that ran and succeeded is sent (as written), got %d", n)
	}
	res.wantRow(t, u904Row{Name: "detach", Reason: StageReasonError})
	res.wantSucceeded(t, "server", "check", "remove")
}

func TestIssue904_R2_WriteAfterTolerantReadThatFails_NotSent_HeaderClean(t *testing.T) {
	u := u904NewUpstream(t, map[string]u904Route{
		"GET /cloudservers/s1": {http.StatusNotFound, `{"kind":"Status","status":"Failure","code":404,"message":"nope"}`},
	})
	stages := []*templates.API{
		u904Continue(u904Stage("server", http.MethodGet, "/cloudservers/s1")),
		u904After(u904Stage("create", http.MethodPost, "/cloudservers"), "server"),
	}
	res := u904Resolve(t, u, stages, false)
	if n := u.n(http.MethodPost, "/cloudservers"); n != 0 {
		t.Fatalf("a write after a failed (continueOnError) read was sent %d time(s)", n)
	}
	for _, bad := range []string{"cyberjoker", "devs", "cloudservers", "127.0.0.1", "http", "nope"} {
		if strings.Contains(res.raw, bad) {
			t.Fatalf("the header carries %q: %s", bad, res.raw)
		}
	}
	res.wantRow(t, u904Row{Name: "server", Reason: StageReasonNotFound})
	res.wantRow(t, u904Row{Name: "create", Reason: StageReasonSkipped, DependsOn: "server"})
}

func TestIssue904_R5_IteratorErrorDependencyFailed_WriteSkipped(t *testing.T) {
	u := u904NewUpstream(t, map[string]u904Route{"GET /cloudservers/s1": {http.StatusOK, `{"id":"s1"}`}})
	bad := u904After(u904Stage("vols", http.MethodGet, "/vols"), "server")
	bad.DependsOn.Iterator = ptr.To(`.server.id | error("boom")`)
	stages := []*templates.API{
		u904Stage("server", http.MethodGet, "/cloudservers/s1"),
		bad,
		u904After(u904Stage("remove", http.MethodDelete, "/cloudservers/s1"), "vols"),
	}
	res := u904Resolve(t, u, stages, false)
	if n := u.n(http.MethodDelete, "/cloudservers/s1"); n != 0 {
		t.Fatalf("a write after an iterator-error dependency was sent %d time(s)", n)
	}
	res.wantRow(t, u904Row{Name: "vols", Reason: StageReasonError})
	res.wantRow(t, u904Row{Name: "remove", Reason: StageReasonSkipped, DependsOn: "vols"})
}

func TestIssue904_R6_AbortedResolve_HeaderIsNeverAllSucceeded(t *testing.T) {
	cases := map[string][]*templates.API{
		"unknown dependency": {
			u904After(u904Stage("remove", http.MethodDelete, "/cloudservers/s1"), "nosuch"),
		},
		"cycle": {
			u904After(u904Stage("a", http.MethodGet, "/a"), "b"),
			u904After(u904Stage("b", http.MethodDelete, "/b"), "a"),
		},
	}
	for name, stages := range cases {
		t.Run(name, func(t *testing.T) {
			u := u904NewUpstream(t, nil)
			res := u904Resolve(t, u, stages, false)
			if n := u.writes(); n != 0 {
				t.Fatalf("an aborted resolve sent %d write(s)", n)
			}
			if res.raw == "[]" {
				t.Fatalf("#904 F3 RED: a resolve that ran nothing answered the header [] (all succeeded)")
			}
			res.wantRow(t, u904Row{Name: "", Reason: StageReasonAborted})
			for _, s := range stages {
				res.wantRow(t, u904Row{Name: s.Name, Reason: StageReasonNotRun})
			}
		})
	}
}

func TestIssue904_R7_DryRun_TolerantReadFails_DependentWriteSkipped(t *testing.T) {
	u := u904NewUpstream(t, map[string]u904Route{
		"GET /cloudservers/s1": {http.StatusNotFound, `{"kind":"Status","status":"Failure","code":404,"message":"nope"}`},
	})
	stages := []*templates.API{
		u904Continue(u904Stage("server", http.MethodGet, "/cloudservers/s1")),
		u904After(u904Stage("create", http.MethodPost, "/cloudservers"), "server"),
	}
	res := u904Resolve(t, u, stages, true)
	if n := u.writes(); n != 0 {
		t.Fatalf("a dry run sent %d write(s)", n)
	}
	res.wantRow(t, u904Row{Name: "create", Reason: StageReasonSkipped, DependsOn: "server"})
}

func TestIssue904_R8_ServerReadFails_RecommendedLifecycle_AsWritten(t *testing.T) {
	u := u904NewUpstream(t, map[string]u904Route{
		"GET /cloudservers/s1": {http.StatusBadRequest, `{"kind":"Status","status":"Failure","code":400,"message":"boom"}`},
	})
	server := u904Continue(u904Stage("server", http.MethodGet, "/cloudservers/s1"))
	detach := u904Continue(u904After(u904Stage("detachVolumes", http.MethodPost, "/volumes/detach"), "server"))
	detach.DependsOn.Iterator = ptr.To(`.server.volumes`)
	del := u904After(u904Stage("delete", http.MethodDelete, "/cloudservers/s1"), "detachVolumes")
	res := u904Resolve(t, u, []*templates.API{server, detach, del}, false)
	if n := u.n(http.MethodPost, "/volumes/detach"); n != 0 {
		t.Fatalf("detachVolumes iterates a failed read: 0 items expected, got %d POST(s)", n)
	}
	if n := u.n(http.MethodDelete, "/cloudservers/s1"); n != 1 {
		t.Fatalf("as written, delete depends on a SUCCEEDED (zero-item) detachVolumes and is sent once; got %d", n)
	}
	res.wantRow(t, u904Row{Name: "server", Reason: StageReasonError})
	res.wantSucceeded(t, "detachVolumes", "delete")
}
