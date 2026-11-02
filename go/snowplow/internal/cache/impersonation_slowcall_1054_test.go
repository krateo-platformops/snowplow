package cache

// impersonation_slowcall_1054_test.go — #1054 (review F6): the impersonation
// capability review is a single-flight. Through ImpersonationPermitted and the
// production middleware: the request that LEADS records the review as its own
// dial (an access review round-trip) and no flight wait; the request that
// JOINS records a flight wait on the leader's review, released completed — or
// leader_failed when the review errored, never "completed".

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/krateo-platformops/snowplow/internal/slowcall"
)

type s1054Log struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (l *s1054Log) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.Write(p)
}

func (l *s1054Log) records() []map[string]any {
	l.mu.Lock()
	defer l.mu.Unlock()
	var out []map[string]any
	for _, line := range strings.Split(strings.TrimSpace(l.b.String()), "\n") {
		var m map[string]any
		if json.Unmarshal([]byte(line), &m) == nil && m["msg"] == "slowcall.request" {
			out = append(out, m)
		}
	}
	return out
}

func s1054ReviewPair(t *testing.T, reviewErr error) (leader, joiner map[string]any) {
	t.Helper()
	const bound = 100 * time.Millisecond
	const user = "s1054-impersonated-user"
	ResetImpersonationCapabilityForTest()
	t.Cleanup(ResetImpersonationCapabilityForTest)
	entered := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	SetImpersonationReviewer(func(ctx context.Context, resource, namespace, name string) (bool, error) {
		once.Do(func() { close(entered) })
		<-release
		return true, reviewErr
	})
	logs := &s1054Log{}
	mon := slowcall.NewMonitorForTest(bound, slog.New(slog.NewJSONHandler(logs, nil)))
	run := func(done chan struct{}) {
		defer close(done)
		h := mon.Middleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			_, _ = ImpersonationPermitted(r.Context(), user, nil)
		}))
		req := httptest.NewRequest(http.MethodGet, "/call", nil)
		req.Pattern = "GET /call"
		h.ServeHTTP(httptest.NewRecorder(), req)
	}
	ld, jd := make(chan struct{}), make(chan struct{})
	go run(ld)
	<-entered
	go run(jd)
	time.Sleep(3 * bound) // the joiner reaches the single-flight and waits
	close(release)
	<-ld
	<-jd
	recs := logs.records()
	if len(recs) != 2 {
		t.Fatalf("PRECONDITION: %d records, want the leader's and the joiner's", len(recs))
	}
	for _, r := range recs {
		if r["wait_kind"] == "impersonation_review" {
			joiner = r
		} else {
			leader = r
		}
	}
	if joiner == nil || leader == nil {
		t.Fatalf("REVIEW RED: no joiner/leader split in %v", recs)
	}
	if b, _ := json.Marshal(recs); strings.Contains(string(b), user) {
		t.Fatalf("the impersonated principal reached a record: %s", b)
	}
	return leader, joiner
}

func TestS1054_ImpersonationReviewLeaderDialsAndJoinerWaits(t *testing.T) {
	leader, joiner := s1054ReviewPair(t, nil)
	if joiner["wait_phase"] != "flight_wait" || joiner["wait_outcome"] != "completed" {
		t.Fatalf("REVIEW RED: joiner wait = %v / %v, want flight_wait / completed", joiner["wait_phase"], joiner["wait_outcome"])
	}
	if _, ok := joiner["dial_kind"]; ok {
		t.Fatalf("REVIEW RED: the joiner records a dial it never sent: %v", joiner)
	}
	if leader["dial_kind"] != "access_review" || leader["dial_verb"] != "create" || leader["wait_kind"] != "access_review" {
		t.Fatalf("REVIEW RED: the leader's own review is not its dial: %v", leader)
	}
	if _, ok := leader["flight_wait_n"]; ok {
		t.Fatalf("REVIEW RED: the leader's own review is labelled a flight wait: %v", leader)
	}
}

func TestS1054_ImpersonationReviewFailureIsNotCompleted(t *testing.T) {
	leader, joiner := s1054ReviewPair(t, errors.New("review refused"))
	if joiner["wait_outcome"] != "leader_failed" {
		t.Fatalf("REVIEW RED: a waiter on a failed review reads %v, want leader_failed", joiner["wait_outcome"])
	}
	if leader["dial_outcome"] != "failed" {
		t.Fatalf("REVIEW RED: the failed review dial reads %v, want failed", leader["dial_outcome"])
	}
}
