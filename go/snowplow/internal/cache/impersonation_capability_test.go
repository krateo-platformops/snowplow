package cache

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"

	"k8s.io/client-go/rest"
)

// #608 (b) — unit arms for the impersonation capability memo. The end-to-end
// arms (real refresher, real HTTP apiserver) live in
// internal/handlers/dispatchers/issue608_impersonation_capability_test.go.

type i608Q struct{ resource, namespace, name string }

// i608Reviewer records every question and answers from allow.
type i608Reviewer struct {
	mu    sync.Mutex
	asked []i608Q
	allow func(q i608Q) (bool, error)
}

func (r *i608Reviewer) fn() ImpersonationReviewFunc {
	return func(_ context.Context, resource, namespace, name string) (bool, error) {
		q := i608Q{resource, namespace, name}
		r.mu.Lock()
		r.asked = append(r.asked, q)
		r.mu.Unlock()
		return r.allow(q)
	}
}

func (r *i608Reviewer) count() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.asked)
}

func i608Setup(t *testing.T, allow func(q i608Q) (bool, error)) *i608Reviewer {
	t.Helper()
	ResetImpersonationCapabilityForTest()
	t.Cleanup(ResetImpersonationCapabilityForTest)
	withImpersonationAvailable(t, true)
	r := &i608Reviewer{allow: allow}
	SetImpersonationReviewer(r.fn())
	return r
}

// usersOnly is 057's grant: every user name, no group, no ServiceAccount.
func usersOnly(q i608Q) (bool, error) { return q.resource == ImpersonateResourceUsers, nil }

func TestIssue608_RefusedGroupYieldsNilConfigAndCountsFallback(t *testing.T) {
	i608Setup(t, usersOnly)
	base := &rest.Config{Host: "https://kubernetes.default.svc"}

	if got := PermittedImpersonatingRESTConfig(context.Background(), base, "alice", []string{"devs"}); got != nil {
		t.Fatalf("a cohort carrying a refused group must get NO impersonating config; got Impersonate=%+v", got.Impersonate)
	}
	if got := PermittedImpersonatingRESTConfig(context.Background(), base, "alice", nil); got == nil {
		t.Fatal("a group-less user cohort is permitted (users granted kind-wide) and must still impersonate (#574)")
	}
	st := ImpersonationCapabilityStats()
	if st["impersonation_fallback_sa"] != 1 || st["impersonation_permitted"] != 1 || st["impersonation_capability_check_error"] != 0 {
		t.Fatalf("counters %v, want fallback_sa=1 permitted=1 check_error=0", st)
	}
}

func TestIssue608_KindWideShortcutCostsOneReviewForAllUsers(t *testing.T) {
	r := i608Setup(t, usersOnly)
	base := &rest.Config{Host: "https://h"}
	for i := 0; i < 500; i++ {
		if PermittedImpersonatingRESTConfig(context.Background(), base, fmt.Sprintf("user-%d", i), nil) == nil {
			t.Fatalf("user-%d refused", i)
		}
	}
	if n := r.count(); n != 1 {
		t.Fatalf("%d reviews for 500 users; the kind-wide answer must cover every name (want 1)", n)
	}
}

func TestIssue608_PlaceholderAndServiceAccountPrincipals(t *testing.T) {
	r := i608Setup(t, func(q i608Q) (bool, error) {
		switch {
		case q.resource == ImpersonateResourceUsers:
			return true, nil
		case q.resource == ImpersonateResourceGroups && q.name == "devs":
			return true, nil
		case q.resource == ImpersonateResourceServiceAccounts && q.namespace == "tenant" && q.name == "bot":
			return true, nil
		}
		return false, nil
	})
	base := &rest.Config{Host: "https://h"}

	// Group-only cohort -> placeholder user + group; both are checked.
	got := PermittedImpersonatingRESTConfig(context.Background(), base, "", []string{"devs"})
	if got == nil || got.Impersonate.UserName != CohortPlaceholderUsername {
		t.Fatalf("group-only cohort with an allowlisted group must impersonate the placeholder; got %+v", got)
	}
	// A ServiceAccount username is authorised as serviceaccounts/<ns>/<name>,
	// exactly as the apiserver's impersonation filter does.
	if PermittedImpersonatingRESTConfig(context.Background(), base, "system:serviceaccount:tenant:bot", nil) == nil {
		t.Fatal("allowlisted ServiceAccount refused")
	}
	if PermittedImpersonatingRESTConfig(context.Background(), base, "system:serviceaccount:other:bot", nil) != nil {
		t.Fatal("a ServiceAccount in a namespace that is not allowlisted must be refused")
	}
	var sawSA bool
	for _, q := range r.asked {
		if q.resource == ImpersonateResourceServiceAccounts && q.namespace == "tenant" && q.name == "bot" {
			sawSA = true
		}
		if q.resource == ImpersonateResourceUsers && q.name != "" {
			t.Fatalf("a per-name users review was sent (%+v) although users are granted kind-wide", q)
		}
	}
	if !sawSA {
		t.Fatalf("the ServiceAccount was never reviewed as serviceaccounts/tenant/bot; asked %+v", r.asked)
	}
}

func TestIssue608_ReviewErrorFallsBackAndIsNotMemoized(t *testing.T) {
	fail := true
	r := i608Setup(t, func(q i608Q) (bool, error) {
		if fail {
			return false, errors.New("apiserver unavailable")
		}
		return true, nil
	})
	base := &rest.Config{Host: "https://h"}
	if PermittedImpersonatingRESTConfig(context.Background(), base, "alice", []string{"devs"}) != nil {
		t.Fatal("a failed review must fall back to the SA transport")
	}
	st := ImpersonationCapabilityStats()
	if st["impersonation_capability_check_error"] != 1 || st["impersonation_fallback_sa"] != 1 {
		t.Fatalf("counters %v, want check_error=1 fallback_sa=1", st)
	}
	fail = false
	if PermittedImpersonatingRESTConfig(context.Background(), base, "alice", []string{"devs"}) == nil {
		t.Fatal("the error was memoized: the next refresh did not re-ask")
	}
	if r.count() < 2 {
		t.Fatalf("only %d reviews; the failed one must be asked again", r.count())
	}
}

func TestIssue608_NoReviewerIsACheckErrorNotASilentImpersonation(t *testing.T) {
	ResetImpersonationCapabilityForTest()
	t.Cleanup(ResetImpersonationCapabilityForTest)
	withImpersonationAvailable(t, true)
	if PermittedImpersonatingRESTConfig(context.Background(), &rest.Config{Host: "https://h"}, "alice", []string{"devs"}) != nil {
		t.Fatal("with no reviewer installed the refresher must keep the re-gated SA transport")
	}
	if ImpersonationCapabilityStats()["impersonation_capability_check_error"] != 1 {
		t.Fatal("a missing reviewer must be counted as a check error")
	}
}

// The bound: more distinct principals than the cap. Authn-supplied groups need
// not be binding subjects, so the population can exceed the snapshot-derived
// cap; the memo must EVICT and keep answering correctly, never refuse to store.
func TestIssue608_MemoBoundEvictsBeyondCap(t *testing.T) {
	PublishRBACSnapshotForTest(nil) // no snapshot: the cap is the floor
	r := i608Setup(t, func(q i608Q) (bool, error) {
		if q.resource == ImpersonateResourceUsers {
			return true, nil
		}
		// even-numbered groups are allowlisted
		var n int
		_, _ = fmt.Sscanf(q.name, "g-%d", &n)
		return q.name != "" && n%2 == 0, nil
	})
	limit := ImpersonationCapabilityMemoCap()
	if limit != impCapMemoFloor {
		t.Fatalf("cap with no snapshot = %d, want the floor %d", limit, impCapMemoFloor)
	}
	base := &rest.Config{Host: "https://h"}
	total := 3 * limit
	for i := 0; i < total; i++ {
		got := PermittedImpersonatingRESTConfig(context.Background(), base, "alice", []string{fmt.Sprintf("g-%d", i)})
		if want := i%2 == 0; (got != nil) != want {
			t.Fatalf("g-%d: permitted=%v, want %v", i, got != nil, want)
		}
		if n := ImpersonationCapabilityMemoLen(); n > limit {
			t.Fatalf("memo holds %d entries, above the cap %d", n, limit)
		}
	}
	st := ImpersonationCapabilityStats()
	if st["memo_evictions"] == 0 {
		t.Fatalf("no evictions after %d distinct groups against a cap of %d: %v", total, limit, st)
	}
	// The most recent principal is memoized (never refused storage): asking
	// again costs no review.
	before := r.count()
	PermittedImpersonatingRESTConfig(context.Background(), base, "alice", []string{fmt.Sprintf("g-%d", total-1)})
	if r.count() != before {
		t.Fatal("the newest answer was not stored — the memo refused to remember at the cap")
	}
	// The kind-wide users answer survives eviction.
	impCapMemo.mu.Lock()
	_, kept := impCapMemo.entries[impPrincipal{resource: ImpersonateResourceUsers}]
	impCapMemo.mu.Unlock()
	if !kept {
		t.Fatal("the kind-wide users answer was evicted; it stands in for every user name")
	}
}

func TestIssue608_MemoInvalidatedByRBACGeneration(t *testing.T) {
	allowed := false
	r := i608Setup(t, func(q i608Q) (bool, error) {
		return q.resource == ImpersonateResourceUsers || allowed, nil
	})
	base := &rest.Config{Host: "https://h"}
	if PermittedImpersonatingRESTConfig(context.Background(), base, "alice", []string{"devs"}) != nil {
		t.Fatal("setup: devs not allowlisted yet")
	}
	allowed = true
	if PermittedImpersonatingRESTConfig(context.Background(), base, "alice", []string{"devs"}) != nil {
		t.Fatal("answer changed without an RBAC generation change — the memo is not memoizing")
	}
	before := r.count()
	BumpRBACGenForTest()
	if PermittedImpersonatingRESTConfig(context.Background(), base, "alice", []string{"devs"}) == nil {
		t.Fatal("after the RBAC generation moved the memo must re-ask and see the new grant")
	}
	if r.count() == before {
		t.Fatal("no review after the generation moved")
	}
}

// Concurrent cells of one cohort must not each send their own review.
func TestIssue608_ConcurrentBurstSendsOneReviewPerPrincipal(t *testing.T) {
	gate := make(chan struct{})
	r := i608Setup(t, func(q i608Q) (bool, error) {
		<-gate
		return q.resource == ImpersonateResourceUsers, nil
	})
	base := &rest.Config{Host: "https://h"}
	var wg sync.WaitGroup
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			PermittedImpersonatingRESTConfig(context.Background(), base, "alice", []string{"devs"})
		}()
	}
	close(gate)
	wg.Wait()
	// users kind-wide, groups kind-wide, groups/devs.
	if n := r.count(); n > 3 {
		t.Fatalf("%d reviews for a 32-cell burst of one cohort; want at most 3", n)
	}
}
