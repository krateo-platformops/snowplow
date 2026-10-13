// impersonation_capability.go — #608 (b): ask before impersonating a cohort.
//
// THE DEFECT. #574 made the background refresher dial as each cell's cohort
// (user plus groups). The startup probe only asks about `impersonate users`, but
// a cohort's dial also carries its GROUPS, and on krateo-057 the chart's group
// allowlist is empty. So every cohort that has a group was dialled with a request
// the apiserver was certain to refuse: 26,462 refused stage reads, each one
// leaving its cell stale until TTL. The refusal was invisible to the code that
// decided to impersonate, which only knew "users: granted".
//
// THE FIX. Before the refresher impersonates, it checks every principal the
// dial would carry — the user (or the ServiceAccount the username names) and
// each group — with a SelfSubjectAccessReview for the `impersonate` verb. If any
// one is refused, the refresh keeps the pre-#574 ServiceAccount transport, which
// carries NO impersonated-dial marker, so rbac.MustRegateSADial and the UAF
// refilter apply exactly as they did before #574. The refresh still runs; a
// missing grant degrades to the re-gated SA read instead of a certain 403.
//
// WHY A SelfSubjectAccessReview AND NOT `kubectl auth can-i --as`. The SSAR is
// evaluated against the credential that sends it, which is the SAME credential
// the apiserver's impersonation filter evaluates when the dial arrives. `can-i
// --as` impersonates first and then asks, which is a different question (and
// has been measured to answer wrongly on 057).
//
// WHY PER PRINCIPAL, NOT PER GROUP SET. The impersonation filter authorises each
// principal independently, so "is this set permitted" is exactly the AND of the
// per-principal answers. Memoizing per principal lets 1000 cohorts that share a
// handful of groups cost a handful of reviews, not 1000.
//
// THE KIND-WIDE SHORTCUT. An RBAC rule with no resourceNames grants every name
// of that resource, and a review with an empty name matches ONLY such a rule.
// So when the nameless review for a resource is allowed, every name of it is
// allowed and the per-name review is skipped. On a cluster using the shipped
// chart, `users` is granted that way, so every username costs one review in
// total; `groups` is allowlisted by name, so each group costs one review.
//
// FRESHNESS. The memo is cleared whenever the RBAC snapshot generation moves
// (RBACGen). Every ClusterRole/Role/binding event schedules a rebuild, so a roll
// that changes snowplow's own ClusterRole — the chart adding a group to the
// allowlist — invalidates every cached answer.
//
// BOUND. Entries are capped by the number of distinct binding subjects in the
// live RBAC snapshot (with a small floor), so the cap follows the cluster and no
// knob is needed. A cohort may carry authn-supplied groups that are NOT binding
// subjects, so the cap can be reached; on overflow an entry is EVICTED and the
// new answer is still memoized — the memo never refuses to remember.
//
// ERRORS ARE NOT MEMOIZED. A failed review falls back to the SA transport for
// that refresh (counted) and is asked again next time.
package cache

import (
	"context"
	"errors"
	"expvar"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	"golang.org/x/sync/singleflight"

	authzv1 "k8s.io/api/authorization/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

// Impersonation resources, as the apiserver's impersonation filter names them.
// All three live in the core ("") API group.
const (
	ImpersonateResourceUsers           = "users"
	ImpersonateResourceGroups          = "groups"
	ImpersonateResourceServiceAccounts = "serviceaccounts"
)

// ImpersonationReviewFunc answers whether snowplow's own credential may
// impersonate one principal. namespace is set only for serviceaccounts; an empty
// name asks about EVERY name of the resource.
type ImpersonationReviewFunc func(ctx context.Context, resource, namespace, name string) (bool, error)

var impersonationReviewer atomic.Pointer[ImpersonationReviewFunc]

// SetImpersonationReviewer installs the capability reviewer. main installs the
// SelfSubjectAccessReview one built by NewSSARImpersonationReviewer next to
// SetImpersonationAvailable. nil uninstalls it.
func SetImpersonationReviewer(fn ImpersonationReviewFunc) {
	if fn == nil {
		impersonationReviewer.Store(nil)
	} else {
		impersonationReviewer.Store(&fn)
	}
	impCapMemo.reset()
}

// errNoImpersonationReviewer — impersonation was declared available but no
// reviewer was installed. That is a wiring fault; failing to the re-gated SA
// transport is the safe answer, and it is counted as a check error so it is
// loud rather than silent.
var errNoImpersonationReviewer = errors.New("impersonation capability reviewer not installed")

// NewSSARImpersonationReviewer builds the production reviewer: one
// SelfSubjectAccessReview per question, sent with rc's own credential (rc must
// be snowplow's ServiceAccount config, NEVER an impersonating copy — the
// question is what THIS credential may do).
func NewSSARImpersonationReviewer(rc *rest.Config) (ImpersonationReviewFunc, error) {
	if rc == nil {
		return nil, errors.New("nil rest.Config")
	}
	cs, err := kubernetes.NewForConfig(rc)
	if err != nil {
		return nil, err
	}
	return func(ctx context.Context, resource, namespace, name string) (bool, error) {
		got, err := cs.AuthorizationV1().SelfSubjectAccessReviews().Create(ctx,
			&authzv1.SelfSubjectAccessReview{
				Spec: authzv1.SelfSubjectAccessReviewSpec{
					ResourceAttributes: &authzv1.ResourceAttributes{
						Verb:      "impersonate",
						Group:     "",
						Resource:  resource,
						Namespace: namespace,
						Name:      name,
					},
				},
			}, metav1.CreateOptions{})
		if err != nil {
			return false, err
		}
		return got.Status.Allowed, nil
	}, nil
}

// impPrincipal is one memo key. name=="" (and namespace=="") is the kind-wide
// question for resource.
type impPrincipal struct {
	resource, namespace, name string
}

func (p impPrincipal) kindWide() bool { return p.name == "" && p.namespace == "" }

// impCapMemoFloor is the cap when the RBAC snapshot is absent or tiny. It is a
// floor, not a tuning knob: below it the memo would thrash on any real cohort
// population before the snapshot has published.
const impCapMemoFloor = 64

type impCapMemoT struct {
	mu      sync.Mutex
	gen     uint64
	entries map[impPrincipal]bool
}

var impCapMemo = &impCapMemoT{entries: map[impPrincipal]bool{}}

// Counters — see ImpersonationCapabilityStats.
var (
	impPermitted       atomic.Uint64
	impFallbackSA      atomic.Uint64
	impCheckError      atomic.Uint64
	impReviews         atomic.Uint64
	impMemoHits        atomic.Uint64
	impMemoEvictions   atomic.Uint64
	impMemoInvalidates atomic.Uint64
)

func (m *impCapMemoT) reset() {
	m.mu.Lock()
	m.entries = map[impPrincipal]bool{}
	m.gen = RBACGen()
	m.mu.Unlock()
}

// syncGenLocked clears the memo when the RBAC snapshot generation moved and
// returns the generation the caller's answers are valid for.
func (m *impCapMemoT) syncGenLocked() uint64 {
	g := RBACGen()
	if g != m.gen {
		if len(m.entries) > 0 {
			impMemoInvalidates.Add(1)
		}
		m.entries = map[impPrincipal]bool{}
		m.gen = g
	}
	return g
}

func (m *impCapMemoT) lookup(p impPrincipal) (allowed, ok bool, gen uint64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	gen = m.syncGenLocked()
	allowed, ok = m.entries[p]
	return allowed, ok, gen
}

// store records an answer obtained under generation gen. An answer computed
// before an RBAC change must not be recorded against the new generation, so it
// is dropped when the generation moved during the review.
func (m *impCapMemoT) store(p impPrincipal, allowed bool, gen uint64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.syncGenLocked() != gen {
		return
	}
	if _, exists := m.entries[p]; !exists {
		limit := impCapMemoCap()
		for len(m.entries) >= limit {
			if !m.evictOneLocked() {
				break
			}
		}
	}
	m.entries[p] = allowed
}

// evictOneLocked drops one per-name entry. Kind-wide answers are kept: there are
// at most three, and each one stands in for an unbounded number of names.
func (m *impCapMemoT) evictOneLocked() bool {
	for k := range m.entries {
		if k.kindWide() {
			continue
		}
		delete(m.entries, k)
		impMemoEvictions.Add(1)
		return true
	}
	return false
}

// impCapMemoCap derives the bound from the live RBAC snapshot: the distinct
// User, Group and ServiceAccount subjects bound anywhere on the cluster, which
// is the population cohort representatives are drawn from.
func impCapMemoCap() int {
	n := 0
	if s := LiveRBACSnapshot(); s != nil {
		n = len(s.CRBsByUser) + len(s.CRBsByGroup) + len(s.CRBsByServiceAccount) +
			len(s.RBsByUserAllNS) + len(s.RBsByGroupAllNS) + len(s.RBsByServiceAccountAllNS)
	}
	if n < impCapMemoFloor {
		n = impCapMemoFloor
	}
	return n
}

// ImpersonationCapabilityMemoLen reports the memo's current size (tests and
// the expvar family).
func ImpersonationCapabilityMemoLen() int {
	impCapMemo.mu.Lock()
	defer impCapMemo.mu.Unlock()
	return len(impCapMemo.entries)
}

// ImpersonationCapabilityMemoCap reports the current self-adapted bound.
func ImpersonationCapabilityMemoCap() int { return impCapMemoCap() }

// impCapFlight collapses concurrent reviews of the same principal under the same
// RBAC generation: a dirty-mark burst hands the refresher pool many cells of one
// cohort at once, and without it every worker would send its own review for the
// same question before the first answer lands.
var impCapFlight singleflight.Group

func review(ctx context.Context, p impPrincipal) (bool, error) {
	allowed, ok, gen := impCapMemo.lookup(p)
	if ok {
		impMemoHits.Add(1)
		return allowed, nil
	}
	fnp := impersonationReviewer.Load()
	if fnp == nil {
		return false, errNoImpersonationReviewer
	}
	key := strconv.FormatUint(gen, 10) + "\x00" + p.resource + "\x00" + p.namespace + "\x00" + p.name
	v, err, _ := impCapFlight.Do(key, func() (any, error) {
		// A flight that finished between our lookup and this Do already stored
		// the answer; do not ask the apiserver twice.
		if a, ok, _ := impCapMemo.lookup(p); ok {
			return a, nil
		}
		impReviews.Add(1)
		a, err := (*fnp)(ctx, p.resource, p.namespace, p.name)
		if err != nil {
			return false, err
		}
		impCapMemo.store(p, a, gen)
		return a, nil
	})
	if err != nil {
		return false, err
	}
	return v.(bool), nil
}

// principalPermitted applies the kind-wide shortcut, then the per-name review.
func principalPermitted(ctx context.Context, p impPrincipal) (bool, error) {
	all, err := review(ctx, impPrincipal{resource: p.resource})
	if err != nil {
		return false, err
	}
	if all {
		return true, nil
	}
	return review(ctx, p)
}

// userPrincipal maps an impersonated username onto the resource the apiserver's
// impersonation filter authorises it against: a ServiceAccount username is
// checked as `serviceaccounts` in its namespace, anything else as `users`.
func userPrincipal(username string) impPrincipal {
	const saPrefix = "system:serviceaccount:"
	if rest, ok := strings.CutPrefix(username, saPrefix); ok {
		if ns, name, ok := strings.Cut(rest, ":"); ok && ns != "" && name != "" && !strings.Contains(name, ":") {
			return impPrincipal{resource: ImpersonateResourceServiceAccounts, namespace: ns, name: name}
		}
	}
	return impPrincipal{resource: ImpersonateResourceUsers, name: username}
}

// ImpersonationPermitted reports whether snowplow may impersonate (username,
// groups) as ONE dial, i.e. whether every principal it would carry is permitted.
func ImpersonationPermitted(ctx context.Context, username string, groups []string) (bool, error) {
	ok, err := principalPermitted(ctx, userPrincipal(username))
	if err != nil || !ok {
		return false, err
	}
	for _, g := range groups {
		ok, err := principalPermitted(ctx, impPrincipal{resource: ImpersonateResourceGroups, name: g})
		if err != nil || !ok {
			return false, err
		}
	}
	return true, nil
}

// PermittedImpersonatingRESTConfig is ImpersonatingRESTConfig behind the #608
// capability check. It returns the impersonating copy only when the apiserver
// will accept every principal on it; otherwise nil, and the caller keeps the
// pre-#574 ServiceAccount transport with its re-gate.
//
// Counting: every non-nil return is impersonation_permitted; every nil return
// for a cohort that WOULD have been impersonated is impersonation_fallback_sa,
// and the subset caused by a failed review is also
// impersonation_capability_check_error. A nil from ImpersonatingRESTConfig
// itself (impersonation unavailable, no identity) is the pre-#608 path and is
// not counted here.
func PermittedImpersonatingRESTConfig(ctx context.Context, base *rest.Config, username string, groups []string) *rest.Config {
	cfg := ImpersonatingRESTConfig(base, username, groups)
	if cfg == nil {
		return nil
	}
	ok, err := ImpersonationPermitted(ctx, cfg.Impersonate.UserName, cfg.Impersonate.Groups)
	if err != nil {
		impCheckError.Add(1)
		impFallbackSA.Add(1)
		return nil
	}
	if !ok {
		impFallbackSA.Add(1)
		return nil
	}
	impPermitted.Add(1)
	return cfg
}

// ImpersonationCapabilityStats is the counter family, keyed by stat. The first
// three are the #608 detectors; on 057 before the group grant,
// impersonation_fallback_sa reads non-zero for every grouped cohort refreshed —
// it is non-zero DURING the defect's condition, not a health zero.
func ImpersonationCapabilityStats() map[string]uint64 {
	return map[string]uint64{
		"impersonation_permitted":              impPermitted.Load(),
		"impersonation_fallback_sa":            impFallbackSA.Load(),
		"impersonation_capability_check_error": impCheckError.Load(),
		"reviews":                              impReviews.Load(),
		"memo_hits":                            impMemoHits.Load(),
		"memo_evictions":                       impMemoEvictions.Load(),
		"memo_invalidations":                   impMemoInvalidates.Load(),
		"memo_entries":                         uint64(ImpersonationCapabilityMemoLen()),
	}
}

// Startup-probe answers, published for /debug/vars and OTLP so the grant state
// is readable without the log: 1 = granted for every name, 0 = not granted
// kind-wide (a per-name allowlist may still exist; the per-cohort check decides).
var (
	impProbeUsers           atomic.Int64
	impProbeGroupsAll       atomic.Int64
	impProbeServiceAccounts atomic.Int64
)

// SetImpersonationProbeResult records the startup probe's kind-wide answers.
func SetImpersonationProbeResult(users, groupsAll, serviceAccountsAll bool) {
	b := func(v bool) int64 {
		if v {
			return 1
		}
		return 0
	}
	impProbeUsers.Store(b(users))
	impProbeGroupsAll.Store(b(groupsAll))
	impProbeServiceAccounts.Store(b(serviceAccountsAll))
}

// ImpersonationProbeResult returns the startup probe's kind-wide answers as a
// stat-keyed map (1 granted, 0 not).
func ImpersonationProbeResult() map[string]int64 {
	return map[string]int64{
		"users":                     impProbeUsers.Load(),
		"groups_all_names":          impProbeGroupsAll.Load(),
		"serviceaccounts_all_names": impProbeServiceAccounts.Load(),
	}
}

var impCapExpvarOnce sync.Once

// RegisterImpersonationCapabilityExpvar publishes snowplow_impersonation and
// snowplow_impersonation_probe on /debug/vars. Idempotent.
func RegisterImpersonationCapabilityExpvar() {
	impCapExpvarOnce.Do(func() {
		expvar.Publish("snowplow_impersonation", expvar.Func(func() any { return ImpersonationCapabilityStats() }))
		expvar.Publish("snowplow_impersonation_probe", expvar.Func(func() any { return ImpersonationProbeResult() }))
	})
}

// ResetImpersonationCapabilityForTest clears the memo, the reviewer and every
// counter.
func ResetImpersonationCapabilityForTest() {
	impersonationReviewer.Store(nil)
	impCapMemo.reset()
	for _, c := range []*atomic.Uint64{&impPermitted, &impFallbackSA, &impCheckError, &impReviews,
		&impMemoHits, &impMemoEvictions, &impMemoInvalidates} {
		c.Store(0)
	}
	SetImpersonationProbeResult(false, false, false)
}

// SetImpersonationAvailableForTest flips the startup probe's answer and returns
// a restore func.
func SetImpersonationAvailableForTest(v bool) func() {
	prev := impersonationAvailable.Load()
	impersonationAvailable.Store(v)
	return func() { impersonationAvailable.Store(prev) }
}
