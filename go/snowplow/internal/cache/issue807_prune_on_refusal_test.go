// issue807_prune_on_refusal_test.go — #807: a version whose LIST is refused
// with 404 AND that the CRD no longer serves is torn down and refused
// registration until the CRD serves it again.
//
// THE MEASURED SHAPE. On 057 (1.12.50) all 9 informers stuck in watch_broken
// were composition.krateo.io versions the cluster no longer served, every
// watch.broken line was a 404, and #219's prune had already run 50 times. The
// versions came back because a resolver touched the retired path (meetupsites
// v0-1-0 is hard-coded in a RESTAction) and EnsureResourceType registered it
// again — after the CRD event that retired it, so no CRD event would ever name
// it again.
//
// The arms drive the INSTALLED reflector error handler (watchErrorHandlerFor)
// with the error shape the reflector hands it, so the 404 classification and
// the hand-off to the CRD-lifecycle worker are under test, not re-implemented.
package cache

import (
	"fmt"
	"strings"
	"testing"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime/schema"
	metadatafake "k8s.io/client-go/metadata/fake"
)

// crdBytesObjServing builds a CRD whose spec.versions lists every name in
// versions, served only where served[name] is true. One version is storage.
func crdBytesObjServing(t *testing.T, group, plural string, versions []string, served map[string]bool) *bytesObject {
	t.Helper()
	parts := make([]string, 0, len(versions))
	for i, v := range versions {
		parts = append(parts, fmt.Sprintf(`{"name":%q,"served":%t,"storage":%t,"schema":{"openAPIV3Schema":%s}}`,
			v, served[v], i == len(versions)-1, narrowButtonSchema))
	}
	raw := []byte(`{
		"apiVersion":"apiextensions.k8s.io/v1",
		"kind":"CustomResourceDefinition",
		"metadata":{"name":"` + plural + "." + group + `"},
		"spec":{
			"group":"` + group + `",
			"names":{"plural":"` + plural + `","kind":"X"},
			"versions":[` + strings.Join(parts, ",") + `]
		}
	}`)
	bo, err := newBytesObjectFromRaw(raw)
	if err != nil {
		t.Fatalf("crdBytesObjServing: newBytesObjectFromRaw failed: %v", err)
	}
	return bo
}

// refusedListErr is the error the reflector hands the watch-error handler when
// the initial LIST is refused with 404: client-go wraps the apiserver's
// StatusError with %w ("failed to list ...").
func refusedListErr(gvr schema.GroupVersionResource) error {
	return fmt.Errorf("failed to list %s: %w", gvr.String(), apierrors.NewNotFound(gvr.GroupResource(), ""))
}

// setup807 builds a watcher with a synced CRD informer, installs it as Global,
// and returns it with the CRD event handlers.
func setup807(t *testing.T, gvrs ...schema.GroupVersionResource) (*ResourceWatcher, func(obj interface{}), func(oldObj, newObj interface{})) {
	t.Helper()
	t.Setenv("CACHE_ENABLED", "true")
	t.Setenv(envCompositionStreamingList, "true")
	withCleanCRDDiscovery(t)
	ResetDepsForTest()
	t.Cleanup(ResetDepsForTest)
	ResetNavigationDiscoveredGroupsForTest()
	t.Cleanup(ResetNavigationDiscoveredGroupsForTest)
	SetProcessSARestConfig(nil)

	crdGVR := CRDGVRForTest()
	rw := newRouteRaceWatcher(t, true, append([]schema.GroupVersionResource{crdGVR}, gvrs...)...)
	t.Cleanup(func() { rw.Stop() })
	crdSync := make(chan struct{})
	close(crdSync)
	rw.mu.Lock()
	rw.syncCh[crdGVR] = crdSync
	rw.mu.Unlock()
	SetGlobal(rw)
	t.Cleanup(func() { SetGlobal(nil) })
	h := rw.depEventHandlers(crdGVR)
	return rw, h.AddFunc, h.UpdateFunc
}

func waitProcessed807(t *testing.T, n uint64, what string) {
	t.Helper()
	if !WaitCRDDiscoveryProcessedForTest(n, 2000) {
		t.Fatalf("worker did not process %s (want %d events processed): %s", what, n, crdDiscoveryStatsString())
	}
}

// TestIssue807_RefusedUnservedVersionIsPrunedAndBlocked — THE falsifier. A
// version the CRD lists as NOT served, registered by a resolver touch, whose
// LIST then 404s: it must be stopped, unregistered, refused re-registration,
// and registrable again once the CRD serves it.
//
// RED without the prune: the informer stays registered with its reflector
// re-LISTing a 404 forever. RED without the block: the next resolver touch
// re-registers it.
func TestIssue807_RefusedUnservedVersionIsPrunedAndBlocked(t *testing.T) {
	const group, plural = "composition.krateo.io", "meetupsites"
	stale := schema.GroupVersionResource{Group: group, Version: "v0-1-0", Resource: plural}
	live := schema.GroupVersionResource{Group: group, Version: "v0-3-0", Resource: plural}
	rw, add, update := setup807(t, stale, live)

	// The CRD still LISTS v0-1-0 but no longer serves it — the served flag,
	// not the presence of the name, is what counts.
	crdOld := crdBytesObjServing(t, group, plural, []string{"v0-1-0", "v0-3-0"}, map[string]bool{"v0-3-0": true})
	add(crdOld)
	waitProcessed807(t, 1, "CRD ADD")

	// A resolver touches the retired path AFTER the CRD event — the 057 shape.
	if added, _ := rw.EnsureResourceType(stale); !added || !rw.IsRegistered(stale) {
		t.Fatalf("precondition: %s must register on first touch (nothing has refused it yet); added=%v registered=%v",
			stale, added, rw.IsRegistered(stale))
	}
	rw.mu.Lock()
	stop, hadStop := rw.informerStop[stale]
	rw.mu.Unlock()
	if !hadStop {
		t.Fatalf("precondition: no per-GVR stop channel for %s", stale)
	}

	rw.watchErrorHandlerFor(stale)(nil, refusedListErr(stale))
	waitProcessed807(t, 2, "the LIST refusal")

	if rw.IsRegistered(stale) {
		t.Fatalf("RED (#807): %s is STILL REGISTERED after its LIST was refused with 404 and the CRD does not "+
			"serve it. Its reflector re-LISTs a 404 forever and it sits in watch_broken for the life of the "+
			"process. counters: %s", stale, crdDiscoveryStatsString())
	}
	select {
	case <-stop:
	default:
		t.Fatalf("RED (#807): %s was unregistered but its stop channel is still OPEN — the reflector keeps running", stale)
	}
	if s := CRDDiscoveryStatsSnapshot(); s.RefusedVersionPruned != 1 || s.ListRefusedChecks != 1 {
		t.Fatalf("refused_version_pruned_total=%d list_refused_checks_total=%d, want 1 and 1", s.RefusedVersionPruned, s.ListRefusedChecks)
	}

	// The block: the next resolver touch must not bring it back.
	added, ch := rw.EnsureResourceType(stale)
	if added || rw.IsRegistered(stale) {
		t.Fatalf("RED (#807): %s was RE-REGISTERED by a resolver touch after it was pruned — the version comes "+
			"back and 404s forever, which is how installers v0-3-435 broke twice on 057", stale)
	}
	select {
	case <-ch:
	default:
		t.Fatalf("a refused registration must return an already-closed channel — a caller waiting on it would block")
	}
	if s := CRDDiscoveryStatsSnapshot(); s.UnservedRegistrationsRefused != 1 {
		t.Fatalf("unserved_version_registrations_refused_total=%d, want 1", s.UnservedRegistrationsRefused)
	}
	if added, _ := rw.EnsureResourceType(live); !added {
		t.Fatalf("over-block: the served sibling %s was refused registration", live)
	}

	// The CRD serves v0-1-0 again: the block lifts and a touch registers it.
	crdBack := crdBytesObjServing(t, group, plural, []string{"v0-1-0", "v0-3-0"}, map[string]bool{"v0-1-0": true, "v0-3-0": true})
	update(crdOld, crdBack)
	waitProcessed807(t, 3, "CRD UPDATE re-serving the version")
	if rw.IsUnservedVersionBlocked(stale) {
		t.Fatalf("RED (#807): %s is still blocked after the CRD serves it again — the version could never be "+
			"cached again without a pod restart", stale)
	}
	if added, _ := rw.EnsureResourceType(stale); !added || !rw.IsRegistered(stale) {
		t.Fatalf("%s did not register after the CRD served it again; added=%v", stale, added)
	}
}

// TestIssue807_RefusalOnServedVersionIsNotPruned — the counter-case. A 404 on a
// version the CRD DOES serve is an apiserver catching up or an informer that
// has not synced yet: it must never be pruned. Neither may a 404 for a kind
// with no CRD record, nor any error that is not a 404.
func TestIssue807_RefusalOnServedVersionIsNotPruned(t *testing.T) {
	const group, plural = "composition.krateo.io", "portals"
	served := schema.GroupVersionResource{Group: group, Version: "v1-8-76", Resource: plural}
	retired := schema.GroupVersionResource{Group: group, Version: "v1-8-75", Resource: plural}
	noCRD := schema.GroupVersionResource{Group: "example.invalid", Version: "v1", Resource: "things"}
	rw, add, _ := setup807(t, served, retired, noCRD)

	add(crdBytesObjServing(t, group, plural, []string{"v1-8-76"}, map[string]bool{"v1-8-76": true}))
	waitProcessed807(t, 1, "CRD ADD")
	for _, g := range []schema.GroupVersionResource{served, retired, noCRD} {
		rw.EnsureResourceType(g)
		if !rw.IsRegistered(g) {
			t.Fatalf("precondition: %s not registered", g)
		}
	}

	// (1) 404 on a served version — the informer has not synced and must stay.
	rw.watchErrorHandlerFor(served)(nil, refusedListErr(served))
	// (2) 404 on a kind no CRD event has described.
	rw.watchErrorHandlerFor(noCRD)(nil, refusedListErr(noCRD))
	waitProcessed807(t, 3, "two LIST refusals")
	// (3) a non-404 error on a version the CRD does NOT serve is never even checked.
	rw.watchErrorHandlerFor(retired)(nil, fmt.Errorf("failed to list %s: %w", retired, apierrors.NewServiceUnavailable("etcd timeout")))

	s := CRDDiscoveryStatsSnapshot()
	if !rw.IsRegistered(served) || s.ListRefusedStillServed != 1 {
		t.Fatalf("RED (#807 over-prune): a 404 on %s, which the CRD serves, registered=%v still_served=%d — an "+
			"unsynced informer must never be pruned", served, rw.IsRegistered(served), s.ListRefusedStillServed)
	}
	if !rw.IsRegistered(noCRD) || s.ListRefusedCRDUnknown != 1 {
		t.Fatalf("RED (#807 over-prune): a 404 on %s, which no CRD record describes, registered=%v crd_unknown=%d",
			noCRD, rw.IsRegistered(noCRD), s.ListRefusedCRDUnknown)
	}
	if !rw.IsRegistered(retired) || s.ListRefusedChecks != 2 {
		t.Fatalf("RED (#807 over-prune): a 503 on %s reached the prune; registered=%v checks=%d (want 2)",
			retired, rw.IsRegistered(retired), s.ListRefusedChecks)
	}
	if s.RefusedVersionPruned != 0 {
		t.Fatalf("refused_version_pruned_total=%d, want 0", s.RefusedVersionPruned)
	}
	for _, g := range []schema.GroupVersionResource{served, retired, noCRD} {
		if rw.IsUnservedVersionBlocked(g) {
			t.Fatalf("%s was blocked without being pruned", g)
		}
	}
}

// TestIssue807_SchemaPruneAlsoBlocks — #219's prune records the same block, so
// a version a CRD upgrade retired is refused on its next touch rather than
// registered once more and pruned again on its first 404.
func TestIssue807_SchemaPruneAlsoBlocks(t *testing.T) {
	const group, plural = "composition.krateo.io", "installers"
	oldGVR := schema.GroupVersionResource{Group: group, Version: "v0-3-435", Resource: plural}
	newGVR := schema.GroupVersionResource{Group: group, Version: "v0-3-436", Resource: plural}
	rw, add, update := setup807(t, oldGVR, newGVR)

	crdOld := crdBytesObjServing(t, group, plural, []string{"v0-3-435"}, map[string]bool{"v0-3-435": true})
	add(crdOld)
	waitProcessed807(t, 1, "CRD ADD")
	rw.EnsureResourceType(oldGVR)
	crdNew := crdBytesObjServing(t, group, plural, []string{"v0-3-436"}, map[string]bool{"v0-3-436": true})
	update(crdOld, crdNew)
	waitProcessed807(t, 2, "CRD UPDATE retiring the version")

	if s := CRDDiscoveryStatsSnapshot(); s.StaleVersionPruned != 1 {
		t.Fatalf("premise: stale_version_pruned_total=%d, want 1", s.StaleVersionPruned)
	}
	if added, _ := rw.EnsureResourceType(oldGVR); added || rw.IsRegistered(oldGVR) {
		t.Fatalf("RED (#807): %s was re-registered after #219 pruned it — the retired version comes back on the "+
			"next resolver touch", oldGVR)
	}
	if added, _ := rw.EnsureResourceType(newGVR); !added {
		t.Fatalf("over-block: the newly served %s was refused", newGVR)
	}
}

// TestIssue807_EveryFunnelRefusesBlockedVersion — the block holds on all three
// registration funnels, not only EnsureResourceType. EnsureResourceTypeMetadataOnly
// and AddResourceType have no production caller today; a future one must not
// become the door a retired version comes back through. Each refusal counts
// exactly once.
func TestIssue807_EveryFunnelRefusesBlockedVersion(t *testing.T) {
	const group, plural = "composition.krateo.io", "frontends"
	stale := schema.GroupVersionResource{Group: group, Version: "v1-8-6", Resource: plural}
	rw, add, _ := setup807(t, stale)
	// Wired so EnsureResourceTypeMetadataOnly reaches its block check rather
	// than returning early on a missing metadata client.
	rw.SetMetadataClient(metadatafake.NewSimpleMetadataClient(metadatafake.NewTestScheme()))

	add(crdBytesObjServing(t, group, plural, []string{"v1-8-6", "v1-8-8"}, map[string]bool{"v1-8-8": true}))
	waitProcessed807(t, 1, "CRD ADD")
	rw.EnsureResourceType(stale)
	rw.watchErrorHandlerFor(stale)(nil, refusedListErr(stale))
	waitProcessed807(t, 2, "the LIST refusal")
	if !rw.IsUnservedVersionBlocked(stale) || rw.IsRegistered(stale) {
		t.Fatalf("precondition: %s blocked=%v registered=%v, want blocked and unregistered",
			stale, rw.IsUnservedVersionBlocked(stale), rw.IsRegistered(stale))
	}

	refused := func() uint64 { return CRDDiscoveryStatsSnapshot().UnservedRegistrationsRefused }
	closedCh := func(ch <-chan struct{}) bool {
		if ch == nil {
			return false
		}
		select {
		case <-ch:
			return true
		default:
			return false
		}
	}
	funnels := []struct {
		name string
		call func() (added bool, ch <-chan struct{}, hasCh bool)
	}{
		{"EnsureResourceType", func() (bool, <-chan struct{}, bool) {
			a, c := rw.EnsureResourceType(stale)
			return a, c, true
		}},
		{"EnsureResourceTypeMetadataOnly", func() (bool, <-chan struct{}, bool) {
			a, c := rw.EnsureResourceTypeMetadataOnly(stale)
			return a, c, true
		}},
		{"AddResourceType", func() (bool, <-chan struct{}, bool) {
			rw.AddResourceType(stale)
			return false, nil, false
		}},
	}
	for _, f := range funnels {
		before := refused()
		added, ch, hasCh := f.call()
		if added || rw.IsRegistered(stale) {
			t.Fatalf("RED (#807): %s REGISTERED the blocked version %s — a retired version comes back through "+
				"this funnel and 404s forever", f.name, stale)
		}
		if hasCh && !closedCh(ch) {
			t.Fatalf("%s refused %s without an already-closed channel — a caller waiting on it would block", f.name, stale)
		}
		if got := refused() - before; got != 1 {
			t.Fatalf("%s moved unserved_version_registrations_refused_total by %d, want exactly 1", f.name, got)
		}
	}
}

// TestIssue807_EmptyServedSetForgetsAndDeclines — a CRD event whose served set
// is empty (no served version, or a versions subtree that could not be read —
// crdServedGVRs cannot tell them apart) must FORGET the CRD's record, not store
// "serves nothing" and not keep the previous one. A 404 then declines as
// crd_unknown: the version stays registered and is not blocked.
func TestIssue807_EmptyServedSetForgetsAndDeclines(t *testing.T) {
	const group, plural = "composition.krateo.io", "krateoobservabilities"
	gvr := schema.GroupVersionResource{Group: group, Version: "v0-1-22", Resource: plural}
	rw, add, update := setup807(t, gvr)

	versions := []string{"v0-1-22", "v0-1-23"}
	withRecord := crdBytesObjServing(t, group, plural, versions, map[string]bool{"v0-1-23": true})
	add(withRecord)
	waitProcessed807(t, 1, "CRD ADD")
	// The same versions, none served: the served set is empty.
	noneServed := crdBytesObjServing(t, group, plural, versions, map[string]bool{})
	update(withRecord, noneServed)
	waitProcessed807(t, 2, "CRD UPDATE with an empty served set")

	rw.EnsureResourceType(gvr)
	rw.watchErrorHandlerFor(gvr)(nil, refusedListErr(gvr))
	waitProcessed807(t, 3, "the LIST refusal")

	s := CRDDiscoveryStatsSnapshot()
	if !rw.IsRegistered(gvr) || rw.IsUnservedVersionBlocked(gvr) || s.RefusedVersionPruned != 0 {
		t.Fatalf("RED (#807 over-prune): after an empty served set, a 404 on %s pruned=%d registered=%v blocked=%v. "+
			"An empty set may be an unreadable spec; deciding against it (or against the record it replaced) "+
			"tears down a live informer", gvr, s.RefusedVersionPruned, rw.IsRegistered(gvr), rw.IsUnservedVersionBlocked(gvr))
	}
	if s.ListRefusedCRDUnknown != 1 {
		t.Fatalf("list_refused_crd_unknown_total=%d, want 1 — the empty set must FORGET the record", s.ListRefusedCRDUnknown)
	}
}
