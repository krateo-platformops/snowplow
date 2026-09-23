// store_verify_watchlist.go — snowplow#237 deliverable B, the PRODUCTION
// branch of the verifying decorator.
//
// # WHY THIS BRANCH IS THE ONE THAT MATTERS
//
// At client-go v0.35.3 the WatchListClient gate defaults to true and nothing in
// this process opts out, so ListAndWatchWithContext calls watchList() first and
// the classic LIST is only a fallback. A detector that covered only the LIST
// path would be a detector that never runs.
//
// It is also the branch the package's own tests cannot reach by default:
// featuregate_watchlist_test.go disables WatchListClient for the WHOLE package
// in TestMain, because the existing fakes never emit an initial-events bookmark
// and a watch-list reflector would block in WaitForCacheSync until the go-test
// timeout. That is a pre-existing coverage hole, it predates #237, and on its
// own it is sufficient to explain how a store-staleness class shipped
// undetected. The arm that closes it for B is
// TestStoreVerification_RunsUnderWatchList, which opts in per-test via
// clientfeaturestesting.SetFeatureDuringTest and drives a fake that DOES
// implement watch-list semantics.
package cache

import (
	"sync"

	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/watch"
)

// initialEventsVerifier wraps a watch-list stream: it accumulates the synthetic
// ADDED events into a metadata-only map and runs the comparison at the
// k8s.io/initial-events-end bookmark, BEFORE forwarding it.
//
// WHY BEFORE. handleListWatch returns as soon as it processes that bookmark,
// and only then does watchList call r.store.Replace(). Comparing before we
// forward is therefore comparing against a store the incoming snapshot has not
// touched — which is the whole premise of B, and it is why this wrapper exists
// rather than a hook somewhere downstream.
//
// MEMORY. Metadata only: a key string plus two short strings per object,
// roughly 110 bytes, so a 50K GVR costs about 8 MB transiently. It is the ONLY
// new allocation B introduces, it is per-GVR rather than global, and it is
// released at the bookmark — set to nil there, not merely left to fall out of
// scope, because the same watch then stays open for its full 5-10 minute
// lifetime and would otherwise pin the map for all of it.
type initialEventsVerifier struct {
	v      *verifyingListerWatcher
	src    watch.Interface
	out    chan watch.Event
	stopCh chan struct{}
	once   sync.Once
}

var _ watch.Interface = (*initialEventsVerifier)(nil)

func newInitialEventsVerifier(v *verifyingListerWatcher, src watch.Interface) *initialEventsVerifier {
	w := &initialEventsVerifier{
		v:      v,
		src:    src,
		out:    make(chan watch.Event),
		stopCh: make(chan struct{}),
	}
	go w.pump()
	return w
}

func (w *initialEventsVerifier) ResultChan() <-chan watch.Event { return w.out }

// Stop stops the inner watch and releases the pump. Idempotent: the reflector
// calls Stop on several paths (retry, clean shutdown, error) and a second close
// would panic.
func (w *initialEventsVerifier) Stop() {
	w.once.Do(func() {
		close(w.stopCh)
		w.src.Stop()
	})
}

// pump forwards every event in order, accumulating until the bookmark.
//
// It never drops, reorders or synthesises an event, and it never fails a watch:
// the comparison it performs is a diagnostic, and a diagnostic that can break
// the data path is worse than no diagnostic. The only thing it adds to the
// stream's timing is the bounded confirmation re-read, which is paid solely on
// a divergence.
func (w *initialEventsVerifier) pump() {
	defer close(w.out)
	upstream := map[string]objIdentity{}
	accumulating := true
	for {
		select {
		case <-w.stopCh:
			return
		case ev, ok := <-w.src.ResultChan():
			if !ok {
				return
			}
			if accumulating {
				switch ev.Type {
				case watch.Added:
					if key, id, ok := identityOf(ev.Object); ok {
						upstream[key] = id
					}
				case watch.Bookmark:
					if isInitialEventsEndBookmark(ev.Object) {
						w.v.verify(upstream)
						upstream = nil
						accumulating = false
					}
				}
			}
			select {
			case w.out <- ev:
			case <-w.stopCh:
				return
			}
		}
	}
}

// isInitialEventsEndBookmark reports whether a Bookmark carries the annotation
// marking the end of the streamed initial state. This is the same predicate
// client-go's own handleListWatch uses to decide the watch-list has completed,
// and it is the only signal that the accumulated set is whole — a comparison
// run before it would diff a partial set and report the remainder as lost ADDs.
func isInitialEventsEndBookmark(obj runtime.Object) bool {
	acc, err := meta.Accessor(obj)
	if err != nil {
		return false
	}
	return acc.GetAnnotations()[metav1.InitialEventsAnnotationKey] == "true"
}
