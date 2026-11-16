//go:build js && wasm

package fakeapiserver

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/labels"
)

// watcherBuffer is how many events a watcher may fall behind before it is closed, as the cacher
// closes a watcher that does not keep up ("Forcing watcher close due to unresponsiveness"); the
// client then re-watches or relists.
const watcherBuffer = 1024

type watchEvent struct {
	Type   string `json:"type"`
	Object any    `json:"object"`
}

type watcher struct {
	gr     string
	ns     string
	ls     labels.Selector
	fs     *selector
	view   func(map[string]any) map[string]any
	ch     chan watchEvent
	closed bool
}

func (w *watcher) wants(obj map[string]any) bool {
	if obj == nil {
		return false
	}
	if w.ns != "" {
		md, _ := obj["metadata"].(map[string]any)
		if ns, _ := md["namespace"].(string); ns != w.ns {
			return false
		}
	}
	return selected(obj, w.ls, w.fs)
}

// translate is one cache event as this watcher sees it: a change across its selector becomes ADDED
// or DELETED, as the cacher's watcher filter does.
func (w *watcher) translate(e event) []watchEvent {
	now, before := w.wants(e.Object), w.wants(e.Prev)
	switch {
	case e.Type == "ADDED" && now:
		return []watchEvent{{"ADDED", w.view(e.Object)}}
	case e.Type == "DELETED" && (now || before):
		return []watchEvent{{"DELETED", w.view(e.Object)}}
	case e.Type == "MODIFIED" && now && before:
		return []watchEvent{{"MODIFIED", w.view(e.Object)}}
	case e.Type == "MODIFIED" && now:
		return []watchEvent{{"ADDED", w.view(e.Object)}}
	case e.Type == "MODIFIED" && before:
		return []watchEvent{{"DELETED", w.view(e.Object)}}
	}
	return nil
}

// deliver queues events; a watcher that is behind by a full buffer is closed. Holds s.mu.
func (s *Server) deliver(w *watcher, evs []watchEvent) {
	for _, ev := range evs {
		if w.closed {
			return
		}
		select {
		case w.ch <- ev:
		default:
			s.closeWatcher(w)
		}
	}
}

// closeWatcher ends the stream. Holds s.mu.
func (s *Server) closeWatcher(w *watcher) {
	if w.closed {
		return
	}
	w.closed = true
	delete(s.watchers, w)
	close(w.ch)
}

// emit records a committed change in the resource's watch cache and sends it to every matching
// watch. Holds s.mu.
func (s *Server) emit(gr, typ string, obj, prev map[string]any) {
	e := event{Type: typ, Object: deepCopy(obj).(map[string]any), RV: s.rv, Time: s.o.Clock()}
	if prev != nil {
		e.Prev = deepCopy(prev).(map[string]any)
	}
	s.cache(gr).add(e)
	for w := range s.watchers {
		if w.gr == gr {
			s.deliver(w, w.translate(e))
		}
	}
}

func boolParam(v string) bool { return v == "true" || v == "1" }

func (s *Server) watch(c *reqCtx, fs *selector) (bool, error) {
	n, err := negotiate(c.r, false)
	if err != nil {
		return false, err
	}
	q := c.r.URL.Query()
	ls, err := parseLabelSelector(q.Get("labelSelector"))
	if err != nil {
		return false, apierrors.NewBadRequest(err.Error())
	}
	rvRaw := q.Get("resourceVersion")
	_, hasSendInitial := q["sendInitialEvents"]
	sendInitial := boolParam(q.Get("sendInitialEvents"))
	bookmarks := boolParam(q.Get("allowWatchBookmarks"))
	if hasSendInitial {
		// apimachinery's ValidateListOptions: sendInitialEvents needs a watch, NotOlderThan and bookmarks.
		if q.Get("resourceVersionMatch") != "NotOlderThan" {
			return false, apierrors.NewInvalid(listOptionsKind, "", fieldErrors("resourceVersionMatch", "Forbidden", "sendInitialEvents requires setting resourceVersionMatch to NotOlderThan"))
		}
		if !bookmarks {
			return false, apierrors.NewInvalid(listOptionsKind, "", fieldErrors("allowWatchBookmarks", "Required", "allowWatchBookmarks must be set to true when sendInitialEvents is set"))
		}
	}
	var rv uint64
	if rvRaw != "" {
		if rv, err = strconv.ParseUint(rvRaw, 10, 64); err != nil {
			return false, apierrors.NewBadRequest(fmt.Sprintf("invalid resource version: %q", rvRaw))
		}
	}
	timeout := 0
	if t := q.Get("timeoutSeconds"); t != "" {
		if timeout, err = strconv.Atoi(t); err != nil || timeout < 0 {
			return false, apierrors.NewBadRequest(fmt.Sprintf("invalid timeoutSeconds: %q", t))
		}
	}
	flusher, _ := c.w.(http.Flusher)
	view := func(o map[string]any) map[string]any {
		if n.meta {
			return metaOf(o, n.v)
		}
		return deepCopy(o).(map[string]any)
	}
	bookmark := func(annotations map[string]any) watchEvent {
		md := map[string]any{"resourceVersion": strconv.FormatUint(s.rv, 10)}
		if annotations != nil {
			md["annotations"] = annotations
		}
		kind, apiVersion := c.api.Kind, c.gv
		if n.meta {
			kind, apiVersion = "PartialObjectMetadata", "meta.k8s.io/"+n.v
		}
		return watchEvent{"BOOKMARK", map[string]any{"kind": kind, "apiVersion": apiVersion, "metadata": md}}
	}

	s.mu.Lock()
	w := &watcher{gr: c.gr, ns: c.ns, ls: ls, fs: fs, view: view, ch: make(chan watchEvent, watcherBuffer)}
	var initial []watchEvent
	var expired error
	switch {
	case sendInitial || (!hasSendInitial && (rvRaw == "" || rv == 0)):
		// A watch-list, or a legacy watch from "" or "0": the current state as ADDED events.
		for _, o := range s.matching(c, ls, fs) {
			initial = append(initial, watchEvent{"ADDED", view(o)})
		}
		if sendInitial {
			initial = append(initial, bookmark(map[string]any{"k8s.io/initial-events-end": "true"}))
		}
	case hasSendInitial && (rvRaw == "" || rv == 0):
		// sendInitialEvents=false from "" or "0": live events only.
	default:
		cache := s.cache(c.gr)
		evs, ok := cache.since(rv)
		if !ok {
			expired = apierrors.NewResourceExpired(fmt.Sprintf("too old resource version: %d (%d)", rv, cache.oldest()-1))
			break
		}
		for _, e := range evs {
			initial = append(initial, w.translate(e)...)
		}
	}
	items := 0
	for _, e := range initial {
		if e.Type == "ADDED" {
			items++
		}
	}
	if expired == nil {
		s.watchers[w] = struct{}{}
	}
	s.mu.Unlock()

	c.line["items"], c.line["status"] = items, 200
	s.o.Log(c.line)
	if expired != nil {
		// The apiserver answers a watch from a resourceVersion that left its window 200, then one
		// ERROR event (410 Expired). The request line says 200 as it does; this event line says the
		// watch expired, so a reader of the log (the sandbox's capture) can fail closed on it.
		s.o.Log(map[string]any{"event": "fake.watchExpired", "what": watchName(c, rvRaw)})
	}
	c.w.Header().Set("Content-Type", n.ct)
	c.w.Header().Set("Transfer-Encoding", "chunked")
	c.w.WriteHeader(http.StatusOK)
	enc := json.NewEncoder(c.w)
	send := func(ev watchEvent) bool {
		if err := enc.Encode(ev); err != nil {
			return false
		}
		if flusher != nil {
			flusher.Flush()
		}
		return true
	}
	if expired != nil {
		var st any = expired.(apierrors.APIStatus).Status()
		send(watchEvent{"ERROR", withStatusKind(st)})
		return true, nil
	}
	for _, ev := range initial {
		if !send(ev) {
			s.stop(w)
			return true, nil
		}
	}

	// Bookmarks every minute (the cacher's bookmarkFrequency) and the watch's own timeout come from
	// the injectable clock, so tests drive them. This goroutine is the timers' one owner: a timer's
	// callback only signals, and only this loop arms the next bookmark or stops a timer, so nothing
	// races on them and no timer outlives the watch.
	tick := make(chan struct{}, 1)
	signal := func(ch chan struct{}) func() {
		return func() {
			select {
			case ch <- struct{}{}:
			default:
			}
		}
	}
	var stopBookmark func() bool
	if bookmarks {
		stopBookmark = s.o.AfterFunc(bookmarkFrequency, signal(tick))
	}
	deadline := make(chan struct{}, 1)
	var stopDeadline func() bool
	if timeout > 0 {
		stopDeadline = s.o.AfterFunc(time.Duration(timeout)*time.Second, signal(deadline))
	}
	defer func() {
		if stopBookmark != nil {
			stopBookmark()
		}
		if stopDeadline != nil {
			stopDeadline()
		}
		s.stop(w)
	}()
	for {
		select {
		case ev, ok := <-w.ch:
			if !ok || !send(ev) {
				return true, nil
			}
		case <-tick:
			s.mu.Lock()
			ev := bookmark(nil)
			s.mu.Unlock()
			if !send(ev) {
				return true, nil
			}
			stopBookmark = s.o.AfterFunc(bookmarkFrequency, signal(tick))
		case <-deadline:
			if bookmarks {
				s.mu.Lock()
				ev := bookmark(nil)
				s.mu.Unlock()
				send(ev)
			}
			return true, nil
		case <-c.r.Context().Done():
			return true, nil
		}
	}
}

func (s *Server) stop(w *watcher) {
	s.mu.Lock()
	s.closeWatcher(w)
	s.mu.Unlock()
}

// withStatusKind is a Status as it travels: with its kind and apiVersion.
func withStatusKind(st any) any {
	b, _ := json.Marshal(st)
	var m map[string]any
	_ = json.Unmarshal(b, &m)
	m["kind"], m["apiVersion"] = "Status", "v1"
	return m
}

// watchName names a watch for its fake.watchExpired line: the resource, the namespace (or all
// namespaces) and the resourceVersion the watch asked for.
func watchName(c *reqCtx, rv string) string {
	where := "all namespaces"
	if c.ns != "" {
		where = "namespace " + c.ns
	}
	resource := c.resource
	if c.group != "" {
		resource += "." + c.group
	}
	return fmt.Sprintf("watch of %s in %s from resourceVersion %s", resource, where, rv)
}
