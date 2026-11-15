// issue1458_watchlist_stub_test.go — the watch-list stub of the #1458 streamed
// list arm (issue1458_rbac_streamed_list_test.go), and the unit test that
// runs the arm over it.
package cache

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/rest"
)

// c1458StubEvent is one change of an RBAC collection.
type c1458StubEvent struct {
	rv  int
	typ string
	obj json.RawMessage
}

// c1458Stub is an httptest apiserver for the four RBAC kinds: create, get,
// update, delete, list, and WATCH — a streamed list when the request carries
// sendInitialEvents=true (every object as ADDED, then the initial-events-end
// bookmark, then the changes), or the changes after a resourceVersion. Every
// other collection is empty, so the watcher's other informers sync.
type c1458Stub struct {
	mu      sync.Mutex
	rv      int
	objs    map[string]map[string]map[string]any
	log     map[string][]c1458StubEvent
	wake    map[chan struct{}]struct{}
	stopped chan struct{}
}

func newC1458Stub() *c1458Stub {
	return &c1458Stub{rv: 100, objs: map[string]map[string]map[string]any{}, log: map[string][]c1458StubEvent{},
		wake: map[chan struct{}]struct{}{}, stopped: make(chan struct{})}
}

var c1458StubKinds = map[string]string{"roles": "Role", "rolebindings": "RoleBinding", "clusterroles": "ClusterRole", "clusterrolebindings": "ClusterRoleBinding"}

func (s *c1458Stub) status(w http.ResponseWriter, code int, reason, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	st := "Failure"
	if code < 300 {
		st = "Success"
	}
	_ = json.NewEncoder(w).Encode(map[string]any{"kind": "Status", "apiVersion": "v1", "metadata": map[string]any{},
		"status": st, "reason": reason, "message": msg, "code": code})
}

// emitLocked records a change and wakes the watches.
func (s *c1458Stub) emitLocked(res, typ string, obj map[string]any) {
	b, _ := json.Marshal(obj)
	s.log[res] = append(s.log[res], c1458StubEvent{rv: s.rv, typ: typ, obj: b})
	for ch := range s.wake {
		select {
		case ch <- struct{}{}:
		default:
		}
	}
}

func (s *c1458Stub) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	const prefix = "/apis/rbac.authorization.k8s.io/v1/"
	if r.URL.Path == "/version" {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"major":"0","minor":"0","gitVersion":"watch-list-stub (the test's own server)","platform":"httptest"}`))
		return
	}
	if !strings.HasPrefix(r.URL.Path, prefix) {
		s.serveOther(w, r)
		return
	}
	parts := strings.Split(strings.TrimPrefix(r.URL.Path, prefix), "/")
	ns := ""
	if len(parts) >= 3 && parts[0] == "namespaces" {
		ns, parts = parts[1], parts[2:]
	}
	res := parts[0]
	kind, known := c1458StubKinds[res]
	if !known {
		s.serveOther(w, r)
		return
	}
	key := func(name string) string {
		if ns != "" {
			return ns + "/" + name
		}
		return name
	}
	if len(parts) == 1 {
		switch r.Method {
		case http.MethodPost:
			var obj map[string]any
			if err := json.NewDecoder(r.Body).Decode(&obj); err != nil {
				s.status(w, 400, "BadRequest", err.Error())
				return
			}
			meta, _ := obj["metadata"].(map[string]any)
			name, _ := meta["name"].(string)
			s.mu.Lock()
			if s.objs[res] == nil {
				s.objs[res] = map[string]map[string]any{}
			}
			if _, exists := s.objs[res][key(name)]; exists {
				s.mu.Unlock()
				s.status(w, 409, "AlreadyExists", name)
				return
			}
			s.rv++
			meta["resourceVersion"] = strconv.Itoa(s.rv)
			meta["uid"] = "uid-" + res + "-" + key(name)
			if ns != "" {
				meta["namespace"] = ns
			}
			obj["kind"], obj["apiVersion"] = kind, k1458API
			s.objs[res][key(name)] = obj
			s.emitLocked(res, "ADDED", obj)
			b, _ := json.Marshal(obj)
			s.mu.Unlock()
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(201)
			_, _ = w.Write(b)
		case http.MethodGet:
			if q.Get("watch") == "true" || q.Get("watch") == "1" {
				s.serveWatch(w, r, res, kind)
				return
			}
			s.mu.Lock()
			keys := make([]string, 0, len(s.objs[res]))
			for k := range s.objs[res] {
				if ns == "" || strings.HasPrefix(k, ns+"/") {
					keys = append(keys, k)
				}
			}
			sort.Strings(keys)
			items := make([]any, 0, len(keys))
			for _, k := range keys {
				items = append(items, s.objs[res][k])
			}
			b, _ := json.Marshal(map[string]any{"kind": kind + "List", "apiVersion": k1458API,
				"metadata": map[string]any{"resourceVersion": strconv.Itoa(s.rv)}, "items": items})
			s.mu.Unlock()
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write(b)
		default:
			s.status(w, 405, "MethodNotAllowed", r.Method)
		}
		return
	}
	name := parts[1]
	s.mu.Lock()
	cur, exists := s.objs[res][key(name)]
	switch r.Method {
	case http.MethodGet:
		if !exists {
			s.mu.Unlock()
			s.status(w, 404, "NotFound", name)
			return
		}
		b, _ := json.Marshal(cur)
		s.mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(b)
	case http.MethodPut:
		var obj map[string]any
		if err := json.NewDecoder(r.Body).Decode(&obj); err != nil || !exists {
			s.mu.Unlock()
			s.status(w, 404, "NotFound", name)
			return
		}
		meta, _ := obj["metadata"].(map[string]any)
		s.rv++
		meta["resourceVersion"] = strconv.Itoa(s.rv)
		meta["uid"] = "uid-" + res + "-" + key(name)
		obj["kind"], obj["apiVersion"] = kind, k1458API
		s.objs[res][key(name)] = obj
		s.emitLocked(res, "MODIFIED", obj)
		b, _ := json.Marshal(obj)
		s.mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(b)
	case http.MethodDelete:
		if !exists {
			s.mu.Unlock()
			s.status(w, 404, "NotFound", name)
			return
		}
		s.rv++
		cur["metadata"].(map[string]any)["resourceVersion"] = strconv.Itoa(s.rv)
		delete(s.objs[res], key(name))
		s.emitLocked(res, "DELETED", cur)
		s.mu.Unlock()
		s.status(w, 200, "", "deleted")
	default:
		s.mu.Unlock()
		s.status(w, 405, "MethodNotAllowed", r.Method)
	}
}

// serveWatch streams one RBAC collection.
func (s *c1458Stub) serveWatch(w http.ResponseWriter, r *http.Request, res, kind string) {
	q := r.URL.Query()
	flusher, _ := w.(http.Flusher)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	enc := json.NewEncoder(w)
	send := func(typ string, obj any) {
		_ = enc.Encode(map[string]any{"type": typ, "object": obj})
	}
	wake := make(chan struct{}, 1)
	s.mu.Lock()
	s.wake[wake] = struct{}{}
	cursor, _ := strconv.Atoi(q.Get("resourceVersion"))
	if q.Get("sendInitialEvents") == "true" {
		keys := make([]string, 0, len(s.objs[res]))
		for k := range s.objs[res] {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			send("ADDED", s.objs[res][k])
		}
		cursor = s.rv
		send("BOOKMARK", map[string]any{"apiVersion": k1458API, "kind": kind, "metadata": map[string]any{
			"resourceVersion": strconv.Itoa(cursor),
			"annotations":     map[string]any{metav1.InitialEventsAnnotationKey: "true"},
		}})
	}
	s.mu.Unlock()
	defer func() {
		s.mu.Lock()
		delete(s.wake, wake)
		s.mu.Unlock()
	}()
	if flusher != nil {
		flusher.Flush()
	}
	for {
		s.mu.Lock()
		var pending []c1458StubEvent
		for _, e := range s.log[res] {
			if e.rv > cursor {
				pending = append(pending, e)
			}
		}
		s.mu.Unlock()
		for _, e := range pending {
			send(e.typ, e.obj)
			cursor = e.rv
		}
		if len(pending) > 0 && flusher != nil {
			flusher.Flush()
		}
		select {
		case <-wake:
		case <-r.Context().Done():
			return
		case <-s.stopped:
			return
		}
	}
}

// serveOther answers every other collection as empty: a watch gets its
// bookmark and stays open, a list is empty, a write is accepted.
func (s *c1458Stub) serveOther(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	apiVersion, kind := reflectorPathListKindFor(r.URL.Path)
	if r.Method != http.MethodGet {
		body, _ := io.ReadAll(r.Body)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(201)
		_, _ = w.Write(body)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	if q.Get("watch") == "true" || q.Get("watch") == "1" {
		w.WriteHeader(http.StatusOK)
		if q.Get("sendInitialEvents") == "true" {
			_ = json.NewEncoder(w).Encode(map[string]any{"type": "BOOKMARK", "object": map[string]any{
				"apiVersion": apiVersion, "kind": strings.TrimSuffix(kind, "List"),
				"metadata": map[string]any{"resourceVersion": "100", "annotations": map[string]any{metav1.InitialEventsAnnotationKey: "true"}},
			}})
		}
		if f, ok := w.(http.Flusher); ok {
			f.Flush()
		}
		select {
		case <-r.Context().Done():
		case <-s.stopped:
		}
		return
	}
	_ = json.NewEncoder(w).Encode(map[string]any{"apiVersion": apiVersion, "kind": kind,
		"metadata": map[string]any{"resourceVersion": "100"}, "items": []any{}})
}

// TestIssue1458Char_StreamedListAndRelist_WatchListStub runs the arm over the
// watch-list stub: the real dynamic client and client-go's real reflector, with
// WatchListClient on, over HTTP.
func TestIssue1458Char_StreamedListAndRelist_WatchListStub(t *testing.T) {
	stub := newC1458Stub()
	srv := httptest.NewServer(stub)
	t.Cleanup(func() {
		close(stub.stopped)
		srv.Close()
	})
	c1458StreamedListArm(t, &rest.Config{Host: srv.URL})
}
