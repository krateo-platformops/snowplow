//go:build js && wasm

package fakeapiserver

import (
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/krateo-platformops/plumbing/endpoints"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/validation/field"
	"k8s.io/apiserver/pkg/endpoints/handlers/negotiation"

	"github.com/krateo-platformops/snowplow/internal/credsecret"
)

// Options configure a Server. Clock, AfterFunc and NewUID are injectable for tests.
type Options struct {
	Snapshot     []byte   // snapshot.json
	Pages        [][]byte // the object pages, objects/<n>.json
	SAToken      string   // the ServiceAccount's bearer token (the mounted token file)
	VisitorToken string   // the visitor's bearer token (the synthesised clientconfig, §8)
	Clock        func() time.Time
	AfterFunc    func(d time.Duration, f func()) (stop func() bool)
	Log          func(line map[string]any)
	NewUID       func() string
}

// Server is the fake apiserver: an http.Handler.
type Server struct {
	o       Options
	snap    Snapshot
	mu      sync.Mutex
	rv      uint64
	startRV uint64                                   // the snapshot's resourceVersion: every watch cache starts from that state
	gvs     map[string]map[string]metav1.APIResource // "group/version" (or "v1") -> name -> resource
	custom  map[string]bool                          // "group/resource" served from a CRD
	// selectable is each custom resource's CRD selectableFields, as field labels.
	selectable map[string][]string
	// crds is each custom resource's CRD schemas, by "group/resource".
	crds     map[string]*crdInfo
	store    map[string]map[string]map[string]any // "group/resource" -> "ns/name" -> object
	caches   map[string]*watchCache
	watchers map[*watcher]struct{}
	conts    map[string]*continuation
	authz    *authorizer
	sa       identity
	visitor  identity
	rules    []Rule
}

type continuation struct {
	items   []map[string]any
	rv      string
	start   int
	created time.Time
}

const (
	continueTTL     = 5 * time.Minute // etcd's default compaction interval; an older continue is 410
	maxJSONPatchOps = 10000           // apiserver patch.go maxJSONPatchOperations
)

// New loads the snapshot. It does not re-validate it: bridge.js ran snapshot-rules.js first.
func New(o Options) (*Server, error) {
	if o.Clock == nil {
		o.Clock = time.Now
	}
	if o.AfterFunc == nil {
		o.AfterFunc = func(d time.Duration, f func()) func() bool { return time.AfterFunc(d, f).Stop }
	}
	if o.Log == nil {
		o.Log = func(map[string]any) {}
	}
	if o.NewUID == nil {
		o.NewUID = newUID
	}
	s := &Server{o: o, gvs: map[string]map[string]metav1.APIResource{}, custom: map[string]bool{}, store: map[string]map[string]map[string]any{},
		caches: map[string]*watchCache{}, watchers: map[*watcher]struct{}{}, conts: map[string]*continuation{}, selectable: map[string][]string{}, crds: map[string]*crdInfo{}}
	if err := json.Unmarshal(o.Snapshot, &s.snap); err != nil {
		return nil, fmt.Errorf("snapshot.json: %w", err)
	}
	rv, err := strconv.ParseUint(s.snap.ResourceVersion, 10, 64)
	if err != nil {
		return nil, fmt.Errorf("snapshot.json resourceVersion %q: %w", s.snap.ResourceVersion, err)
	}
	s.rv, s.startRV = rv, rv
	for gv, list := range s.snap.Discovery.Resources {
		m := map[string]metav1.APIResource{}
		for _, r := range list.APIResources {
			m[r.Name] = r
		}
		s.gvs[gv] = m
	}
	var objects []map[string]any
	for i, p := range o.Pages {
		var pg page
		if err := json.Unmarshal(p, &pg); err != nil {
			return nil, fmt.Errorf("object page %d: %w", i, err)
		}
		for _, it := range pg.Items {
			// The stored form (normalize): numbers as json.Number, as every write stores them, so the
			// first no-op write to a loaded object compares equal and commits nothing.
			n, err := normalize(it)
			if err != nil {
				return nil, fmt.Errorf("object page %d: %w", i, err)
			}
			objects = append(objects, n)
		}
	}
	for _, obj := range objects {
		if obj["kind"] == "CustomResourceDefinition" {
			spec, _ := obj["spec"].(map[string]any)
			names, _ := spec["names"].(map[string]any)
			gr := fmt.Sprint(spec["group"]) + "/" + fmt.Sprint(names["plural"])
			s.custom[gr] = true
			if _, info, err := loadCRD(obj); err != nil {
				return nil, fmt.Errorf("CustomResourceDefinition %s: %w", gr, err)
			} else {
				s.crds[gr] = info
			}
			// spec.versions[].selectableFields[].jsonPath (".spec.color") is the field label "spec.color".
			versions, _ := spec["versions"].([]any)
			for _, v := range versions {
				vm, _ := v.(map[string]any)
				sf, _ := vm["selectableFields"].([]any)
				for _, f := range sf {
					fm, _ := f.(map[string]any)
					if jp, ok := fm["jsonPath"].(string); ok && !hasString(s.selectable[gr], strings.TrimPrefix(jp, ".")) {
						s.selectable[gr] = append(s.selectable[gr], strings.TrimPrefix(jp, "."))
					}
				}
			}
		}
	}
	for _, obj := range objects {
		loc, ok := s.locate(obj)
		if !ok {
			return nil, fmt.Errorf("object %v %v: its kind is not in discovery", obj["apiVersion"], obj["kind"])
		}
		s.bucket(loc.gr)[loc.key] = obj
	}
	id := s.snap.Identity
	s.sa = serviceAccountIdentity(id.ServiceAccount.Namespace, id.ServiceAccount.Name)
	s.sa.Label = labelSA
	s.visitor = identity{User: id.Visitor.Username, Groups: append(append([]string{}, id.Visitor.Groups...), "system:authenticated"), Label: labelVisitor}
	// §8: the one Secret, synthesised; the snapshot holds none. It is a bearer-token credential
	// under the visitor's exact name (credsecret.For's first, preferred name: the session name, or
	// the step-up one for a step-up identity), owned through the username annotation
	// (plumbing endpoints.OwnsSecret's annotation arm), so snowplow's own-credential readers serve
	// it to the visitor and to no one else. authn's certificate Secret is not reproduced: the
	// browser's fetch cannot present a client certificate, so the fake authenticates the visitor by
	// the token.
	name := credsecret.For(id.Visitor.Username, id.Visitor.Groups)[0]
	s.bucket("/secrets")[id.AuthnNamespace+"/"+name] = map[string]any{
		"apiVersion": "v1", "kind": "Secret", "type": "Opaque",
		"metadata": map[string]any{"name": name, "namespace": id.AuthnNamespace, "uid": o.NewUID(), "resourceVersion": s.snap.ResourceVersion, "creationTimestamp": s.now(),
			"annotations": map[string]any{endpoints.UsernameAnnotation: id.Visitor.Username}},
		"data": map[string]any{
			"server-url": base64.StdEncoding.EncodeToString([]byte("https://kubernetes.sandbox.invalid")),
			"token":      base64.StdEncoding.EncodeToString([]byte(o.VisitorToken)),
		},
	}
	if sec, err := normalize(s.bucket("/secrets")[id.AuthnNamespace+"/"+name]); err == nil {
		s.bucket("/secrets")[id.AuthnNamespace+"/"+name] = sec
	}
	if s.authz, err = newAuthorizer(objects); err != nil {
		return nil, err
	}
	s.rules = s.snap.Scripted.ApplyStatus
	return s, nil
}

func newUID() string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	b[6] = b[6]&0x0f | 0x40
	b[8] = b[8]&0x3f | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:])
}

func (s *Server) now() string { return s.o.Clock().UTC().Format(time.RFC3339) }

type location struct {
	group, version, resource, namespace, name string
	gr, key                                   string
	api                                       metav1.APIResource
}

func splitGV(gv string) (string, string) {
	if i := strings.Index(gv, "/"); i >= 0 {
		return gv[:i], gv[i+1:]
	}
	return "", gv
}

func (s *Server) locate(obj map[string]any) (location, bool) {
	gv, _ := obj["apiVersion"].(string)
	kind, _ := obj["kind"].(string)
	for _, r := range s.gvs[gv] {
		if r.Kind == kind && !strings.Contains(r.Name, "/") {
			g, v := splitGV(gv)
			md, _ := obj["metadata"].(map[string]any)
			ns, _ := md["namespace"].(string)
			name, _ := md["name"].(string)
			return location{group: g, version: v, resource: r.Name, namespace: ns, name: name, gr: g + "/" + r.Name, key: ns + "/" + name, api: r}, true
		}
	}
	return location{}, false
}

func (s *Server) bucket(gr string) map[string]map[string]any {
	if s.store[gr] == nil {
		s.store[gr] = map[string]map[string]any{}
	}
	return s.store[gr]
}

func (s *Server) cache(gr string) *watchCache {
	if s.caches[gr] == nil {
		// The cache holds the events since the state every list started from, the snapshot.
		s.caches[gr] = newWatchCache(s.startRV)
	}
	return s.caches[gr]
}

// ------------------------------------------------------------------ errors and responses

// statusError is an apiserver error: an apimachinery StatusError, written as its Status.
func writeStatus(w http.ResponseWriter, err error) int {
	var se apierrors.APIStatus
	st := metav1.Status{Status: metav1.StatusFailure, Code: http.StatusInternalServerError, Reason: metav1.StatusReasonInternalError, Message: err.Error()}
	if errors.As(err, &se) {
		st = se.Status()
	}
	st.TypeMeta = metav1.TypeMeta{Kind: "Status", APIVersion: "v1"}
	writeJSON(w, int(st.Code), "application/json", st)
	return int(st.Code)
}

func writeJSON(w http.ResponseWriter, code int, contentType string, v any) {
	b, err := json.Marshal(v)
	if err != nil {
		code, b = http.StatusInternalServerError, []byte(`{"kind":"Status","apiVersion":"v1","metadata":{},"status":"Failure","message":"fake env: encoding failed","reason":"InternalError","code":500}`)
	}
	w.Header().Set("Content-Type", contentType)
	w.WriteHeader(code)
	_, _ = w.Write(b)
}

func notFoundPath() error {
	return apierrors.NewGenericServerResponse(http.StatusNotFound, "", schema.GroupResource{}, "", "the server could not find the requested resource", 0, false)
}

// unimplemented logs fake.unimplemented and returns the loud 500.
func (s *Server) unimplemented(what string) error {
	s.o.Log(map[string]any{"event": "fake.unimplemented", "what": what})
	return apierrors.NewInternalError(fmt.Errorf("fake env: %s is not implemented (SNAPSHOT.md §5)", what))
}

// ------------------------------------------------------------------ authentication

// authenticate is the only way a request gets an identity: no Authorization header is anonymous,
// the ServiceAccount's token and the visitor's are theirs, anything else is 401. With impersonate
// below, a request's log line says sa, visitor, other or none. It never says controller: that
// line is the scripted status write's (applyStatus), which is no request, and no token or header
// names it.
func (s *Server) authenticate(r *http.Request) (identity, error) {
	auth := r.Header.Get("Authorization")
	if auth == "" {
		return identity{User: "system:anonymous", Groups: []string{"system:unauthenticated"}, Label: labelNone}, nil
	}
	tok, ok := strings.CutPrefix(auth, "Bearer ")
	tok = strings.TrimSpace(tok)
	switch {
	case ok && tok == s.o.SAToken:
		return s.sa, nil
	case ok && tok == s.o.VisitorToken:
		return s.visitor, nil
	}
	return identity{}, apierrors.NewUnauthorized("Unauthorized")
}

// impersonate applies Impersonate-* headers as k8s.io/apiserver v0.36.5
// pkg/endpoints/filters/impersonation/impersonation.go does: each impersonated value needs the
// impersonate verb on its own resource (:72-122); groups or extras or a uid without a user is
// 400 (:268-271); without Impersonate-Group a ServiceAccount gets its fixed groups (:85-91); then a
// non-anonymous user gains system:authenticated unless a group already is it or
// system:unauthenticated, and system:anonymous gains system:unauthenticated (:126-157).
func (s *Server) impersonate(r *http.Request, id identity) (identity, error) {
	user := r.Header.Get("Impersonate-User")
	groups := r.Header.Values("Impersonate-Group")
	groupsSpecified := len(groups) > 0
	uid := r.Header.Get("Impersonate-Uid")
	type extra struct{ key, value string }
	var extras []extra
	for h, vs := range r.Header {
		if k, ok := strings.CutPrefix(h, "Impersonate-Extra-"); ok {
			key := strings.ToLower(k)
			if u, err := url.PathUnescape(key); err == nil {
				key = u
			}
			for _, v := range vs {
				extras = append(extras, extra{key, v})
			}
		}
	}
	if user == "" {
		if groupsSpecified || uid != "" || len(extras) > 0 {
			return id, apierrors.NewBadRequest("requested impersonation of groups, extra or uid without impersonating a user")
		}
		return id, nil
	}
	check := func(at attributes) error {
		if !s.authz.allowed(id, at) {
			return apierrors.NewForbidden(schema.GroupResource{Group: at.Group, Resource: at.Resource}, at.Name, errors.New(reason(id, at)))
		}
		return nil
	}
	out := identity{User: user}
	if rest, ok := strings.CutPrefix(user, saPrefix); ok && strings.Count(rest, ":") == 1 {
		ns, name, _ := strings.Cut(rest, ":")
		if err := check(attributes{Verb: "impersonate", Resource: "serviceaccounts", Namespace: ns, Name: name}); err != nil {
			return id, err
		}
		if !groupsSpecified {
			out.Groups = []string{"system:serviceaccounts", "system:serviceaccounts:" + ns}
		}
	} else if err := check(attributes{Verb: "impersonate", Resource: "users", Name: user}); err != nil {
		return id, err
	}
	for _, g := range groups {
		if err := check(attributes{Verb: "impersonate", Resource: "groups", Name: g}); err != nil {
			return id, err
		}
		out.Groups = append(out.Groups, g)
	}
	for _, e := range extras {
		if err := check(attributes{Verb: "impersonate", Group: "authentication.k8s.io", Resource: "userextras", Subresource: e.key, Name: e.value}); err != nil {
			return id, err
		}
	}
	if uid != "" {
		if err := check(attributes{Verb: "impersonate", Group: "authentication.k8s.io", Resource: "uids", Name: uid}); err != nil {
			return id, err
		}
	}
	if user != "system:anonymous" {
		if !hasString(out.Groups, "system:authenticated") && !hasString(out.Groups, "system:unauthenticated") {
			out.Groups = append(out.Groups, "system:authenticated")
		}
	} else if !hasString(out.Groups, "system:unauthenticated") {
		out.Groups = append(out.Groups, "system:unauthenticated")
	}
	out.Label = labelOther
	switch user {
	case s.visitor.User:
		out.Label = labelVisitor
	case s.sa.User:
		out.Label = labelSA
	}
	return out, nil
}

func hasString(list []string, v string) bool {
	for _, x := range list {
		if x == v {
			return true
		}
	}
	return false
}

// ------------------------------------------------------------------ serving

// requestLine is a request-log line (sandbox SNAPSHOT.md §4): exactly the 10 keys verb, group,
// version, resource, namespace, name, query, status, items and identity. items is 0 for a verb that
// returns no items (writes, reviews, discovery, errors); a get, list or watch sets it. identity is
// the request's (sa, visitor, other or none), or controller on the line of a scripted status write
// (applyStatus), which no request made. The fake's only other lines are its event lines:
// applyStatus.failed, fake.unimplemented and fake.watchExpired.
func requestLine(query string) map[string]any {
	return map[string]any{"verb": "", "group": "", "version": "", "resource": "", "namespace": "", "name": "", "query": query, "status": 0, "items": 0, "identity": labelNone}
}

// ServeHTTP serves one request and logs it.
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	line := requestLine(r.URL.RawQuery)
	id, err := s.authenticate(r)
	if err == nil {
		line["identity"] = id.Label
		id, err = s.impersonate(r, id)
		line["identity"] = id.Label
	}
	if err == nil {
		var streamed bool
		streamed, err = s.route(w, r, id, line)
		if streamed {
			return // a watch logged itself when it started
		}
	}
	if err != nil {
		line["status"] = writeStatus(w, err)
	}
	s.o.Log(line)
}

// recorder captures the status a handler wrote, for the log.
type recorder struct {
	http.ResponseWriter
	status int
}

func (r *recorder) WriteHeader(code int) { r.status = code; r.ResponseWriter.WriteHeader(code) }
func (r *recorder) Flush() {
	if f, ok := r.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

func (s *Server) nonResource(path string, id identity, line map[string]any) error {
	line["verb"] = "get"
	at := attributes{Verb: "get", Path: path, NonResource: true}
	if !s.authz.allowed(id, at) {
		return apierrors.NewForbidden(schema.GroupResource{}, "", errors.New(reason(id, at)))
	}
	return nil
}

// reqCtx is one resource request.
type reqCtx struct {
	r                                                   *http.Request
	w                                                   http.ResponseWriter
	id                                                  identity
	line                                                map[string]any
	group, version, gv, resource, subresource, ns, name string
	gr                                                  string
	api                                                 metav1.APIResource
	verb                                                string
	// dryRun: the write runs in full and commits nothing (dryRun=All).
	dryRun bool
}

// parseDryRun reads dryRun as the apiserver's option validation does: "All" is the only value
// (k8s.io/apimachinery pkg/apis/meta/v1/validation ValidateDryRun), anything else is 422.
func parseDryRun(verb string, q url.Values) (bool, error) {
	vs, ok := q["dryRun"]
	if !ok {
		return false, nil
	}
	kind := map[string]string{"create": "CreateOptions", "update": "UpdateOptions", "patch": "PatchOptions", "delete": "DeleteOptions", "deletecollection": "DeleteOptions"}[verb]
	if kind == "" {
		return false, nil
	}
	for _, v := range vs {
		if v != "All" {
			return false, apierrors.NewInvalid(schema.GroupKind{Group: "meta.k8s.io", Kind: kind}, "", field.ErrorList{field.NotSupported(field.NewPath("dryRun"), v, []string{"All"})})
		}
	}
	return true, nil
}

func (s *Server) route(w http.ResponseWriter, r *http.Request, id identity, line map[string]any) (bool, error) {
	path := r.URL.Path
	parts := strings.FieldsFunc(path, func(c rune) bool { return c == '/' })
	switch {
	case path == "/version":
		if err := s.nonResource(path, id, line); err != nil {
			return false, err
		}
		v := s.snap.CapturedFrom.Server
		major, minor := "", ""
		if m := regexp.MustCompile(`^v(\d+)\.(\d+)`).FindStringSubmatch(v); m != nil {
			major, minor = m[1], m[2]
		}
		writeJSON(w, 200, "application/json", map[string]any{"major": major, "minor": minor, "gitVersion": v})
		line["status"] = 200
		return false, nil
	case path == "/api" || path == "/apis":
		if err := s.nonResource(path, id, line); err != nil {
			return false, err
		}
		raw := s.snap.Discovery.API
		if path == "/apis" {
			raw = s.snap.Discovery.APIs
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(200)
		_, _ = w.Write(raw)
		line["status"] = 200
		return false, nil
	case path == "/healthz" || path == "/livez" || path == "/readyz":
		if err := s.nonResource(path, id, line); err != nil {
			return false, err
		}
		w.WriteHeader(200)
		_, _ = w.Write([]byte("ok"))
		line["status"] = 200
		return false, nil
	}
	var group, version string
	var rest []string
	switch {
	case len(parts) >= 2 && parts[0] == "api":
		version, rest = parts[1], parts[2:]
	case len(parts) >= 3 && parts[0] == "apis":
		group, version, rest = parts[1], parts[2], parts[3:]
	default:
		if err := s.nonResource(path, id, line); err != nil {
			return false, err
		}
		return false, notFoundPath()
	}
	gv := version
	if group != "" {
		gv = group + "/" + version
	}
	line["group"], line["version"] = group, version
	resources := s.gvs[gv]
	if len(rest) == 0 {
		if err := s.nonResource(path, id, line); err != nil {
			return false, err
		}
		if resources == nil {
			return false, notFoundPath()
		}
		writeJSON(w, 200, "application/json", s.snap.Discovery.Resources[gv])
		line["status"] = 200
		return false, nil
	}
	// The path is split as the apiserver's RequestInfoFactory splits it (k8s.io/apiserver
	// pkg/endpoints/request/requestinfo.go): namespaces/<ns>/<rest> is namespaced unless <rest> is a
	// namespace's own subresource (status, finalize); a namespace object authorizes in itself.
	ns := ""
	if rest[0] == "namespaces" && len(rest) > 2 && rest[2] != "status" && rest[2] != "finalize" {
		ns, rest = rest[1], rest[2:]
	}
	resource := rest[0]
	name, sub := "", ""
	if len(rest) > 1 {
		name = rest[1]
	}
	if len(rest) > 2 {
		sub = rest[2]
	}
	authzNS := ns
	if group == "" && resource == "namespaces" && ns == "" {
		authzNS = name
	}
	line["resource"], line["namespace"], line["name"] = resource, ns, name
	if sub != "" {
		line["resource"] = resource + "/" + sub
	}
	q := r.URL.Query()
	watch := q.Get("watch") == "true" || q.Get("watch") == "1"
	var verb string
	switch r.Method {
	case http.MethodGet:
		switch {
		case name != "":
			verb = "get"
		case watch:
			verb = "watch"
		default:
			verb = "list"
		}
	case http.MethodPost:
		verb = "create"
	case http.MethodPut:
		verb = "update"
	case http.MethodPatch:
		verb = "patch"
	case http.MethodDelete:
		verb = "delete"
		if name == "" {
			verb = "deletecollection"
		}
	default:
		return false, apierrors.NewMethodNotSupported(schema.GroupResource{Group: group, Resource: resource}, r.Method)
	}
	line["verb"] = verb
	if verb == "list" || verb == "watch" {
		line["name"] = "*"
		if ns == "" {
			line["namespace"] = "*"
		}
	}
	at := attributes{Verb: verb, Group: group, Resource: resource, Subresource: sub, Namespace: authzNS, Name: name}
	if at.Name == "" && (verb == "list" || verb == "watch") {
		at.Name = selectorName(q.Get("fieldSelector")) // RequestInfo: a list/watch pinned by metadata.name= authorizes as that name
	}
	// Authorization comes before routing, as in the apiserver's handler chain: an identity that may
	// not read a resource gets 403 for it whether or not it exists.
	if !s.authz.allowed(id, at) {
		return false, apierrors.NewForbidden(schema.GroupResource{Group: group, Resource: resource}, name, errors.New(reason(id, at)))
	}
	api, ok := resources[resource]
	if !ok || len(rest) > 3 {
		return false, notFoundPath()
	}
	if sub != "" {
		if _, ok := resources[resource+"/"+sub]; !ok {
			return false, notFoundPath()
		}
	}
	if !api.Namespaced && ns != "" {
		return false, notFoundPath()
	}
	// The selector is the handler's to parse, after authorization: a caller that may not list gets
	// 403 whatever its selector says.
	var fs *selector
	if verb == "list" || verb == "watch" {
		var err error
		if fs, err = parseFieldSelector(q.Get("fieldSelector"), s.fieldsFor(group+"/"+resource)); err != nil {
			return false, apierrors.NewBadRequest(err.Error())
		}
	}
	dryRun, err := parseDryRun(verb, q)
	if err != nil {
		return false, err
	}
	c := &reqCtx{r: r, w: w, id: id, line: line, group: group, version: version, gv: gv, resource: resource, subresource: sub, ns: ns, name: name, gr: group + "/" + resource, api: api, verb: verb, dryRun: dryRun}
	if group == "authorization.k8s.io" && verb == "create" {
		line["verb"] = "review"
		return false, s.review(c)
	}
	switch verb {
	case "get":
		return false, s.get(c)
	case "list":
		return false, s.list(c, fs)
	case "watch":
		return s.watch(c, fs)
	case "create", "update", "patch", "delete":
		if group == "" && resource == "secrets" {
			return false, s.unimplemented(verb + " on secrets: the fake env serves only the synthesised clientconfig")
		}
		switch verb {
		case "create":
			return false, s.create(c)
		case "update":
			return false, s.update(c)
		case "patch":
			return false, s.patch(c)
		default:
			return false, s.remove(c)
		}
	}
	return false, s.unimplemented(verb + " on " + c.gr)
}

// ------------------------------------------------------------------ representation

type negotiated struct {
	meta bool
	ct   string
	v    string
}

// negotiate is the response media type: JSON, or PartialObjectMetadata(List) for a metadata client;
// anything else is 406.
func negotiate(r *http.Request, list bool) (negotiated, error) {
	accept := r.Header.Get("Accept")
	if accept == "" {
		accept = "*/*"
	}
	for _, raw := range strings.Split(accept, ",") {
		fields := strings.Split(strings.TrimSpace(raw), ";")
		typ := strings.TrimSpace(fields[0])
		p := map[string]string{}
		for _, f := range fields[1:] {
			if k, v, ok := strings.Cut(strings.TrimSpace(f), "="); ok {
				p[strings.TrimSpace(k)] = strings.TrimSpace(v)
			}
		}
		if typ == "*/*" || typ == "application/*" || (typ == "application/json" && p["as"] == "") {
			return negotiated{ct: "application/json"}, nil
		}
		if typ == "application/json" && p["g"] == "meta.k8s.io" && (p["v"] == "v1" || p["v"] == "v1beta1") && (p["as"] == "PartialObjectMetadata" || p["as"] == "PartialObjectMetadataList") {
			as := "PartialObjectMetadata"
			if list {
				as = "PartialObjectMetadataList"
			}
			return negotiated{meta: true, v: p["v"], ct: "application/json;as=" + as + ";g=meta.k8s.io;v=" + p["v"]}, nil
		}
	}
	return negotiated{}, negotiation.NewNotAcceptableError([]string{"application/json", "application/json;as=PartialObjectMetadata;g=meta.k8s.io;v=v1"})
}

func metaOf(obj map[string]any, v string) map[string]any {
	return map[string]any{"kind": "PartialObjectMetadata", "apiVersion": "meta.k8s.io/" + v, "metadata": deepCopy(obj["metadata"])}
}

// item is one list item: built-in lists encode items without apiVersion/kind, CR lists keep them.
func (s *Server) item(c *reqCtx, obj map[string]any, n negotiated) map[string]any {
	if n.meta {
		return metaOf(obj, n.v)
	}
	out := deepCopy(obj).(map[string]any)
	if !s.custom[c.gr] {
		delete(out, "apiVersion")
		delete(out, "kind")
	}
	return out
}

func deepCopy(v any) any {
	switch t := v.(type) {
	case map[string]any:
		m := make(map[string]any, len(t))
		for k, x := range t {
			m[k] = deepCopy(x)
		}
		return m
	case []any:
		a := make([]any, len(t))
		for i, x := range t {
			a[i] = deepCopy(x)
		}
		return a
	}
	return v
}

// ------------------------------------------------------------------ reads

func (s *Server) notFound(c *reqCtx, name string) error {
	return apierrors.NewNotFound(schema.GroupResource{Group: c.group, Resource: c.resource}, name)
}

func (s *Server) object(c *reqCtx) (map[string]any, error) {
	obj := s.bucket(c.gr)[c.ns+"/"+c.name]
	if obj == nil {
		return nil, s.notFound(c, c.name)
	}
	return obj, nil
}

func (s *Server) get(c *reqCtx) error {
	n, err := negotiate(c.r, false)
	if err != nil {
		return err
	}
	s.mu.Lock()
	obj, err := s.object(c)
	var out map[string]any
	if err == nil {
		out = deepCopy(obj).(map[string]any)
		if n.meta {
			out = metaOf(obj, n.v)
		}
	}
	s.mu.Unlock()
	if err != nil {
		return err
	}
	c.line["items"] = 1
	writeJSON(c.w, 200, n.ct, out)
	c.line["status"] = 200
	return nil
}

func (s *Server) matching(c *reqCtx, ls labels.Selector, fs *selector) []map[string]any {
	type kv struct {
		k string
		o map[string]any
	}
	var out []kv
	for k, o := range s.bucket(c.gr) {
		if c.ns != "" && !strings.HasPrefix(k, c.ns+"/") {
			continue
		}
		if !selected(o, ls, fs) {
			continue
		}
		out = append(out, kv{k, o})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].k < out[j].k })
	objs := make([]map[string]any, len(out))
	for i, x := range out {
		objs[i] = x.o
	}
	return objs
}

func (s *Server) list(c *reqCtx, fs *selector) error {
	n, err := negotiate(c.r, true)
	if err != nil {
		return err
	}
	q := c.r.URL.Query()
	ls, err := parseLabelSelector(q.Get("labelSelector"))
	if err != nil {
		return apierrors.NewBadRequest(err.Error())
	}
	rv := q.Get("resourceVersion")
	limit, _ := strconv.Atoi(q.Get("limit"))
	cont := q.Get("continue")
	s.mu.Lock()
	defer s.mu.Unlock()
	// A continue token lives as long as etcd keeps its revision: the apiserver compacts every
	// 5 minutes (k8s.io/apiserver v0.36.5 pkg/storage/storagebackend/config.go:38).
	for tok, cn := range s.conts {
		if s.o.Clock().Sub(cn.created) > continueTTL {
			delete(s.conts, tok)
		}
	}
	kind, apiVersion := c.api.Kind+"List", c.gv
	if n.meta {
		kind, apiVersion = "PartialObjectMetadataList", "meta.k8s.io/"+n.v
	}
	var all []map[string]any
	listRV, start := strconv.FormatUint(s.rv, 10), 0
	if cont != "" {
		if rv != "" {
			return apierrors.NewBadRequest("specifying resource version is not allowed when using continue")
		}
		cn := s.conts[cont]
		if cn == nil || s.o.Clock().Sub(cn.created) > continueTTL {
			return apierrors.NewResourceExpired("The provided continue parameter is too old to display a consistent list result. You can start a new list without the continue parameter, or use the continue token in this response to retrieve the remainder of the results. Continuing with the provided token results in an inconsistent list - objects that were created, modified, or deleted between the time the first chunk was returned and now may show up in the list.")
		}
		all, listRV, start = cn.items, cn.rv, cn.start
	} else {
		for _, o := range s.matching(c, ls, fs) {
			all = append(all, deepCopy(o).(map[string]any))
		}
	}
	md := map[string]any{"resourceVersion": listRV}
	// No limit, or resourceVersion=0 (served from the watch cache, which ignores limit): the rest.
	end := len(all)
	if limit > 0 && !(cont == "" && rv == "0") {
		end = min(start+limit, len(all))
	}
	pageItems := all[start:end]
	if end < len(all) {
		tok := base64.RawURLEncoding.EncodeToString([]byte(fmt.Sprintf("%s/%s/%d/%s", c.gr, listRV, end, s.o.NewUID())))
		s.conts[tok] = &continuation{items: all, rv: listRV, start: end, created: s.o.Clock()}
		md["continue"] = tok
		if q.Get("labelSelector") == "" && q.Get("fieldSelector") == "" {
			md["remainingItemCount"] = len(all) - end
		}
	}
	items := make([]any, 0, len(pageItems))
	for _, o := range pageItems {
		items = append(items, s.item(c, o, n))
	}
	c.line["items"] = len(items)
	writeJSON(c.w, 200, n.ct, map[string]any{"kind": kind, "apiVersion": apiVersion, "metadata": md, "items": items})
	c.line["status"] = 200
	return nil
}
