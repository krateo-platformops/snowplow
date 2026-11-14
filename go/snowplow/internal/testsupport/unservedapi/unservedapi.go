// Package unservedapi is a test apiserver for reads of a type the cluster does
// not serve (#1414). It answers as kube-apiserver does on the wire, so the
// client under test (client-go discovery, the RESTMapper, the dynamic client)
// sees what it would see in a cluster:
//
//   - discovery is the aggregated document (apidiscovery.k8s.io/v2
//     APIGroupDiscoveryList) when the request's Accept asks for it, as every
//     apiserver since 1.27 answers; with LegacyDiscovery it is the unaggregated
//     documents (APIVersions, APIGroupList), as an apiserver with the
//     aggregated endpoint off answers. A group/version's own document
//     (/api/v1, /apis/<group>/<version>) is served under both;
//   - a path no handler serves (an unserved group, version or resource) is the
//     apiserver's `404 page not found` in text/plain, not a Status;
//   - a served resource's missing object is the apiserver's NotFound Status;
//   - a group whose aggregated backend is down answers 503 `service
//     unavailable` in text/plain on its document and on its resources alike
//     (the aggregator's proxy error), and is marked Stale in the aggregated
//     document;
//   - with a Token set, a request that does not present it is anonymous, and
//     anonymous is refused (403 Forbidden Status) on every path.
//
// It serves core/v1 configmaps (ns/x, other/x) and pods, and
// widgets.templates.krateo.io/v1beta1 buttons (ns/x). Nothing else: flexes has
// no CRD. It also answers /metrics and the pod log and service proxy
// subresources, so a request that escaped its resource would SUCCEED here and
// be seen.
//
// Every request is recorded: its path, how it authenticated (never the
// credential's value) and whether it asked to impersonate.
package unservedapi

import (
	"io"
	"net/http"
	"regexp"
	"strings"
	"sync"
)

const (
	// Group and GV are the served CRD group.
	Group = "widgets.templates.krateo.io"
	GV    = Group + "/v1beta1"

	aggregatedType = "application/json;g=apidiscovery.k8s.io;v=v2;as=APIGroupDiscoveryList"
	verbs          = `["create","delete","deletecollection","get","list","patch","update","watch"]`
)

// Request is what the server recorded of one request.
type Request struct {
	Method string
	Path   string
	// Auth is "absent", "bearer-match", "bearer-other" or "other-scheme".
	Auth string
	// Impersonates: the request carried an Impersonate-* header.
	Impersonates bool
}

// Server is the test apiserver. The zero value serves aggregated discovery and
// accepts every request.
type Server struct {
	LegacyDiscovery bool // answer the unaggregated discovery documents only
	DiscoveryDown   bool // /api and /apis answer 503
	// DiscoveryRefused, when 401 or 403, is the Status /api and /apis answer:
	// the credential may not discover.
	DiscoveryRefused  int
	WidgetsBackendOff bool // the widgets group's backend is down: 503 on its document and resources
	// The widgets group's discovery is behind its serving: the aggregated
	// document marks it Stale and its own document answers 503
	// WidgetsDocFailures times (-1: always), while its resources are served.
	// The mapper then has no mapping for a resource that IS served.
	WidgetsDiscoveryLags bool
	WidgetsDocFailures   int
	// Token, when set, is the one credential the server accepts.
	Token string

	mu   sync.Mutex
	seen []Request
}

// Requests returns every request received, in order.
func (s *Server) Requests() []Request {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]Request(nil), s.seen...)
}

// Paths returns the paths of every request received, in order.
func (s *Server) Paths() []string {
	var out []string
	for _, r := range s.Requests() {
		out = append(out, r.Path)
	}
	return out
}

var discoveryPath = regexp.MustCompile(`^/(api|apis|api/[^/]+|apis/[^/]+/[^/]+)$`)

// IsDiscovery reports whether path is a discovery document.
func IsDiscovery(path string) bool { return discoveryPath.MatchString(path) }

// objectPath is the one shape a read of a namespaced object may dial:
// /api[s/<group>]/<version>/namespaces/<ns>/<resource>/<name>.
var objectPath = regexp.MustCompile(`^/(api|apis/[^/]+)/[^/]+/namespaces/[^/]+/[^/]+/[^/]+$`)

// ObjectPaths returns the non-discovery paths received, and those among them
// that are not the path of one namespaced object.
func (s *Server) ObjectPaths() (all, malformed []string) {
	for _, p := range s.Paths() {
		if IsDiscovery(p) {
			continue
		}
		all = append(all, p)
		if !objectPath.MatchString(p) {
			malformed = append(malformed, p)
		}
	}
	return all, malformed
}

func (s *Server) docFails() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.WidgetsDocFailures < 0 {
		return true
	}
	if s.WidgetsDocFailures == 0 {
		return false
	}
	s.WidgetsDocFailures--
	return true
}

func classifyAuth(h, token string) string {
	switch {
	case h == "":
		return "absent"
	case token != "" && h == "Bearer "+token:
		return "bearer-match"
	case strings.HasPrefix(h, "Bearer "):
		return "bearer-other"
	default:
		return "other-scheme"
	}
}

func notFoundStatus(resource, group, name string) string {
	qualified, details := resource, `"name":"`+name+`","kind":"`+resource+`"`
	if group != "" {
		qualified = resource + "." + group
		details = `"name":"` + name + `","group":"` + group + `","kind":"` + resource + `"`
	}
	return `{"kind":"Status","apiVersion":"v1","metadata":{},"status":"Failure",` +
		`"message":"` + qualified + ` \"` + name + `\" not found","reason":"NotFound","details":{` + details + `},"code":404}`
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	rec := Request{Method: r.Method, Path: r.URL.Path, Auth: classifyAuth(r.Header.Get("Authorization"), s.Token)}
	for h := range r.Header {
		if strings.HasPrefix(h, "Impersonate-") {
			rec.Impersonates = true
		}
	}
	s.mu.Lock()
	s.seen = append(s.seen, rec)
	s.mu.Unlock()

	plain := func(code int, body string) {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		w.Header().Set("X-Content-Type-Options", "nosniff")
		w.WriteHeader(code)
		_, _ = io.WriteString(w, body)
	}
	json := func(code int, contentType, body string) {
		w.Header().Set("Content-Type", contentType)
		w.WriteHeader(code)
		_, _ = io.WriteString(w, body)
	}

	if s.Token != "" && rec.Auth != "bearer-match" {
		json(http.StatusForbidden, "application/json", `{"kind":"Status","apiVersion":"v1","metadata":{},"status":"Failure",`+
			`"message":"forbidden: User \"system:anonymous\" cannot get path \"`+r.URL.Path+`\"","reason":"Forbidden","details":{},"code":403}`)
		return
	}

	aggregated := !s.LegacyDiscovery && strings.Contains(r.Header.Get("Accept"), "g=apidiscovery.k8s.io;v=v2;as=APIGroupDiscoveryList")
	path := r.URL.Path
	const buttons = "/apis/" + GV + "/namespaces/ns/buttons/"
	const configmaps = "/api/v1/namespaces/ns/configmaps/"

	switch {
	case s.DiscoveryDown && (path == "/api" || path == "/apis"):
		plain(http.StatusServiceUnavailable, "service unavailable\n")
	case s.DiscoveryRefused == http.StatusUnauthorized && (path == "/api" || path == "/apis"):
		json(http.StatusUnauthorized, "application/json", `{"kind":"Status","apiVersion":"v1","metadata":{},"status":"Failure",`+
			`"message":"Unauthorized","reason":"Unauthorized","code":401}`)
	case s.DiscoveryRefused == http.StatusForbidden && (path == "/api" || path == "/apis"):
		json(http.StatusForbidden, "application/json", `{"kind":"Status","apiVersion":"v1","metadata":{},"status":"Failure",`+
			`"message":"forbidden: User \"requester\" cannot get path \"`+path+`\"","reason":"Forbidden","details":{},"code":403}`)

	case path == "/api" && aggregated:
		json(http.StatusOK, aggregatedType, `{"kind":"APIGroupDiscoveryList","apiVersion":"apidiscovery.k8s.io/v2","metadata":{},"items":[`+
			`{"metadata":{"creationTimestamp":null},"versions":[{"version":"v1","resources":[`+
			`{"resource":"configmaps","responseKind":{"group":"","version":"","kind":"ConfigMap"},"scope":"Namespaced","singularResource":"configmap","verbs":`+verbs+`,"shortNames":["cm"]},`+
			`{"resource":"pods","responseKind":{"group":"","version":"","kind":"Pod"},"scope":"Namespaced","singularResource":"pod","verbs":`+verbs+`,"shortNames":["po"],`+
			`"subresources":[{"subresource":"log","responseKind":{"group":"","version":"","kind":"Pod"},"verbs":["get"]}]}`+
			`],"freshness":"Current"}]}]}`)
	case path == "/apis" && aggregated:
		freshness := "Current"
		if s.WidgetsBackendOff || s.WidgetsDiscoveryLags {
			// The aggregator keeps the group in the document with what it last knew, marked Stale.
			freshness = "Stale"
		}
		json(http.StatusOK, aggregatedType, `{"kind":"APIGroupDiscoveryList","apiVersion":"apidiscovery.k8s.io/v2","metadata":{},"items":[`+
			`{"metadata":{"name":"`+Group+`","creationTimestamp":null},"versions":[{"version":"v1beta1","resources":[`+
			`{"resource":"buttons","responseKind":{"group":"`+Group+`","version":"v1beta1","kind":"Button"},"scope":"Namespaced","singularResource":"button","verbs":`+verbs+`,"categories":["widgets","krateo"]}`+
			`],"freshness":"`+freshness+`"}]}]}`)

	case path == "/api":
		json(http.StatusOK, "application/json", `{"kind":"APIVersions","versions":["v1"],"serverAddressByClientCIDRs":[{"clientCIDR":"0.0.0.0/0","serverAddress":"10.0.0.1:443"}]}`)
	case path == "/apis":
		json(http.StatusOK, "application/json", `{"kind":"APIGroupList","apiVersion":"v1","groups":[{"name":"`+Group+`",`+
			`"versions":[{"groupVersion":"`+GV+`","version":"v1beta1"}],"preferredVersion":{"groupVersion":"`+GV+`","version":"v1beta1"}}]}`)
	case path == "/api/v1":
		json(http.StatusOK, "application/json", `{"kind":"APIResourceList","groupVersion":"v1","resources":[`+
			`{"name":"configmaps","singularName":"configmap","namespaced":true,"kind":"ConfigMap","verbs":`+verbs+`,"shortNames":["cm"]},`+
			`{"name":"pods","singularName":"pod","namespaced":true,"kind":"Pod","verbs":`+verbs+`,"shortNames":["po"]},`+
			`{"name":"pods/log","singularName":"","namespaced":true,"kind":"Pod","verbs":["get"]}]}`)

	case strings.HasPrefix(path, "/apis/"+GV) && s.WidgetsBackendOff:
		plain(http.StatusServiceUnavailable, "service unavailable\n")
	case path == "/apis/"+GV && s.WidgetsDiscoveryLags && s.docFails():
		plain(http.StatusServiceUnavailable, "service unavailable\n")
	case path == "/apis/"+GV:
		json(http.StatusOK, "application/json", `{"kind":"APIResourceList","apiVersion":"v1","groupVersion":"`+GV+`","resources":[`+
			`{"name":"buttons","singularName":"button","namespaced":true,"kind":"Button","verbs":`+verbs+`,"categories":["widgets","krateo"]}]}`)

	case path == configmaps+"x" || path == "/api/v1/namespaces/other/configmaps/x":
		ns := strings.Split(path, "/")[4]
		json(http.StatusOK, "application/json", `{"apiVersion":"v1","kind":"ConfigMap","metadata":{"name":"x","namespace":"`+ns+`"}}`)
	case path == buttons+"x":
		json(http.StatusOK, "application/json", `{"apiVersion":"`+GV+`","kind":"Button","metadata":{"name":"x","namespace":"ns"}}`)
	case path == "/apis/"+GV+"/namespaces/ns/buttons":
		json(http.StatusOK, "application/json", `{"apiVersion":"`+GV+`","kind":"ButtonList","metadata":{"resourceVersion":"1"},"items":[`+
			`{"apiVersion":"`+GV+`","kind":"Button","metadata":{"name":"x","namespace":"ns"}}]}`)
	case strings.HasPrefix(path, buttons) && !strings.Contains(strings.TrimPrefix(path, buttons), "/"):
		json(http.StatusNotFound, "application/json", notFoundStatus("buttons", Group, strings.TrimPrefix(path, buttons)))
	case strings.HasPrefix(path, configmaps) && !strings.Contains(strings.TrimPrefix(path, configmaps), "/"):
		json(http.StatusNotFound, "application/json", notFoundStatus("configmaps", "", strings.TrimPrefix(path, configmaps)))

	// What a request that escaped its resource would reach.
	case path == "/metrics":
		plain(http.StatusOK, "# HELP apiserver_request_total\n")
	case path == "/api/v1/namespaces/ns/pods/p/log":
		plain(http.StatusOK, "a log line\n")
	case path == "/api/v1/namespaces/ns/services/svc:80/proxy/metrics":
		plain(http.StatusOK, "# HELP up\n")

	default:
		plain(http.StatusNotFound, "404 page not found\n")
	}
}
