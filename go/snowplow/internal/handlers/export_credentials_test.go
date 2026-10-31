package handlers

// #970 after the rebase onto #1019 and #1024: an export resolves every page
// through the /call lane, so it uses the same credential and the same
// cluster as the caller's own /call:
//   - #1019: a stage whose endpointRef names another cluster is sent to that
//     cluster, never answered from this cluster's informer or cells;
//   - #1024: a caller whose token carries krateo:loa:2 dials with the
//     step-up credential, never the session one, and gets nothing from the
//     session one when the step-up credential is gone.
// Each arm runs both export lanes, GET and POST, in a fresh harness.

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	xcontext "github.com/krateo-platformops/plumbing/context"
	"github.com/krateo-platformops/plumbing/endpoints"
	"github.com/krateo-platformops/plumbing/jwtutil"
	"github.com/krateo-platformops/snowplow/internal/credsecret"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	// remoteRAName970 reads the team-rows ConfigMaps through the named
	// endpointRef remoteSecret970: another cluster.
	remoteRAName970 = "team-rows-remote"
	remoteSecret970 = "other-cluster-970"
	// dialRAName970 reads the team-rows ConfigMaps and GETs dialPath970, a
	// path that is not a Kubernetes resource, so no informer or cell serves
	// it: the GET is dialed with the caller's own credential.
	dialRAName970 = "team-rows-dial"
	dialPath970   = "/ext/probe/things"
)

// clusterRecorder970 is an apiserver that records every request (method,
// path and bearer token) and answers every GET with a ConfigMapList of two
// green ConfigMaps named after the cluster.
type clusterRecorder970 struct {
	name string
	srv  *httptest.Server
	mu   sync.Mutex
	reqs []string
}

func newClusterRecorder970(t *testing.T, name string) *clusterRecorder970 {
	t.Helper()
	c := &clusterRecorder970{name: name}
	c.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		c.mu.Lock()
		c.reqs = append(c.reqs, r.Method+" "+r.URL.Path+" "+strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer "))
		c.mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"apiVersion":"v1","kind":"ConfigMapList","metadata":{"resourceVersion":"1"},"items":[` +
			`{"apiVersion":"v1","kind":"ConfigMap","metadata":{"name":"` + name + `-0","namespace":"team-a"},"data":{"team":"green"}},` +
			`{"apiVersion":"v1","kind":"ConfigMap","metadata":{"name":"` + name + `-1","namespace":"team-a"},"data":{"team":"green"}}]}`))
	}))
	t.Cleanup(c.srv.Close)
	return c
}

func (c *clusterRecorder970) take() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := c.reqs
	c.reqs = nil
	return out
}

func clientConfigSecret970(name, serverURL, token string) *corev1.Secret {
	return &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: extrasNS970},
		Data:       map[string][]byte{"server-url": []byte(serverURL), "token": []byte(token)},
	}
}

// serveAsGroups970 is serveAs970 with the caller's groups, and the caller's
// endpoint at serverURL (the one the auth middleware selected).
func serveAsGroups970(h http.Handler, method, target, body, serverURL, token string, groups []string) *httptest.ResponseRecorder {
	var rd io.Reader
	if body != "" {
		rd = strings.NewReader(body)
	}
	req := httptest.NewRequest(method, target, rd)
	req = req.WithContext(xcontext.BuildContext(req.Context(),
		xcontext.WithAccessToken("jwt-userA"),
		xcontext.WithUserInfo(jwtutil.UserInfo{Username: "userA", Groups: groups}),
		xcontext.WithUserConfig(endpoints.Endpoint{ServerURL: serverURL, Token: token}),
	))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

// TestExport970_RemoteEndpointRefIsSentToItsCluster — #1019 through an
// export: the RESTAction's one stage names another cluster's endpointRef
// with an apiserver-shaped path this cluster's ConfigMaps informer serves.
// The export sends the read to the other cluster and exports its rows, not
// this cluster's.
func TestExport970_RemoteEndpointRefIsSentToItsCluster(t *testing.T) {
	for _, l := range exportLanes970(remoteRAName970) {
		t.Run(l.name, func(t *testing.T) {
			local := newClusterRecorder970(t, "local")
			remote := newClusterRecorder970(t, "remote")
			seedExtrasHarnessWithSecrets(t, local.srv.URL, clientConfigSecret970(remoteSecret970, remote.srv.URL, "token-remote"))
			rec := serveAs970(l.h(), l.method, l.target, l.body, local.srv.URL)
			var rows []row970
			if rec.Code != http.StatusOK || json.Unmarshal(rec.Body.Bytes(), &rows) != nil {
				t.Fatalf("export: %d %s", rec.Code, rec.Body.String())
			}
			got := remote.take()
			t.Logf("rows=%+v remote=%v local=%v", rows, got, local.take())
			sent := false
			for _, r := range got {
				sent = sent || strings.HasPrefix(r, "GET /api/v1/namespaces/team-a/configmaps ")
			}
			if !sent {
				t.Errorf("the other cluster received no GET of the stage's path (%v): the export answered it from this cluster", got)
			}
			if len(rows) != 2 || rows[0].ID != "remote-0" || rows[1].ID != "remote-1" {
				t.Errorf("exported rows %+v, want the other cluster's remote-0 and remote-1", rows)
			}
		})
	}
}

// TestExport970_StepUpCallerDialsWithTheStepUpCredential — #1024 through an
// export: a caller whose token carries krateo:loa:2 dials the stage no
// informer serves with its step-up credential, on every lane; the session
// credential is never used. With the step-up credential gone the stage
// fails and the session credential is still never used. Control: a session
// caller dials with the session credential.
func TestExport970_StepUpCallerDialsWithTheStepUpCredential(t *testing.T) {
	stepUp := []string{"devs", credsecret.StepUpGroup}
	for _, l := range exportLanes970(dialRAName970) {
		t.Run(l.name+"/step-up", func(t *testing.T) {
			session := newClusterRecorder970(t, "session")
			elevated := newClusterRecorder970(t, "stepup")
			seedExtrasHarnessWithSecrets(t, session.srv.URL, clientConfigSecret970("usera-clientconfig-stepup", elevated.srv.URL, "token-userA-stepup"))
			rec := serveAsGroups970(l.h(), l.method, l.target, l.body, elevated.srv.URL, "token-userA-stepup", stepUp)
			if rec.Code != http.StatusOK {
				t.Fatalf("export: %d %s", rec.Code, rec.Body.String())
			}
			got, leaked := elevated.take(), session.take()
			t.Logf("stepup=%v session=%v outcomes=%s", got, leaked, rec.Header().Get("X-Snowplow-Stage-Outcomes"))
			if len(leaked) != 0 {
				t.Errorf("a step-up export used the session credential: %v", leaked)
			}
			if want := "GET " + dialPath970 + " token-userA-stepup"; !contains970(got, want) {
				t.Errorf("the step-up credential's server saw %v, want %q", got, want)
			}
		})
		t.Run(l.name+"/step-up credential gone", func(t *testing.T) {
			session := newClusterRecorder970(t, "session")
			seedExtrasHarnessWithSecrets(t, session.srv.URL)
			rec := serveAsGroups970(l.h(), l.method, l.target, l.body, session.srv.URL, "token-userA", stepUp)
			leaked := session.take()
			oc := rec.Header().Get("X-Snowplow-Stage-Outcomes")
			t.Logf("%d session=%v outcomes=%s", rec.Code, leaked, oc)
			if len(leaked) != 0 {
				t.Errorf("a step-up export without its step-up credential fell back to the session one: %v", leaked)
			}
			if rec.Code == http.StatusOK && !strings.Contains(oc, `"name":"probe"`) {
				t.Errorf("export %d with outcomes %q: the stage without a credential is not reported", rec.Code, oc)
			}
		})
		t.Run(l.name+"/control session caller", func(t *testing.T) {
			session := newClusterRecorder970(t, "session")
			elevated := newClusterRecorder970(t, "stepup")
			seedExtrasHarnessWithSecrets(t, session.srv.URL, clientConfigSecret970("usera-clientconfig-stepup", elevated.srv.URL, "token-userA-stepup"))
			rec := serveAsGroups970(l.h(), l.method, l.target, l.body, session.srv.URL, "token-userA", []string{"devs"})
			if rec.Code != http.StatusOK {
				t.Fatalf("export: %d %s", rec.Code, rec.Body.String())
			}
			if got := session.take(); !contains970(got, "GET "+dialPath970+" token-userA") {
				t.Fatalf("control: the session caller's dial %v is not on the session credential: the arm cannot see a dial", got)
			}
			if got := elevated.take(); len(got) != 0 {
				t.Errorf("a session export used the step-up credential: %v", got)
			}
		})
	}
}

func contains970(reqs []string, want string) bool {
	for _, r := range reqs {
		if r == want {
			return true
		}
	}
	return false
}
