package audit

// audit_1291_test.go — #1291: snowplow's writer on the shared library
// (github.com/krateo-platformops/audit v0.6.0, schema v1).
//
//   A   the actor class is the validated token's `principal` claim through
//       audit.ParseClaim: each known class is written; "" and an unknown
//       value are omitted and the record is marked actor_class, never
//       defaulted to human (Diego's ruling).
//   Dg  a delegated (RFC 8693) token names ONE principal: user.name and
//       enduser.id are the outermost actor, its class the actor's,
//       on_behalf_of the subject, delegation the signed chain; the record
//       conforms as a request and as an action writer.
//   C   conformance: a /call-shaped and a stage-shaped record pass the
//       library's Check for writer request (and an agent's /call for action),
//       and its falsifiers (every required key, every closed set) bite.
//   N   a denied record (snowplow refused) carries no status code and
//       conforms (audit#13).
//   M   krateo.audit.records.incomplete{marker}: the shared writer's OTLP
//       counter and the /debug/vars mirror count the same records by the same
//       markers.
//   S   no identity on a span: the span the record is emitted on gets only
//       the boolean krateo.audit.recorded.
//   P   the preview flag is read from apiserver state: the Namespace's label,
//       false for a target in no namespace or a Namespace that does not
//       exist, unknown (marked preview) when it cannot be read.
//   R   the Namespace reader serves a synced informer hit from memory and
//       reads a miss with one GET; NotFound is an answer, not an error.
//   V   krateo.audit.verb: the apiserver's RequestInfoFactory verb for a
//       Kubernetes target, the lowercase method otherwise.

import (
	"context"
	"errors"
	"expvar"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"

	krateoaudit "github.com/krateo-platformops/audit"
	"github.com/krateo-platformops/audit/audittest"
	xcontext "github.com/krateo-platformops/plumbing/context"
	"github.com/krateo-platformops/plumbing/jwtutil"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/log/logtest"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	metadatafake "k8s.io/client-go/metadata/fake"
	clienttesting "k8s.io/client-go/testing"
	"k8s.io/client-go/tools/cache"
)

// fakeNamespaces1291 is a NamespaceReader over a fixed set of Namespaces.
type fakeNamespaces1291 struct {
	labels map[string]map[string]string
	err    error
}

func (f fakeNamespaces1291) NamespaceLabels(_ context.Context, name string) (map[string]string, bool, error) {
	if f.err != nil {
		return nil, false, f.err
	}
	l, ok := f.labels[name]
	return l, ok, nil
}

// TestMain installs a Namespace reader in which every namespace the package's
// records name exists, unlabelled: a record's preview flag is then false, as
// on a cluster with no preview sandbox. Arms that test the flag swap it.
func TestMain(m *testing.M) {
	SetNamespaceReader(fakeNamespaces1291{labels: map[string]map[string]string{
		"team-a": {}, "team-b": {}, "ns1": {}, "ns": {}, "default": {},
	}})
	m.Run()
}

// withNamespaces1291 swaps the Namespace reader for one test.
func withNamespaces1291(t *testing.T, r NamespaceReader) {
	t.Helper()
	prev := namespaces.Load()
	SetNamespaceReader(r)
	t.Cleanup(func() { namespaces.Store(prev) })
}

func userCtx(ui jwtutil.UserInfo) context.Context {
	return WithSessionID(xcontext.BuildContext(context.Background(), xcontext.WithUserInfo(ui)), "s-test")
}

// delegatedCtx1291 is the request context of an RFC 8693 token: agent-x acts
// for alice, through the portal's own service.
func delegatedCtx1291() context.Context {
	return userCtx(jwtutil.UserInfo{
		Username: "alice", Groups: []string{"devs"}, Principal: jwtutil.PrincipalHuman,
		Act: &jwtutil.Actor{Sub: "agent-x", Principal: jwtutil.PrincipalAgent,
			Act: &jwtutil.Actor{Sub: "svc-portal", Principal: jwtutil.PrincipalService}},
	})
}

// served1291 is ctx as a served request leaves it: a session id and what the
// request was received with (audit.Middleware), so a record made on it carries
// everything a request writer must write.
func served1291(ctx context.Context) context.Context {
	req := httptest.NewRequest(http.MethodPost, "/call?name=demo", nil)
	req.Header.Set("User-Agent", "ua-1291")
	return withReceivedRequest(WithSessionID(ctx, "s-1291"), req)
}

// callEvent1291 is the record of a successful /call create, as the /call site
// builds it.
func callEvent1291() Event {
	return Event{
		Action: "call", HTTPMethod: http.MethodPost, Verb: "create", Outcome: OutcomeSuccess, StatusCode: http.StatusOK,
		Group: "composition.krateo.io", Version: "v1alpha1", Resource: "fireworksapps", ObjectName: "demo", Namespace: "team-a",
		ObjectUID: "uid-1291", ObjectGeneration: 2, ObjectResourceVersion: "9",
	}
}

// record1291 emits ev on ctx through snowplow's emitter into the library's
// recorder (a real Logs SDK) and returns the one record.
func record1291(t *testing.T, ctx context.Context, ev Event) *audittest.Recorder {
	t.Helper()
	rec := audittest.NewRecorder(t)
	New(rec.Logger(Scope)).Emit(ctx, ev)
	return rec
}

func attrMap1291(t *testing.T, rec *audittest.Recorder) map[string]attribute.Value {
	t.Helper()
	out := map[string]attribute.Value{}
	for _, kv := range audittest.Attributes(rec.Only(t)) {
		out[string(kv.Key)] = kv.Value
	}
	return out
}

func TestAudit1291_A_ActorClassIsTheSignedClaimOrAbsent(t *testing.T) {
	for _, c := range []struct {
		principal jwtutil.Principal
		want      string // "" = absent and marked
	}{
		{jwtutil.PrincipalHuman, "human"},
		{jwtutil.PrincipalAgent, "agent"},
		{jwtutil.PrincipalController, "controller"},
		{jwtutil.PrincipalService, "service"},
		{"", ""},      // a token minted before authn 0.39.0 (2026-11-09 11:45Z)
		{"robot", ""}, // a value this schema does not know
	} {
		t.Run(string(c.principal)+"|", func(t *testing.T) {
			before := IncompleteCount(IncompleteActorClass)
			got := attrMap1291(t, record1291(t, userCtx(jwtutil.UserInfo{Username: "alice", Principal: c.principal}), callEvent1291()))
			v, has := got[krateoaudit.KeyActorClass]
			marked := hasMarker(got[krateoaudit.KeyIncomplete].AsString(), IncompleteActorClass)
			switch {
			case c.want != "" && (!has || v.AsString() != c.want):
				t.Fatalf("actor_class = %v (present %t), want %q", v, has, c.want)
			case c.want != "" && marked:
				t.Fatalf("a known class is marked actor_class")
			case c.want == "" && has:
				t.Fatalf("actor_class = %q for principal %q: never defaulted, it must be absent", v.AsString(), c.principal)
			case c.want == "" && !marked:
				t.Fatalf("an absent class is not marked actor_class (incomplete=%q)", got[krateoaudit.KeyIncomplete].AsString())
			}
			wantDelta := int64(0)
			if c.want == "" {
				wantDelta = 1
			}
			if d := IncompleteCount(IncompleteActorClass) - before; d != wantDelta {
				t.Fatalf("incomplete{actor_class} moved by %d, want %d", d, wantDelta)
			}
		})
	}
}

func TestAudit1291_Dg_DelegatedTokenNamesOnePrincipal(t *testing.T) {
	rec := record1291(t, served1291(delegatedCtx1291()), callEvent1291())
	got := attrMap1291(t, rec)
	for k, want := range map[string]string{
		"user.name":                           "agent-x",
		"enduser.id":                          "agent-x",
		"krateo.audit.actor_class":            "agent",
		"krateo.audit.on_behalf_of.user.name": "alice",
	} {
		if got[k].AsString() != want {
			t.Errorf("%s = %q, want %q", k, got[k].AsString(), want)
		}
	}
	wantChain := []string{"agent:agent-x", "service:svc-portal", "human:alice"}
	if v := got["krateo.audit.delegation"]; v.Type() != attribute.STRINGSLICE || !slices.Equal(v.AsStringSlice(), wantChain) {
		t.Errorf("krateo.audit.delegation = %v, want %v", v, wantChain)
	}
	r := rec.Only(t)
	audittest.AssertConformant(t, krateoaudit.WriterRequest, r)
	audittest.AssertConformant(t, krateoaudit.WriterAction, r)
	audittest.AssertRequiredKeysDetected(t, krateoaudit.WriterRequest, r)

	// A plain token: no delegation keys at all.
	plain := attrMap1291(t, record1291(t, actorCtx(), callEvent1291()))
	for _, k := range []string{"krateo.audit.on_behalf_of.user.name", "krateo.audit.delegation"} {
		if v, ok := plain[k]; ok {
			t.Errorf("a token with no act chain carries %s=%v", k, v)
		}
	}
	// A link whose class the token does not give is written ":<subject>".
	unknown := attrMap1291(t, record1291(t, userCtx(jwtutil.UserInfo{Username: "alice",
		Act: &jwtutil.Actor{Sub: "agent-y"}}), callEvent1291()))
	if v := unknown["krateo.audit.delegation"].AsStringSlice(); !slices.Equal(v, []string{":agent-y", ":alice"}) {
		t.Errorf("delegation without classes = %v, want [:agent-y :alice]", v)
	}
	if _, ok := unknown["krateo.audit.actor_class"]; ok {
		t.Errorf("an actor with no principal claim got a class")
	}
}

func TestAudit1291_C_RecordShapesConform(t *testing.T) {
	stage := Event{
		Action: "restaction", HTTPMethod: http.MethodPost, Verb: "post", Outcome: OutcomeFailure,
		ServerAddress: "render.example:8443", RESTActionNamespace: "krateo-system", RESTActionName: "ra", RESTActionStage: "s",
		Message: MessageNoResponse,
	}
	dry := callEvent1291()
	dry.DryRun, dry.ObjectUID, dry.ObjectGeneration, dry.ObjectResourceVersion = true, "", 0, ""
	export := Event{Action: "export", HTTPMethod: http.MethodGet, Verb: "get", // F1: the method sent
		Outcome: OutcomeSuccess, StatusCode: http.StatusOK,
		Group: "templates.krateo.io", Version: "v1", Resource: "restactions", ObjectName: "ra", Namespace: "team-a"}
	cluster := Event{Action: "call", HTTPMethod: http.MethodDelete, Verb: "delete", Outcome: OutcomeSuccess, StatusCode: http.StatusOK,
		Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "clusterroles", ObjectName: "r", ObjectUID: "u"}
	req := httptest.NewRequest(http.MethodPost, "/call?name=demo", nil)
	req.Header.Set("User-Agent", "ua-1291")
	for name, ev := range map[string]Event{"call": callEvent1291(), "call.dryRun": dry, "stage.endpoint": stage, "export": export, "cluster": cluster} {
		t.Run(name, func(t *testing.T) {
			ctx := withReceivedRequest(WithSessionID(actorCtx(), "s-1291"), req)
			r := record1291(t, ctx, ev).Only(t)
			audittest.AssertConformant(t, krateoaudit.WriterRequest, r)
			audittest.AssertRequiredKeysDetected(t, krateoaudit.WriterRequest, r)
			audittest.AssertEnumsDetected(t, krateoaudit.WriterRequest, r)
		})
	}
	t.Run("agent call is an action record too", func(t *testing.T) {
		ctx := withReceivedRequest(WithSessionID(userCtx(jwtutil.UserInfo{Username: "system:serviceaccount:a:b",
			Principal: jwtutil.PrincipalAgent}), "s-1291"), req)
		r := record1291(t, ctx, callEvent1291()).Only(t)
		audittest.AssertConformant(t, krateoaudit.WriterRequest, r)
		audittest.AssertConformant(t, krateoaudit.WriterAction, r)
		audittest.AssertRequiredKeysDetected(t, krateoaudit.WriterAction, r)
	})
}

func TestAudit1291_N_DeniedRecordCarriesNoStatusCode(t *testing.T) {
	ev := Event{Action: "restaction", HTTPMethod: http.MethodPost, Verb: "post", Outcome: OutcomeDenied,
		ServerAddress: "169.254.169.254", RESTActionNamespace: "krateo-system", RESTActionName: "ra", RESTActionStage: "s",
		Message: MessageDestinationRefused}
	r := record1291(t, served1291(actorCtx()), ev).Only(t)
	audittest.AssertConformant(t, krateoaudit.WriterRequest, r)
	for _, kv := range audittest.Attributes(r) {
		if string(kv.Key) == krateoaudit.KeyHTTPStatusCode {
			t.Fatalf("a denied record carries %s", kv.Key)
		}
	}
	// Control: the checker refuses a status code there, so the arm can fail.
	audittest.AssertViolation(t, krateoaudit.WriterRequest,
		append(audittest.Attributes(r), attribute.Int(krateoaudit.KeyHTTPStatusCode, 502)), krateoaudit.KeyHTTPStatusCode)
}

func TestAudit1291_M_IncompleteCountedOnOTLPAndDebugVars(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	rec := logtest.NewRecorder()
	e := New(rec.Logger(Scope), krateoaudit.WithMeterProvider(mp))

	before := map[string]int64{}
	for _, m := range krateoaudit.Markers() {
		before[m] = IncompleteCount(m)
	}
	withNamespaces1291(t, nil)                                // the Namespace cannot be read: preview
	ev := callEvent1291()                                     // a Kubernetes target in team-a
	ev.Verb = ""                                              // verb
	e.Emit(WithSessionID(context.Background(), "s-test"), ev) // no actor: actor, actor_class
	e.Emit(actorCtx(), callEvent1291())                       // preview only
	want := map[string]int64{IncompleteActor: 1, IncompleteActorClass: 1, IncompleteVerb: 1, IncompletePreview: 2}

	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatal(err)
	}
	otlp := map[string]int64{}
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != krateoaudit.MetricIncomplete {
				continue
			}
			for _, dp := range m.Data.(metricdata.Sum[int64]).DataPoints {
				v, _ := dp.Attributes.Value("marker")
				otlp[v.AsString()] += dp.Value
			}
		}
	}
	for _, m := range krateoaudit.Markers() {
		if otlp[m] != want[m] {
			t.Errorf("OTLP %s{marker=%s} = %d, want %d", krateoaudit.MetricIncomplete, m, otlp[m], want[m])
		}
		if d := IncompleteCount(m) - before[m]; d != want[m] {
			t.Errorf("/debug/vars %s{%s} moved by %d, want %d (the OTLP point says %d)", IncompleteVar, m, d, want[m], otlp[m])
		}
	}
	if expvar.Get(IncompleteVar) != incomplete {
		t.Fatalf("/debug/vars does not publish the count under %q", IncompleteVar)
	}
}

func TestAudit1291_S_NoIdentityOnTheSpan(t *testing.T) {
	sr := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(sr))
	ctx, span := tp.Tracer("t").Start(delegatedCtx1291(), "server")
	req := httptest.NewRequest(http.MethodPost, "/call?name=demo", nil)
	req.Header.Set("User-Agent", "ua-1291")
	ctx = withReceivedRequest(WithSessionID(ctx, "s-1291"), req)
	rec := record1291(t, ctx, callEvent1291())
	span.End()
	got := attrMap1291(t, rec)
	if got["user.name"].AsString() != "agent-x" {
		t.Fatalf("precondition: the record carries no actor")
	}
	ended := sr.Ended()
	if len(ended) != 1 {
		t.Fatalf("spans = %d, want 1", len(ended))
	}
	attrs := ended[0].Attributes()
	if len(attrs) != 1 || string(attrs[0].Key) != krateoaudit.SpanKeyRecorded || !attrs[0].Value.AsBool() {
		t.Fatalf("span attributes = %v, want only %s=true", attrs, krateoaudit.SpanKeyRecorded)
	}
	for _, kv := range attrs {
		for _, v := range []string{"agent-x", "alice", "devs", "ua-1291", "/call?name=demo", "s-1291"} {
			if strings.Contains(kv.Value.Emit(), v) {
				t.Errorf("span attribute %s carries identity", kv.Key)
			}
		}
	}
}

func TestAudit1291_P_PreviewIsTheNamespacesLabel(t *testing.T) {
	sandbox := map[string]string{PreviewLabel: PreviewLabelSandbox}
	cases := []struct {
		name      string
		reader    NamespaceReader
		namespace string
		want      string // "true" | "false" | "" (absent, marked)
	}{
		{"labelled sandbox", fakeNamespaces1291{labels: map[string]map[string]string{"team-a": sandbox}}, "team-a", "true"},
		{"another purpose", fakeNamespaces1291{labels: map[string]map[string]string{"team-a": {PreviewLabel: "prod"}}}, "team-a", "false"},
		{"unlabelled", fakeNamespaces1291{labels: map[string]map[string]string{"team-a": {}}}, "team-a", "false"},
		{"no such namespace", fakeNamespaces1291{}, "team-a", "false"},
		{"cluster-scoped target", nil, "", "false"},
		{"namespace unreadable", fakeNamespaces1291{err: errors.New("apiserver unavailable")}, "team-a", ""},
		{"no reader", nil, "team-a", ""},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			withNamespaces1291(t, c.reader)
			ev := callEvent1291()
			ev.Namespace = c.namespace
			got := attrMap1291(t, record1291(t, actorCtx(), ev))
			v, has := got[krateoaudit.KeyPreview]
			marked := hasMarker(got[krateoaudit.KeyIncomplete].AsString(), IncompletePreview)
			switch {
			case c.want == "" && (has || !marked):
				t.Fatalf("preview = %v (present %t), marked %t; want absent and marked", v, has, marked)
			case c.want != "" && (!has || v.Emit() != c.want || marked):
				t.Fatalf("preview = %v (present %t), marked %t; want %s", v, has, marked, c.want)
			}
		})
	}
	// A record that names only an external endpoint carries no flag and is
	// not marked: there is no Namespace to read.
	withNamespaces1291(t, nil)
	got := attrMap1291(t, record1291(t, actorCtx(), Event{Action: "restaction", HTTPMethod: http.MethodPost, Verb: "post",
		Outcome: OutcomeSuccess, StatusCode: 201, ServerAddress: "render.example"}))
	if v, ok := got[krateoaudit.KeyPreview]; ok {
		t.Fatalf("an external endpoint record carries preview=%v", v)
	}
	if v, ok := got[krateoaudit.KeyIncomplete]; ok {
		t.Fatalf("an external endpoint record is marked %v", v)
	}
}

func namespaceObject1291(name string, labels map[string]string) *metav1.PartialObjectMetadata {
	return &metav1.PartialObjectMetadata{
		TypeMeta:   metav1.TypeMeta{APIVersion: "v1", Kind: "Namespace"},
		ObjectMeta: metav1.ObjectMeta{Name: name, Labels: labels},
	}
}

func gets1291(c *metadatafake.FakeMetadataClient) int {
	n := 0
	for _, a := range c.Actions() {
		if a.GetVerb() == "get" {
			n++
		}
	}
	return n
}

func TestAudit1291_R_NamespaceReaderServesTheInformerAndGetsAMiss(t *testing.T) {
	sandbox := map[string]string{PreviewLabel: PreviewLabelSandbox}
	scheme := runtime.NewScheme()
	_ = metav1.AddMetaToScheme(scheme)
	client := metadatafake.NewSimpleMetadataClient(scheme,
		namespaceObject1291("preview-1", sandbox), namespaceObject1291("team-a", nil))
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	r := StartNamespaceReader(ctx, client).(*informerNamespaceReader)
	if !cache.WaitForCacheSync(ctx.Done(), r.synced) {
		t.Fatal("the namespace informer did not sync")
	}

	labels, found, err := r.NamespaceLabels(ctx, "preview-1")
	if err != nil || !found || labels[PreviewLabel] != PreviewLabelSandbox {
		t.Fatalf("preview-1: labels %v found %t err %v", labels, found, err)
	}
	if n := gets1291(client); n != 0 {
		t.Fatalf("a synced informer hit issued %d GETs, want 0", n)
	}

	// A Namespace the informer does not hold yet: read with one GET. Created
	// in the tracker behind the informer's back (a reactor answers the GET).
	client.PrependReactor("get", "namespaces", func(a clienttesting.Action) (bool, runtime.Object, error) {
		if a.(clienttesting.GetAction).GetName() == "fresh" {
			return true, namespaceObject1291("fresh", sandbox), nil
		}
		return false, nil, nil
	})
	labels, found, err = r.NamespaceLabels(ctx, "fresh")
	if err != nil || !found || labels[PreviewLabel] != PreviewLabelSandbox {
		t.Fatalf("fresh: labels %v found %t err %v", labels, found, err)
	}
	if n := gets1291(client); n != 1 {
		t.Fatalf("an informer miss issued %d GETs, want 1", n)
	}

	// NotFound is an answer.
	if _, found, err = r.NamespaceLabels(ctx, "gone"); err != nil || found {
		t.Fatalf("gone: found %t err %v, want not found and no error", found, err)
	}
	// Any other error is not.
	client.PrependReactor("get", "namespaces", func(clienttesting.Action) (bool, runtime.Object, error) {
		return true, nil, apierrors.NewServiceUnavailable("down")
	})
	if _, _, err = r.NamespaceLabels(ctx, "elsewhere"); err == nil {
		t.Fatal("an apiserver error read as an answer")
	}
	// The informer still answers what it holds.
	if labels, found, err = r.NamespaceLabels(ctx, "team-a"); err != nil || !found || len(labels) != 0 {
		t.Fatalf("team-a: labels %v found %t err %v", labels, found, err)
	}
}

func TestAudit1291_V_VerbIsTheAuthoritysOwn(t *testing.T) {
	for _, c := range []struct{ method, uri, want string }{
		{http.MethodPost, "/api/v1/namespaces/default/configmaps", "create"},
		{http.MethodPost, "/api/v1/namespaces/default/configmaps?dryRun=All", "create"},
		{http.MethodPut, "/api/v1/namespaces/default/configmaps/cm", "update"},
		{http.MethodPut, "/apis/composition.krateo.io/v1alpha1/namespaces/a/fireworksapps/demo/status", "update"},
		{http.MethodPatch, "/apis/apps/v1/namespaces/a/deployments/d/scale", "patch"},
		{http.MethodDelete, "/api/v1/namespaces/default/configmaps/cm", "delete"},
		{http.MethodDelete, "/api/v1/namespaces/default/configmaps", "deletecollection"},
		{http.MethodGet, "/apis/templates.krateo.io/v1/namespaces/a/restactions/ra", "get"},
		{http.MethodGet, "/apis/templates.krateo.io/v1/namespaces/a/restactions", "list"},
		{http.MethodPost, "/apis/rbac.authorization.k8s.io/v1/clusterroles", "create"},
		{http.MethodPost, "/apis/authorization.k8s.io/v1/selfsubjectaccessreviews", "create"},
		{http.MethodPost, "/healthz", "post"}, // a non-resource path: the lowercase method
		{"MKCOL", "/api/v1/namespaces/default/configmaps", ""},
	} {
		if got := KubernetesVerb(c.method, c.uri); got != c.want {
			t.Errorf("KubernetesVerb(%s %s) = %q, want %q", c.method, c.uri, got, c.want)
		}
	}
	for _, c := range []struct{ method, want string }{
		{http.MethodPost, "post"}, {http.MethodPut, "put"}, {http.MethodPatch, "patch"}, {http.MethodDelete, "delete"},
		{"post", "post"}, {"MKCOL", ""}, {"", ""},
	} {
		if got := MethodVerb(c.method); got != c.want {
			t.Errorf("MethodVerb(%q) = %q, want %q", c.method, got, c.want)
		}
	}
}
