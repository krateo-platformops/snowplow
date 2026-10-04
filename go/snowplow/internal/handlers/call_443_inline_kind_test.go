//go:build unit
// +build unit

// call_443_inline_kind_test.go — #443 part 2, the inline dry-run resolve on
// the real apiserver (kind cluster "krateo"), every request made with the
// caller's own x509 (e2e.SignUp):
//
//   - a_ParityGolden Arm A: stored ra-x is updated to S2; the caller raw-reads
//     it (GET /call?raw=true) and inline-resolves exactly that body. The reply
//     is byte-identical to GET /call of the stored ra-x, for two callers with
//     different RBAC, and the two callers' replies differ.
//   - a_ParityGolden Arm B: the body is the apply manifest (no server
//     metadata); the .status bytes are identical to the stored resolve's.
//   - NothingPersisted: an inline draft with a write-verb stage returns the
//     dry-run stage error; neither the draft nor the object the stage would
//     create exists afterwards.
//
// Fixture (S2): K=3 stages × M>=2 iterator items (dependsOn + path
// templating), per-stage filters, a top-level filter and an empty-result
// error stage.

package handlers_test

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	"github.com/krateo-platformops/plumbing/e2e"
	v1 "github.com/krateo-platformops/snowplow/apis/templates/v1"
	"github.com/krateo-platformops/snowplow/internal/handlers"
	"github.com/krateo-platformops/snowplow/internal/handlers/dispatchers"
	"github.com/krateo-platformops/snowplow/internal/handlers/middleware"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/e2e-framework/klient/decoder"
	"sigs.k8s.io/e2e-framework/klient/k8s/resources"
	"sigs.k8s.io/e2e-framework/pkg/envconf"
	"sigs.k8s.io/e2e-framework/pkg/features"
)

const (
	ra443Name    = "inline443-ra"
	draft443Name = "inline443-draft"
	cm443Name    = "inline443-cm"
	evil443Name  = "inline443-evil"
)

func s2Spec443() map[string]any {
	ns := "demo-system"
	// Stage filters on this (cache-off, apiserver-served) path see the stage
	// output under its own name ({"cms": <list>}), hence ".cms.items". The
	// top-level filter projects names only, so the reply carries no
	// resourceVersion (a cluster-global counter that moves between calls).
	return map[string]any{
		"api": []any{
			map[string]any{"name": "cms", "continueOnError": true, "errorKey": "cmsErr",
				"path": "/api/v1/namespaces/" + ns + "/configmaps", "filter": "[.cms.items[] | {name: .metadata.name}] | sort_by(.name)"},
			map[string]any{"name": "each", "continueOnError": true, "errorKey": "eachErr",
				"dependsOn": map[string]any{"name": "cms", "iterator": ".cms"},
				"path":      `${ "/api/v1/namespaces/` + ns + `/configmaps/" + .name }`},
			map[string]any{"name": "missing", "continueOnError": true, "errorKey": "missingErr",
				"path": "/api/v1/namespaces/" + ns + "/configmaps/does-not-exist-443"},
		},
		"filter": `{cms: [.cms[]?.name], each: ([.each | if type == "array" then .[] else . end | .metadata.name?] | sort), errs: [.cmsErr, .eachErr, .missingErr]}`,
	}
}

func manifest443(name string, spec map[string]any) map[string]any {
	return map[string]any{
		"apiVersion": "templates.krateo.io/v1", "kind": "RESTAction",
		"metadata": map[string]any{"name": name, "namespace": namespace},
		"spec":     spec,
	}
}

type kind443 struct {
	adminCtx, devCtx context.Context
}

func (k *kind443) dispatchMap(cfg *envconf.Config) map[string]http.Handler {
	return map[string]http.Handler{
		"restactions.templates.krateo.io": dispatchers.RESTActionWithServiceAccountConfig(cfg.Client().RESTConfig()),
	}
}

func (k *kind443) storedGET(ctx context.Context, cfg *envconf.Config, name string, raw bool) *httptest.ResponseRecorder {
	q := "/call?apiVersion=templates.krateo.io/v1&resource=restactions&namespace=" + namespace + "&name=" + name
	if raw {
		q += "&raw=true"
	}
	rec := httptest.NewRecorder()
	handlers.Dispatcher(k.dispatchMap(cfg))(handlers.Call()).ServeHTTP(rec, httptest.NewRequestWithContext(ctx, http.MethodGet, q, nil))
	return rec
}

func (k *kind443) inlinePOST(t *testing.T, ctx context.Context, cfg *envconf.Config, obj map[string]any) *httptest.ResponseRecorder {
	t.Helper()
	name, _ := obj["metadata"].(map[string]any)["name"].(string)
	body, err := json.Marshal(map[string]any{"extras": map[string]any{}, "object": obj})
	if err != nil {
		t.Fatal(err)
	}
	q := "/call/read?apiVersion=templates.krateo.io/v1&resource=restactions&namespace=" + namespace + "&name=" + name
	rec := httptest.NewRecorder()
	h := middleware.BodyExtrasDecode(handlers.ReadDispatcher(k.dispatchMap(cfg))(handlers.CallRead()))
	h.ServeHTTP(rec, httptest.NewRequestWithContext(ctx, http.MethodPost, q, io.NopCloser(bytes.NewReader(body))))
	return rec
}

func statusBytes443(t *testing.T, b []byte) string {
	t.Helper()
	var m map[string]any
	if err := json.Unmarshal(b, &m); err != nil {
		t.Fatalf("decode reply: %v (%s)", err, b)
	}
	out, _ := json.Marshal(m["status"])
	return string(out)
}

func TestS443_Kind_Inline(t *testing.T) {
	os.Setenv("DEBUG", "0")
	// The RESTAction handler reads each caller's <user>-clientconfig Secret from
	// AUTHN_NAMESPACE; e2e.SignUp writes it to the test namespace.
	t.Setenv("AUTHN_NAMESPACE", namespace)
	k := &kind443{}
	save := func(dst *context.Context) features.Func {
		return func(ctx context.Context, _ *testing.T, _ *envconf.Config) context.Context {
			*dst = ctx
			return ctx
		}
	}
	f := features.New("S443-inline").
		Setup(e2e.Logger("test")).
		Setup(func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			r := adminRes443(t, cfg)
			if err := decoder.ApplyWithManifestDir(ctx, r, testdataPath, "rbac.clusterroles-156.yaml", []resources.CreateOption{}); err != nil {
				t.Fatalf("cluster-admins binding: %v", err)
			}
			cm := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: cm443Name, Namespace: namespace}, Data: map[string]string{"k": "v"}}
			if err := r.Create(ctx, cm); err != nil && !apierrors.IsAlreadyExists(err) {
				t.Fatalf("create configmap: %v", err)
			}
			// Stored ra-x with S1 (a single stage); Arm A updates it to S2.
			s1 := &v1.RESTAction{}
			if err := runtime.DefaultUnstructuredConverter.FromUnstructured(manifest443(ra443Name, map[string]any{
				"api": []any{map[string]any{"name": "one", "path": "/api/v1/namespaces/" + namespace + "/configmaps/" + cm443Name}},
			}), s1); err != nil {
				t.Fatal(err)
			}
			_ = r.Delete(ctx, s1)
			if err := r.Create(ctx, s1); err != nil {
				t.Fatalf("create stored ra-x: %v", err)
			}
			return ctx
		}).
		Setup(e2e.SignUp(e2e.SignUpOptions{Username: "inlineadmin443", Groups: []string{"cluster-admins"}, Namespace: namespace,
			JWTSignKey: signKeyPEM282(t), JWTKeyID: "test-kid-443ia"})).
		Setup(save(&k.adminCtx)).
		Setup(e2e.SignUp(e2e.SignUpOptions{Username: "inlinedev443", Groups: []string{"devs"}, Namespace: namespace,
			JWTSignKey: signKeyPEM282(t), JWTKeyID: "test-kid-443id"})).
		Setup(save(&k.devCtx)).
		Assess("a_ParityGolden", func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			r := adminRes443(t, cfg)
			// Apply S2 to the stored ra-x.
			cur := &v1.RESTAction{}
			if err := r.Get(ctx, ra443Name, namespace, cur); err != nil {
				t.Fatalf("get ra-x: %v", err)
			}
			s2 := &v1.RESTAction{}
			if err := runtime.DefaultUnstructuredConverter.FromUnstructured(manifest443(ra443Name, s2Spec443()), s2); err != nil {
				t.Fatal(err)
			}
			cur.Spec = s2.Spec
			if err := r.Update(ctx, cur); err != nil {
				t.Fatalf("apply S2: %v", err)
			}
			replies := map[string]string{}
			for _, c := range []struct {
				who string
				ctx context.Context
			}{{"admin", k.adminCtx}, {"dev", k.devCtx}} {
				// Arm A — raw-read body2, inline-resolve it, compare to the stored resolve.
				raw := k.storedGET(c.ctx, cfg, ra443Name, true)
				if raw.Code != http.StatusOK || raw.Header().Get(handlers.HeaderRaw) != "true" {
					t.Fatalf("%s raw read: code=%d body=%s", c.who, raw.Code, raw.Body.String())
				}
				var body2 map[string]any
				if err := json.Unmarshal(raw.Body.Bytes(), &body2); err != nil {
					t.Fatal(err)
				}
				stored := k.storedGET(c.ctx, cfg, ra443Name, false)
				inline := k.inlinePOST(t, c.ctx, cfg, body2)
				if stored.Code != http.StatusOK || inline.Code != http.StatusOK {
					t.Fatalf("%s: codes stored=%d inline=%d (inline body=%s)", c.who, stored.Code, inline.Code, inline.Body.String())
				}
				if inline.Header().Get(handlers.HeaderDryRun) != "All" || inline.Header().Get(handlers.HeaderResolveSource) != "request-body" {
					t.Errorf("%s: inline echoes missing: %v", c.who, inline.Header())
				}
				if inline.Body.String() != stored.Body.String() {
					t.Errorf("%s Arm A: inline reply differs from the stored resolve\n stored=%s\n inline=%s", c.who, stored.Body.String(), inline.Body.String())
				}
				// Arm B — the apply manifest as the body: .status bytes identical.
				inlineB := k.inlinePOST(t, c.ctx, cfg, manifest443(ra443Name, s2Spec443()))
				if got, want := statusBytes443(t, inlineB.Body.Bytes()), statusBytes443(t, stored.Body.Bytes()); got != want {
					t.Errorf("%s Arm B: inline .status differs from the stored resolve's\n stored=%s\n inline=%s", c.who, want, got)
				}
				replies[c.who] = statusBytes443(t, stored.Body.Bytes())
				t.Logf("%s status=%s", c.who, replies[c.who])
			}
			// cms lists >= 2 names and the iterator re-reads each one, so both
			// names appear twice (once under cms, once under each).
			for _, n := range []string{cm443Name, "kube-root-ca.crt"} {
				if bytes.Count([]byte(replies["admin"]), []byte(`"`+n+`"`)) < 2 {
					t.Errorf("admin's reply does not show the iterator stage reading %q (M>=2 not exercised): %s", n, replies["admin"])
				}
			}
			if replies["admin"] == replies["dev"] {
				t.Errorf("admin and dev got identical replies — the fixture does not discriminate the callers' RBAC: %s", replies["admin"])
			}
			return ctx
		}).
		Assess("NothingPersisted", func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			draft := manifest443(draft443Name, map[string]any{"api": []any{
				map[string]any{"name": "create", "verb": "POST", "continueOnError": true, "errorKey": "createErr",
					"path":    "/api/v1/namespaces/" + namespace + "/configmaps",
					"payload": `{"apiVersion":"v1","kind":"ConfigMap","metadata":{"name":"` + evil443Name + `"}}`},
			}})
			rec := k.inlinePOST(t, k.adminCtx, cfg, draft)
			if rec.Code != http.StatusOK {
				t.Fatalf("inline draft: code=%d body=%s", rec.Code, rec.Body.String())
			}
			if !bytes.Contains(rec.Body.Bytes(), []byte(`dry-run: stage \"create\" verb POST is not executed`)) {
				t.Errorf("the envelope lacks the dry-run stage error: %s", rec.Body.String())
			}
			r := adminRes443(t, cfg)
			if err := r.Get(ctx, evil443Name, namespace, &corev1.ConfigMap{}); !apierrors.IsNotFound(err) {
				t.Errorf("the write-verb stage's ConfigMap exists (err=%v): a dry-run resolve executed a write", err)
			}
			if err := r.Get(ctx, draft443Name, namespace, &v1.RESTAction{}); !apierrors.IsNotFound(err) {
				t.Errorf("the inline draft RESTAction exists (err=%v): an inline resolve persisted its body", err)
			}
			return ctx
		}).
		Teardown(func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			r := adminRes443(t, cfg)
			ra := &v1.RESTAction{}
			ra.Name, ra.Namespace = ra443Name, namespace
			_ = r.Delete(ctx, ra)
			_ = r.Delete(ctx, &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: cm443Name, Namespace: namespace}})
			_ = r.Delete(ctx, &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: evil443Name, Namespace: namespace}})
			_ = rbacv1.AddToScheme(r.GetScheme())
			_ = r.Delete(ctx, &rbacv1.ClusterRoleBinding{ObjectMeta: metav1.ObjectMeta{Name: "issue156-cluster-admins"}})
			return ctx
		}).
		Feature()
	testenv.Test(t, f)
}
