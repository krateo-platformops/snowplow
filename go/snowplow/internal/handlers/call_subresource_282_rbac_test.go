//go:build integration
// +build integration

// #282 arm 2 — the security-load-bearing DISCRIMINATING pair, kind-backed
// (mirrors the #156 apiserver-RBAC family). The property under test: a plain
// grant on the PARENT resource does NOT authorize the `status` subresource, and
// snowplow's /call?subresource=status routing makes the apiserver enforce the
// STATUS-specific grant.
//
//   - Forbidden: a `devs` user has update+patch on restactions (parent, via
//     rbac.restactions.yaml) but NOT restactions/status → the status PATCH is
//     403. Crucially, this REDs without part 1: if the subresource segment were
//     not appended, the request would hit the PARENT path (which devs CAN
//     patch) and SUCCEED — so the 403 proves the request reached /status.
//   - Allowed: a status-writers-282 user has patch on restactions/status
//     (rbac.restactions-status-282.yaml) → the same status PATCH succeeds (200).
//
// Both run under the caller's own x509 (SignUp) through runWSScoped; snowplow
// adds no SAR — the apiserver is the authority.

package handlers_test

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"net/http"
	"os"
	"path/filepath"
	"testing"

	"github.com/krateo-platformops/plumbing/e2e"
	"github.com/krateo-platformops/plumbing/http/request"
	"github.com/krateo-platformops/plumbing/ptr"
	"github.com/krateo-platformops/snowplow/apis"
	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/e2e-framework/klient/decoder"
	"sigs.k8s.io/e2e-framework/klient/k8s/resources"
	"sigs.k8s.io/e2e-framework/pkg/envconf"
	"sigs.k8s.io/e2e-framework/pkg/features"
)

// statusPatchOpts is the /call?subresource=status merge-PATCH both arms issue.
func statusPatchOpts() request.RequestOptions {
	return request.RequestOptions{
		RequestInfo: request.RequestInfo{
			Verb:    ptr.To(http.MethodPatch),
			Path:    "/call?apiVersion=templates.krateo.io/v1&resource=restactions&namespace=demo-system&name=kube-get&subresource=status",
			Headers: []string{"Content-Type: application/merge-patch+json"},
			Payload: ptr.To(`{"status":{"phase":"decided-282"}}`),
		},
	}
}

// createKubeGetRESTAction creates the target RESTAction (idempotent) using the
// admin client — the object must exist for the ALLOWED arm's status patch.
func createKubeGetRESTAction(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
	r, err := resources.New(cfg.Client().RESTConfig())
	if err != nil {
		t.Fatal(err)
	}
	if err := apis.AddToScheme(r.GetScheme()); err != nil {
		t.Fatal(err)
	}
	r.WithNamespace(namespace)
	if err := decoder.DecodeEachFile(
		ctx, os.DirFS(filepath.Join(testdataPath, "restactions")), "kube-get.yaml",
		decoder.CreateIgnoreAlreadyExists(r),
		decoder.MutateNamespace(namespace),
	); err != nil {
		t.Fatalf("creating kube-get RESTAction: %v", err)
	}
	return ctx
}

func signKeyPEM282(t *testing.T) string {
	privateKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("generating RSA key: %v", err)
	}
	return string(pem.EncodeToMemory(&pem.Block{
		Type:  "RSA PRIVATE KEY",
		Bytes: x509.MarshalPKCS1PrivateKey(privateKey),
	}))
}

// TestSubresource282_StatusWrite_ParentGrantForbidden — a devs user (parent
// update/patch, NO restactions/status) PATCHing status through /call gets 403.
// REDs without part 1 (a mis-route to the parent path would succeed).
func TestSubresource282_StatusWrite_ParentGrantForbidden(t *testing.T) {
	os.Setenv("DEBUG", "0")
	f := features.New("Issue282-status-forbidden").
		Setup(e2e.Logger("test")).
		Setup(createKubeGetRESTAction).
		Setup(e2e.SignUp(e2e.SignUpOptions{
			Username:   "statusdev",
			Groups:     []string{"devs"},
			Namespace:  namespace,
			JWTSignKey: signKeyPEM282(t),
			JWTKeyID:   "test-kid-282f",
		})).
		Assess("status PATCH forbidden by apiserver (parent grant does not cover /status)",
			runWSScoped(statusPatchOpts(), http.StatusForbidden)).
		Feature()
	testenv.Test(t, f)
}

// TestSubresource282_StatusWrite_StatusGrantAllowed — a status-writers-282 user
// (patch on restactions/status) PATCHing status through /call succeeds (200).
func TestSubresource282_StatusWrite_StatusGrantAllowed(t *testing.T) {
	os.Setenv("DEBUG", "0")
	f := features.New("Issue282-status-allowed").
		Setup(e2e.Logger("test")).
		Setup(createKubeGetRESTAction).
		// Grant status-writers-282 → restactions/status.
		Setup(func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			r, err := resources.New(cfg.Client().RESTConfig())
			if err != nil {
				t.Fatal(err)
			}
			if err := decoder.ApplyWithManifestDir(ctx, r, testdataPath, "rbac.restactions-status-282.yaml", []resources.CreateOption{}); err != nil {
				t.Fatalf("applying status-writer grant: %v", err)
			}
			return ctx
		}).
		Setup(e2e.SignUp(e2e.SignUpOptions{
			Username:   "statuswriter",
			Groups:     []string{"status-writers-282"},
			Namespace:  namespace,
			JWTSignKey: signKeyPEM282(t),
			JWTKeyID:   "test-kid-282a",
		})).
		Assess("status PATCH allowed by apiserver (holds restactions/status grant)",
			runWSScoped(statusPatchOpts(), http.StatusOK)).
		// #223 -count hygiene: the cluster-scoped grant persists across iterations.
		Teardown(func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			r, err := resources.New(cfg.Client().RESTConfig())
			if err != nil {
				t.Fatalf("#282 teardown: resources.New: %v", err)
			}
			_ = rbacv1.AddToScheme(r.GetScheme())
			if err := r.Delete(ctx, &rbacv1.ClusterRoleBinding{ObjectMeta: metav1.ObjectMeta{Name: "restactions-status-writers-282"}}); err != nil {
				t.Logf("#282 teardown: delete ClusterRoleBinding: %v", err)
			}
			if err := r.Delete(ctx, &rbacv1.ClusterRole{ObjectMeta: metav1.ObjectMeta{Name: "restactions-status-writer-282"}}); err != nil {
				t.Logf("#282 teardown: delete ClusterRole: %v", err)
			}
			return ctx
		}).
		Feature()
	testenv.Test(t, f)
}
