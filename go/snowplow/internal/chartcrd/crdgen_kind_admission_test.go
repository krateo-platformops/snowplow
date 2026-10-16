//go:build integration

package chartcrd

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/krateo-platformops/snowplow/internal/testsupport/kindguard"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	apiextclient "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/tools/clientcmd"
	"sigs.k8s.io/e2e-framework/support/kind"
)

// crdgen_kind_admission_test.go: #657, the real-apiserver half of the guard.
//
// The in-process half (crdgen_installable_test.go) uses this module's copy of
// the apiserver validator, which is version 1.35. 057 runs v1.36.4, and the
// cost estimator and its budget are per-minor. So this arm submits the crdgen
// output to a kind apiserver at exactly 057's version:
//
//  1. The CRD compiled from the chart schema is accepted (dry-run=All, then created).
//  2. The same CRD with maxItems/maxLength stripped (the 1.12.45 shape) is
//     refused with the cost error. If that refusal ever disappears, the guard
//     fails instead of passing for no reason.
//  3. CRs exercise the RULES, not just the CRD's admission. Privileged names
//     are refused per item, ordinary names pass, and maxItems/maxLength are
//     enforced.

// 057's apiserver is v1.36.4-gke. Pin the node image by digest, so the
// estimator under test cannot drift with a re-tagged image.
const (
	kindNodeImage   = "kindest/node:v1.36.4@sha256:099e049362a1526b2db71494e1947aae99bd16290d7c895f2b7ea312e3cbfaed"
	wantServerMinor = "36"
)

func TestIssue657_Kind_CrdgenCRDAdmittedAndRulesEnforced(t *testing.T) {
	schemaBytes := readFile(t, chartSchemaPath)
	requireNoCoreProviderRewrite(t, schemaBytes)
	crd, err := generate(t, schemaBytes)
	if err != nil {
		t.Fatalf("crdgen refuses the chart schema (see TestIssue657_CrdgenCompilesChartSchema): %v", err)
	}

	ctx := context.Background()
	// A per-process name: kind's Create reuses an existing cluster of the same
	// name and Destroy deletes unconditionally, so a shared name could destroy
	// a cluster another process is using (#522).
	clusterName := kindguard.ClusterName() // krateo-sptest-<pid>: per-process (#522), marked as ours (#742)
	// #742 — reap orphaned krateo-<pid> clusters; delete this one on SIGINT/SIGTERM.
	kindguard.Prepare(clusterName)
	provider := kind.NewCluster(clusterName).WithOpts(kind.WithImage(kindNodeImage))
	cctx, ccancel := context.WithTimeout(ctx, 6*time.Minute)
	defer ccancel()
	kubeconfig, err := provider.Create(cctx)
	if err != nil {
		t.Fatalf("create kind cluster %s: %v", clusterName, err)
	}
	t.Cleanup(func() {
		dctx, dcancel := context.WithTimeout(context.Background(), 3*time.Minute)
		defer dcancel()
		if err := provider.Destroy(dctx); err != nil {
			t.Logf("destroy kind cluster %s: %v", clusterName, err)
		}
		kindguard.Release()
	})
	cfg, err := clientcmd.BuildConfigFromFlags("", kubeconfig)
	if err != nil {
		t.Fatal(err)
	}

	// The estimator under test is the apiserver's. Prove it is 057's minor.
	dc, err := discovery.NewDiscoveryClientForConfig(cfg)
	if err != nil {
		t.Fatal(err)
	}
	sv, err := dc.ServerVersion()
	if err != nil {
		t.Fatal(err)
	}
	if strings.TrimSuffix(sv.Minor, "+") != wantServerMinor {
		t.Fatalf("kind apiserver is %s, want minor %s (057)", sv.GitVersion, wantServerMinor)
	}
	t.Logf("apiserver %s (node image %s)", sv.GitVersion, kindNodeImage)

	ext, err := apiextclient.NewForConfig(cfg)
	if err != nil {
		t.Fatal(err)
	}
	crds := ext.ApiextensionsV1().CustomResourceDefinitions()
	dryRun := metav1.CreateOptions{DryRun: []string{metav1.DryRunAll}}

	// 2 first: the unbounded twin, dry-run only, so the name stays free.
	unbounded := crd.DeepCopy()
	for _, v := range unbounded.Spec.Versions {
		if v.Schema == nil || v.Schema.OpenAPIV3Schema == nil {
			continue
		}
		rbac := v.Schema.OpenAPIV3Schema.Properties["spec"].Properties["rbac"]
		for _, f := range celItemFields {
			arr := rbac.Properties[f]
			arr.MaxItems = nil
			arr.Items.Schema.MaxLength = nil
			rbac.Properties[f] = arr
		}
		v.Schema.OpenAPIV3Schema.Properties["spec"].Properties["rbac"] = rbac
	}
	_, err = crds.Create(ctx, unbounded, dryRun)
	if err == nil {
		t.Fatal("the v1.36 apiserver ACCEPTED the crdgen CRD with the impersonate* bounds stripped: either crdgen " +
			"stopped emitting the item CEL or the estimator changed; this arm no longer proves the bounds matter")
	}
	if !strings.Contains(err.Error(), "properties[impersonateServiceAccounts].items.x-kubernetes-validations") ||
		!strings.Contains(err.Error(), costOverBudget) {
		t.Fatalf("unbounded CRD refused, but not for the impersonateServiceAccounts rule cost: %v", err)
	}
	t.Logf("unbounded twin refused as expected: %v", err)

	// 1: the CRD the chart produces is admitted (dry-run), then created for real.
	if _, err := crds.Create(ctx, crd, dryRun); err != nil {
		t.Fatalf("the v1.36 apiserver refuses core-provider's crdgen CRD for helm/snowplow (#657): %v", err)
	}
	if _, err := crds.Create(ctx, crd, metav1.CreateOptions{}); err != nil {
		t.Fatalf("create CRD: %v", err)
	}
	waitEstablished(t, ctx, ext, crd.Name)

	// 3: CRs against the live rules.
	dyn, err := dynamic.NewForConfig(cfg)
	if err != nil {
		t.Fatal(err)
	}
	var served string
	for _, v := range crd.Spec.Versions {
		if v.Served {
			served = v.Name
		}
	}
	gvr := schema.GroupVersionResource{Group: crd.Spec.Group, Version: served, Resource: crd.Spec.Names.Plural}
	cr := func(field string, names []string) *unstructured.Unstructured {
		list := make([]any, len(names))
		for i, n := range names {
			list[i] = n
		}
		return &unstructured.Unstructured{Object: map[string]any{
			"apiVersion": crd.Spec.Group + "/" + served,
			"kind":       crd.Spec.Names.Kind,
			"metadata":   map[string]any{"name": "issue657", "namespace": "default"},
			"spec":       map[string]any{"rbac": map[string]any{field: list}},
		}}
	}
	apply := func(field string, names []string) error {
		_, err := dyn.Resource(gvr).Namespace("default").Create(ctx, cr(field, names), dryRun)
		return err
	}

	for _, c := range rejectCases {
		t.Run("reject/"+c.field+"/"+c.name, func(t *testing.T) {
			err := apply(c.field, []string{"devs", c.name})
			if err == nil {
				t.Fatalf("the apiserver admitted spec.rbac.%s=[devs, %s]", c.field, c.name)
			}
			// Refused by the item RULE, not by something incidental.
			if !strings.Contains(err.Error(), "spec.rbac."+c.field+"[1]") {
				t.Fatalf("spec.rbac.%s=[devs, %s] refused, but not at the item: %v", c.field, c.name, err)
			}
			t.Logf("refused: %v", err)
		})
	}
	for _, c := range acceptCases {
		t.Run("accept/"+c.field, func(t *testing.T) {
			if err := apply(c.field, c.names); err != nil {
				t.Fatalf("the apiserver refused spec.rbac.%s=%v: %v", c.field, c.names, err)
			}
		})
	}
	for _, f := range celItemFields {
		t.Run("bounds/"+f, func(t *testing.T) {
			arr := rbacField(t, crd, f)
			if arr.MaxItems == nil || arr.Items.Schema.MaxLength == nil {
				t.Fatalf("spec.rbac.%s is not bounded in the CRD (maxItems and items.maxLength are both required, #657)", f)
			}
			n := int(*arr.MaxItems)
			full := make([]string, n)
			for i := range full {
				full[i] = fmt.Sprintf("cohort-%d", i)
			}
			if err := apply(f, full); err != nil {
				t.Fatalf("%d items (= maxItems) refused: %v", n, err)
			}
			if err := apply(f, append(full, "one-more")); err == nil || !strings.Contains(err.Error(), "Too many") {
				t.Fatalf("%d items (> maxItems) not refused for size: %v", n+1, err)
			}
			long := strings.Repeat("a", int(*arr.Items.Schema.MaxLength)+1)
			if err := apply(f, []string{long}); err == nil || !strings.Contains(err.Error(), "Too long") {
				t.Fatalf("a %d-char name (> maxLength) not refused for length: %v", len(long), err)
			}
		})
	}
}

func waitEstablished(t *testing.T, ctx context.Context, ext *apiextclient.Clientset, name string) {
	t.Helper()
	deadline := time.Now().Add(60 * time.Second)
	for time.Now().Before(deadline) {
		got, err := ext.ApiextensionsV1().CustomResourceDefinitions().Get(ctx, name, metav1.GetOptions{})
		if err == nil {
			for _, c := range got.Status.Conditions {
				if c.Type == apiextensionsv1.Established && c.Status == apiextensionsv1.ConditionTrue {
					return
				}
			}
		}
		time.Sleep(500 * time.Millisecond)
	}
	t.Fatalf("CRD %s not Established within 60s", name)
}
