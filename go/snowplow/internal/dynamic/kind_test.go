//go:build integration
// +build integration

package dynamic

import (
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"k8s.io/apimachinery/pkg/runtime/schema"

	"sigs.k8s.io/e2e-framework/pkg/env"
	"sigs.k8s.io/e2e-framework/pkg/envconf"
	"sigs.k8s.io/e2e-framework/pkg/envfuncs"
	"sigs.k8s.io/e2e-framework/pkg/features"
	"sigs.k8s.io/e2e-framework/support/kind"

	"github.com/krateo-platformops/snowplow/internal/cache"
	xenv "github.com/krateo-platformops/plumbing/env"
)

var (
	testenv     env.Environment
	clusterName string
	namespace   string
)

func TestMain(m *testing.M) {
	xenv.SetTestMode(true)

	namespace = "demo-system"
	// #522: a PER-PROCESS cluster name. kind.Cluster.Create is reuse-if-exists
	// and Destroy is an unconditional `kind delete cluster --name <name>` with no
	// ownership check, while testEnv.Run executes Setup BEFORE m.Run — so with a
	// name shared across packages, ANY invocation of this binary (a -list, or a
	// -run matching zero tests) created and then destroyed a machine-global
	// cluster another process was using. Deriving the name from the pid makes
	// every process name, and therefore delete, only its own cluster. It stays
	// reapable — krateo-<pid> is collectable once that pid is gone — which keeps
	// the orphan cleanup the shared name provided by accident and which a random
	// name would lose.
	clusterName = fmt.Sprintf("krateo-%d", os.Getpid())
	testenv = env.New()

	testenv.Setup(
		envfuncs.CreateCluster(kind.NewProvider(), clusterName),
		envfuncs.CreateNamespace(namespace),
	).Finish(
		envfuncs.DeleteNamespace(namespace),
		envfuncs.DestroyCluster(clusterName),
	)

	os.Exit(testenv.Run(m))
}

func TestKindFor(t *testing.T) {
	os.Setenv("DEBUG", "0")

	f := features.New("Setup").
		Assess("KindFor", func(ctx context.Context, t *testing.T, c *envconf.Config) context.Context {
			// KindFor was refactored out of internal/dynamic; cache.KindForGVR is its
			// replacement (returns the Kind string for a GVR).
			got, err := cache.KindForGVR(ctx, schema.GroupVersionResource{Version: "v1", Resource: "configmaps"}, c.Client().RESTConfig())
			assert.Nil(t, err)
			assert.Equal(t, "ConfigMap", got)

			return ctx
		}).
		Feature()

	testenv.Test(t, f)
}
