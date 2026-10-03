package dispatchers

// learned_identity_262_driftguard_test.go — #262 × #424.
//
// TestS262_LearnedSeedPutUnderClassDriftGuard — a learned seed is a real
// resolve under the class's identity and writes through the SAME terminal Put
// as every cohort (seedTerminalPut), so #424's identity-class drift guard still
// applies: a grant to the learned user that lands DURING its seed resolve moves
// its binding set away from the key the seed minted, and the write must be
// declined (the body would otherwise sit under a key the user's NEW class no
// longer derives, served to whoever still derives it).

import (
	"context"
	"testing"
	"time"

	xcontext "github.com/krateo-platformops/plumbing/context"
	"github.com/krateo-platformops/snowplow/internal/cache"
	"github.com/krateo-platformops/snowplow/internal/rbac"
	"github.com/krateo-platformops/snowplow/internal/resolvers/widgets"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
)

func TestS262_LearnedSeedPutUnderClassDriftGuard(t *testing.T) {
	env := l262Setup(t, l262Opts{
		extraUsers: []string{"ivan"},
		secrets: []*corev1.Secret{
			l262ClientconfigSecret(t, "ivan-clientconfig", "ivan", []string{l262Group}, time.Now().Add(-time.Hour), "1"),
		},
		widgets: 1, ras: 1,
	})
	l262Stubs(t, l262StubOpts{})
	l262WaitLearned(t, 1)
	ivanCtx := l262CustomerCtx("ivan", []string{l262Group})
	preKeys, handle := l262Keys(t, env, ivanCtx)

	granted := false
	inner := widgetsResolveFn
	widgetsResolveFn = func(ctx context.Context, o widgets.ResolveOptions) (*widgets.Widget, error) {
		if ui, err := xcontext.UserInfo(ctx); err == nil && ui.Username == "ivan" && !granted {
			granted = true
			before := rbac.SubjectBindingSetDigest("ivan", []string{l262Group})
			rb := l262ExtraRB("ivan")
			rb.Name, rb.UID = "l262-extra-ivan-2", types.UID("uid-l262-extra-ivan-2")
			rb.TypeMeta = metav1.TypeMeta{APIVersion: "rbac.authorization.k8s.io/v1", Kind: "RoleBinding"}
			obj, err := runtime.DefaultUnstructuredConverter.ToUnstructured(rb)
			if err != nil {
				t.Error(err)
			}
			if _, err := env.dyn.Resource(l262RBGVR).Namespace(l262XNS).Create(context.Background(),
				&unstructured.Unstructured{Object: obj}, metav1.CreateOptions{}); err != nil {
				t.Error(err)
			}
			for i := 0; i < 500 && rbac.SubjectBindingSetDigest("ivan", []string{l262Group}) == before; i++ {
				cache.RebuildRBACSnapshotForTest(env.rw)
				time.Sleep(10 * time.Millisecond)
			}
		}
		return inner(ctx, o)
	}
	t.Cleanup(func() { widgetsResolveFn = inner })

	declinedBefore := identityClassDriftDeclinedForTest("seed", "binding_set")
	l262Boot(t, env, seedModeBoot)
	if !granted {
		t.Fatal("NON-VACUITY: the learned seed never resolved under ivan")
	}
	if d := identityClassDriftDeclinedForTest("seed", "binding_set") - declinedBefore; d < 1 {
		t.Fatalf("#424 guard did not decline the learned seed whose class drifted mid-resolve (declines +%d)", d)
	}
	if _, ok := handle.GetNoTouch(preKeys[0]); ok {
		t.Fatal("the drifted learned seed's body must NOT be written under the pre-grant key")
	}
}
