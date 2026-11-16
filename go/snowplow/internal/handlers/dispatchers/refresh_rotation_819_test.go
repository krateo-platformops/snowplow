// refresh_rotation_819_test.go — #819 / #828 review: the rotation tracker
// compares the WHOLE identity class. A key folds both halves (binding-set
// digest and RBAC sub-generation), so a move of either alone rotates it and
// must be announced; neither moving must not.

package dispatchers

import (
	"context"
	"testing"

	xcontext "github.com/krateo-platformops/plumbing/context"
	"github.com/krateo-platformops/plumbing/jwtutil"
	"github.com/krateo-platformops/snowplow/internal/cache"
	"github.com/krateo-platformops/snowplow/internal/rbac"
)

func TestIssue819_TrackerComparesBothHalvesOfTheClass(t *testing.T) {
	ui := jwtutil.UserInfo{Username: "u-819", Groups: []string{"g-819"}}
	tr := NewSubscriptionRotationTracker(xcontext.BuildContext(context.Background(), xcontext.WithUserInfo(ui)))
	cur := rbac.IdentityClassOf(ui)

	tr.keys["same"] = &trackedArm{minted: cur}
	tr.keys["digest-moved"] = &trackedArm{minted: cache.IdentityClass{SubjectBindingSet: cur.SubjectBindingSet + "-stale", RBACSubGen: cur.RBACSubGen}}
	tr.keys["subgen-moved"] = &trackedArm{minted: cache.IdentityClass{SubjectBindingSet: cur.SubjectBindingSet, RBACSubGen: cur.RBACSubGen + 1}}

	got := tr.Rotated()
	want := []string{"digest-moved", "subgen-moved"}
	if len(got) != len(want) || got[0] != want[0] || got[1] != want[1] {
		t.Fatalf("Rotated()=%v, want %v — a move of the binding-set digest alone, or of the sub-gen alone, rotates "+
			"the key; an unchanged class does not", got, want)
	}
	if again := tr.Rotated(); len(again) != 0 {
		t.Fatalf("second Rotated()=%v — a key is announced once", again)
	}
}
