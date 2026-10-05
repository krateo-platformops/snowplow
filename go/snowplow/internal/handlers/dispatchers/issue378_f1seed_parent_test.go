package dispatchers

// issue378_f1seed_parent_test.go — #378 F1-seed, the DISCRIMINATING arm (brief
// issuecomment-5990884686). PARENT-SHA ONLY: it drives the dead seed-path re-mint
// (reseedFromInputs, seedModeReMint) that this PR deletes, so it is committed
// with the RED falsifiers and removed with the retirement. It feeds the SAME 32
// F1 cells to reseedFromInputs inside the lead window and shows which classes the
// seed path cannot re-mint under their key (they still miss at the cap): the
// retirement is correct, not cosmetic.

import (
	"context"
	"testing"

	"github.com/krateo-platformops/snowplow/internal/cache"
)

func TestIssue378_F1Seed_ReseedFromInputsCannotReMintTheProductionShapes(t *testing.T) {
	r378RunF1(t, func(e *r378Env) {
		c := cache.ResolvedCache()
		var ins []*cache.ResolvedKeyInputs
		for _, cell := range e.cells {
			if ent, ok := c.GetNoTouch(cell.key); ok && ent.Inputs != nil {
				in := *ent.Inputs
				ins = append(ins, &in)
			}
		}
		deps := rePrewarmDeps{saEP: *e.saEP, saRC: e.saRC, authnNS: psAuthnNS}
		re := reseedFromInputs(context.Background(), deps, ins)
		t.Logf("F1-seed: fed %d cells to reseedFromInputs inside the lead window; re-enqueue=%d", len(ins), len(re))
	})
}
