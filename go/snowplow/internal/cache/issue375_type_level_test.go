// issue375_type_level_test.go — #375 (TL ruling): the TYPE-LEVEL dirty-mark sources
// must advance the dependency generation too.

package cache

import (
	"context"
	"testing"

	"k8s.io/apimachinery/pkg/runtime/schema"
)

// The four GVR-lifecycle dirty-mark handlers are CRD add, CRD delete, schema relist
// and store repair. Each fires INSIDE a resolve's [read, Put] window, and the
// resolve's accepted Put must remark. Its type-level mark can be consumed while the
// key is not resident (skipped_no_entry), which is exactly the #375 race. The arm
// covers a warm LIST bucket, a warm exact bucket (skipped for CRD_ADD, which is
// LIST-only by contract) and a COLD LIST bucket (reached via the GVR floor).
// RED on the pre-ruling tree. NEUTER: drop the bumpResourceTypeGen calls.
func TestIssue375_TypeLevelDirtyMarkInsideWindow_Remarks(t *testing.T) {
	type src struct {
		name     string
		fire     func(d *DepTracker, g schema.GroupVersionResource) int
		listOnly bool
	}
	for _, s := range []src{
		{"CRD_ADD", (*DepTracker).OnResourceTypeAvailable, true},
		{"CRD_DELETE", (*DepTracker).OnResourceTypeRemoved, false},
		{"SCHEMA_RELIST", (*DepTracker).OnResourceTypeSchemaRelisted, false},
		{"STORE_REPAIR", (*DepTracker).OnResourceTypeStoreRepaired, false},
	} {
		t.Run(s.name, func(t *testing.T) {
			c, rl, _ := setup375(t)
			cases := []struct {
				key  string
				name string // listWildcard = LIST
				warm bool
			}{{"tl-list", listWildcard, true}, {"tl-exact", "obj", true}, {"tl-cold-list", listWildcard, false}}
			type open struct {
				ctx  context.Context
				gen0 uint64
			}
			opens := map[string]open{}
			for _, cs := range cases {
				if s.listOnly && cs.name != listWildcard {
					continue
				}
				if cs.warm {
					Deps().Record(context.Background(), "tl-other-"+cs.key, g375A, "ns", cs.name)
				}
				opens[cs.key] = open{ctx: WithL1KeyContext(context.Background(), cs.key), gen0: c.CaptureGen(cs.key)}
			}
			s.fire(Deps(), g375A) // the type-level event lands inside every open window
			for _, cs := range cases {
				o, ok := opens[cs.key]
				if !ok {
					continue
				}
				Deps().Record(o.ctx, cs.key, g375A, "ns", cs.name)
				if !c.PutIfGen(o.ctx, cs.key, body375(`{}`), o.gen0) {
					t.Fatalf("setup: PutIfGen refused")
				}
				if got := rl.count(cs.key); got != 1 {
					t.Errorf("#375 type-level RED (%s, %s): remarks=%d want 1 — a GVR-lifecycle dirty-mark "+
						"inside [read, Put] must advance the generation", s.name, cs.key, got)
				}
			}
		})
	}
}
