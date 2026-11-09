// learned_units_by_class_test.go — run 23 CPU profile: seedLearnedWidgetsFor /
// seedLearnedRAsFor walked every seed's learned list once per admitted class
// and recomputed cache.LearnedClassKey for every unit on every walk —
// O(classes × units) keys, 2.4% of all CPU, 6.2% of allocated bytes, 9.1% of
// allocated objects. learnedUnitsByClass computes each unit's key once. These
// pin that the per-class unit sequence is exactly the walk's, and measure it.
//
//	go test ./internal/handlers/dispatchers/ -run '^$' -bench LearnedUnits -benchmem -count=6
package dispatchers

import (
	"fmt"
	"reflect"
	"testing"
)

// learnedWalkBefore is the pre-fix per-class walk: every seed, every learned
// target, keep those whose key is k. The reference the parity test compares
// against.
func learnedWalkBefore(learned [][]seedTarget, k string) []learnedUnit {
	var out []learnedUnit
	for i, ts := range learned {
		for _, c := range ts {
			if learnedKey(c) != k {
				continue
			}
			out = append(out, learnedUnit{seed: i, c: c})
		}
	}
	return out
}

// learnedFixture is seeds × learned targets drawn from `classes` identity
// classes. Each seed carries a rotating subset of the classes, some twice, and
// the same class appears with its groups in a different order and with and
// without system:authenticated — all one class key (LearnedClassKey sorts the
// effective group set) — so the grouping must merge them in walk order.
func learnedFixture(seeds, classes int) [][]seedTarget {
	out := make([][]seedTarget, seeds)
	for i := range out {
		var ts []seedTarget
		for j := 0; j < classes; j++ {
			if (i+j)%3 == 0 {
				continue // this seed does not carry class j
			}
			user := fmt.Sprintf("user-%03d", j)
			groups := []string{fmt.Sprintf("team-%02d", j%7), "devs"}
			switch (i + j) % 4 {
			case 1:
				groups = []string{"devs", fmt.Sprintf("team-%02d", j%7)}
			case 2:
				groups = append(groups, "system:authenticated")
			}
			ts = append(ts, seedTarget{
				BindingUID: fmt.Sprintf("uid-%d-%d", i, j),
				Username:   user, Groups: groups, Learned: true, Population: i + j,
			})
			if (i*j)%11 == 5 {
				dup := ts[len(ts)-1]
				dup.BindingUID += "-dup"
				ts = append(ts, dup)
			}
		}
		out[i] = ts
	}
	out = append(out, nil, []seedTarget{}) // seeds with no learned units
	return out
}

// TestLearnedUnitsByClass_IdenticalToPerClassWalk — for every class key (and a
// key no unit has), the index yields exactly the units, in exactly the order,
// the pre-fix walk visited; and its key set is exactly the walk's.
func TestLearnedUnitsByClass_IdenticalToPerClassWalk(t *testing.T) {
	for _, sz := range [][2]int{{0, 0}, {1, 1}, {5, 3}, {40, 25}, {120, 60}} {
		learned := learnedFixture(sz[0], sz[1])
		idx := learnedUnitsByClass(learned)
		keys := map[string]struct{}{}
		for _, ts := range learned {
			for _, c := range ts {
				keys[learnedKey(c)] = struct{}{}
			}
		}
		if len(idx) != len(keys) {
			t.Fatalf("%v: index has %d classes, the units carry %d", sz, len(idx), len(keys))
		}
		keys[learnedKey(seedTarget{Username: "nobody"})] = struct{}{}
		for k := range keys {
			want := learnedWalkBefore(learned, k)
			got := idx[k]
			if len(want) == 0 && len(got) == 0 {
				continue
			}
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("%v: class %q: index units differ from the walk's\n got %v\nwant %v", sz, k, got, want)
			}
		}
	}
}

var learnedUnitsSink int

// BenchmarkLearnedUnits_SeedAllClasses — one pass's learned seeding shape:
// every admitted class asks for its units over the widget seeds. path=before
// is the per-class walk; path=after builds the index once and reads it.
func BenchmarkLearnedUnits_SeedAllClasses(b *testing.B) {
	for _, sz := range [][2]int{{100, 20}, {300, 80}} {
		learned := learnedFixture(sz[0], sz[1])
		var order []string
		seen := map[string]struct{}{}
		for _, ts := range learned {
			for _, c := range ts {
				k := learnedKey(c)
				if _, ok := seen[k]; !ok {
					seen[k] = struct{}{}
					order = append(order, k)
				}
			}
		}
		name := fmt.Sprintf("seeds=%d/classes=%d", sz[0], sz[1])
		b.Run(name+"/path=before", func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				n := 0
				for _, k := range order {
					for i, ts := range learned {
						for _, c := range ts {
							if learnedKey(c) != k {
								continue
							}
							n += i
						}
					}
				}
				learnedUnitsSink = n
			}
		})
		b.Run(name+"/path=after", func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				n := 0
				idx := learnedUnitsByClass(learned)
				for _, k := range order {
					for _, u := range idx[k] {
						n += u.seed
					}
				}
				learnedUnitsSink = n
			}
		})
	}
}
