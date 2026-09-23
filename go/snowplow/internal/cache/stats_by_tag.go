// stats_by_tag.go — 1.12.6 C7. ONE derivation for every stats family that
// has a struct behind it.
//
// WHY. 1.12.6 shipped three metric drifts in one release (enqueue_update_total
// absorbing the C1 degraded count, the five C4 refresher counters missing
// from OTLP, the four C2 relist-bridge counters missing from OTLP, the docs
// and every guard), and all three landed in the same place: a HAND-WRITTEN
// field list that mirrored a snapshot struct one field at a time. A new
// struct field is simply not copied, and nothing fails. The families that
// already flattened into a `stat -> value` map (snowplow_deps,
// snowplow_resolved_cache) never drifted, because expvar and OTLP range over
// the SAME map.
//
// This file makes the struct itself the single source: every published field
// carries a `stat:"<name>"` tag and an optional `kind:"counter|gauge"` tag
// (counter is the default), and the map every surface publishes is DERIVED
// from those tags by reflection. Adding a field without a tag is caught by
// the parity arm (TestC7_StatsByTag_EveryNumericFieldIsTaggedOrExcluded),
// which asks the TYPE for its fields — not a list in the test that would
// drift the same way. A field that is deliberately published elsewhere is
// excluded with `stat:"-"` and a comment saying where it goes.
//
// Reflection cost: a handful of fields per scrape (expvar) or per OTLP
// collection (default 60 s). Nothing on the serving path.

package cache

import (
	"fmt"
	"reflect"
	"sort"
	"strings"
)

// StatSpec describes one published stat derived from a tagged struct field.
type StatSpec struct {
	// Stat is the published key (the `stat` tag), e.g. "relist_bridge_timeout_total".
	Stat string
	// Kind is "counter" (monotonic; the default) or "gauge" (a current value).
	Kind string
	// Float is true when the field is a float (published as Float64 on OTLP).
	Float bool
	// Field is the Go field name, for error messages only.
	Field string
	// Desc is the `desc` tag: the OTLP instrument description for a
	// per-stat instrument (ignored on a shared, stat-labelled one).
	Desc string
}

// statSpecsOf lists the published stats of a tagged struct type, in
// declaration order. Panics on a malformed tag: a stats struct is package
// code and a bad tag is a programming error caught by the package's own
// tests, never a runtime condition.
func statSpecsOf(t reflect.Type) []StatSpec {
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	var out []StatSpec
	for i := 0; i < t.NumField(); i++ {
		f := t.Field(i)
		tag, ok := f.Tag.Lookup("stat")
		if !ok || tag == "-" {
			continue
		}
		if tag == "" {
			panic(fmt.Sprintf("stats_by_tag: %s.%s has an empty stat tag", t.Name(), f.Name))
		}
		kind := f.Tag.Get("kind")
		switch kind {
		case "":
			kind = "counter"
		case "counter", "gauge":
		default:
			panic(fmt.Sprintf("stats_by_tag: %s.%s has kind %q (want counter|gauge)", t.Name(), f.Name, kind))
		}
		if !isNumericKind(f.Type.Kind()) {
			panic(fmt.Sprintf("stats_by_tag: %s.%s is tagged but not numeric (%s)", t.Name(), f.Name, f.Type))
		}
		out = append(out, StatSpec{
			Stat:  tag,
			Kind:  kind,
			Float: f.Type.Kind() == reflect.Float32 || f.Type.Kind() == reflect.Float64,
			Field: f.Name,
			Desc:  f.Tag.Get("desc"),
		})
	}
	return out
}

// untaggedNumericFields returns the numeric fields of a stats struct that
// carry NO stat tag at all — neither a name nor the explicit "-" exclusion.
// The parity arm asserts this is empty for every published family: a field
// added to a snapshot struct must say where it is published, or that it is
// not.
func untaggedNumericFields(t reflect.Type) []string {
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	var out []string
	for i := 0; i < t.NumField(); i++ {
		f := t.Field(i)
		if !isNumericKind(f.Type.Kind()) {
			continue
		}
		if _, ok := f.Tag.Lookup("stat"); !ok {
			out = append(out, f.Name)
		}
	}
	return out
}

func isNumericKind(k reflect.Kind) bool {
	switch k {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64,
		reflect.Float32, reflect.Float64:
		return true
	}
	return false
}

// statsByTag flattens a tagged stats struct into stat -> value. Integer
// fields become int64, float fields stay float64 — the JSON /debug/vars
// renders both, and the OTLP mirror picks the instrument by StatSpec.Float.
// Unexported fields are readable (reflect permits reads through Int/Uint/
// Float on any field), so the refresher's package-private struct works too.
func statsByTag(s any) map[string]any {
	v := reflect.ValueOf(s)
	if v.Kind() == reflect.Ptr {
		v = v.Elem()
	}
	t := v.Type()
	out := make(map[string]any, t.NumField())
	for i := 0; i < t.NumField(); i++ {
		f := t.Field(i)
		tag, ok := f.Tag.Lookup("stat")
		if !ok || tag == "-" {
			continue
		}
		fv := v.Field(i)
		switch fv.Kind() {
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			out[tag] = fv.Int()
		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			out[tag] = int64(fv.Uint())
		case reflect.Float32, reflect.Float64:
			out[tag] = fv.Float()
		}
	}
	return out
}

// statsInt64ByTag is statsByTag for an all-integer family (a float field
// would be truncated, so the caller asserts none exist via statSpecsOf).
func statsInt64ByTag(s any) map[string]int64 {
	m := statsByTag(s)
	out := make(map[string]int64, len(m))
	for k, v := range m {
		switch x := v.(type) {
		case int64:
			out[k] = x
		case float64:
			out[k] = int64(x)
		}
	}
	return out
}

// statNames returns the sorted stat keys of a spec list (for messages).
func statNames(specs []StatSpec) []string {
	out := make([]string, 0, len(specs))
	for _, s := range specs {
		out = append(out, s.Stat)
	}
	sort.Strings(out)
	return out
}

// StatFamily names one tagged stats struct and the expvar key it publishes
// under, so cross-package arms (internal/metrics) can walk every family and
// compare the derived spec list against what actually left the process.
type StatFamily struct {
	// Expvar is the top-level /debug/vars key ("" when the family publishes
	// one top-level expvar per stat, as the refresher does).
	Expvar string
	// ExpvarPrefix is the per-stat top-level key prefix when Expvar is "".
	ExpvarPrefix string
	// OTelName is the shared OTLP instrument labelled by stat ("" when every
	// stat has an instrument of its own, as the broadcaster does).
	OTelName string
	// OTelPrefix is the per-stat OTLP instrument prefix when OTelName is "".
	OTelPrefix string
	// Desc describes the shared OTLP instrument (OTelName).
	Desc string
	// Values reads the live family as stat -> value (int64 or float64) —
	// the ONE map expvar and the OTLP mirror both publish.
	Values func() map[string]any
	// Specs are the published stats derived from the struct tags.
	Specs []StatSpec
	// Untagged are numeric fields with no stat tag — must be empty.
	Untagged []string
	// typ is the struct type, for messages.
	typ reflect.Type
}

// Name is the family's struct type name.
func (f StatFamily) Name() string { return f.typ.Name() }

// TaggedStatFamilies lists every stats family whose surfaces are derived
// from struct tags. The parity arms range over THIS list, so a new family
// registers here once and is guarded on all four surfaces from then on.
func TaggedStatFamilies() []StatFamily {
	return []StatFamily{
		{
			Expvar:   "snowplow_crd_discovery",
			OTelName: "snowplow_crd_discovery",
			Desc:     "CRD-discovery bridge counters, labelled by stat.",
			Values:   func() map[string]any { return statsByTag(CRDDiscoveryStatsSnapshot()) },
			Specs:    statSpecsOf(reflect.TypeOf(CRDDiscoveryStats{})),
			Untagged: untaggedNumericFields(reflect.TypeOf(CRDDiscoveryStats{})),
			typ:      reflect.TypeOf(CRDDiscoveryStats{}),
		},
		{
			Expvar:     "snowplow_refresh_broadcaster",
			OTelPrefix: "snowplow_refresh_broadcaster_",
			Values:     RefreshBroadcasterStatsByStat,
			Specs:      statSpecsOf(reflect.TypeOf(RefreshBroadcasterStats{})),
			Untagged:   untaggedNumericFields(reflect.TypeOf(RefreshBroadcasterStats{})),
			typ:        reflect.TypeOf(RefreshBroadcasterStats{}),
		},
		{
			ExpvarPrefix: "snowplow_refresher_",
			OTelName:     "snowplow_refresher",
			OTelPrefix:   "snowplow_refresher_",
			Desc:         "Refresher worker-pool counters, labelled by stat.",
			Values:       refresherStatsValues,
			Specs:        refresherStatSpecs(),
			Untagged: append(untaggedNumericFields(reflect.TypeOf(refresherStats{})),
				untaggedNumericFields(reflect.TypeOf(RefreshTerminalStats{}))...),
			typ: reflect.TypeOf(refresherStats{}),
		},
		// 1.12.7 — informer watch/servability failure counters. Shared
		// instrument labelled by stat, like crd_discovery.
		//
		// APPENDED, not inserted: the OTLP parity arm indexes this slice
		// POSITIONALLY (fams[0..2]), so inserting a family mid-list silently
		// re-points those assertions at the wrong struct.
		{
			Expvar:   "snowplow_informer_watch",
			OTelName: "snowplow_informer_watch",
			Desc:     "Informer watch + servability-confirmation failure counters, labelled by stat.",
			Values:   InformerWatchStatsByStat,
			Specs:    statSpecsOf(reflect.TypeOf(InformerWatchStats{})),
			Untagged: untaggedNumericFields(reflect.TypeOf(InformerWatchStats{})),
			typ:      reflect.TypeOf(InformerWatchStats{}),
		},
		// #237 A3 — per-GVR reflector-path attribution. Shared instrument
		// labelled by stat, like crd_discovery and informer_watch. The per-GVR
		// breakdown cannot ride here (a stat tag yields one scalar and this
		// system has no label facility); it is published alongside as
		// snowplow_reflector_path_by_gvr.
		//
		// APPENDED, not inserted — see the note above.
		{
			Expvar:   "snowplow_reflector_path",
			OTelName: "snowplow_reflector_path",
			Desc:     "Reflector LIST/WATCH wire-shape counters, labelled by stat.",
			Values:   ReflectorPathStatsByStat,
			Specs:    statSpecsOf(reflect.TypeOf(ReflectorPathStats{})),
			Untagged: untaggedNumericFields(reflect.TypeOf(ReflectorPathStats{})),
			typ:      reflect.TypeOf(ReflectorPathStats{}),
		},
		// #237 B — store-divergence detection. Counters ride the shared,
		// stat-labelled instrument; the GAUGES cannot (a shared observable
		// counter is monotonic by construction) so they take OTelPrefix and
		// get one instrument each. The two by-reason breakdowns ride alongside
		// as their own expvar maps, like confirm_retracted_by_reason.
		//
		// APPENDED, not inserted — see the note above.
		{
			Expvar:     "snowplow_store_verification",
			OTelName:   "snowplow_store_verification",
			OTelPrefix: "snowplow_store_verification_",
			Desc:       "Informer-store divergence detection and repair counters, labelled by stat.",
			Values:     StoreVerificationStatsByStat,
			Specs:      statSpecsOf(reflect.TypeOf(StoreVerificationStats{})),
			Untagged:   untaggedNumericFields(reflect.TypeOf(StoreVerificationStats{})),
			typ:        reflect.TypeOf(StoreVerificationStats{}),
		},
	}
}

// OTelInstrumentName is the OTLP instrument a stat is published under.
// A family with a shared instrument (OTelName) publishes its counters there
// labelled by stat; everything else gets prefix + stat, plus "_total" for a
// counter whose stat name does not already carry it. This is the ONE place
// the naming rule lives; the mirror and the parity arm both call it.

// OTelShared reports whether the stat rides the family's shared,
// stat-labelled instrument (true) or an instrument of its own (false).
func (f StatFamily) OTelShared(s StatSpec) bool {
	return f.OTelInstrumentName(s) == f.OTelName && f.OTelName != ""
}

func (f StatFamily) OTelInstrumentName(s StatSpec) string {
	if f.OTelPrefix == "" || (f.OTelName != "" && s.Kind == "counter") {
		return f.OTelName
	}
	name := f.OTelPrefix + s.Stat
	if s.Kind == "counter" && !strings.HasSuffix(s.Stat, "_total") {
		name += "_total"
	}
	return name
}

// ExpvarKey is the /debug/vars top-level key for a stat of a per-stat expvar
// family (the refresher): prefix + stat + "_total" for counters.
func (f StatFamily) ExpvarKey(s StatSpec) string {
	if f.ExpvarPrefix == "" {
		return f.Expvar
	}
	name := f.ExpvarPrefix + s.Stat
	if s.Kind == "counter" && !strings.HasSuffix(s.Stat, "_total") {
		name += "_total"
	}
	return name
}
