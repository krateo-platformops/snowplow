package api

// #1116 (SECURITY) — extras that name a key the resolver writes (a stage name,
// an errorKey, slice) are refused before anything is read or dialled.
//
// The attack (RUN on main a4ad8ecd before the fix): tenant A asks for tenant
// B's ticket and sends extras.stage1 = a fake stage-1 output carrying B's id.
// Stage 1 (the caller's read) is Forbidden and never overwrites the key, so
// stage 2 builds its path from the fake and dials the external endpoint with
// the snowplow-held endpointRef credential for B's ticket.

import (
	"net/http"
	"reflect"
	"strings"
	"testing"

	"github.com/krateo-platformops/plumbing/ptr"
	templates "github.com/krateo-platformops/snowplow/apis/templates/v1"
)

var rxFakeList = map[string]any{"items": []any{map[string]any{"status": map[string]any{"id": float64(2002)}}}}

// rxAttackStages is the attack's RESTAction: stage 1 lists the ticket as the
// caller, stage 2 dials the endpointRef with stage 1's id.
func rxAttackStages(coe bool) []*templates.API {
	return []*templates.API{rxStage1List(coe), rxStage2(rxIDList)}
}

// rxNoDials fails unless the resolve read and dialled nothing: no apiserver
// request as the caller, no ServiceAccount request, no external request.
func rxNoDials(t *testing.T, label string, o rxOut) {
	t.Helper()
	if len(o.kube)+len(o.sa)+len(o.zd) != 0 {
		t.Errorf("%s: the refused resolve dialled: kube=%s sa=%s external=%s", label, rxWho(o.kube), rxWho(o.sa), rxWho(o.zd))
	}
	if len(o.dict) != 0 {
		t.Errorf("%s: the refused resolve produced a dict: %s", label, rxShort(o.dictS, 300))
	}
	if !strings.Contains(o.header, `"reason":"Aborted"`) || strings.Contains(o.header, `"ok":true`) {
		t.Errorf("%s: outcomes must read Aborted with every stage NotRun, got %s", label, o.header)
	}
}

// TestReservedExtras1116_AttackRefusedOnEveryArm — the attack makes 0 dials
// and produces nothing on all four dispatch arms, for a LIST and a GET stage
// 1, a denied and an allowed stage 1, a user with no grant, and stage 1 with
// and without continueOnError.
func TestReservedExtras1116_AttackRefusedOnEveryArm(t *testing.T) {
	e := newRxEnv(t)
	fakeGet := map[string]any{"status": map[string]any{"id": float64(2002)}}
	type c struct {
		label, user, ns, ticket, shape string
		coe                            bool
	}
	cases := []c{
		{"A asks B's (denied) LIST", rxUA, "team-b", "t-b1", "list", true},
		{"A asks B's (denied) LIST, continueOnError=false", rxUA, "team-b", "t-b1", "list", false},
		{"A own (allowed) LIST", rxUA, "team-a", "t-a1", "list", true},
		{"no-grant user LIST", rxUN, "team-a", "t-a1", "list", true},
		{"A asks B's (denied) GET", rxUA, "team-b", "tk-b1", "get", true},
	}
	for _, arm := range rxArms {
		for _, cs := range cases {
			s1, idx, fake := rxStage1List(cs.coe), rxIDList, any(rxFakeList)
			if cs.shape == "get" {
				s1, idx, fake = rxStage1Get(cs.coe), rxIDGet, fakeGet
			}
			ex := map[string]any{"namespace": cs.ns, "ticket": cs.ticket, "stage1": fake}
			o := e.resolve(arm, cs.user, true, ex, s1, rxStage2(idx))
			rxNoDials(t, string(arm)+" "+cs.label, o)
		}
	}
}

// TestReservedExtras1116_EveryReservedKeyRefused — a later stage's name, an
// explicit errorKey, the default errorKey of a stage that declares none, and
// slice (with and without pagination) are each refused with 0 dials.
func TestReservedExtras1116_EveryReservedKeyRefused(t *testing.T) {
	e := newRxEnv(t)
	noErrKey := rxStage2(rxIDList)
	noErrKey.ErrorKey = nil
	for _, arm := range rxArms {
		for _, cs := range []struct {
			label  string
			key    string
			stages []*templates.API
		}{
			{"a later stage's name", "stage2", rxAttackStages(true)},
			{"an explicit errorKey", "stage1Err", rxAttackStages(true)},
			{"the default errorKey", "error", []*templates.API{rxStage1List(true), noErrKey}},
			{"slice", "slice", rxAttackStages(true)},
		} {
			ex := map[string]any{"namespace": "team-a", "ticket": "t-a1", cs.key: map[string]any{"forged": true}}
			o := e.resolve(arm, rxUA, true, ex, cs.stages...)
			rxNoDials(t, string(arm)+" "+cs.label, o)
		}
	}
}

// TestReservedExtras1116_LegitimateExtrasStillResolve — extras that name no
// reserved key resolve exactly as before: A's own ticket dials the endpoint
// for A's id, B's ticket is denied to A and stage 2 dials nothing. "extras"
// and "error" are not reserved when no stage writes them (every stage here
// declares its errorKey).
func TestReservedExtras1116_LegitimateExtrasStillResolve(t *testing.T) {
	e := newRxEnv(t)
	for _, arm := range []rxArm{rxContent, rxOff} {
		o := e.resolve(arm, rxUA, true, map[string]any{"namespace": "team-a", "ticket": "t-a1",
			"extras": "x", "error": "x", "q": "x"}, rxAttackStages(true)...)
		if len(o.zd) != 1 || !strings.Contains(o.zd[0].uri, "/1001.json") {
			t.Errorf("%s: A's own ticket must dial the endpoint for 1001 once, got %s", arm, rxWho(o.zd))
		}
		o = e.resolve(arm, rxUA, true, map[string]any{"namespace": "team-b", "ticket": "t-b1"}, rxAttackStages(true)...)
		if len(o.zd) != 0 {
			t.Errorf("%s: B's ticket is denied to A; stage 2 must dial nothing, got %s", arm, rxWho(o.zd))
		}
	}
}

// TestReservedExtras1116_ReservedDictKeys — the reserved set is exactly the
// keys the resolver writes: stage names, effective errorKeys, slice.
func TestReservedExtras1116_ReservedDictKeys(t *testing.T) {
	items := []*templates.API{
		{Name: "a", ErrorKey: ptr.To("aErr")},
		{Name: "b"},                        // no errorKey: writes "error"
		{Name: "c", ErrorKey: ptr.To("b")}, // an errorKey equal to a stage name: the stage owns it
	}
	got := ReservedDictKeys(items)
	want := map[string]ReservedKey{
		"a":     {Key: "a", Owner: ReservedByStage, Stage: "a"},
		"b":     {Key: "b", Owner: ReservedByStage, Stage: "b"},
		"c":     {Key: "c", Owner: ReservedByStage, Stage: "c"},
		"aErr":  {Key: "aErr", Owner: ReservedByErrorKey, Stage: "a"},
		"error": {Key: "error", Owner: ReservedByErrorKey, Stage: "b"},
		"slice": {Key: "slice", Owner: ReservedBySlice},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("ReservedDictKeys:\n got  %+v\n want %+v", got, want)
	}

	// Every offending key is named, sorted; a clean request is nil.
	err := CheckReservedExtras(items, map[string]any{"slice": 1, "a": 1, "error": 1, "namespace": "x"})
	re, ok := AsReservedExtrasError(err)
	if !ok {
		t.Fatalf("CheckReservedExtras: want *ReservedExtrasError, got %v", err)
	}
	var keys []string
	for _, k := range re.Keys {
		keys = append(keys, k.Key)
	}
	if strings.Join(keys, ",") != "a,error,slice" {
		t.Errorf("refused keys = %v, want [a error slice]", keys)
	}
	if err := CheckReservedExtras(items, map[string]any{"namespace": "x", "extras": 1, "aerr": 1}); err != nil {
		t.Errorf("non-reserved extras refused: %v", err)
	}
	// The match is exact (#1127 review R7): a key that differs by whitespace,
	// case or a prefix shadows nothing in the dict (jq .k is an exact lookup),
	// so it is accepted.
	for _, k := range []string{" a", "a ", "A", "aa", "aErr ", " slice", "Slice", "error\t"} {
		if err := CheckReservedExtras(items, map[string]any{k: 1}); err != nil {
			t.Errorf("key %q is not a reserved key, but was refused: %v", k, err)
		}
	}
	// "error" is reserved only when a stage writes it.
	if err := CheckReservedExtras(items[:1], map[string]any{"error": 1}); err != nil {
		t.Errorf("\"error\" refused although every stage declares its errorKey: %v", err)
	}
}

// TestReservedExtras1116_PortalCorpusShapes — the stage names of the portal
// RESTActions measured exploitable (#1116 corpus scan, portal 10e359e) are
// reserved: the seeds that reached a victim row are refused.
func TestReservedExtras1116_PortalCorpusShapes(t *testing.T) {
	uaf := &templates.UserAccessFilterSpec{Verb: "list", Group: "composition.krateo.io", ResourcesFrom: "[ (.crds // [])[] | .plural ]"}
	for _, cs := range []struct {
		ra     string
		stages []*templates.API
		seed   string
	}{
		{"compositions-list", []*templates.API{{Name: "crds", ErrorKey: ptr.To("crdsError")},
			{Name: "allCompositions", ErrorKey: ptr.To("allCompositionsError"), UserAccessFilter: uaf}}, "crds"},
		{"composition-detail", []*templates.API{{Name: "crds"}, {Name: "found", UserAccessFilter: uaf}}, "crds"},
		{"blueprint-detail", []*templates.API{{Name: "compdef"}, {Name: "catalog", ErrorKey: ptr.To("catalogError")},
			{Name: "comps", ErrorKey: ptr.To("compsError"), UserAccessFilter: uaf}}, "compdef"},
		{"composition-events", []*templates.API{{Name: "crds"}, {Name: "found"}, {Name: "getEvents",
			EndpointRef: &templates.Reference{Name: "sse-proxy-internal-endpoint", Namespace: "krateo-system"}}}, "found"},
		{"upgrade-impact", []*templates.API{{Name: "compdef"}, {Name: "impact", Verb: ptr.To(http.MethodPost), ErrorKey: ptr.To("impactError"),
			EndpointRef: &templates.Reference{Name: "blueprint-render-endpoint", Namespace: "krateo-system"}}}, "compdef"},
	} {
		if _, ok := AsReservedExtrasError(CheckReservedExtras(cs.stages, map[string]any{cs.seed: []any{}})); !ok {
			t.Errorf("%s: a seeded %q is not refused", cs.ra, cs.seed)
		}
	}
}

// TestReservedExtras1116_RefusalIsCounted — every refused key is counted by
// owner in snowplow_extras_reserved_key_refused_total.
func TestReservedExtras1116_RefusalIsCounted(t *testing.T) {
	before := ReservedExtrasRefusedSnapshot()
	items := []*templates.API{{Name: "s"}}
	ctx := ctxWithUser(rxUA, rxGrp)
	if err := RefuseReservedExtras(ctx, items, map[string]any{"s": 1, "error": 1, "slice": 1}, "ns", "ra"); err == nil {
		t.Fatal("RefuseReservedExtras: want a refusal")
	}
	after := ReservedExtrasRefusedSnapshot()
	for _, owner := range []string{ReservedByStage, ReservedByErrorKey, ReservedBySlice} {
		if after[owner]-before[owner] != 1 {
			t.Errorf("owner %s counted %d, want 1", owner, after[owner]-before[owner])
		}
	}
	if err := RefuseReservedExtras(ctx, items, map[string]any{"x": 1}, "ns", "ra"); err != nil {
		t.Errorf("clean extras refused: %v", err)
	}
}
