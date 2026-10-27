// phase1_seed_templated_endpointref_test.go — #113 C-113-3 seed-skip falsifier.
//
// A RESTAction whose api-step endpointRef.name is TEMPLATED (request-extras-driven
// endpoint selection, e.g. hub-spoke) cannot be seeded: the boot seed has no
// request extras, so the endpoint resolve misses → the stage TRUNCATES. The
// §4-nuance is the load-bearing reason to SKIP: that endpoint-resolve miss bumps
// NEITHER the stageErrSink NOR the extTouchedSink (the external bump is downstream
// of a SUCCESSFUL endpoint resolve), so declineSeedPutOnError would NOT decline —
// the seed would Put a TRUNCATED body under the no-extras key, poisoning the cell
// until TTL. The skip is what prevents that.
//
// The skip predicate is the REAL prod hasTemplatedEndpointRef over the fetched
// RESTAction CR (spec.api[].endpointRef.name), NOT a hand-evaluated predicate:
// this file drives it over real templatesv1.RESTAction shapes. It also proves the
// §4 both-sinks-zero nuance via the REAL declineSeedPutOnError — the arm that
// shows WHY the skip is needed (the GTTL-1 gate can't catch this miss class).
// Hermetic, -race, no cluster.

package dispatchers

import (
	"context"
	"os"
	"strings"
	"testing"

	templatesv1 "github.com/krateo-platformops/snowplow/apis/templates/v1"
	"github.com/krateo-platformops/snowplow/internal/cache"
)

func raWithSteps(steps ...*templatesv1.API) *templatesv1.RESTAction {
	return &templatesv1.RESTAction{Spec: templatesv1.RESTActionSpec{API: steps}}
}

func step(name string, epRef *templatesv1.Reference) *templatesv1.API {
	return &templatesv1.API{Name: name, EndpointRef: epRef}
}

// C-113-3 predicate — hasTemplatedEndpointRef fires on a TEMPLATED api-step
// endpointRef.name and NOT on literal / nil-EndpointRef / nil-step shapes. This
// is the REAL prod predicate the seed-skip consumes.
func TestC113_3_HasTemplatedEndpointRef_FiresOnTemplateOnly(t *testing.T) {
	cases := []struct {
		name string
		ra   *templatesv1.RESTAction
		want bool
	}{
		{
			name: "templated endpointRef.name → skip",
			ra:   raWithSteps(step("s1", &templatesv1.Reference{Name: `${ .name + "-endpoint" }`, Namespace: "krateo-system"})),
			want: true,
		},
		{
			name: "templated in a LATER step (not just the first) → skip",
			ra: raWithSteps(
				step("s1", &templatesv1.Reference{Name: "static-endpoint", Namespace: "krateo-system"}),
				step("s2", &templatesv1.Reference{Name: `${ .want }`, Namespace: "krateo-system"}),
			),
			want: true,
		},
		{
			name: "literal endpointRef.name → do NOT skip (byte-identical to pre-#113)",
			ra:   raWithSteps(step("s1", &templatesv1.Reference{Name: "spoke-a-endpoint", Namespace: "krateo-system"})),
			want: false,
		},
		{
			name: "literal `-clientconfig` endpointRef.name → do NOT skip (author literal, not request-driven)",
			ra:   raWithSteps(step("s1", &templatesv1.Reference{Name: "alice-clientconfig", Namespace: "krateo-system"})),
			want: false,
		},
		{
			name: "nil EndpointRef (internal path) → do NOT skip",
			ra:   raWithSteps(step("s1", nil)),
			want: false,
		},
		{
			name: "nil api-step element → do NOT skip (nil-guard)",
			ra:   raWithSteps(nil, step("s2", &templatesv1.Reference{Name: "static", Namespace: "krateo-system"})),
			want: false,
		},
		{
			name: "no api steps → do NOT skip",
			ra:   raWithSteps(),
			want: false,
		},
		{
			name: "nil RESTAction → do NOT skip",
			ra:   nil,
			want: false,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := hasTemplatedEndpointRef(tc.ra); got != tc.want {
				t.Fatalf("hasTemplatedEndpointRef = %v; want %v", got, tc.want)
			}
		})
	}
}

// C-113-3 §4 NUANCE (the load-bearing reason to skip, not noise) — an
// endpoint-resolve MISS on a templated-endpointRef seed bumps NEITHER sink, so
// declineSeedPutOnError does NOT decline; the seed would Put a truncated body.
// This arm proves the GTTL-1 gate CANNOT catch this miss class, which is exactly
// why the SKIP (not the decline gate) must own it. Drives the REAL
// declineSeedPutOnError with both sinks at their post-miss state (Count()==0).
func TestC113_3_EndpointMiss_NeitherSinkBumps_DeclineGateCannotCatch(t *testing.T) {
	engineLatchTestMu.Lock()
	defer engineLatchTestMu.Unlock()

	ctx := context.Background()
	// Model the seed's post-endpoint-miss sink state: an endpoint resolve failure
	// (stageReturn at resolve.go) is UPSTREAM of both the item-level httpcall error
	// bump (stageErrSink) and the external-touch bump (extTouchedSink), so BOTH
	// sinks read Count()==0 after such a miss.
	_, stageSink := cache.WithStageErrorSink(ctx)
	_, extSink := cache.WithExternalTouchedSink(ctx)
	if stageSink.Count() != 0 || extSink.Count() != 0 {
		t.Fatalf("setup: fresh sinks must read 0/0; got %d/%d", stageSink.Count(), extSink.Count())
	}

	// The REAL GTTL-1 gate: with BOTH sinks at 0, it does NOT decline → the
	// truncated body WOULD be Put. This is the poisoning the seed-skip prevents.
	if declineSeedPutOnError(ctx, "restactions", "krateo/hub-spoke-ra", "key/hub-spoke", cache.PutGateVerdict(cache.PutGateSinks{Stage: stageSink, Forbidden: nil, External: extSink}, "restactions"), stageSink, extSink) {
		t.Fatal("C-113-3 §4 nuance BROKEN: declineSeedPutOnError declined with BOTH sinks at 0 — if the gate DID catch the templated-endpointRef miss, the seed-skip would be redundant; the whole point is that it does NOT, so the skip is load-bearing")
	}

	// Control (discriminator): the gate DOES decline once a sink is bumped — proving
	// the gate's verdict is real, just blind to the endpoint-miss class above.
	stageSink.Bump("someStage", "an actual swallowed stage error")
	if !declineSeedPutOnError(ctx, "restactions", "krateo/hub-spoke-ra", "key/hub-spoke", cache.PutGateVerdict(cache.PutGateSinks{Stage: stageSink, Forbidden: nil, External: extSink}, "restactions"), stageSink, extSink) {
		t.Fatal("control: a bumped stage sink MUST decline")
	}
}

// C-113-3 wiring guard (source-level) — the seed-skip is wired into
// seedOneRestaction BEFORE restactions.Resolve, keyed on the REAL predicate.
// Guards against a future edit dropping the skip (which silently re-opens the
// truncated-Put poisoning). Mirrors TestSeedPrimitives_InstallErrorSinks.
func TestC113_3_SeedSkipWiredBeforeResolve(t *testing.T) {
	src, err := os.ReadFile("phase1_pip_seed.go")
	if err != nil {
		t.Fatalf("read phase1_pip_seed.go: %v", err)
	}
	s := string(src)
	for _, want := range []string{
		// The skip routes through the hasTemplatedEndpointRefFn seam (prod
		// default = hasTemplatedEndpointRef), so the falsifier can neuter the
		// predicate. The wiring guard tracks the seam call site.
		"if hasTemplatedEndpointRefFn(&cr) {",
		"phase1.seed.skip.templated_endpointref",
	} {
		if !strings.Contains(s, want) {
			t.Fatalf("#113 seed-skip wiring: expected %q in seedOneRestaction before restactions.Resolve; not found", want)
		}
	}
	// The skip MUST precede restactions.Resolve (else it can't prevent the Put).
	skipIdx := strings.Index(s, "if hasTemplatedEndpointRefFn(&cr) {")
	resolveIdx := strings.Index(s, "res, err := restactions.Resolve(resCtx, restactions.ResolveOptions{")
	if skipIdx < 0 || resolveIdx < 0 || skipIdx > resolveIdx {
		t.Fatalf("#113 seed-skip must appear BEFORE restactions.Resolve (skipIdx=%d resolveIdx=%d)", skipIdx, resolveIdx)
	}
}
