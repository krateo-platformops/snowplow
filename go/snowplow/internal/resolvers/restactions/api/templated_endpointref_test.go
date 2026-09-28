// templated_endpointref_test.go — #113 templated endpointRef falsifiers.
//
// #113 lets a RESTAction api-step select its dispatch endpoint Secret from
// REQUEST extras by templating endpointRef.name through the SAME jq/extras
// evaluator that already renders path/payload/headers. extras are user-controlled
// query params, so an UNMITIGATED `${.name}` lets a caller select another user's
// `<user>-clientconfig` Secret and dial the spoke step with THAT user's apiserver
// credentials — a credential-selection escalation. The design closes it with two
// guardrails, both enforced at TWO layers:
//   (a) V1 templates the NAME only; namespace stays the author-literal.
//   (b) a templated ref may never resolve to the reserved `-clientconfig` suffix.
//
// This file is the GATE-BLOCKING security proof (C-113-1) plus the request-
// templated-marker default (C-113-2), namespace-literal (C-113-5), and static-ref
// regression (C-113-6) arms. The seed-skip (C-113-3) lives in the dispatchers
// package (phase1_seed_templated_endpointref_test.go). Hermetic, -race, no cluster.
//
// The C-113-1 "never DIALED" proof is STRONG: the secrets snapshot is seeded with
// a forge-target `admin-clientconfig` carrying a SENTINEL server-url. If the guard
// is bypassed, resolveOne HITS the snapshot and RETURNS that admin ServerURL —
// the escalation SUCCEEDS, observable as a non-empty sentinel URL. With the guard
// present the sentinel is NEVER returned (the lookup never fires). "Refused" is
// proven by the admin credential never being SELECTED, not merely by an error.

package api

import (
	"context"
	"strings"
	"testing"

	"github.com/krateo-platformops/plumbing/endpoints"
	templates "github.com/krateo-platformops/snowplow/apis/templates/v1"
	"github.com/krateo-platformops/snowplow/internal/cache"
	corev1 "k8s.io/api/core/v1"
)

const forgedAdminServerURL = "https://ADMIN-CREDS-FORGED.invalid"

// seedForgeableAdminClientconfig makes the secrets cache servable in authnNS with
// an `admin-clientconfig` Secret carrying a SENTINEL server-url. If a lookup for
// `admin-clientconfig` ever HITS (guard bypassed), FromInformerSecret returns the
// sentinel ServerURL — the observable escalation.
func seedForgeableAdminClientconfig(t *testing.T, authnNS string) {
	t.Helper()
	t.Setenv("CACHE_ENABLED", "true")
	cache.ResetSecretsInformerForTest()
	cache.ResetSecretsSnapshotForTest()
	cache.PublishSecretsSnapshotForTest(&cache.SecretsSnapshot{
		ByName: map[string]*corev1.Secret{
			"admin-clientconfig": mkSecret(authnNS, "admin-clientconfig", map[string]string{
				"server-url": forgedAdminServerURL,
			}),
		},
	})
	cache.ForceSecretsCacheReadyForTest(authnNS)
	t.Cleanup(func() {
		cache.ResetSecretsSnapshotForTest()
		cache.ResetSecretsInformerForTest()
	})
}

// ─────────────────────────────────────────────────────────────────────────
// C-113-1 layer (b) — resolveOne's defense-in-depth reserved-suffix refusal.
// A REQUEST-TEMPLATED ref resolving to `admin-clientconfig` is refused at the
// choke point EVERY ref lookup passes, so a FUTURE templating caller that forgets
// the eval-site check still cannot dial a per-user credential Secret.
func TestT1_ClientconfigForgery_RefusedInResolveOne_NeverDials(t *testing.T) {
	authnNS := "krateo-system"
	seedForgeableAdminClientconfig(t, authnNS)

	m := &endpointReferenceMapper{authnNS: authnNS, username: "victim", rc: nil}
	forged := &templates.Reference{Name: "admin-clientconfig", Namespace: authnNS}

	// GREEN: templated=true → refused BEFORE the snapshot lookup. The sentinel
	// admin ServerURL must NEVER be returned.
	ep, _, err := m.resolveOne(context.Background(), forged, true /*templated*/)
	if err == nil {
		t.Fatal("C-113-1 layer(b) VIOLATED: resolveOne accepted a templated ref resolving to admin-clientconfig — a request-driven endpointRef selected a per-user credential Secret (escalation)")
	}
	if ep.ServerURL == forgedAdminServerURL {
		t.Fatalf("C-113-1 layer(b) ESCALATION: resolveOne DIALED the admin credential (returned the sentinel ServerURL %q) despite the guard", forgedAdminServerURL)
	}
	if !strings.Contains(err.Error(), "guardrail b") {
		t.Fatalf("C-113-1 layer(b): expected the reserved-suffix guardrail error; got %v", err)
	}

	// RED arm (the escalation the guard prevents): the IDENTICAL forged ref with
	// templated=FALSE is NOT gated by layer (b) — it flows to the snapshot lookup
	// and HITS the seeded admin-clientconfig, returning the sentinel ServerURL.
	// This is EXACTLY what a request-templated ref must never be able to reach;
	// it proves the guard's verdict flips SOLELY on the templated marker (if the
	// marker didn't gate, the templated call above would have returned this too).
	epRed, _, errRed := m.resolveOne(context.Background(), forged, false /*NOT templated → the pre-guard path*/)
	if errRed != nil || epRed.ServerURL != forgedAdminServerURL {
		t.Fatalf("C-113-1 RED-control broke: an UNGATED (templated=false) lookup of admin-clientconfig must HIT the seeded sentinel (proving the forge target is reachable absent the marker); got ep=%+v err=%v", epRed, errRed)
	}
}

// ─────────────────────────────────────────────────────────────────────────
// C-113-1 layer (a) — the eval site (resolveStageEndpoint→evalEndpointRef)
// refuses FAST, before resolveOne is ever called, so the Secret lookup never
// fires. Drives the REAL evalEndpointRef with a template that folds a request
// extra into a `-clientconfig` name.
func TestT1_ClientconfigForgery_RefusedAtEvalSite_NeverReachesResolveOne(t *testing.T) {
	authnNS := "krateo-system"
	seedForgeableAdminClientconfig(t, authnNS)

	// r.dict carries the request extras at top level (exactly as path sees them):
	// extras.want = "admin" here. The template folds it into `admin-clientconfig`.
	r := &resolveRun{
		dict: map[string]any{"want": "admin"},
	}
	forgedTemplate := &templates.Reference{Name: `${ .want + "-clientconfig" }`, Namespace: authnNS}

	ref, templated, guardErr := r.evalEndpointRef(forgedTemplate)
	if guardErr == nil {
		t.Fatal("C-113-1 layer(a) VIOLATED: evalEndpointRef did NOT refuse a templated name resolving to admin-clientconfig — the escalation reaches resolveOne/the Secret lookup")
	}
	if ref != nil {
		t.Fatalf("C-113-1 layer(a): a refused template must return a nil ref (never handed to resolveOne); got %+v", ref)
	}
	if !templated {
		t.Fatal("C-113-1 layer(a): a refused template must still report templated=true")
	}
	if !strings.Contains(guardErr.Error(), "guardrail b") {
		t.Fatalf("C-113-1 layer(a): expected the reserved-suffix guardrail error; got %v", guardErr)
	}

	// Discriminating: the SAME derivation without the `-clientconfig` fold
	// resolves cleanly (proves the refusal keys on the reserved suffix, not on
	// templating per se).
	okTemplate := &templates.Reference{Name: `${ .want + "-endpoint" }`, Namespace: authnNS}
	okRef, okTemplated, okErr := r.evalEndpointRef(okTemplate)
	if okErr != nil {
		t.Fatalf("C-113-1 layer(a) control: a non-clientconfig templated name must NOT be refused; got %v", okErr)
	}
	if okRef == nil || okRef.Name != "admin-endpoint" || !okTemplated {
		t.Fatalf("C-113-1 layer(a) control: expected resolved name admin-endpoint (templated); got %+v templated=%v", okRef, okTemplated)
	}
	if okRef.Namespace != authnNS {
		t.Fatalf("C-113-5: namespace must stay the author-literal %q; got %q", authnNS, okRef.Namespace)
	}
}

// ─────────────────────────────────────────────────────────────────────────
// C-113-2 — the request-templated marker DEFAULT. resolveOne's OWN internal
// nil-ref→`<user>-clientconfig` synthesis must NEVER be refused (templated=false),
// while a request-templated `-clientconfig` ref IS. Two RED directions.
func TestC113_2_MarkerDefault_InternalSynthesisResolves_TemplatedRefused(t *testing.T) {
	authnNS := "krateo-system"
	t.Setenv("CACHE_ENABLED", "true")
	cache.ResetSecretsInformerForTest()
	cache.ResetSecretsSnapshotForTest()
	// Seed the VICTIM's own clientconfig so the internal nil-ref synthesis has a
	// real cell to resolve (the legitimate path).
	cache.PublishSecretsSnapshotForTest(&cache.SecretsSnapshot{
		ByName: map[string]*corev1.Secret{
			"victim-clientconfig": mkSecret(authnNS, "victim-clientconfig", map[string]string{
				"server-url": "https://victim.example.com",
			}),
		},
	})
	cache.ForceSecretsCacheReadyForTest(authnNS)
	t.Cleanup(func() {
		cache.ResetSecretsSnapshotForTest()
		cache.ResetSecretsInformerForTest()
	})

	m := &endpointReferenceMapper{authnNS: authnNS, username: "victim", rc: nil}

	// (i) DEFAULT direction — the internal nil-ref path synthesizes
	// `victim-clientconfig` and MUST still resolve when UNMARKED (templated=false,
	// the default every internal/static caller passes). The internal path resolved
	// SOMETHING (no guardrail refusal); its ServerURL is the internal-dispatch
	// override (kubernetes.default.svc) OR the seeded victim URL under TestMode.
	ep, isSA, err := m.resolveOne(context.Background(), nil /*internal nil-ref*/, false)
	if err != nil {
		t.Fatalf("C-113-2 (i) VIOLATED: the internal nil-ref clientconfig synthesis was refused when UNMARKED (templated=false); the internal path must never be gated by guardrail (b); got %v", err)
	}
	if ep.ServerURL == "" {
		t.Fatalf("C-113-2 (i): internal synthesis resolved to an empty endpoint; got ep=%+v", ep)
	}
	// #267 provenance: this nil-ref path consulted the ctx-carried internal endpoint
	// (the SA endpoint) → resolveOne must report isSAEndpoint=true so the dial site
	// threads the SA token file. (Here there is no internal endpoint on ctx, so it
	// fell through to the clientconfig synthesis → isSA=false; assert that shape.)
	if isSA {
		t.Fatalf("C-113-2 (i): a clientconfig-synthesis (no internal endpoint on ctx) must report isSAEndpoint=false; got true")
	}

	// #267 provenance TRUE side (symmetric with the false assertion above, the
	// load-bearing mechanism): a nil ref WITH an internal (SA) endpoint on ctx MUST
	// report isSAEndpoint=true — this is what makes runStage stamp
	// WithServiceAccountDial so the dial threads the self-reloading SA token FILE.
	saCtx := cache.WithInternalEndpoint(context.Background(),
		&endpoints.Endpoint{ServerURL: "https://kubernetes.default.svc", Token: "sa-token"})
	saResolved, saIsSA, saErr := m.resolveOne(saCtx, nil /*internal nil-ref*/, false)
	if saErr != nil {
		t.Fatalf("C-113-2 provenance: resolveOne with an internal endpoint on ctx must resolve; got %v", saErr)
	}
	if !saIsSA {
		t.Fatalf("#267 provenance: resolveOne consulting the ctx-carried internal (SA) endpoint MUST report isSAEndpoint=true; got false — the dial-site SA token-file wiring would be silently disabled")
	}
	if saResolved.ServerURL != "https://kubernetes.default.svc" {
		t.Fatalf("C-113-2 provenance: the internal-endpoint branch must return the SA endpoint verbatim; got %+v", saResolved)
	}

	// (i)-discriminator — the marker is LOAD-BEARING: pass the SAME kind of
	// `-clientconfig` name as a REQUEST-TEMPLATED ref (templated=true) and it IS
	// refused. This is why the marker MUST default false: if the internal path
	// inherited templated=true, this very refusal would break its own synthesis.
	// (The internal synthesis itself is guard-safe because its ref is nil at the
	// guard, but the marker's default is what keeps a `-clientconfig`-shaped name
	// resolvable for the internal path and refused for a request-driven one.)
	_, _, markedErr := m.resolveOne(context.Background(), &templates.Reference{Name: "victim-clientconfig", Namespace: authnNS}, true /*templated*/)
	if markedErr == nil {
		t.Fatal("C-113-2 (i)-discriminator: a templated `-clientconfig` ref must be refused — proving the templated marker is what gates guardrail (b), so the internal path MUST pass false")
	}

	// (ii) TEMPLATED direction — a request-templated ref to ANY `-clientconfig`
	// name is refused. (Uses victim's own name to show even self-selection via a
	// template is refused: the boundary is the reserved suffix, not cross-user.)
	forged := &templates.Reference{Name: "victim-clientconfig", Namespace: authnNS}
	_, _, terr := m.resolveOne(context.Background(), forged, true /*templated*/)
	if terr == nil {
		t.Fatal("C-113-2 (ii) VIOLATED: a request-templated `-clientconfig` ref was NOT refused")
	}
	if !strings.Contains(terr.Error(), "guardrail b") {
		t.Fatalf("C-113-2 (ii): expected the guardrail error; got %v", terr)
	}
}

// ─────────────────────────────────────────────────────────────────────────
// C-113-5 — ref.Namespace is NEVER templated in V1 (literal only). A template in
// the NAME renders; the namespace passes through byte-identically. RED (hypothetical
// namespace templating) = a `${...}` namespace would let extras redirect the
// Secret lookup cross-namespace, reachable clientconfig-forgery across namespaces.
func TestC113_5_NamespaceNeverTemplated(t *testing.T) {
	r := &resolveRun{dict: map[string]any{"want": "spoke-a"}}
	// A templated NAME + a namespace that LOOKS like a template: the namespace
	// must pass through as the literal string, NEVER evaluated.
	ref := &templates.Reference{Name: `${ .want + "-endpoint" }`, Namespace: `${ .want }`}
	out, templated, err := r.evalEndpointRef(ref)
	if err != nil {
		t.Fatalf("unexpected guard error: %v", err)
	}
	if !templated || out == nil {
		t.Fatalf("expected a templated resolved ref; got %+v templated=%v", out, templated)
	}
	if out.Name != "spoke-a-endpoint" {
		t.Fatalf("name must render: got %q want spoke-a-endpoint", out.Name)
	}
	// The load-bearing assert: the namespace is the UNEVALUATED literal — extras
	// did NOT rewrite it to "spoke-a".
	if out.Namespace != `${ .want }` {
		t.Fatalf("C-113-5 VIOLATED: namespace was templated/evaluated (got %q) — V1 keeps namespace literal to bound the credential-store trust boundary", out.Namespace)
	}
}

// ─────────────────────────────────────────────────────────────────────────
// C-113-6 — a STATIC (non-template) endpointRef.name is byte-identical to
// pre-#113: the MaybeQuery gate skips evaluation, templated=false, NOT gated —
// INCLUDING a literal `-clientconfig` internal ref (only TEMPLATED refs are
// gated; a chart author writing a literal is the internal path's own business).
func TestC113_6_StaticRefUnchanged_LiteralClientconfigNotRefused(t *testing.T) {
	r := &resolveRun{dict: map[string]any{"want": "spoke-a"}}

	// A plain literal name: passes through unchanged, NOT templated.
	lit := &templates.Reference{Name: "spoke-a-endpoint", Namespace: "krateo-system"}
	out, templated, err := r.evalEndpointRef(lit)
	if err != nil || templated || out != lit {
		t.Fatalf("C-113-6: a literal name must pass through UNCHANGED, not templated, same pointer; got out=%+v templated=%v err=%v (want same ref, false, nil)", out, templated, err)
	}

	// A literal `-clientconfig` name (author-written, not request-driven) is NOT
	// refused at the eval site — only templated refs are gated. It returns
	// templated=false so resolveOne's layer (b) (gated on templated) also lets it
	// through, preserving the internal path byte-for-byte.
	litCC := &templates.Reference{Name: "some-clientconfig", Namespace: "krateo-system"}
	outCC, templatedCC, errCC := r.evalEndpointRef(litCC)
	if errCC != nil {
		t.Fatalf("C-113-6 VIOLATED: a LITERAL `-clientconfig` name was refused at the eval site — only TEMPLATED refs must be gated; got %v", errCC)
	}
	if templatedCC || outCC != litCC {
		t.Fatalf("C-113-6: a literal `-clientconfig` ref must pass through unchanged (templated=false); got out=%+v templated=%v", outCC, templatedCC)
	}

	// And a nil ref (internal path) is a clean pass-through, not templated.
	outNil, templatedNil, errNil := r.evalEndpointRef(nil)
	if errNil != nil || templatedNil || outNil != nil {
		t.Fatalf("C-113-6: a nil ref must pass through as (nil,false,nil); got out=%+v templated=%v err=%v", outNil, templatedNil, errNil)
	}
}

// compile-time assertion that endpoints is used (endpoints.Endpoint referenced in
// the resolveOne signature exercised above).
var _ = endpoints.Endpoint{}
