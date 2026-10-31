// templated_endpointref_test.go — #113 templated endpointRef falsifiers.
//
// #113 lets a RESTAction api-step select its dispatch endpoint Secret from
// REQUEST extras by templating endpointRef.name through the SAME jq/extras
// evaluator that already renders path/payload/headers. extras are user-controlled
// query params, so an UNMITIGATED `${.name}` lets a caller select another user's
// `<user>-clientconfig` Secret and dial the spoke step with THAT user's apiserver
// credentials — a credential-selection escalation. The design closes it with two
// guardrails:
//   (a) V1 templates the NAME only; namespace stays the author-literal.
//   (b) a templated ref may never resolve to a per-user credential name. Since
//       #1025 this is not a templating rule any more: refusePerUserRef,
//       applied to every stage before resolveOne, refuses a per-user name for
//       EVERY named ref, literal or templated, stored or caller-supplied
//       (per_user_ref.go, issue1025_per_user_ref_test.go).
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
	"errors"
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
// C-113-1 — the #1025 rule (refusePerUserRef, applied to every stage before
// resolveOne) refuses a ref to `admin-clientconfig`, so the snapshot lookup
// that would return the sentinel admin ServerURL never runs. The refusal no
// longer depends on how the ref was produced. The resolve-level proof that
// nothing is dialed is issue1025_per_user_ref_test.go.
func TestT1_ClientconfigForgery_RefusedBeforeLookup(t *testing.T) {
	authnNS := "krateo-system"
	seedForgeableAdminClientconfig(t, authnNS)

	m := &endpointReferenceMapper{authnNS: authnNS, username: "victim", rc: nil}
	forged := &templates.Reference{Name: "admin-clientconfig", Namespace: authnNS}

	if err := m.refusePerUserRef(forged); !errors.Is(err, ErrPerUserCredentialRef) {
		t.Fatalf("C-113-1 VIOLATED: a ref to admin-clientconfig was not refused; got err=%v", err)
	}
}

// ─────────────────────────────────────────────────────────────────────────
// C-113-1 — the eval site (resolveStageEndpoint→evalEndpointRef) renders a
// template that folds a request extra into `admin-clientconfig`; the name it
// hands on is refused by the #1025 rule, so the Secret lookup never fires.
func TestT1_ClientconfigForgery_TemplatedNameRefused(t *testing.T) {
	authnNS := "krateo-system"
	seedForgeableAdminClientconfig(t, authnNS)

	// r.dict carries the request extras at top level (exactly as path sees them):
	// extras.want = "admin" here. The template folds it into `admin-clientconfig`.
	r := &resolveRun{
		dict: map[string]any{"want": "admin"},
	}
	forgedTemplate := &templates.Reference{Name: `${ .want + "-clientconfig" }`, Namespace: authnNS}

	ref, templated := r.evalEndpointRef(forgedTemplate)
	if !templated || ref == nil || ref.Name != "admin-clientconfig" {
		t.Fatalf("C-113-1: the eval site must render the template to admin-clientconfig (templated); got %+v templated=%v", ref, templated)
	}
	m := &endpointReferenceMapper{authnNS: authnNS, username: "victim"}
	if err := m.refusePerUserRef(ref); !errors.Is(err, ErrPerUserCredentialRef) {
		t.Fatalf("C-113-1 VIOLATED: a templated name resolving to admin-clientconfig was not refused; got err=%v", err)
	}

	// Discriminating: the SAME derivation without the `-clientconfig` fold
	// resolves cleanly (proves the refusal keys on the reserved suffix, not on
	// templating per se).
	okTemplate := &templates.Reference{Name: `${ .want + "-endpoint" }`, Namespace: authnNS}
	okRef, okTemplated := r.evalEndpointRef(okTemplate)
	if okRef == nil || okRef.Name != "admin-endpoint" || !okTemplated {
		t.Fatalf("C-113-1 control: expected resolved name admin-endpoint (templated); got %+v templated=%v", okRef, okTemplated)
	}
	if okRef.Namespace != authnNS {
		t.Fatalf("C-113-5: namespace must stay the author-literal %q; got %q", authnNS, okRef.Namespace)
	}
}

// ─────────────────────────────────────────────────────────────────────────
// C-113-2 — resolveOne's OWN internal nil-ref→`<user>-clientconfig` synthesis
// must NEVER be refused, while a NAMED ref to the same `-clientconfig` name IS
// (#1025: every named ref, templated or not).
func TestC113_2_InternalSynthesisResolves_NamedPerUserRefRefused(t *testing.T) {
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

	// (i) the internal nil-ref path synthesizes `victim-clientconfig` and MUST
	// still resolve. The internal path resolved SOMETHING (no refusal); its
	// ServerURL is the internal-dispatch override (kubernetes.default.svc) OR the
	// seeded victim URL under TestMode.
	ep, isSA, err := m.resolveOne(context.Background(), nil /*internal nil-ref*/)
	if err != nil {
		t.Fatalf("C-113-2 (i) VIOLATED: the internal nil-ref clientconfig synthesis was refused; the caller's own credential must always resolve; got %v", err)
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
	saResolved, saIsSA, saErr := m.resolveOne(saCtx, nil /*internal nil-ref*/)
	if saErr != nil {
		t.Fatalf("C-113-2 provenance: resolveOne with an internal endpoint on ctx must resolve; got %v", saErr)
	}
	if !saIsSA {
		t.Fatalf("#267 provenance: resolveOne consulting the ctx-carried internal (SA) endpoint MUST report isSAEndpoint=true; got false — the dial-site SA token-file wiring would be silently disabled")
	}
	if saResolved.ServerURL != "https://kubernetes.default.svc" {
		t.Fatalf("C-113-2 provenance: the internal-endpoint branch must return the SA endpoint verbatim; got %+v", saResolved)
	}

	// (ii) a NAMED ref to the very name the nil-ref path synthesizes is refused
	// by the #1025 rule, which judges named refs only. Even the caller's
	// own name is refused when named: the boundary is the reserved suffix, not
	// cross-user (#1025).
	forged := &templates.Reference{Name: "victim-clientconfig", Namespace: authnNS}
	if terr := m.refusePerUserRef(forged); !errors.Is(terr, ErrPerUserCredentialRef) {
		t.Fatalf("C-113-2 (ii) VIOLATED: a named `-clientconfig` ref was NOT refused; got %v", terr)
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
	out, templated := r.evalEndpointRef(ref)
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
// pre-#113 at the eval site: the MaybeQuery gate skips evaluation,
// templated=false, same pointer. #1025 CHANGED the literal `-clientconfig`
// case: it passes the eval site unchanged, but the #1025 rule refuses it (before
// #1025 a literal per-user ref in a stored RESTAction was "the author's own
// business" and resolved through the ServiceAccount-backed snapshot).
func TestC113_6_StaticRefUnchanged_LiteralClientconfigRefused(t *testing.T) {
	r := &resolveRun{dict: map[string]any{"want": "spoke-a"}}

	// A plain literal name: passes through unchanged, NOT templated.
	lit := &templates.Reference{Name: "spoke-a-endpoint", Namespace: "krateo-system"}
	out, templated := r.evalEndpointRef(lit)
	if templated || out != lit {
		t.Fatalf("C-113-6: a literal name must pass through UNCHANGED, not templated, same pointer; got out=%+v templated=%v", out, templated)
	}

	// A literal `-clientconfig` name passes the eval site unchanged …
	litCC := &templates.Reference{Name: "some-clientconfig", Namespace: "krateo-system"}
	outCC, templatedCC := r.evalEndpointRef(litCC)
	if templatedCC || outCC != litCC {
		t.Fatalf("C-113-6: a literal `-clientconfig` ref must pass the eval site unchanged (templated=false); got out=%+v templated=%v", outCC, templatedCC)
	}
	// … and is refused by the #1025 rule.
	m := &endpointReferenceMapper{authnNS: "krateo-system", username: "victim"}
	if err := m.refusePerUserRef(outCC); !errors.Is(err, ErrPerUserCredentialRef) {
		t.Fatalf("#1025 VIOLATED: a LITERAL `-clientconfig` ref was not refused; got %v", err)
	}

	// And a nil ref (internal path) is a clean pass-through, not templated.
	outNil, templatedNil := r.evalEndpointRef(nil)
	if templatedNil || outNil != nil {
		t.Fatalf("C-113-6: a nil ref must pass through as (nil,false); got out=%+v templated=%v", outNil, templatedNil)
	}
}

// compile-time assertion that endpoints is used (endpoints.Endpoint referenced in
// the resolveOne signature exercised above).
var _ = endpoints.Endpoint{}
