// sensitive398_carriers_test.go — #398 "arm per carrier" (reviewer-416
// CONDITION 3): each resolved-output decline site has its own arm, so dropping
// any one of them fails a test.
//
//   TestS398_Carrier_RefresherReResolveOfSecretStepNotRePut — a RESTAction edited
//       to add a Secret step, re-resolved by the REFRESHER under snowplow's
//       ServiceAccount transport (the real resolveAndPopulateL1 + the real
//       restactions resolver). The SA-transport path is not the "external"
//       branch, so only the #398 refresher decline stands between the Secret and
//       the re-Put.
//   TestS398_Carrier_WidgetContentShellNotSeeded — the identity-free widgetContent
//       shell: a walker resolve that read a Secret (its sink bumped, as the
//       resolver's per-call dispatch does) must not seed the shell.
// The raFullList carrier's two Put sites have their arms in package apiref
// (sensitive398_ra_full_list_test.go).

package dispatchers

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/krateo-platformops/plumbing/endpoints"
	"github.com/krateo-platformops/snowplow/internal/cache"
	"github.com/krateo-platformops/snowplow/internal/objects"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/client-go/rest"
)

func TestS398_Carrier_RefresherReResolveOfSecretStepNotRePut(t *testing.T) {
	k423Env(t)
	a := s398Arm()
	psBuildWatcher(t, a)
	srv := psFakeAPIServer(t, a, k423Counters())
	psSeedClientconfigs(t, srv.URL)

	// A resident restactions cell for alice, minted by the production key
	// derivation, holding the RA's pre-edit (Secret-free) body.
	aliceCtx := psUserCtx(a, psAlice)
	key, in := k423RAKey(t, aliceCtx)
	in.RepresentativeUsername, in.RepresentativeGroups = psAlice, psGroups(a)
	cache.ResolvedCache().Put(key, &cache.ResolvedEntry{RawJSON: []byte(`{"pre":"edit"}`), Inputs: in})

	// The RA was edited to add a Secret step; the refresher re-resolves it.
	edited := psRACR(a)
	restore := setResolveOnceForTest(func(ctx context.Context, rin cache.ResolvedKeyInputs) ([]byte, error) {
		ctx = cache.WithBackgroundResolve(ctx)
		return resolveRestActionForRefresh(ctx, objects.Result{GVR: h1RAGVR, Unstructured: edited.DeepCopy()}, rin, psAuthnNS)
	})
	defer restore()
	saSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ") != "tok-sa" {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		b, _ := json.Marshal(map[string]any{"apiVersion": "v1", "kind": "Secret",
			"metadata": map[string]any{"name": psTargetObj, "namespace": psTargetNS},
			"data":     map[string]any{"password": psWireValue(a)}})
		_, _ = w.Write(b)
	}))
	defer saSrv.Close()
	saEP := &endpoints.Endpoint{ServerURL: saSrv.URL, Token: "tok-sa"}
	saRC := &rest.Config{Host: saSrv.URL, BearerToken: "tok-sa"}

	before := sensitiveSkipped398()
	if err := resolveAndPopulateL1(context.Background(), *in, saEP, saRC); err != nil {
		t.Fatalf("refresh: %v", err)
	}
	if e, ok := cache.ResolvedCache().GetNoTouch(key); ok && e != nil && psHasSentinel(e.RawJSON) {
		t.Fatalf("#398 REFRESHER CARRIER: the refresher re-Put a Secret body into alice's restactions cell")
	}
	if sensitiveSkipped398() <= before {
		t.Fatalf("the refresher's sensitive decline did not fire (the arm did not reach it)")
	}
}

func TestS398_Carrier_WidgetContentShellNotSeeded(t *testing.T) {
	t.Setenv("CACHE_ENABLED", "true")
	t.Setenv("RESOLVED_CACHE_ENABLED", "true")
	cache.ResetResolvedCacheForTest()
	t.Cleanup(cache.ResetResolvedCacheForTest)

	in := h1WidgetUnstructured(map[string]any{})
	res := h1WidgetUnstructured(map[string]any{})
	_ = unstructured.SetNestedField(res.Object, psSentinel, "status", "widgetData", "password")

	// The walker's resolve ctx with the sink installed and bumped — exactly what
	// the resolver's per-call dispatch does for a core v1/secrets call.
	ctx, sink := cache.WithSensitiveTouchedSink(context.Background())
	sink.Bump()
	before := sensitiveSkipped398()
	populateWidgetContentL1(ctx, h1WidgetGVR, in, -1, -1, res, 0)

	c := cache.ResolvedCache()
	for _, k := range c.KeysForTest() {
		if e, ok := c.GetNoTouch(k); ok && e != nil && psHasSentinel(e.RawJSON) {
			t.Fatalf("#398 WIDGETCONTENT CARRIER: the identity-free content shell was seeded with a Secret body")
		}
	}
	if sensitiveSkipped398() <= before {
		t.Fatalf("the widgetContent sensitive decline did not fire")
	}

	// CONTROL: the same populate WITHOUT a sensitive read seeds the shell.
	populateWidgetContentL1(context.Background(), h1WidgetGVR, in, -1, -1, res, 0)
	if len(c.KeysForTest()) == 0 {
		t.Fatalf("CONTROL: a non-sensitive populate must seed the shell (the arm would be vacuous otherwise)")
	}
}
