package cache

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"net/http"
	"testing"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/rest"
)

// #608 — the refresh failure classifier. The headline row is the 057 one: a
// 403 whose message is the literal "unknown" (client-go could not decode the
// Status body on .Raw()), wrapped by the validate stage, wrapped again by the
// refresher's own %w prefixes. It must read forbidden/validate, never
// unclassified.
func TestIssue608_ClassifyRefreshError(t *testing.T) {
	unknown403 := apierrors.NewGenericServerResponse(http.StatusForbidden, "get", schema.GroupResource{}, "", "unknown", 0, false)
	wrap := func(stage string, err error) error {
		return fmt.Errorf("resolveAndPopulateL1 widgets/x: resolve widget: %w",
			WrapResolveStage(stage, stage+" prefix", err))
	}
	bad := &apierrors.StatusError{}
	bad.ErrStatus.Code = http.StatusBadRequest
	bad.ErrStatus.Reason = "BadRequest"

	for _, tc := range []struct {
		name              string
		err               error
		wantClass, wantSt string
		nonRetryable      bool
	}{
		{"057 unknown 403 at validate", wrap(ResolveStageValidate, unknown403), RefreshErrForbidden, ResolveStageValidate, true},
		{"404 at apiref", wrap(ResolveStageApiRef, apierrors.NewNotFound(schema.GroupResource{Resource: "x"}, "n")), RefreshErrNotFound, ResolveStageApiRef, false},
		{"schema 400 at validate", wrap(ResolveStageValidate, bad), RefreshErrSchemaInvalid, ResolveStageValidate, false},
		{"400 elsewhere is not schema", wrap(ResolveStageApiRef, bad), RefreshErrUnclassified, ResolveStageApiRef, false},
		{"jq at widgetData", wrap(ResolveStageWidgetData, errors.New("jq: cannot iterate over null")), RefreshErrJQ, ResolveStageWidgetData, false},
		{"5xx", wrap(ResolveStageResourcesRefs, apierrors.NewInternalError(errors.New("boom"))), RefreshErrUnavailable, ResolveStageResourcesRefs, false},
		{"deadline", fmt.Errorf("x: %w", context.DeadlineExceeded), RefreshErrTimeout, ResolveStageNone, false},
		{"no kind match", wrap(ResolveStageValidate, &meta.NoKindMatchError{GroupKind: schema.GroupKind{Kind: "K"}}), RefreshErrDiscovery, ResolveStageValidate, false},
		{"unstaged plain", errors.New("encode failed"), RefreshErrUnclassified, ResolveStageNone, false},
		{"401", apierrors.NewUnauthorized("no"), RefreshErrUnauthorized, ResolveStageNone, false},
		// #630 — the re-fetch of the cell's own CR is its own stage, and a 403
		// on it is forbidden + non-retryable like every other 403.
		{"403 at refetch", fmt.Errorf("resolveAndPopulateL1 widgets/x: %w",
			WrapResolveStage(ResolveStageRefetch, "re-fetch flexes/x", apierrors.NewForbidden(schema.GroupResource{Resource: "flexes"}, "x", errors.New("no")))),
			RefreshErrForbidden, ResolveStageRefetch, true},
		{"404 at refetch", WrapResolveStage(ResolveStageRefetch, "re-fetch flexes/x",
			apierrors.NewNotFound(schema.GroupResource{Resource: "flexes"}, "x")), RefreshErrNotFound, ResolveStageRefetch, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c, s := ClassifyRefreshError(tc.err)
			if c != tc.wantClass || s != tc.wantSt {
				t.Errorf("ClassifyRefreshError = (%s, %s), want (%s, %s) for %v", c, s, tc.wantClass, tc.wantSt, tc.err)
			}
			if got := IsNonRetryableRefreshError(tc.err); got != tc.nonRetryable {
				t.Errorf("IsNonRetryableRefreshError = %v, want %v", got, tc.nonRetryable)
			}
		})
	}
}

// The counter is bounded: any class/stage outside the closed set folds into
// unclassified/none instead of minting a new series.
func TestIssue608_RefreshFailedByClass_ClosedSet(t *testing.T) {
	ResetRefreshFailedByClassForTest()
	t.Cleanup(ResetRefreshFailedByClassForTest)
	recordRefreshFailedClass(RefreshErrForbidden, ResolveStageValidate)
	recordRefreshFailedClass("made-up", "made-up")
	// #630 — refetch is IN the closed set (its own series, not folded into
	// none), and its spelling is the literal the dispatchers arm asserts.
	if ResolveStageRefetch != "refetch" {
		t.Fatalf("ResolveStageRefetch = %q, want \"refetch\"", ResolveStageRefetch)
	}
	recordRefreshFailedClass(RefreshErrForbidden, ResolveStageRefetch)
	got := map[string]uint64{}
	for _, c := range RefreshFailedByClassSnapshot() {
		got[c.Class+"/"+c.Stage] = c.N
	}
	want := map[string]uint64{"forbidden/validate": 1, "unclassified/none": 1, "forbidden/refetch": 1}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("by-class snapshot = %v, want %v", got, want)
	}
}

// #608 — never impersonate yourself. The identity-free refresh runs under
// snowplow's own SA username; impersonating it can only lose rights, and 057
// refuses it outright.
func TestIssue608_ImpersonatingRESTConfig_NeverImpersonatesTheTransportItself(t *testing.T) {
	withImpersonationAvailable(t, true)
	enc := base64.RawURLEncoding
	const self = "system:serviceaccount:krateo-system:snowplow"
	tok := enc.EncodeToString([]byte(`{"alg":"RS256"}`)) + "." +
		enc.EncodeToString([]byte(`{"sub":"`+self+`"}`)) + "." + enc.EncodeToString([]byte("s"))
	base := &rest.Config{Host: "https://kubernetes.default.svc", BearerToken: tok}

	if got := ImpersonatingRESTConfig(base, self, nil); got != nil {
		t.Fatalf("impersonating the transport's own SA (%s) must return nil (keep the SA config), got %+v", self, got.Impersonate)
	}
	// Any OTHER ServiceAccount is still a cohort and is impersonated.
	if got := ImpersonatingRESTConfig(base, "system:serviceaccount:other:sa", nil); got == nil {
		t.Fatalf("a different ServiceAccount must still be impersonated")
	}
	// A config without an SA JWT keeps the pre-#608 behaviour exactly.
	if got := ImpersonatingRESTConfig(&rest.Config{Host: "h"}, self, nil); got == nil {
		t.Fatalf("with no readable transport identity the guard must not fire")
	}
}
