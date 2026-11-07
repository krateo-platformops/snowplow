package cache

// requester_credential_1229_test.go — #1229: RequesterCredentialFor and
// ClientConfigFor answer the same question, whose credential a read dials with,
// for every shape of internal configuration a context can carry. The mark at
// objects.getFromAPIServer relies on that.

import (
	"context"
	"testing"

	xcontext "github.com/krateo-platformops/plumbing/context"
	"github.com/krateo-platformops/plumbing/endpoints"
	"k8s.io/client-go/rest"
)

func TestIssue1229_RequesterCredentialForAgreesWithClientConfigFor(t *testing.T) {
	internal := &rest.Config{Host: "https://internal.invalid"}
	ep := endpoints.Endpoint{ServerURL: "https://requester.invalid", Token: "t"}
	for _, c := range []struct {
		name          string
		carried       any // nil = none
		wantRequester bool
	}{
		{name: "none", wantRequester: true},
		{name: "a_valid_internal_config", carried: internal, wantRequester: false},
		{name: "a_nil_pointer", carried: (*rest.Config)(nil), wantRequester: true},
		{name: "a_wrong_type", carried: "not a rest config", wantRequester: true},
	} {
		t.Run(c.name, func(t *testing.T) {
			ctx := xcontext.BuildContext(context.Background(), xcontext.WithUserConfig(ep))
			if c.carried != nil {
				ctx = WithInternalRESTConfig(ctx, c.carried)
			}
			rc, err := ClientConfigFor(ctx, ep)
			if err != nil || rc == nil {
				t.Fatalf("ClientConfigFor must hand back a config for every shape: rc=%v err=%v", rc, err)
			}
			usedInternal := rc == internal
			if got := RequesterCredentialFor(ctx); got != c.wantRequester || got == usedInternal {
				t.Fatalf("RequesterCredentialFor=%v, ClientConfigFor used the internal config=%v, want requester=%v: the two disagree on whose credential is dialled",
					got, usedInternal, c.wantRequester)
			}
		})
	}
}
