package middleware_test

import (
	"net/http"
	"strings"
	"testing"

	"github.com/krateo-platformops/snowplow/internal/handlers/middleware"
)

// A Secret exists under the caller's OWN credential name but is not the
// caller's: its username annotation names someone else, or its certificate's CN
// does. The caller gets the missing-credential 401, the handler is never
// reached and so nothing is dialled with that Secret's endpoint. Both sources:
// the informer snapshot and the apiserver re-read. The mutant that drops the
// ownership check (credsecret.Check returning Held for any Secret) must fail it.
func TestCredentialForeignSecretUnderCallersName_Gets401_NeverServed(t *testing.T) {
	const caller = "carol"
	const foreignURL = "https://foreign.example/k8s"
	cases := []struct {
		name              string
		annotated, certCN string
	}{
		{"annotation names another user", "mallory", ""},
		{"certificate CN is another user", "", "mallory"},
		{"annotation is the caller's, certificate CN is another user", caller, "mallory"},
	}
	for _, c := range cases {
		for _, src := range []string{"informer", "apiserver"} {
			t.Run(c.name+"/"+src, func(t *testing.T) {
				foreign := o1382Secret(t, caller+"-clientconfig", foreignURL, c.annotated, c.certCN)
				if src == "informer" {
					su994Informer(t, foreign)
				} else {
					su994Informer(t)
				}
				su994APIServer(t, foreign)
				res := o1382Serve(t, o1382Token(t, caller, "devs"))
				if res.reached || res.ep.ServerURL == foreignURL || res.ep.Token != "" {
					t.Fatalf("the caller was served another user's Secret: reached=%v server=%q", res.reached, res.ep.ServerURL)
				}
				if res.code != http.StatusUnauthorized || !strings.Contains(res.body, middleware.CauseSessionCredentialMissing) {
					t.Fatalf("code=%d body=%s; want the missing-credential 401", res.code, res.body)
				}
			})
		}
	}
	// Control: the same Secret annotated for the caller IS served.
	own := o1382Secret(t, caller+"-clientconfig", foreignURL, caller, "")
	su994Informer(t, own)
	su994APIServer(t, own)
	if res := o1382Serve(t, o1382Token(t, caller, "devs")); !res.reached || res.ep.ServerURL != foreignURL {
		t.Fatalf("control: the caller's own Secret was not served: code=%d body=%s", res.code, res.body)
	}
}
