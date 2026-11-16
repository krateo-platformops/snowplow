package handlers

// authn_fixture_1382_test.go — #1382: a fixture credential Secret must be
// provably its user's, as authn writes it: since authn#140 authn stamps
// endpoints.UsernameAnnotation with the exact username; before it, the
// Secret's client certificate carried the username as its CommonName. The
// readers use a Secret only when endpoints.OwnsSecret proves one of the two,
// so a token-only fixture with neither (which authn never writes) is refused
// like another user's Secret.

import (
	"slices"

	"github.com/krateo-platformops/plumbing/endpoints"
	corev1 "k8s.io/api/core/v1"

	"github.com/krateo-platformops/snowplow/internal/credsecret"
)

// authnOwned stamps sec as the credential Secret of whichever of users it is
// named for (one of credsecret.AllNames), the way authn writes it, and returns
// it. A Secret named for none of them is left as it is.
func authnOwned(sec *corev1.Secret, users ...string) *corev1.Secret {
	for _, u := range users {
		if sec != nil && slices.Contains(credsecret.AllNames(u), sec.Name) {
			if sec.Annotations == nil {
				sec.Annotations = map[string]string{}
			}
			sec.Annotations[endpoints.UsernameAnnotation] = u
			return sec
		}
	}
	return sec
}
