package api

// authn_fixture_1382_test.go — #1382: a fixture credential Secret must be
// provably its user's, as authn writes it. Since authn#140 authn stamps
// endpoints.UsernameAnnotation with the exact username on every session and
// step-up Secret; before it, the Secret's client certificate carried the
// username as its CommonName. The reader uses a Secret only when
// endpoints.OwnsSecret proves one of the two, so a token-only fixture with
// neither (which authn never writes) is refused like another user's Secret.

import (
	"slices"

	"github.com/krateo-platformops/plumbing/endpoints"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/krateo-platformops/snowplow/internal/credsecret"
)

// authnOwned stamps sec as the credential Secret of whichever of users it is
// named for (one of credsecret.AllNames), the way authn writes it, and returns
// it. A Secret named for none of them (a named endpointRef's Secret, or
// another user's) is left as it is.
func authnOwned(sec *corev1.Secret, users ...string) *corev1.Secret {
	if sec == nil {
		return sec
	}
	for _, u := range users {
		if slices.Contains(credsecret.AllNames(u), sec.Name) {
			if sec.Annotations == nil {
				sec.Annotations = map[string]string{}
			}
			sec.Annotations[endpoints.UsernameAnnotation] = u
			return sec
		}
	}
	return sec
}

// authnMeta is the ObjectMeta authn writes on username's credential Secret
// called name in ns.
func authnMeta(ns, name, username string) metav1.ObjectMeta {
	return metav1.ObjectMeta{Namespace: ns, Name: name,
		Annotations: map[string]string{endpoints.UsernameAnnotation: username}}
}
