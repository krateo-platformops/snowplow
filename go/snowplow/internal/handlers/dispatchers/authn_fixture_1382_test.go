package dispatchers

// authn_fixture_1382_test.go — #1382: a fixture credential Secret must be
// provably its user's, as authn writes it: since authn#140 authn stamps
// endpoints.UsernameAnnotation with the exact username; before it, the
// Secret's client certificate carried the username as its CommonName. The
// readers use a Secret only when endpoints.OwnsSecret proves one of the two,
// so a token-only fixture with neither (which authn never writes) is refused
// like another user's Secret.

import (
	"github.com/krateo-platformops/plumbing/endpoints"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// authnMeta is the ObjectMeta authn writes on username's credential Secret
// called name in ns.
func authnMeta(ns, name, username string) metav1.ObjectMeta {
	return metav1.ObjectMeta{Namespace: ns, Name: name,
		Annotations: map[string]string{endpoints.UsernameAnnotation: username}}
}
