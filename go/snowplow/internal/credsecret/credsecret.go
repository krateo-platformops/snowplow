// Package credsecret names authn's per-user credential Secrets (#994).
//
// authn keeps two credentials per user in AUTHN_NAMESPACE, both holding the
// same five data keys:
//
//   - the session credential, `<DNS1123(username)>-clientconfig`, written at
//     every login;
//   - the step-up credential, `<DNS1123(username)>-clientconfig-stepup`,
//     written by POST /oidc/stepup (authn#49, authn 0.30.0). Its certificate
//     carries the extra group krateo:loa:2 and lives about 15 minutes; the
//     step-up JWT carries the same group in its `groups` claim and expires no
//     later than the certificate.
//
// Selection keys on that group, read from the VALIDATED token's own groups.
// The name is always built from the token's own username, so a token can only
// ever select its own user's Secrets. authn strips the krateo:loa: prefix from
// every other group source, so only a step-up token carries the group.
//
// Session names always end in "-clientconfig" and step-up names in
// "-clientconfig-stepup", so the two sets are disjoint: no username's session
// Secret is another username's step-up Secret. Every snowplow site that names,
// reserves or recognises these Secrets goes through this package, so each
// suffix is spelled once.
package credsecret

import (
	"slices"
	"strings"

	"github.com/krateo-platformops/plumbing/kubeutil"
)

const (
	// StepUpGroup is the group authn grants a step-up login (authn
	// userinfo.StepUpGroup(2)). Only authn may grant it.
	StepUpGroup = "krateo:loa:2"

	// SessionSuffix ends every session credential Secret name (authn
	// storage.ClientConfigSecretName).
	SessionSuffix = "-clientconfig"
	// StepUpSuffix ends every step-up credential Secret name (authn
	// storage.StepUpSecretName).
	StepUpSuffix = SessionSuffix + "-stepup"
)

// IsStepUp reports whether a validated token's groups mark a step-up login.
func IsStepUp(groups []string) bool {
	return slices.Contains(groups, StepUpGroup)
}

// SessionName is username's session credential Secret name.
func SessionName(username string) string {
	return kubeutil.MakeDNS1123Compatible(username) + SessionSuffix
}

// StepUpName is username's step-up credential Secret name.
func StepUpName(username string) string {
	return kubeutil.MakeDNS1123Compatible(username) + StepUpSuffix
}

// For is the credential Secret a validated identity reads: the step-up Secret
// when its groups carry StepUpGroup, else the session Secret. There is no
// fallback between the two: a step-up identity whose Secret is gone has no
// credential, and a session identity never reads a step-up Secret.
func For(username string, groups []string) string {
	if IsStepUp(groups) {
		return StepUpName(username)
	}
	return SessionName(username)
}

// IsPerUserName reports whether name is a per-user credential Secret name of
// either kind. It is the reserved set: a request-driven or caller-supplied
// endpointRef may never resolve to one (#113 guardrail b, #443).
func IsPerUserName(name string) bool {
	return IsSessionName(name) || IsStepUpName(name)
}

// IsSessionName reports whether name is a session credential Secret name. A
// step-up name is NOT one: it ends in StepUpSuffix, never in SessionSuffix.
func IsSessionName(name string) bool {
	return strings.HasSuffix(name, SessionSuffix)
}

// IsStepUpName reports whether name is a step-up credential Secret name.
func IsStepUpName(name string) bool {
	return strings.HasSuffix(name, StepUpSuffix)
}
