// Package credsecret names authn's per-user credential Secrets (#994, #1382).
//
// authn keeps two credentials per user in AUTHN_NAMESPACE, both holding the
// same five data keys:
//
//   - the session credential, `<name>-clientconfig`, written at every login;
//   - the step-up credential, `<name>-clientconfig-stepup`, written by POST
//     /oidc/stepup (authn#49, authn 0.30.0). Its certificate carries the extra
//     group krateo:loa:2 and lives about 15 minutes; the step-up JWT carries
//     the same group in its `groups` claim and expires no later than the
//     certificate.
//
// `<name>` is plumbing's to derive, never snowplow's (#1382). Since authn#140
// a Secret is named after the EXACT username whenever that is a valid Secret
// name (endpoints.ExactSecretName); before it, after the username made
// DNS-1123 compatible (endpoints.SecretName, the legacy name). Through the
// migration a reader tries endpoints.SessionSecretNames: the exact name, then
// the legacy one where it differs. Two usernames can share a legacy name
// (john.doe and johndoe are both johndoe-clientconfig), and a legacy name can
// be another username's exact name, so a Secret found under any of them is the
// requester's only once endpoints.OwnsSecret proves it. This package only
// names; the readers (api.FromInformerOwnSecret, api.FromAPIServerOwnSecret)
// prove ownership.
//
// Selection keys on the step-up group, read from the VALIDATED token's own
// groups. The names are always built from the token's own username, so a token
// can only ever select its own user's names. authn strips the krateo:loa:
// prefix from every other group source, so only a step-up token carries the
// group.
//
// Session names always end in endpoints.ClientConfigSuffix and step-up names
// in endpoints.StepUpSuffix, so the two sets are disjoint: no username's
// session Secret is another username's step-up Secret. Every snowplow site
// that names, reserves or recognises these Secrets goes through this package,
// and no suffix or derivation is spelled in snowplow
// (TestIssue1382_NoLocalClientconfigDerivation).
package credsecret

import (
	"slices"
	"strings"

	"github.com/krateo-platformops/plumbing/endpoints"
)

// StepUpGroup is the group authn grants a step-up login (authn
// userinfo.StepUpGroup(2)). Only authn may grant it.
const StepUpGroup = "krateo:loa:2"

// IsStepUp reports whether a validated token's groups mark a step-up login.
func IsStepUp(groups []string) bool {
	return slices.Contains(groups, StepUpGroup)
}

// SessionNames is the names username's session credential Secret may have,
// in the order a reader tries them: endpoints.SessionSecretNames.
func SessionNames(username string) []string {
	return endpoints.SessionSecretNames(username)
}

// StepUpNames is the names username's step-up credential Secret may have, in
// the same order: each session name with endpoints.ClientConfigSuffix
// replaced by endpoints.StepUpSuffix, as authn names it
// (`<name>-clientconfig-stepup`, authn#140). plumbing names the session
// Secret only, so this is the one place the step-up form is put together, and
// it is put together from plumbing's names and suffixes alone.
func StepUpNames(username string) []string {
	session := SessionNames(username)
	out := make([]string, len(session))
	for i, n := range session {
		out[i] = strings.TrimSuffix(n, endpoints.ClientConfigSuffix) + endpoints.StepUpSuffix
	}
	return out
}

// For is the credential Secret names a validated identity reads, in the order
// it reads them: the step-up names when its groups carry StepUpGroup, else
// the session names. There is no fallback between the two kinds: a step-up
// identity whose Secret is gone has no credential, and a session identity
// never reads a step-up Secret. The list is never empty; the first name is
// the preferred one (the exact name, when the username has one).
func For(username string, groups []string) []string {
	if IsStepUp(groups) {
		return StepUpNames(username)
	}
	return SessionNames(username)
}

// AllNames is every name either of username's credential Secrets may have:
// the step-up names, then the session names. It is for the sites that must
// recognise or scrub them all (a log redaction), never for a read.
func AllNames(username string) []string {
	return append(StepUpNames(username), SessionNames(username)...)
}

// IsPerUserName reports whether name is a per-user credential Secret name of
// either kind. It is the reserved set: no named endpointRef may resolve to one,
// whoever vouches for the RESTAction and whether the name is literal or
// templated (#1025, which generalised #113 guardrail b and #443).
func IsPerUserName(name string) bool {
	return IsSessionName(name) || IsStepUpName(name)
}

// IsSessionName reports whether name is a session credential Secret name. A
// step-up name is NOT one: it ends in endpoints.StepUpSuffix, never in
// endpoints.ClientConfigSuffix.
func IsSessionName(name string) bool {
	return strings.HasSuffix(name, endpoints.ClientConfigSuffix)
}

// IsStepUpName reports whether name is a step-up credential Secret name.
func IsStepUpName(name string) bool {
	return strings.HasSuffix(name, endpoints.StepUpSuffix)
}
