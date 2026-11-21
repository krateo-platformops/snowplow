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
//
// #1560 — a third kind: the DELEGATED credential authn's /delegate mints for a
// token that carries `act` (authn#167). There is one per (person, actor):
// endpoints.DelegatedSecretName(username, actor), where actor is the token's
// outermost act.sub. It is proven the token's by
// endpoints.CheckDelegatedCredential (annotations, certificate CN and validity,
// and Organizations equal to the token's groups as issued), never by
// OwnsSecret. Select is the one selector for all three kinds, used by the auth
// middleware and by every RESTAction own-credential stage alike, so the two
// can never read different Secrets for one token. The delegated name is
// "delegated-" and 64 hex digits: it never ends in a session or step-up
// suffix, so the three kinds are disjoint.
package credsecret

import (
	"slices"
	"strings"
	"time"

	"github.com/krateo-platformops/plumbing/endpoints"
	"github.com/krateo-platformops/plumbing/jwtutil"
	corev1 "k8s.io/api/core/v1"
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
// recognise or scrub them all (a log redaction), never for a read. A
// delegated name is not in it: it depends on the actor too, and the log
// scrubber recognises its fixed shape instead (redact.ErrorText).
func AllNames(username string) []string {
	return append(StepUpNames(username), SessionNames(username)...)
}

// IsPerUserName reports whether name is a per-user credential Secret name of
// any kind: session, step-up or delegated. It is the reserved set: no named
// endpointRef may resolve to one, whoever vouches for the RESTAction and
// whether the name is literal or templated (#1025, which generalised #113
// guardrail b and #443). #1560 — a delegated name holds one person's
// credential for one actor; a stored RESTAction naming it would dial as that
// person.
func IsPerUserName(name string) bool {
	return IsSessionName(name) || IsStepUpName(name) || IsDelegatedName(name)
}

// IsDelegatedName reports whether name is a delegated credential Secret name
// (endpoints.IsDelegatedSecretName): "delegated-" and 64 lowercase hex digits.
// It is neither a session nor a step-up name.
func IsDelegatedName(name string) bool {
	return endpoints.IsDelegatedSecretName(name)
}

// Kind is which of a person's credentials a validated token reads.
type Kind int

const (
	// KindSession is `<name>-clientconfig`, written at every login.
	KindSession Kind = iota
	// KindStepUp is `<name>-clientconfig-stepup`, for a token whose groups
	// carry StepUpGroup (#994).
	KindStepUp
	// KindDelegated is endpoints.DelegatedSecretName(username, actor), for a
	// token that carries `act` (#1560, authn#167).
	KindDelegated
)

// String names the kind, for logs.
func (k Kind) String() string {
	switch k {
	case KindSession:
		return "session"
	case KindStepUp:
		return "step-up"
	case KindDelegated:
		return "delegated"
	}
	return "unknown"
}

// Credential is the credential Secret a validated token reads: its Names, in
// the order a reader tries them, and Check, which a Secret found under one of
// them must pass before it is used. Build it with Select only.
type Credential struct {
	Kind  Kind
	Names []string

	username string
	actor    string
	groups   []string
}

// Select is THE credential selector (#994, #1382, #1560). The auth middleware
// and a RESTAction stage that dials the caller's own credential both call it
// with the validated token's UserInfo, so they always read the same Secret:
//
//   - a token with `act` reads its delegated credential,
//     endpoints.DelegatedSecretName(Username, Actor()), Actor() being the
//     outermost act.sub. Its groups are not looked at to choose: a delegated
//     token has no step-up branch (authn strips krateo:loa:* when it issues
//     one);
//   - any other token reads For(Username, Groups): the step-up names when the
//     groups carry StepUpGroup, else the session names. Unchanged by #1560.
//
// There is no fallback between kinds: a delegated token whose Secret is
// missing has no credential; it never reads the person's session Secret.
func Select(user jwtutil.UserInfo) Credential {
	if user.Act != nil {
		actor := user.Actor()
		return Credential{
			Kind:     KindDelegated,
			Names:    []string{endpoints.DelegatedSecretName(user.Username, actor)},
			username: user.Username,
			actor:    actor,
			groups:   append([]string(nil), user.Groups...),
		}
	}
	kind := KindSession
	if IsStepUp(user.Groups) {
		kind = KindStepUp
	}
	return Credential{Kind: kind, Names: For(user.Username, user.Groups), username: user.Username}
}

// Outcome is what a Secret found under one of a Credential's names holds for
// the token.
type Outcome int

const (
	// Held: the token's credential; a reader may use it.
	Held Outcome = iota
	// NotOwned: someone else's Secret, or no readable credential. For a
	// session or step-up token, endpoints.OwnsSecret does not hold; for a
	// delegated one, endpoints.DelegatedCredentialNotOwned.
	NotOwned
	// Expired: the delegated credential's certificate is not valid now.
	Expired
	// Mismatch: the delegated credential is valid now, but its certificate's
	// Organizations are not the token's groups.
	Mismatch
)

// String names the outcome, for logs.
func (o Outcome) String() string {
	switch o {
	case Held:
		return "held"
	case NotOwned:
		return "not-owned"
	case Expired:
		return "expired"
	case Mismatch:
		return "mismatch"
	}
	return "unknown"
}

// Check reports what sec holds for the token c was selected for, at now. A
// session or step-up Secret is Held when endpoints.OwnsSecret proves it the
// user's (#1382), and NotOwned otherwise. A delegated Secret is judged by
// endpoints.CheckDelegatedCredential with the token's username, actor and
// groups as issued.
func (c Credential) Check(sec *corev1.Secret, now time.Time) Outcome {
	if c.Kind != KindDelegated {
		if sec != nil && endpoints.OwnsSecret(sec, c.username) {
			return Held
		}
		return NotOwned
	}
	switch endpoints.CheckDelegatedCredential(sec, c.username, c.actor, c.groups, now) {
	case endpoints.DelegatedCredentialHeld:
		return Held
	case endpoints.DelegatedCredentialExpired:
		return Expired
	case endpoints.DelegatedCredentialMismatch:
		return Mismatch
	}
	return NotOwned
}

// RefusedError is a reader's answer when a Secret exists under the
// credential's name and is the token's person's and actor's, but cannot serve
// this token: its certificate is Expired, or a Mismatch. It names no Secret
// and no identity. A Secret that is absent, or NotOwned, is reported as a
// NotFound instead, exactly as a missing session Secret is.
type RefusedError struct {
	Outcome Outcome
}

func (e *RefusedError) Error() string {
	if e.Outcome == Expired {
		return "the delegated credential has expired"
	}
	return "the delegated credential does not match the token's groups"
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
