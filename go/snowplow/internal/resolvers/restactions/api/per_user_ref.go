// per_user_ref.go — #1025: a stage's endpointRef never resolves to a per-user
// credential Secret.
//
// THE RULE. authn keeps two credentials per user in AUTHN_NAMESPACE: the
// session `<user>-clientconfig` and the step-up `<user>-clientconfig-stepup`
// (credsecret). They are chosen ONLY from the caller's own validated identity:
// a stage with no endpointRef dials credsecret.For(username, groups). A NAMED
// endpointRef that names one of them is refused, whoever vouches for the spec:
//   - a stored RESTAction, whether its name is literal or request-templated;
//   - a caller-supplied (inline draft) spec (#443).
//
// Before #1025 the refusal covered only the templated ref (#113 guardrail b)
// and the caller-supplied one (#443), as two checks keyed on how the ref was
// produced. A literal ref in a stored RESTAction resolved through snowplow's
// ServiceAccount-backed Secrets snapshot, so a RESTAction author could dial as
// any user, including with that user's elevated step-up credential. Now ONE
// check, refusePerUserRef, keys on the name alone. It is called once, at the
// top of resolveStageEndpoint, which every stage passes BEFORE it branches on
// userAccessFilter: a UAF stage ignores its endpointRef (it dials the
// ServiceAccount, or the caller's own credential for an inline draft), yet a
// per-user ref there is refused, logged and counted like any other. It runs on
// the name that would be looked up: a templated name is already DNS-1123
// normalised by evalEndpointRef, and a literal name is looked up exactly as
// written.
//
// THE REFUSAL is an endpoint-resolve failure, the same posture the #113 and
// #443 refusals had: the stage fails (stage outcome Error), the resolve
// truncates, and nothing is dialed or read.
//
// OBSERVABILITY. One ERROR line at the stage ("endpointRef refused") carrying
// the stage, the provenance, whether the name was templated and the Secret
// name as a redact label, never in clear (the name embeds a username). One
// count per refusal in snowplow_endpointref_per_user_refused_total{provenance},
// both cells pre-populated (a 0 is a reading). Not gated on the cache mode:
// the rule holds on every resolve.
package api

import (
	"errors"
	"expvar"
	"sync"
	"sync/atomic"

	templates "github.com/krateo-platformops/snowplow/apis/templates/v1"
	"github.com/krateo-platformops/snowplow/internal/credsecret"
)

// refusePerUserRef is THE #1025 rule: a named ref (nil is the caller's own
// credential, never refused) whose name is a per-user credential name is
// refused and counted under the resolve's provenance.
func (m *endpointReferenceMapper) refusePerUserRef(ref *templates.Reference) error {
	if ref != nil && credsecret.IsPerUserName(ref.Name) {
		recordPerUserRefRefused(m.provenance)
		return ErrPerUserCredentialRef
	}
	return nil
}

// ErrPerUserCredentialRef is refusePerUserRef's refusal of a named endpointRef
// that names a per-user credential Secret (#1025). Its text carries no name.
var ErrPerUserCredentialRef = errors.New(
	"endpointRef names a per-user credential Secret; per-user credentials are chosen only from the caller's own identity (omit endpointRef)")

// Provenance labels of snowplow_endpointref_per_user_refused_total.
const (
	perUserRefProvenanceStored         = "stored"
	perUserRefProvenanceCallerSupplied = "caller-supplied"
)

// provenanceLabel is the closed-set label of a Provenance.
func provenanceLabel(p Provenance) string {
	if p == ProvenanceCallerSupplied {
		return perUserRefProvenanceCallerSupplied
	}
	return perUserRefProvenanceStored
}

var perUserRefRefused struct {
	stored         atomic.Uint64
	callerSupplied atomic.Uint64
}

func recordPerUserRefRefused(p Provenance) {
	if p == ProvenanceCallerSupplied {
		perUserRefRefused.callerSupplied.Add(1)
		return
	}
	perUserRefRefused.stored.Add(1)
}

// PerUserRefRefusedSnapshot returns the {provenance} refusal counts.
func PerUserRefRefusedSnapshot() map[string]uint64 {
	return map[string]uint64{
		perUserRefProvenanceStored:         perUserRefRefused.stored.Load(),
		perUserRefProvenanceCallerSupplied: perUserRefRefused.callerSupplied.Load(),
	}
}

var perUserRefExpvarOnce sync.Once

func init() { registerPerUserRefExpvar() }

// registerPerUserRefExpvar publishes snowplow_endpointref_per_user_refused_total.
// Ungated on the cache mode (listed in the CFG-1 nonCacheInitPublishers
// exception): the refusal happens on every RESTAction resolve.
func registerPerUserRefExpvar() {
	perUserRefExpvarOnce.Do(func() {
		expvar.Publish("snowplow_endpointref_per_user_refused_total", expvar.Func(func() any {
			return PerUserRefRefusedSnapshot()
		}))
	})
}
