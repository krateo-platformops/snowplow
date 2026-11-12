package credhttp

// destination.go — #1131: the destination policy, on the address actually
// dialled.
//
// THE POLICY is Diego's ruling, implemented once, in plumbing's dialguard, and
// shared with core-provider (#1135). Refused on every request, with or without
// credentials: link-local and cloud metadata (169.254.0.0/16, fe80::/10),
// unspecified (0.0.0.0, ::) and multicast (224.0.0.0/4, ff00::/8), in any
// spelling. Allowed: loopback and private ranges, where in-cluster services and
// on-prem endpoints live. There is no knob and no allowlist. A gap in the policy
// is fixed in plumbing, never here.
//
// WHY THE DIALER. dialguard.Control runs once per candidate address, immediately
// before the connect, so the IP checked is the IP dialled: a name that answers
// differently between two dials (DNS rebinding) and any spelling of a hostname
// are covered by construction, and a redirect hop that stays in the origin is
// checked too, because it dials.
//
// EVERY DIAL THAT CARRIES A NAMED ENDPOINT:
//   - the owned branch of a stage fetch: EndpointDialHolder (budget.go);
//   - the plumbing branch of a stage fetch, and every /call passthrough
//     (credhttp.Do): plumbing's own transport, which dials through dialguard;
//   - a client built here that names no transport: guardedTransport.
//
// destination_guard_1131_test.go fails CI on a client, transport or dialer that
// leaves this set. Own-cluster client-go clients (Harden) dial the apiserver by
// provenance and are out of the policy's scope (#1131); they never carry a named
// endpoint.
//
// WHAT THE POLICY CANNOT SEE. With a proxy (the Secret's proxy-url, or the
// process's HTTP_PROXY), the address dialled is the proxy's. The proxy reaches
// the destination from its own network, not from this pod.
//
// A REFUSAL IS NOT A NETWORK FAILURE, and the two have opposite remedies: it is
// never retried (send.go), and it reaches the stage as its own reason
// (DestinationStatus), so nobody goes looking for a connectivity problem.

import (
	"errors"
	"net/http"

	"github.com/krateo-platformops/plumbing/dialguard"
	"github.com/krateo-platformops/plumbing/http/response"
)

// DestinationRefusedReason is the reason of the envelope a refused destination
// is reported with.
const DestinationRefusedReason = "DestinationRefused"

// guardedTransport is what every client built here uses when it names no
// transport of its own. It has http.DefaultTransport's settings and the guarded
// dialer, and it is shared, as http.DefaultTransport was.
//
// It goes through dialguard.RoundTripper, as plumbing requires of any client
// built on dialguard.Transport (plumbing v1.52.0, dialguard/roundtripper.go).
// On a server build that returns the transport itself. On js/wasm it refuses
// every redirect, because the browser's fetch would follow one where neither
// CheckRedirect nor RefuseRedirectHops sees it (#1360).
var guardedTransport http.RoundTripper = dialguard.RoundTripper(dialguard.Transport())

// DestinationRefused reports whether err is the destination policy's refusal.
func DestinationRefused(err error) bool {
	var d *dialguard.ErrDenied
	return errors.As(err, &d)
}

// DestinationStatus shapes a refused destination (#1131) into the envelope a
// stage or a /call passthrough reports: 502 Bad Gateway, as for a refused
// redirect (#729), a Failure, reason DestinationRefused, with the refusal's own
// text: the address, and which range it is in, never the request's URL.
// ok=false for any other error.
func DestinationStatus(err error) (*response.Status, bool) {
	var d *dialguard.ErrDenied
	if !errors.As(err, &d) {
		return nil, false
	}
	st := response.New(http.StatusBadGateway, d)
	st.Status = response.StatusFailure
	st.Reason = response.StatusReason(DestinationRefusedReason)
	return st, true
}
