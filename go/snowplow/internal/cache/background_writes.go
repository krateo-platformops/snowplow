// background_writes.go — #903: background work only reads.
//
// THE RULE (owner ruling 2026-10-25, extending #615 to every endpoint kind).
// A RESTAction write stage (any verb but GET/HEAD/OPTIONS/TRACE) is sent only
// on a customer's own request. Whatever credential the stage uses —
// snowplow's ServiceAccount, an endpointRef Secret, an external URL with its
// own token — a resolve that is not a customer's own request refuses it
// (restactions/api, dispatchOneCall). Background work keeps resolving reads.
//
// WHO IS NOT A CUSTOMER. NonCustomerWriteOrigin is the one answer, from three
// signals, each already the single source for its population:
//   - RefDenialOrigin != serve: every background driver (refresher with its
//     viewed tier and the D2 dependent replace, cohort seed and keep-warm,
//     prewarm engine boot, the prewarm walks, the content prewarm, the
//     pagination drain). The #563 / #720 censuses keep every production
//     background root marked.
//   - a #547 demand revalidation: it replays a customer's request detached,
//     after their answer was written, and nobody waits on its result.
//   - a background HOP: the HTTP self-loopback of a background resolve (a stage
//     calling snowplow's own /call) re-enters snowplow as a fresh request whose
//     ctx carries no marker. The emitting resolve sends BackgroundHeader on
//     every egress to snowplow itself (never to another host), and the ingest
//     (dispatchers.reseedBackgroundFromHeaders) re-installs the hop marker.
//
// THE HOP MARKER ONLY RESTRICTS. It is read by the write refusal and by the
// egress that propagates it, nothing else: it does not change RefDenialOrigin,
// the memory gates, the cache key or any log level. So, like #443's
// InertHeader, it is honoured on any request with a snowplow-validated
// identity: a caller who forges it only stops their own request's writes.
package cache

import "context"

// BackgroundHeader marks an HTTP self-loopback made by a non-customer resolve.
// Sent only to snowplow's own host; honoured only as a restriction.
const BackgroundHeader = "X-Snowplow-Background"

// The non-customer write origins that RefDenialOrigin does not name.
const (
	// NonCustomerDemandRevalidation — a #547 demand revalidation.
	NonCustomerDemandRevalidation = "demand-revalidation"
	// NonCustomerBackgroundHop — an HTTP self-loopback of a background resolve.
	NonCustomerBackgroundHop = "background-hop"
)

// NonCustomerWriteOrigins is the CLOSED set NonCustomerWriteOrigin can return
// (besides ""), in emit order: every pre-populated cell of the refusal counter.
var NonCustomerWriteOrigins = []string{
	BackgroundOriginRefresher,
	BackgroundOriginCohortSeed,
	BackgroundOriginPrewarmEngineBoot,
	RefDenialOriginPrewarmPath,
	RefDenialOriginBackgroundUnattributed,
	NonCustomerDemandRevalidation,
	NonCustomerBackgroundHop,
}

type backgroundHopCtxKey struct{}

// WithBackgroundHop marks ctx as a request that a background resolve made to
// snowplow itself.
func WithBackgroundHop(ctx context.Context) context.Context {
	if ctx == nil {
		return ctx
	}
	return context.WithValue(ctx, backgroundHopCtxKey{}, true)
}

// BackgroundHop reports WithBackgroundHop.
func BackgroundHop(ctx context.Context) bool {
	if ctx == nil {
		return false
	}
	v, _ := ctx.Value(backgroundHopCtxKey{}).(bool)
	return v
}

// NonCustomerWriteOrigin names who is resolving when the resolve is NOT a
// customer's own request, and returns "" when it is. A write stage is sent
// only when it returns "".
func NonCustomerWriteOrigin(ctx context.Context) string {
	if o := RefDenialOrigin(ctx); o != RefDenialOriginServe {
		return o
	}
	if _, ok := DemandRevalidationFromContext(ctx); ok {
		return NonCustomerDemandRevalidation
	}
	if BackgroundHop(ctx) {
		return NonCustomerBackgroundHop
	}
	return ""
}
