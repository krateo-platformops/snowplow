package credhttp

// shared_attempt.go — #1320: what a send learns about an attempt it did not
// put on the wire itself.
//
// An attempt of an external read may be answered by an identical request
// another reader already has on the wire (restactions/api
// external_fetch_flight.go). The send policy still has to CLASSIFY that
// attempt: whether it had a connection (exhausted_connect or exhausted_answer,
// retry at once or after a backoff) and which phase it failed in (the retry
// cause). It must NOT MEASURE it: the connection was opened once, by the
// request on the wire, and its time to first byte is that request's, not the
// time since this reader joined. So the wire request's connection events
// reach a joined attempt here, through its classifiers only, and never
// through net/http/httptrace, where the connection counters (connections.go)
// and the retry reserve's sample (send.go, attemptTrace.freshAnswer) listen.
// One request on the wire feeds each instrument once: through the hooks of
// the reader that started it.

import "context"

// SharedAttemptEvent is one connection-phase event of a request on the wire,
// as far as an attempt is classified by it.
type SharedAttemptEvent int

const (
	SharedDNSStart SharedAttemptEvent = iota
	SharedDNSDone
	SharedConnectStart
	SharedConnectDone
	SharedTLSStart
	SharedTLSDone
	SharedGotConn
	SharedWroteHeaders
)

type sharedAttemptKey struct{}

// sharedAttemptSinks is the classifiers of one attempt, innermost last.
type sharedAttemptSinks []func(ev SharedAttemptEvent, failed bool)

// withSharedAttemptSink returns ctx with one more classifier for the attempt
// sent on it.
func withSharedAttemptSink(ctx context.Context, sink func(ev SharedAttemptEvent, failed bool)) context.Context {
	prev, _ := ctx.Value(sharedAttemptKey{}).(sharedAttemptSinks)
	next := make(sharedAttemptSinks, len(prev), len(prev)+1)
	copy(next, prev)
	return context.WithValue(ctx, sharedAttemptKey{}, append(next, sink))
}

// ObserveSharedAttempt tells the attempt sent on ctx that the request on the
// wire it shares had ev (failed: the phase ended in an error). Only the
// attempt's classification hears it: no connection is counted and no reserve
// sample is taken. A no-op for a ctx that carries no attempt.
func ObserveSharedAttempt(ctx context.Context, ev SharedAttemptEvent, failed bool) {
	sinks, _ := ctx.Value(sharedAttemptKey{}).(sharedAttemptSinks)
	for _, sink := range sinks {
		sink(ev, failed)
	}
}

// shared is the budgeted attempt's classifier: whether the shared request had
// a connection and whether its headers were written.
func (x *budgetedAttempt) shared(ev SharedAttemptEvent, _ bool) {
	x.mu.Lock()
	switch ev {
	case SharedGotConn:
		x.gotConn = true
	case SharedWroteHeaders:
		x.wrote = true
	}
	x.mu.Unlock()
}

// shared is the attempt trace's classifier: the phase flags cause() reads.
// It never sets getConn or firstByte, so a joined attempt has no reserve
// sample (freshAnswer).
func (t *attemptTrace) shared(ev SharedAttemptEvent, failed bool) {
	t.mu.Lock()
	switch ev {
	case SharedDNSStart:
		t.dnsOpen = true
	case SharedDNSDone:
		t.dnsOpen = false
		if failed {
			t.dnsErr = true
		}
	case SharedConnectStart:
		t.connectOpen = true
	case SharedConnectDone:
		t.connectOpen = false
		if failed {
			t.connectErr = true
		} else {
			t.connected = true
		}
	case SharedTLSStart:
		t.tlsOpen = true
	case SharedTLSDone:
		t.tlsOpen = false
		if failed {
			t.tlsErr = true
		}
	case SharedGotConn:
		t.connected = true
	}
	t.mu.Unlock()
}
