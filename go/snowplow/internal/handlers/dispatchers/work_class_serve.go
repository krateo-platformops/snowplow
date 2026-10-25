package dispatchers

// work_class_serve.go — #871 (measurement only): the work class of one
// resolve-path dispatch (restactions / widgets ServeHTTP). See
// internal/workclass for what the label is for.

import (
	"net/http"

	"github.com/krateo-platformops/snowplow/internal/workclass"
)

// beginServeWorkClass labels one restactions / widgets dispatch and returns the
// request carrying the label on its context, plus end (defer it). Called first
// in ServeHTTP, right after the inert re-seed, so every later read of
// req.Context() — the resolve, its stage goroutines, a fill leader's pprof.Do —
// keeps the label.
//
//   - A demand revalidation (#547, demand_revalidate.go) re-enters on a detached
//     copy of an already-served request: no user waits on it, so it is a FillBG
//     unit.
//   - A withdrawn stale serve's re-run (#710 PR-2b, stale_write_point.go) is the
//     same customer request re-entering, synchronously, inside its own unit: no
//     new unit, the label it already carries stays.
//   - A trusted HTTP self-loopback (isTrustedSelfLoopback) whose caller was a
//     background unit carries that unit's class in workclass.Header (resolve.go
//     egress): the hop continues the caller's unit — labelled, not counted. It
//     is still marked customer in flight below exactly as before: the label
//     changes nothing the dispatch does (the #871 pre-build check, P2 E2).
//   - Anything else is a Customer unit — the same population
//     markCustomerInFlight counts.
//
// Measurement only: nothing reads the label to decide anything.
func beginServeWorkClass(req *http.Request) (*http.Request, func()) {
	switch {
	case isStaleRerun(req):
		return req, func() {}
	case isDemandRevalidation(req):
		ctx, end := workclass.Begin(req.Context(), workclass.FillBG)
		return req.WithContext(ctx), end
	}
	if c := loopbackWorkClass(req); c.Background() {
		ctx, end := workclass.Continue(req.Context(), c)
		return req.WithContext(ctx), end
	}
	ctx, end := workclass.Begin(req.Context(), workclass.Customer)
	return req.WithContext(ctx), end
}

// loopbackWorkClass is the background class a trusted self-loopback carries in
// workclass.Header, else None. Like the depth and ancestor headers it is honoured
// only on a trusted self-loopback. A forged value can mislabel only the forging
// caller's own CPU samples; it is never an input to any decision.
func loopbackWorkClass(req *http.Request) workclass.Class {
	v := req.Header.Get(workclass.Header)
	if v == "" || !isTrustedSelfLoopback(req) {
		return workclass.None
	}
	c, ok := workclass.Parse(v)
	if !ok || !c.Background() {
		return workclass.None
	}
	return c
}
