package api

import (
	"net/http"

	"github.com/krateo-platformops/plumbing/ptr"
)

// isWriteVerb reports whether a stage verb (nil = GET) can change state on the
// server. It is isSafeMethod's complement (stage_audit.go), the one write
// classifier of the resolver, so every rule that asks "is this a write?" —
// #902's audit, #903, #904, #905 — gives the same answer for the same verb.
func isWriteVerb(verb *string) bool {
	return !isSafeMethod(ptr.Deref(verb, http.MethodGet))
}

// isPlainGet reports whether a stage or call verb (nil = GET) is exactly GET:
// the only verb a read optimisation may serve or stand in for (#1011).
//
// Every read optimisation answers with what a GET would have returned: the
// cluster-list collapse (one cluster-scope GET LIST for an iterator's
// per-item calls), the content and list cells, the informer, internal and
// discovery dispatch, the in-process resolve. Each serves a call only when
// this holds, and the collapse admits a stage only when it holds, so a call
// is never answered or replaced by a request it is not:
//   - a write must reach its server: rewritten into a GET it is never sent,
//     and the stage reports success (#1011);
//   - a HEAD, OPTIONS or TRACE is safe but is not a GET: answered with a GET
//     body, the stage would get a response its request does not produce.
//
// The comparison is exact, as every dispatch gate's was before it: an empty or
// lower-case verb is not admitted, which can only cost an optimisation, never
// answer a request with the wrong response. isPlainGet implies !isWriteVerb;
// the converse does not hold.
func isPlainGet(verb *string) bool {
	return ptr.Deref(verb, http.MethodGet) == http.MethodGet
}
