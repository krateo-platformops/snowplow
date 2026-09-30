// Package util holds small, dependency-free helpers for parsing the /call
// HTTP request: the optional extras JSON context, the target GVR and
// namespaced name, call-path pagination, and an ETA formatter. It is a leaf
// package shared by the handlers and resolvers.
package util

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
)

// extrasCtxKey is the private context key under which a pre-decoded extras
// map is stashed (see WithExtras). #186: the POST /call/read body-decode
// middleware stashes the decoded `extras` here so a large extras input never
// has to ride the URL query — where it is charged against the gateway's
// request-header budget and 431s past ~16 KiB.
type extrasCtxKey struct{}

// WithExtras returns a copy of ctx carrying the given decoded extras map.
// Only the POST /call/read body-decode middleware calls this; every GET
// request leaves ctx untouched so ParseExtras stays on the query path and
// the historical behaviour is byte-identical.
func WithExtras(ctx context.Context, extras map[string]any) context.Context {
	return context.WithValue(ctx, extrasCtxKey{}, extras)
}

// ExtrasFromContext returns the stashed decoded extras map and true when a
// body-decode middleware put one on the context; (nil, false) otherwise.
func ExtrasFromContext(ctx context.Context) (map[string]any, bool) {
	m, ok := ctx.Value(extrasCtxKey{}).(map[string]any)
	return m, ok
}

// ParseExtras resolves the request's optional `extras` JSON context.
//
// #186 — CONTEXT-FIRST, QUERY-FALLBACK. When a body-decode middleware has
// stashed a decoded map on the request context (the POST /call/read path),
// that map is returned verbatim. Otherwise the historical query path runs
// (`?extras=<json>`), byte-identical to pre-#186 for every GET caller — an
// untouched context yields (nil, false) here, so /rbac and every other
// query-only caller are transparently unaffected.
//
// KEY PARITY is by construction: both channels converge on ONE decoded
// map[string]any, which folds through the SAME cache.HashExtras into the SAME
// ComputeKey — so a request carrying its extras in the body derives the
// identical L1 cell a query-supplied request would, and HITs it. The only
// requirement is that both channels json.Unmarshal to an identical map (same
// float64 numeric typing); they do, because the body middleware unmarshals the
// `extras` value with encoding/json exactly as this query path does.
func ParseExtras(req *http.Request) (res map[string]any, err error) {
	res = map[string]any{}

	if m, ok := ExtrasFromContext(req.Context()); ok {
		return m, nil
	}

	extrasParam := req.URL.Query().Get("extras")
	if extrasParam == "" {
		return
	}

	err = json.Unmarshal([]byte(extrasParam), &res)
	if err != nil {
		err = fmt.Errorf("invalid 'extras' parameter: %w", err)
		return
	}

	return
}
