package api

// needs_request_extras.go — #1477 (generalises #113): does a resolve of this
// RESTAction, carrying these extras, need request extras it does not carry?
//
// A stage's endpointRef.name may be a jq template (#113, hub-spoke): the
// resolver renders it against the resolve dict (evalEndpointRef). The dict is
// the extras the resolve carries plus what the resolver writes itself (each
// stage's output under its name, each errorKey, the pagination block:
// ReservedDictKeys, which no extras may name, #1116). A template that reads a
// dict key outside both sets renders against a key nobody supplied: the name
// normalises to a Secret that is not the one any customer's request would
// select, the endpoint read misses, and the stage truncates. No customer key
// can match the cell such a resolve produces, because a customer request that
// renders the template carries the key, and the cache keys fold the extras.
//
// So a background resolve (a seed, the discovery walk, the content prewarm)
// that would render such a template is useless work, and possibly a truncated
// body: its caller skips it. The decision reads only the declared template and
// the extras the resolve carries; never a resource, RESTAction or widget name.

import (
	"github.com/krateo-platformops/plumbing/jqutil"
	templates "github.com/krateo-platformops/snowplow/apis/templates/v1"
	jqsupport "github.com/krateo-platformops/snowplow/internal/support/jq"
)

// NeedsRequestExtras reports whether some api step of items has a templated
// endpointRef.name that reads a dict key neither carried (the extras the
// resolve will carry) nor written by the resolver itself. A template whose
// reads cannot be determined (jqsupport.RootKeys ok=false: it does not parse,
// or reads its input as a whole or through a function) counts as needing
// extras: rendered with none, it is what #113 skips.
//
// A nil step and a nil endpointRef (the stage dials the caller's own
// clientconfig or the ServiceAccount) need nothing; nor does a literal name.
func NeedsRequestExtras(items []*templates.API, carried map[string]any) bool {
	var reserved map[string]ReservedKey
	for _, step := range items {
		if step == nil || step.EndpointRef == nil {
			continue
		}
		q, isTemplate := jqutil.MaybeQuery(step.EndpointRef.Name)
		if !isTemplate {
			continue
		}
		keys, ok := jqsupport.RootKeys(q)
		if !ok {
			return true
		}
		if reserved == nil {
			reserved = ReservedDictKeys(items)
		}
		for _, k := range keys {
			if _, written := reserved[k]; written {
				continue
			}
			if _, has := carried[k]; !has {
				return true
			}
		}
	}
	return false
}
