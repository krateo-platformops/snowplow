package widgets

// extras_free.go — #777 (b′): a widget whose resolve has NO path from the
// request extras to its body.
//
// The request extras reach a widget body through exactly three spec surfaces
// (resolve.go, Resolve):
//   - spec.apiRef — resolveApiRefFn threads the extras (mergeRequestWins) into
//     the apiRef RESTAction, whose dict is SEEDED from them (a key equal to a
//     stage name is upstream data), whose templates read them, and which
//     forwards them to nested resolves and endpointRef dials;
//   - spec.widgetDataTemplate — mergeExtras(ds, opts.Extras) puts them in the
//     dict its jq evaluates against;
//   - spec.resourcesRefsTemplate (+ spec.resourcesRefsTemplateExtras) — the same
//     dict, through resourcesrefstemplate.Resolve.
//
// A widget carrying none of them renders a byte-identical body whatever extras
// a request sends, so the F6 undeclared-extras quarantine
// (dispatchers.requestExtrasFullyDeclared) protects nothing for it: caching it
// under the declared-only key cannot serve one request's extras-shaped body to
// another. TestB777_ExtrasConsumersAreEnumerated keeps the Go side of that list
// honest; ExtrasFreeSpecFields keeps the CRD side honest.

import "github.com/krateo-platformops/plumbing/maps"

// ExtrasFreeSpecFields is the CLOSED allowlist of top-level spec fields known to
// carry no path from the request extras to the body. A widget is exempt only if
// EVERY key of its spec is listed here, so a field added to the CRD later —
// which may be a new extras consumer — keeps the quarantine until someone
// reviews it in. Each entry, and why it is extras-free:
//   - widgetData: the static widget body (GetWidgetData returns the spec value
//     verbatim; only a widgetDataTemplate writes into it, from the extras dict).
//   - resourcesRefs: static child references; resourcesrefs.Resolve takes no
//     extras parameter (its signature is pinned by
//     TestB777_StaticResourcesRefsTakeNoExtras).
//   - keyExtras: declares which extras partition the cache key; it is read only
//     by the key derivation, never by the resolve.
//
// The live chrome widgets this exemption exists for (header-context,
// tenant-chip, krateo-platformops/portal helm/portal/templates) carry exactly
// these three fields. Read by tests; never mutated.
var ExtrasFreeSpecFields = map[string]struct{}{
	widgetDataKey:    {},
	resourcesRefsKey: {},
	keyExtrasKey:     {},
}

// WidgetReadsNoRequestExtras reports whether the widget's resolve has NO path
// from request extras to the body (#777 b′).
//
// PRESENCE, NOT PARSE: it reads the raw spec map, never the typed accessors.
// Those fail SOFT on a malformed field (GetWidgetDataTemplate returns an error
// and resolveWidgetData falls back to the static body), which here would turn
// "present but unreadable" into "absent" — the wrong direction for F6. Any spec
// key outside ExtrasFreeSpecFields (apiRef, widgetDataTemplate,
// resourcesRefsTemplate, resourcesRefsTemplateExtras, identityContext, or a
// field this list has never seen), even null or empty, means false; so does a
// missing or non-map spec.
func WidgetReadsNoRequestExtras(cr map[string]any) bool {
	spec, ok, err := maps.NestedMapNoCopy(cr, "spec") // keys only; never mutated

	if err != nil || !ok {
		return false
	}
	for k := range spec {
		if _, free := ExtrasFreeSpecFields[k]; !free {
			return false
		}
	}
	return true
}
