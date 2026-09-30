package api

import (
	"context"
	"log/slog"
	"net/http"
	"strings"

	httpcall "github.com/krateo-platformops/plumbing/http/request"
	"github.com/krateo-platformops/plumbing/jqutil"
	"github.com/krateo-platformops/plumbing/ptr"
	templates "github.com/krateo-platformops/snowplow/apis/templates/v1"
	"github.com/krateo-platformops/snowplow/internal/cache"
	jqsupport "github.com/krateo-platformops/snowplow/internal/support/jq"
	"k8s.io/apimachinery/pkg/util/validation"
)

// Ship 0.30.127: phase1IteratorCap (added 0.30.111) was DELETED. The cap
// truncated a `dependsOn.iterator` stage to its first 3 elements under a
// Phase-1 context — and that silently broke the Phase-1 navigation walk:
// the sidebar-nav-menu's apiRef RESTAction iterates a per-namespace
// navmenuitems LIST, and at bench scale the first 3 namespaces
// (bench-ns-01/02/03) hold ZERO navmenuitems — the real nav-menu-item-*
// CRs live in krateo-system, past the cap. The navmenu's
// resourcesRefsTemplate then expanded to zero children and the walk
// descended nothing past the roots (F2's warmed=2 defect, and a latent
// regression of #83). Iterator stages now expand FULLY; the storm guard
// for the expansion is the existing bounded errgroup
// (g.SetLimit(iterParallelism(ctx)), resolve.go) — no new mechanism.

func createRequestOptions(ctx context.Context, log *slog.Logger, in *templates.API, dict map[string]any) (all []httpcall.RequestOptions) {
	it := ""
	if in.DependsOn != nil {
		it = ptr.Deref(in.DependsOn.Iterator, "")
	}

	if len(it) == 0 {
		all = make([]httpcall.RequestOptions, 0, 1)
		el, ok, sr := createRequestOption(in, dict)
		if ok {
			all = append(all, el)
		} else {
			recordMalformedDialSkip(log, in.Name, el.Path, sr)
		}
		return
	}

	all = []httpcall.RequestOptions{}

	action := func(sa any) error {
		el, ok, sr := createRequestOption(in, sa)
		if !ok {
			recordMalformedDialSkip(log, in.Name, el.Path, sr)
			return nil
		}
		all = append(all, el)
		return nil
	}

	err := jqutil.ForEach(ctx, jqutil.EvalOptions{Query: it, Unquote: true, Data: dict}, action)
	if err != nil {
		if jqsupport.IsBenignNilIteration(err) {
			// Iterator walked a null/absent upstream value → zero request
			// options; the stage continues exactly as the empty-iterator case
			// (C-3). Data-dependent and benign — DEBUG, not the ERROR that would
			// flood the WARN-floor firehose on a healthy cluster.
			log.Debug("iterator yielded no items (nil upstream)", slog.String("query", it), slog.Any("err", err))
		} else {
			log.Error("unable to execute iterator", slog.String("query", it), slog.Any("err", err))
		}
	}

	return all
}

// recordMalformedDialSkip is the SINGLE shared skip path for the #288 guard,
// called at BOTH plan-append sites (the non-iterator single call and the
// iterator ForEach action). It COUNTS the skip (pm-1218 gate C1 — observable via
// snowplow_malformed_dial_skipped_total in /debug/vars, so an empty-interp spike
// can't hide as an invisible skip storm) and emits the per-event DEBUG for
// coordinate-shape triage, mirroring the C-3 empty-iterator precedent (a
// "nothing sensible to call" continue is benign + data-dependent — DEBUG, never
// the WARN-floor firehose).
func recordMalformedDialSkip(log *slog.Logger, name, path string, sr skipReason) {
	// Two-tier (pm-1217): the bounded CLASS key drives the per-reason metric (what
	// a spike alert needs — 3 fixed compile-time keys, no per-path cardinality); the
	// finer sub-reason DETAIL rides the DEBUG line for coordinate-shape triage.
	bumpMalformedDialSkipped(sr.class)
	log.Debug("skipping api call: "+sr.detail+" — not dialing",
		slog.String("name", name),
		slog.String("path", path),
		slog.String("reason_class", sr.class),
		slog.String("reason", sr.detail),
	)
}

// createRequestOption renders one api-call RequestOptions from the stage `in`
// and the per-call data `ds`. The second return is the #288 VALIDITY verdict:
// false means the rendered path is a malformed single-object apiserver path (an
// interpolated name/namespace segment collapsed to empty, or is DNS-1123-invalid
// — e.g. a trailing '-'), so the caller must NOT append/dial it. valid=true for
// well-formed paths AND for out-of-scope paths (external URLs, LIST paths) —
// byte-identical to pre-#288 behaviour.
func createRequestOption(in *templates.API, ds any) (out httpcall.RequestOptions, valid bool, sr skipReason) {
	out.ContinueOnError = ptr.Deref(in.ContinueOnError, false)
	out.ErrorKey = ptr.Deref(in.ErrorKey, "error")

	path, pathErr := evalJQE(in.Path, ds)
	out.Path = path
	out.Verb = ptr.To(ptr.Deref(in.Verb, http.MethodGet))

	if in.Payload != nil {
		out.Payload = ptr.To(evalJQ(*in.Payload, ds))
	}

	if in.Headers != nil {
		out.Headers = make([]string, 0, len(in.Headers))
		//copy(el.Headers, in.Headers)
		for _, h := range in.Headers {
			out.Headers = append(out.Headers, evalJQ(h, ds))
		}
	}

	if pathErr != nil {
		// #293 sub-case 2: the path jq expression ERRORED. evalJQE surfaces the
		// error (evalJQ would have masqueraded it as the path string and dialed it
		// as a garbage apiserver path → 404). Mark invalid so the caller SKIPS the
		// dial; the error text is surfaced in out.Path for the skip DEBUG only,
		// never dialed. Payload/header jq errors keep evalJQ's swallow behaviour
		// (out of #293 scope, different blast radius — deferred follow-up).
		out.Path = pathErr.Error()
		return out, false, skipReason{class: reasonJQPathError, detail: "path jq expression errored: " + pathErr.Error()}
	}
	valid, sr = validInterpolatedPath(out.Path, ptr.Deref(out.Verb, http.MethodGet))
	return
}

// validInterpolatedPath reports whether a rendered api-call path is safe to dial
// (#288). It guards ONLY single-object apiserver paths: it parses the rendered
// path and rejects the empty-interpolation shapes seen on 057 —
//   - a COLLAPSED empty namespace segment (`/api/v1/namespaces//configmaps/...`),
//     keyed on the literal "/namespaces//" (the DOUBLE slash), NOT merely ns==""
//     (which is ALSO true for a legit cluster-scoped namespace-by-name GET like
//     /api/v1/namespaces/kube-system) and NOT the general "//" (which would
//     false-reject a proxy subresource URL, e.g. /pods/p/proxy/http://host);
//   - a name/namespace segment that is present but DNS-1123-invalid (e.g. a
//     trailing '-'), via IsDNS1123Subdomain / IsDNS1123Label (VALIDATE, not
//     sanitize — sanitizing would dial a wrong-but-valid name and defeat #288);
//   - a collapsed trailing NAME on a MUTATING verb: PUT/PATCH with name=="" is
//     always a collapse (the apiserver 405s a nameless collection PUT/PATCH), and
//     a DELETE with name=="" that left a trailing "/" on the rendered path is a
//     collapsed by-name DELETE (vs a genuine collection DELETE written WITHOUT a
//     trailing slash) — without this a `DELETE .../rolebindings/${name}` with an
//     empty name would dial a COLLECTION delete (blast radius). The trailing-slash
//     check reads the RAW rendered path (query-stripped) BEFORE ParseAPIServerPath
//     ToDep's internal TrimRight erases it.
//
// Untouched (byte-identical to pre-#288): non-apiserver paths (external URLs,
// parseOK=false), nameless LIST (GET, name=="") and nameless create (POST,
// name==""), and every well-formed single-object path.
//
// Two residuals, both fail SAFE (reject rather than over-dial), documented per
// arch-1217 review:
//   - a collection DELETE deliberately written WITH a trailing slash
//     (`.../configmaps/`, no interpolation) is false-rejected; RAs write the
//     collection path without the trailing slash, so this is rare.
//   - a name that collapses BEFORE a subresource (`.../resource//subresource`, a
//     mid-path "//") is NOT caught — the targeted "/namespaces//" key deliberately
//     avoids the general "//" (proxy-URL edge above). Exotic; follow-up if seen.
//
// (Out of scope, filed #293: an UNRENDERED `${...}` / garbage path from an evalJQ
// error — parseOK=false, left to pre-#288 dial behaviour.)
func validInterpolatedPath(path, verb string) (bool, skipReason) {
	// #293 sub-case 1: an UNRENDERED "${" template (jqutil.MaybeQuery leaves it
	// literal on unbalanced braces) is never a dialable apiserver path. Reject it
	// before the parseOK=false early-return below (which would otherwise pass it
	// through as a legit external path). A genuine external URL never carries
	// "${", so external-dial behaviour is unchanged.
	if strings.Contains(path, "${") {
		return false, skipReason{class: reasonUnrenderedTemplate, detail: "unrendered ${...} template (#293)"}
	}
	_, ns, name, ok := cache.ParseAPIServerPathToDep(path)
	if !ok {
		// Not a single-object/list apiserver path we guard (external URL,
		// unresolved template, or a shape ParseAPIServerPathToDep rejects).
		return true, skipReason{}
	}

	// Inspect the RAW rendered path, query-stripped (mirror the parser's own
	// query strip) but NOT trailing-slash-trimmed — the collapse discriminators
	// below depend on the "//" and trailing "/" the parser would erase.
	p := path
	if i := strings.IndexByte(p, '?'); i >= 0 {
		p = p[:i]
	}

	// Collapsed NAMESPACE — the empty segment "/namespaces//" (blocker fix).
	if strings.Contains(p, "/namespaces//") {
		return false, skipReason{class: reasonEmptyInterp, detail: "collapsed empty namespace segment (#288)"}
	}
	// Present-but-invalid namespace / name segments.
	if ns != "" && len(validation.IsDNS1123Label(ns)) > 0 {
		return false, skipReason{class: reasonEmptyInterp, detail: "DNS-1123-invalid namespace segment (#288)"}
	}
	if name != "" && len(validation.IsDNS1123Subdomain(name)) > 0 {
		return false, skipReason{class: reasonEmptyInterp, detail: "DNS-1123-invalid name segment (#288)"}
	}

	// Collapsed trailing NAME on a mutating verb (safety-gap b). name=="" is a
	// legitimate nameless op ONLY for GET (list) and POST (collection create).
	if name == "" {
		switch verb {
		case http.MethodPut, http.MethodPatch:
			// No nameless update exists (apiserver 405s a collection PUT/PATCH),
			// so an empty name here is always a collapse.
			return false, skipReason{class: reasonEmptyInterp, detail: "collapsed empty name on a mutating verb (#288)"}
		case http.MethodDelete:
			// A collapsed by-name DELETE leaves a trailing "/" (`.../configmaps/`
			// from `".../configmaps/"+(.name)` with name==""); a genuine
			// collection DELETE is written `.../configmaps` (no trailing slash).
			if strings.HasSuffix(p, "/") {
				return false, skipReason{class: reasonEmptyInterp, detail: "collapsed by-name DELETE (empty name, trailing slash) (#288)"}
			}
		}
	}

	return true, skipReason{}
}

// evalJQE evaluates a jq template and SURFACES the Eval error instead of
// swallowing it into the output string. The api-call PATH render uses it so a
// failed jq expression is NOT dialed as a garbage apiserver path (#293
// sub-case 2). A non-template string (MaybeQuery !ok) returns unchanged with a
// nil error.
func evalJQE(q string, ds any) (string, error) {
	q, ok := jqutil.MaybeQuery(q)
	if !ok {
		return q, nil
	}

	out, err := jqutil.Eval(context.TODO(),
		jqutil.EvalOptions{
			Query:        q,
			Unquote:      true,
			Data:         ds,
			ModuleLoader: jqsupport.ModuleLoader(),
		})
	if err != nil {
		return "", err
	}
	return out, nil
}

// evalJQ preserves the pre-#293 contract (jq errors swallowed into the returned
// string) for the payload/header renders, which are out of #293 scope.
func evalJQ(q string, ds any) string {
	out, err := evalJQE(q, ds)
	if err != nil {
		return err.Error()
	}
	return out
}
