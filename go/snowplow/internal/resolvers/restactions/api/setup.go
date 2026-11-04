package api

import (
	"context"
	"errors"
	"fmt"
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

// skippedOption is one request option the #288 dial guard refused, carried with
// the verdict that refused it. The DISPATCH path throws these away — not dialing
// is the whole point. The dispatch-free read-set enumeration must see them,
// because "do not dial this" and "cannot enumerate this" are different
// questions with different right answers (#504).
type skippedOption struct {
	path   string
	reason skipReason
}

// createRequestOptions returns the valid options of a stage that dials this
// cluster's apiserver (the #288 shape guard applies) and drops the refused ones
// into recordMalformedDialSkip. Tests use it; the resolve stage loop calls
// createRequestOptionsWithSkips with the stage's own target, and the inspect
// pass (#504) with apiserver=true.
func createRequestOptions(ctx context.Context, log *slog.Logger, in *templates.API, dict map[string]any) (all []httpcall.RequestOptions) {
	all, _, _ = createRequestOptionsWithSkips(ctx, log, in, dict, true)
	return all
}

// createRequestOptionsChecked is createRequestOptions plus the iterator's
// compile/eval error (#638): a non-benign iterator error is a STAGE error
// (stage-error sink bumped so no L1 Put accepts the partial/empty plan, outcome
// Error, dict[errorKey] written), never a log line the stage silently continues
// past.
func createRequestOptionsChecked(ctx context.Context, log *slog.Logger, in *templates.API, dict map[string]any) ([]httpcall.RequestOptions, error) {
	all, _, iterErr := createRequestOptionsWithSkips(ctx, log, in, dict, true)
	return all, iterErr
}

// createRequestOptionsWithSkips is createRequestOptions plus the refused
// options and their verdicts, and the stage's target. The resolve stage loop
// reads the skips of a write stage (#1112 D2: each is a stage failure with its
// reason, never a silent success), and the inspect pass classifies them (#504).
// The metrics/log side effect (recordMalformedDialSkip) happens here, exactly
// once per skip, so the #288/#293 counters read the same from every caller.
//
// apiserver (#1112 D2) is true when the stage dials this cluster's apiserver
// (targetsOwnCluster: the ServiceAccount or the requester's own clientconfig):
// only then does the #288 SHAPE guard judge the rendered path, because only
// then is it known to be a Kubernetes object path. A named endpointRef names
// another endpoint, whose paths may parse as apiserver paths without being one
// (`/api/v2/tickets/<id>` on a ticketing API, with ids that are not DNS-1123
// names). The template checks (an unrendered ${...}, a jq error in the path,
// payload or header) apply to every stage, whatever it dials.
//
// iterErr (#638) is the iterator's compile/eval error: nil for a non-iterator
// stage and for a benign nil-upstream iteration (IsBenignNilIteration).
func createRequestOptionsWithSkips(ctx context.Context, log *slog.Logger, in *templates.API, dict map[string]any, apiserver bool) (all []httpcall.RequestOptions, skips []skippedOption, iterErr error) {
	it := ""
	if in.DependsOn != nil {
		it = ptr.Deref(in.DependsOn.Iterator, "")
	}

	if len(it) == 0 {
		all = make([]httpcall.RequestOptions, 0, 1)
		el, ok, sr := createRequestOption(in, dict, apiserver)
		if ok {
			all = append(all, el)
		} else {
			recordMalformedDialSkip(log, in.Name, el.Path, sr)
			skips = append(skips, skippedOption{path: el.Path, reason: sr})
		}
		return
	}

	all = []httpcall.RequestOptions{}

	// #638 — the iterator is evaluated by jqsupport.IterateValues: EXACTLY the
	// pre-#638 jqutil.ForEach element semantics (one array → its elements; a
	// single null or no value → the C-3 empty plan), with runtime errors
	// returned with the value preview REMOVED. Anything else (a non-array, or
	// jq's stream form) is an error the stage now FAILS with; pre-#638 it was
	// logged and the stage silently dispatched nothing.
	elems, err := jqsupport.IterateValues(ctx, it, dict, nil)
	if err != nil {
		if errors.Is(err, jqsupport.ErrIteratorNoValue) {
			// The iterator read a null or no value: the upstream produced
			// nothing. Same empty plan as pre-#638 (which logged it at ERROR
			// as "must return a JSON array"); benign — DEBUG.
			log.Debug("iterator yielded no items (null or no value)", slog.String("query", it))
		} else if jqsupport.IsBenignNilIteration(err) {
			// Iterator walked a null/absent upstream value → zero request
			// options; the stage continues exactly as the empty-iterator case
			// (C-3). Data-dependent and benign — DEBUG, not the ERROR that would
			// flood the WARN-floor firehose on a healthy cluster.
			log.Debug("iterator yielded no items (nil upstream)", slog.String("query", it), slog.Any("err", err))
		} else {
			// err is query-derived (parse/compile) or a sanitized runtime
			// error: no response bytes reach the log or the stage error.
			log.Error("unable to execute iterator", slog.String("query", it), slog.Any("err", err))
			iterErr = fmt.Errorf("iterator %q failed: %w", it, err)
		}
		return all, skips, iterErr
	}

	for _, sa := range elems {
		el, ok, sr := createRequestOption(in, sa, apiserver)
		if !ok {
			recordMalformedDialSkip(log, in.Name, el.Path, sr)
			skips = append(skips, skippedOption{path: el.Path, reason: sr})
			continue
		}
		all = append(all, el)
	}

	return all, skips, iterErr
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
// and the per-call data `ds`. The second return is the VALIDITY verdict: false
// means the call must NOT be appended or dialled — a template failed (#293
// unrendered ${...}, #302 a jq error in the path, payload or header), or, on an
// apiserver stage (apiserver=true), the rendered path is a malformed
// single-object apiserver path (#288: an interpolated name/namespace segment
// collapsed to empty, or is DNS-1123-invalid — e.g. a trailing '-').
// valid=true for well-formed paths AND for out-of-scope paths (external URLs,
// LIST paths).
func createRequestOption(in *templates.API, ds any, apiserver bool) (out httpcall.RequestOptions, valid bool, sr skipReason) {
	out.ContinueOnError = ptr.Deref(in.ContinueOnError, false)
	out.ErrorKey = ptr.Deref(in.ErrorKey, "error")

	path, pathErr := evalJQE(in.Path, ds)
	out.Path = path
	out.Verb = ptr.To(ptr.Deref(in.Verb, http.MethodGet))

	// #302 — payload/header render through evalJQE too, so a jq eval error is
	// SURFACED (not swallowed into the value by evalJQ). A swallowed error would
	// be dialed as a garbage request BODY, or a garbage HEADER value (e.g.
	// Authorization); on error we SKIP the dial below, mirroring the path limb.
	// out.Payload / out.Headers hold the evalJQE result ("" on error) — never the
	// error text — and are not dialed at all on the skip path.
	var payloadErr, headerErr error
	if in.Payload != nil {
		var pv string
		pv, payloadErr = evalJQE(*in.Payload, ds)
		out.Payload = ptr.To(pv)
	}

	if in.Headers != nil {
		out.Headers = make([]string, 0, len(in.Headers))
		for _, h := range in.Headers {
			hv, herr := evalJQE(h, ds)
			if herr != nil && headerErr == nil {
				headerErr = herr // first erroring header wins the skip reason
			}
			out.Headers = append(out.Headers, hv)
		}
	}

	if pathErr != nil {
		// #293 sub-case 2: the path jq expression ERRORED. evalJQE surfaces the
		// error (evalJQ would have masqueraded it as the path string and dialed it
		// as a garbage apiserver path → 404). Mark invalid so the caller SKIPS the
		// dial; the error text is surfaced in out.Path for the skip DEBUG only,
		// never dialed.
		out.Path = pathErr.Error()
		return out, false, skipReason{class: reasonJQPathError, detail: "path jq expression errored: " + pathErr.Error()}
	}
	// #302 — a payload or header jq error must NEVER be dialed (a garbage request
	// body, or a garbage Authorization/header value). Skip the dial, mirroring the
	// path limb; the error text rides skipReason.detail (the DEBUG), never the
	// wire. Precedence: path first (it decides dialability), then payload, then
	// header.
	if payloadErr != nil {
		return out, false, skipReason{class: reasonJQPayloadError, detail: "payload jq expression errored: " + payloadErr.Error()}
	}
	if headerErr != nil {
		return out, false, skipReason{class: reasonJQHeaderError, detail: "header jq expression errored: " + headerErr.Error()}
	}
	if !apiserver {
		// #1112 D2 — a named endpoint: the collapse refusal applies (a missing
		// segment is malformed for any REST endpoint, and on a spoke apiserver
		// reached through a named ref an empty namespace would widen the read),
		// but not the Kubernetes name-validity check (another API's ids are not
		// DNS-1123 names).
		valid, sr = collapsedPath(out.Path, ptr.Deref(out.Verb, http.MethodGet))
		return
	}
	valid, sr = validInterpolatedPath(out.Path, ptr.Deref(out.Verb, http.MethodGet))
	return
}

// unrenderedTemplate reports whether a rendered path still carries a "${"
// template (#293 sub-case 1: jqutil.MaybeQuery leaves it literal on unbalanced
// braces). Never a dialable path, whatever the endpoint: a genuine URL never
// carries "${".
func unrenderedTemplate(path string) bool { return strings.Contains(path, "${") }

// unrenderedTemplateSkip is the verdict on an unrendered template.
var unrenderedTemplateSkip = skipReason{class: reasonUnrenderedTemplate, detail: "unrendered ${...} template (#293)"}

// collapsedPath reports whether a rendered api-call path still has every
// segment its template meant to fill: the refusal that applies to EVERY dial,
// this cluster's apiserver and a named endpoint alike (#288, split by #1112 D2).
// A missing segment is malformed for any REST endpoint, and the shapes refused
// here are the ones whose dial would silently mean something wider:
//   - an UNRENDERED "${" template (#293 sub-case 1; a genuine URL never carries
//     "${");
//   - a COLLAPSED empty namespace segment (`/api/v1/namespaces//configmaps/...`),
//     keyed on the literal "/namespaces//" (the DOUBLE slash), NOT merely ns==""
//     (which is ALSO true for a legit cluster-scoped namespace-by-name GET like
//     /api/v1/namespaces/kube-system) and NOT the general "//" (which would
//     false-reject a proxy subresource URL, e.g. /pods/p/proxy/http://host). On a
//     spoke apiserver reached through a named ref (cluster-compositions'
//     `<name>-endpoint`) it would otherwise read every namespace;
//   - a collapsed trailing NAME on a mutating verb: an apiserver-shaped path
//     whose last segment rendered empty, leaving a trailing "/"
//     (`DELETE .../rolebindings/` from `".../rolebindings/" + .name`): dialled,
//     a by-name DELETE becomes a COLLECTION delete (blast radius). The check
//     reads the RAW rendered path (query-stripped) BEFORE ParseAPIServerPathToDep's
//     internal TrimRight erases the slash.
//
// Two residuals, both fail SAFE (reject rather than over-dial):
//   - a collection write deliberately written WITH a trailing slash
//     (`.../configmaps/`, no interpolation) is false-rejected; RAs write the
//     collection path without the trailing slash, so this is rare;
//   - a name that collapses BEFORE a subresource (`.../resource//subresource`, a
//     mid-path "//") is NOT caught — the targeted "/namespaces//" key deliberately
//     avoids the general "//" (proxy-URL edge above).
func collapsedPath(path, verb string) (bool, skipReason) {
	if unrenderedTemplate(path) {
		return false, unrenderedTemplateSkip
	}
	p := rawPath(path)
	if strings.Contains(p, "/namespaces//") {
		return false, skipReason{class: reasonEmptyInterp, detail: "collapsed empty namespace segment (#288)", collapsed: true}
	}
	switch verb {
	case http.MethodPut, http.MethodPatch, http.MethodDelete:
		if _, _, name, ok := cache.ParseAPIServerPathToDep(path); ok && name == "" && strings.HasSuffix(p, "/") {
			return false, skipReason{class: reasonEmptyInterp, detail: "collapsed empty name on a mutating verb (empty last segment, trailing slash) (#288)", collapsed: true}
		}
	}
	return true, skipReason{}
}

// rawPath is path without its query and fragment (#1073 F1: splitPathQuery,
// the parser's own split), but NOT trailing-slash-trimmed, which would erase
// the collapse discriminators.
func rawPath(path string) string {
	p, _, _ := splitPathQuery(path)
	return p
}

// validInterpolatedPath reports whether a rendered api-call path is safe to dial
// on THIS cluster's apiserver (#288): collapsedPath, plus the checks that hold
// only for a Kubernetes object path, so only for an own-cluster dial (#1112 D2:
// on a named endpoint they refused another API's ids, `/api/v2/tickets/Ab_12`):
//   - a name/namespace segment that is present but DNS-1123-invalid (e.g. a
//     trailing '-'), via IsDNS1123Subdomain / IsDNS1123Label (VALIDATE, not
//     sanitize — sanitizing would dial a wrong-but-valid name and defeat #288);
//   - PUT/PATCH with name=="" even without a trailing slash: the apiserver has
//     no nameless update (it 405s a collection PUT/PATCH), so an empty name is
//     always a collapse.
//
// Untouched: non-apiserver paths (parseOK=false), nameless LIST (GET,
// name=="") and nameless create (POST, name==""), and every well-formed
// single-object path.
func validInterpolatedPath(path, verb string) (bool, skipReason) {
	if ok, sr := collapsedPath(path, verb); !ok {
		return false, sr
	}
	_, ns, name, ok := cache.ParseAPIServerPathToDep(path)
	if !ok {
		// Not a single-object/list apiserver path we guard (external URL,
		// unresolved template, or a shape ParseAPIServerPathToDep rejects).
		return true, skipReason{}
	}
	if ns != "" && len(validation.IsDNS1123Label(ns)) > 0 {
		return false, skipReason{class: reasonEmptyInterp, detail: "DNS-1123-invalid namespace segment (#288)"}
	}
	if name != "" && len(validation.IsDNS1123Subdomain(name)) > 0 {
		return false, skipReason{class: reasonEmptyInterp, detail: "DNS-1123-invalid name segment (#288)"}
	}
	if name == "" && (verb == http.MethodPut || verb == http.MethodPatch) {
		return false, skipReason{class: reasonEmptyInterp, detail: "collapsed empty name on a mutating verb (#288)"}
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
		// #638 — a runtime error carries a preview of the evaluated value (the
		// upstream response); the skip reasons and stage errors built from it
		// are logged. Sanitize: query-derived errors pass, runtime errors keep
		// only their class.
		return "", jqsupport.SanitizeError(err)
	}
	return out, nil
}

// evalJQ is the pre-#293 jq render that SWALLOWS an Eval error into the returned
// string (err.Error()). As of #302 (payload/header → evalJQE) and #341
// (cluster_list GVR-probe + endpointRef.name → evalJQE) it has ZERO PRODUCTION
// callers — every prod render now uses evalJQE and SURFACES the error. It is
// retained TEST-ONLY as three fixtures: the #293 swallow-contract oracle
// (TestFalsifier293_EvalJQErrorContract), the #341 byte-parity oracle
// (TestFalsifier341_EndpointRefName_JQError_Surfaced), and the empty-interp
// render helper (setup_empty_interp_guard_test). DO NOT add a production caller:
// routing a new render through this reintroduces the exact swallow #302/#341
// removed — use evalJQE and surface the error.
func evalJQ(q string, ds any) string {
	out, err := evalJQE(q, ds)
	if err != nil {
		return err.Error()
	}
	return out
}
