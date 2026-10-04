// Package handlers holds snowplow's top-level HTTP handlers. It exposes the
// primary /call endpoint (which dispatches resource requests to the
// per-GVR resolvers via the dispatchers proxy), the /health and /readyz
// probes, and supporting endpoints for listing, jq evaluation, conversion,
// and plural lookups.
package handlers

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"path"
	"sort"
	"strconv"
	"strings"
	"time"

	xcontext "github.com/krateo-platformops/plumbing/context"
	"github.com/krateo-platformops/plumbing/env"
	"github.com/krateo-platformops/plumbing/http/request"
	"github.com/krateo-platformops/plumbing/http/response"
	"github.com/krateo-platformops/plumbing/ptr"
	"github.com/krateo-platformops/snowplow/internal/cache"
	"github.com/krateo-platformops/snowplow/internal/dynamic"
	"github.com/krateo-platformops/snowplow/internal/handlers/util"
	"github.com/krateo-platformops/snowplow/internal/support/audit"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
)

func Call() http.Handler {
	return &callHandler{
		authnNS: env.String("AUTHN_NAMESPACE", ""),
		verbose: env.True("DEBUG"),
		// Issue #156: scope resolution is consulted ONLY when a /call
		// request omits `namespace` (a 400 today). The default backs the
		// resolver with the process-wide SA discovery singleton
		// (dynamic.SharedSAScopeForGVR) — no rc threading through main.go's
		// ~6 Call() mounts. Tests inject a fake via CallWithScopeResolver /
		// the export_test seam so hermetic cases never touch a real mapper.
		// The SA mapper is warmed at boot (Phase 1) and memoized, so a
		// cluster-scoped /call inherits that warmup; only a boot-window first
		// namespace-absent call can pay discovery synchronously, and it is
		// self-healing (CRD add/update/delete invalidates via mapper.Reset()).
		scopeResolver: dynamic.SharedSAScopeForGVR,
	}
}

// CallRead is the read-only /call handler for the POST /call/read
// body-carrying read route (#186). It is the ReadDispatcher's terminal
// fallthrough: a POST /call/read addressing a GVR that has no resolve handler
// lands here. Being read-only, it FORCES the apiserver verb to GET and NEVER
// forwards the request body as an object payload — so the body-carrying read
// route can never create or mutate a resource, even on a dispatch miss. Every
// other field (auth, scope, addressing) is identical to Call().
func CallRead() http.Handler {
	return &callHandler{
		authnNS:       env.String("AUTHN_NAMESPACE", ""),
		verbose:       env.True("DEBUG"),
		scopeResolver: dynamic.SharedSAScopeForGVR,
		readOnly:      true,
	}
}

// CallDryRun is the /call/dry-run write handler (#443): the twin of CallRead.
// It is mounted ONLY on POST|PUT|PATCH /call/dry-run, behind the same
// middleware chain as the write-verb /call routes, and it always sends the
// apiserver dryRun=All, so the apiserver validates and admits the request and
// persists nothing. It has no dispatcher in front of it, so it never touches
// the L1 cache, the informers or the refresher.
//
// The route is separate from /call on purpose. A pre-#443 snowplow forwards
// only page/perPage, so it would treat POST /call?dryRun=All as a REAL
// create. It has no /call/dry-run pattern, so during a rolling deploy an old
// pod answers a dry-run with 404 and writes nothing.
func CallDryRun() http.Handler {
	return &callHandler{
		authnNS:       env.String("AUTHN_NAMESPACE", ""),
		verbose:       env.True("DEBUG"),
		scopeResolver: dynamic.SharedSAScopeForGVR,
		dryRun:        true,
	}
}

var _ http.Handler = (*callHandler)(nil)

// scopeResolverFn resolves whether a GVR is namespace-scoped. It returns
// namespaced=true for a namespaced resource, false for a cluster-scoped
// one, and a non-nil error when scope cannot be determined (unknown GVR,
// mapper not synced). The /call handler FAILS-CLOSED (400) on error — it
// NEVER guesses a scope on a write path.
type scopeResolverFn func(gvr schema.GroupVersionResource) (namespaced bool, err error)

type callHandler struct {
	authnNS string
	verbose bool
	// scopeResolver is consulted ONLY on the namespace-absent branch of
	// validateRequest (Issue #156). The namespace-present path is
	// byte-identical to pre-#156 behaviour and NEVER calls this — so a
	// cold/erroring resolver can never regress a currently-working
	// namespaced request.
	scopeResolver scopeResolverFn
	// readOnly marks the POST /call/read handler (#186 — handlers.CallRead).
	// When set, validateRequest FORCES opts.verb to GET and SKIPS reading the
	// request body into opts.dat, so ServeHTTP never sets callOpts.Payload and
	// the apiserver call is an unconditional read — the body-carrying read
	// route cannot create/mutate a resource. Zero value (false) is the
	// historical Call() write-capable behaviour, byte-identical.
	readOnly bool
	// dryRun marks the /call/dry-run handler (#443 — handlers.CallDryRun).
	// When set, the outbound apiserver request always carries dryRun=All and
	// only the write verbs POST/PUT/PATCH are accepted. Zero value (false) is
	// the plain /call, where an inbound dryRun is a 400.
	dryRun bool
}

// @Summary Call Endpoint
// @Description Handle Resources
// @ID call
// @Param  apiVersion       query   string  true  "Resource API Group and Version"
// @Param  resource         query   string  true  "Resource Plural"
// @Param  name             query   string  true  "Resource name"
// @Param  namespace        query   string  false "Resource namespace (required for namespaced resources; omit for cluster-scoped, e.g. ClusterRole/Node — issue #156)"
// @Param  page             query   string  false "Pagination desired page"
// @Param  perPage          query   string  false "Pagination desired per page items"
// @Param  extras           query   string  false "JSON encoded map of extra params"
// @Param data body string false "Object"
// @Produce  json
// @Success 200 {object} map[string]any
// @Failure 400 {object} response.Status
// @Failure 401 {object} response.Status
// @Failure 404 {object} response.Status
// @Failure 500 {object} response.Status
// @Router /call [get]
// @Router /call [post]
// @Router /call [put]
// @Router /call [patch]
// @Router /call [delete]
func (r *callHandler) ServeHTTP(wri http.ResponseWriter, req *http.Request) {
	opts, err := r.validateRequest(req)
	if err != nil {
		response.BadRequest(wri, err)
		return
	}

	uri, err := buildURIPath(opts)
	if err != nil {
		response.InternalError(wri, err)
		return
	}

	log := xcontext.Logger(req.Context())

	start := time.Now()

	ep, err := xcontext.UserConfig(req.Context())
	if err != nil {
		log.Error("unable to get user endpoint", slog.Any("err", err))
		response.Unauthorized(wri, err)
		return
	}
	ep.Debug = r.verbose

	log.Debug("user config succesfully loaded", slog.Any("endpoint", ep))

	// #443 — the echo headers say what the apiserver call actually carried.
	// They are read back from the BUILT outbound URI (not from the inbound
	// query), and set before the first byte of any response is written, so
	// they ride both a 2xx and an apiserver failure. A validation 400 returns
	// above, before this point, and carries none of them.
	setCallEchoHeaders(wri.Header(), uri, opts)

	dict := map[string]any{}
	callOpts := request.RequestOptions{
		RequestInfo: request.RequestInfo{
			Path: uri,
			Verb: ptr.To(strings.ToUpper(opts.verb)),
			Headers: []string{
				"Accept: application/json",
			},
		},
		Endpoint:        &ep,
		ResponseHandler: callResponseHandler(dict),
	}
	if opts.dat != nil && has([]string{http.MethodPost, http.MethodPut, http.MethodPatch}, opts.verb) {
		callOpts.Headers = append(callOpts.Headers,
			fmt.Sprintf("Content-Type: %s", opts.contentType),
		)
		callOpts.Payload = ptr.To(string(opts.dat))
	}

	// Ship D (0.30.141) — F-1: handlers.Call() is the dispatcher's
	// fallthrough lane for GVR groups not in the
	// dispatchers.All() map (every "raw apiserver passthrough" /call).
	// Record BEFORE request.Do so a panicking plumbing call still
	// counts (AC-D.3 ordering).
	//
	// #443 — a raw read (raw=true) of a restactions / widgets object is a
	// deliberate skip of the resolver, so it is recorded under its own reason
	// and is never confused with an unhandled-GVR passthrough.
	reason := cache.ReasonClientBuild
	if opts.raw {
		reason = cache.ReasonRawRead
	}
	cache.RecordApiserverFallthrough(req.Context(), reason, "")
	rt := request.Do(req.Context(), callOpts)

	// Audit correlation: every WRITE through /call emits a
	// normalized AuditEvent carrying the request correlation id (see
	// internal/support/audit) so a portal action is linkable to the
	// object it mutated. Reads are deliberately not audited here (volume;
	// they are already covered by the request log + trace id).
	if has([]string{http.MethodPost, http.MethodPut, http.MethodPatch, http.MethodDelete}, opts.verb) {
		outcome, code, msg := "success", http.StatusOK, ""
		if rt.Status == response.StatusFailure {
			outcome, code, msg = "failure", rt.Code, rt.Message
		}
		action := "call"
		if opts.dryRun {
			action = "call.dryRun"
		}
		audit.Emit(req.Context(), audit.Event{
			Action:      action,
			Verb:        strings.ToUpper(opts.verb),
			Group:       opts.gvr.Group,
			Version:     opts.gvr.Version,
			Resource:    opts.gvr.Resource,
			Subresource: opts.subresource,
			Name:        opts.nsn.Name,
			Namespace:   opts.nsn.Namespace,
			Outcome:     outcome,
			Code:        code,
			Message:     msg,
		})
	}

	if rt.Status == response.StatusFailure {
		log.Error("unable to call endpoint",
			slog.String("verb", strings.ToUpper(opts.verb)),
			slog.String("uri", uri),
			slog.String("err", rt.Message))
		response.Encode(wri, rt)
		return
	}

	log.Info("endpoint call done",
		slog.String("verb", strings.ToUpper(opts.verb)),
		slog.String("uri", uri),
		slog.String("duration", util.ETA(start)),
	)

	wri.Header().Set("Content-Type", "application/json")
	wri.WriteHeader(http.StatusOK)

	enc := json.NewEncoder(wri)
	enc.SetIndent("", "  ")
	if err := enc.Encode(dict); err != nil {
		log.Error("unable to serve api call response", slog.Any("err", err))
	}
}

func (r *callHandler) validateRequest(req *http.Request) (opts callOptions, err error) {
	// #443 part 2 — an inline object is resolved only by the RESTAction
	// handler. BodyExtrasDecode already refuses one for any other GVR; this is
	// the backstop for one that still reaches the passthrough (e.g. with
	// raw=true), so a stored object is never read in place of a draft.
	if _, inline := util.InlineObject(req.Context()); inline {
		err = fmt.Errorf("an inline object is resolved only for resource=restactions, without raw")
		return
	}
	opts.verb = req.Method
	// #186 — the read-only /call/read handler forces GET regardless of the
	// inbound method (always POST on that route) so a body-carrying READ can
	// never turn into an apiserver create/update. The body is deliberately NOT
	// read into opts.dat below (see the readOnly guard on the body read), so
	// callOpts.Payload stays nil in ServeHTTP and no audit WRITE is emitted.
	if r.readOnly {
		opts.verb = http.MethodGet
	}
	if has([]string{http.MethodPost, http.MethodPut, http.MethodPatch}, opts.verb) {
		opts.contentType = req.Header.Get("Content-type")
		if opts.contentType == "" {
			opts.contentType = "application/json"
		}
	}

	opts.gvr, err = util.ParseGVR(req)
	if err != nil {
		return
	}

	// Issue #156 — scope-aware validation. We must NOT call the shared
	// util.ParseNamespacedName here: it hard-rejects an empty `namespace`
	// before scope is known (nsn.go:18), which is exactly the
	// cluster-scoped write we now want to serve. Read name/namespace
	// directly and apply the /call-LOCAL rule below. (util.ParseNamespacedName
	// stays byte-unchanged for its other prod caller, dispatchers/helpers.go:38.)
	name := req.URL.Query().Get("name")
	namespace := req.URL.Query().Get("namespace")

	if namespace != "" {
		// namespace PRESENT → today's namespaced path, BYTE-IDENTICAL, and
		// we deliberately do NOT consult the scope mapper: a namespaced
		// write must not gain a boot-window / discovery-lag failure mode.
		// Preserve the exact pre-#156 name-required check (nsn.go:12-14):
		// util.ParseNamespacedName required a non-empty name for EVERY
		// verb, so keep that (POST-without-name 400s today → still 400s).
		if name == "" {
			err = fmt.Errorf("missing 'name' query parameter")
			return
		}
		opts.namespaced = true
		opts.nsn = types.NamespacedName{Name: name, Namespace: namespace}
	} else {
		// namespace ABSENT → this 400s today (util.ParseNamespacedName
		// rejects the empty namespace). NOW resolve scope: only take the
		// new cluster-scoped path on a POSITIVE cluster-scope. Everything
		// else (namespaced GVR, mapper miss/error/unknown) FAILS-CLOSED
		// with a 400 — exactly as /call returns today — never a silent
		// namespaced fallback and never a panic.
		var namespaced bool
		namespaced, err = r.resolveScope(opts.gvr)
		if err != nil {
			// Scope unknown (CRD not yet discovered, mapper not synced,
			// ambiguous resource). Fail-closed: 400, same family as today's
			// missing-namespace 400. Forces a retry once discovery settles.
			err = fmt.Errorf("unable to resolve scope for resource %q (namespace omitted): %w", opts.gvr.Resource, err)
			return
		}
		if namespaced {
			// Genuinely namespaced GVR but no namespace supplied →
			// backward-compatible 400 (byte-identical intent to today's
			// missing-'namespace' rejection).
			err = fmt.Errorf("missing 'namespace' query parameter")
			return
		}
		// cluster-scoped GVR → the new capability. name is required for
		// by-name verbs (GET/PUT/PATCH/DELETE); POST create may omit it
		// (apiserver assigns / uses metadata.name in the body). buildURIPath
		// omits the namespaces/<ns> segment when namespaced==false.
		if name == "" && has([]string{http.MethodGet, http.MethodPut, http.MethodPatch, http.MethodDelete}, opts.verb) {
			err = fmt.Errorf("missing 'name' query parameter")
			return
		}
		opts.namespaced = false
		opts.nsn = types.NamespacedName{Name: name} // Namespace deliberately empty
	}

	// #282 — a subresource (e.g. "status", "scale") addresses
	// .../{name}/<subresource>. A subresource lives on a NAMED object, so it
	// requires a by-name verb (GET/PUT/PATCH/DELETE); a POST (collection
	// create) has no subresource target and is rejected. The by-name verbs
	// already require a non-empty name in both branches above, so a valid
	// subresource request always has a name to hang it on. RBAC is unchanged:
	// the request carries the USER's credentials to request.Do, so the
	// apiserver enforces the DISTINCT subresource RBAC verb natively (a plain
	// grant on the resource does NOT authorize its <subresource>).
	//
	// Scope note (reviewers): for #282 — a CR status is GET/PUT/PATCH/DELETE —
	// the by-name-verb rule is exact and fails CLOSED (a nameless GET carrying a
	// subresource is already a 400 via the name-required checks in both branches
	// above, so a malformed .../<subresource> path is never built). A FUTURE
	// by-name POST subresource (e.g. pods/eviction, serviceaccounts/token,
	// pods/binding) would relax this to "requires a non-empty name", not
	// "rejects POST" — those are by-name POSTs. Out of scope here.
	opts.subresource = req.URL.Query().Get("subresource")
	if opts.subresource != "" && !has([]string{http.MethodGet, http.MethodPut, http.MethodPatch, http.MethodDelete}, opts.verb) {
		err = fmt.Errorf("subresource %q requires a by-name verb (GET/PUT/PATCH/DELETE); it cannot be used with %s", opts.subresource, opts.verb)
		return
	}

	// #443 — dryRun, fieldValidation and raw. Every check runs before the
	// body is read and before any apiserver call, so a rejected request
	// issues zero outbound requests.
	if err = r.validate443(req.URL.Query(), &opts); err != nil {
		return
	}

	if val := req.URL.Query().Get("perPage"); val != "" {
		opts.perPage, err = strconv.Atoi(val)
		if err != nil {
			return
		}
	}

	if val := req.URL.Query().Get("page"); val != "" {
		opts.page, err = strconv.Atoi(val)
		if err != nil {
			return
		}
	}

	// #186 — a read-only handler NEVER reads the body into opts.dat, so
	// callOpts.Payload can never be set (ServeHTTP gates Payload on
	// opts.dat != nil AND a write verb). On POST /call/read the body has
	// already been consumed by the BodyExtrasDecode middleware anyway.
	if req.Body != nil && !r.readOnly {
		opts.dat, err = io.ReadAll(io.LimitReader(req.Body, 1048576))
		if err != nil {
			return
		}
	}

	return
}

// strictParams are the #443 query keys whose exact spelling is load-bearing.
var strictParams = []string{"dryRun", "fieldValidation"}

// normalizeParamKey folds case and drops '_' and '-', so "dry_run", "DryRun"
// and "dry-run" all compare equal to "dryRun".
func normalizeParamKey(k string) string {
	return strings.ToLower(strings.NewReplacer("_", "", "-", "").Replace(k))
}

// nearMissParam returns the exact spelling a query key nearly matches ("" when
// the key is exact or unrelated).
func nearMissParam(key string) string {
	n := normalizeParamKey(key)
	for _, p := range strictParams {
		if key != p && n == normalizeParamKey(p) {
			return p
		}
	}
	return ""
}

// nearMissParamError reports the first query key (in sorted order, so the
// message is deterministic) that is a misspelling of a strict parameter.
func nearMissParamError(q url.Values) error {
	keys := make([]string, 0, len(q))
	for k := range q {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		if want := nearMissParam(k); want != "" {
			hint := ""
			if want == "dryRun" {
				hint = " (dry-run writes use /call/dry-run, which always sends dryRun=All)"
			}
			return fmt.Errorf("unknown query parameter %q: the parameter is spelled %q%s", k, want, hint)
		}
	}
	return nil
}

// writeVerbs are the verbs that send an object to the apiserver: the only
// verbs dryRun and fieldValidation apply to.
var writeVerbs = []string{http.MethodPost, http.MethodPut, http.MethodPatch}

// validate443 applies the #443 query rules on top of the addressing checks.
// Values are case-sensitive and single-valued, and any value outside the
// accepted set is a 400, never silently dropped:
//
//   - dryRun: only on the /call/dry-run handler, and only exactly "All". On
//     plain /call it is always a 400 ("dry-run writes use /call/dry-run"),
//     because forwarding it there would make the route choice meaningless and
//     dropping it would turn a dry run into a real write.
//   - The /call/dry-run handler serves only POST/PUT/PATCH.
//   - fieldValidation: only on POST/PUT/PATCH, and only Ignore or Strict.
//     Warn is a 400 until apiserver Warning headers can be passed through.
//   - raw: only exactly "true", and only on a read (GET /call or the
//     read-only POST /call/read). It is set by a client that wants the stored
//     object rather than the resolved one; the dispatcher has already fallen
//     through to this handler when it saw the parameter.
func (r *callHandler) validate443(q url.Values, opts *callOptions) error {
	// reviewer-415 H1 — a misspelled dryRun / fieldValidation key (dryrun,
	// DryRun, dry_run, FieldValidation, field_validation, …) is a 400 naming
	// the correct spelling. Dropping it silently would turn
	// POST /call?dryrun=All into a REAL write with no echo, which is the very
	// hazard the /call/dry-run route split exists to prevent.
	if err := nearMissParamError(q); err != nil {
		return err
	}
	if vals, present := q["dryRun"]; present {
		if !r.dryRun {
			return fmt.Errorf("dry-run writes use /call/dry-run")
		}
		if len(vals) != 1 || vals[0] != "All" {
			return fmt.Errorf("invalid 'dryRun' value %q: the only accepted value is \"All\"", strings.Join(vals, ","))
		}
	}
	if r.dryRun {
		if !has(writeVerbs, opts.verb) {
			return fmt.Errorf("/call/dry-run accepts only POST, PUT and PATCH, not %s", opts.verb)
		}
		opts.dryRun = true
	}

	if vals, present := q["fieldValidation"]; present {
		if !has(writeVerbs, opts.verb) {
			return fmt.Errorf("'fieldValidation' applies only to POST, PUT and PATCH, not %s", opts.verb)
		}
		if len(vals) != 1 {
			return fmt.Errorf("'fieldValidation' must be given once")
		}
		switch vals[0] {
		case "Ignore", "Strict":
			opts.fieldValidation = vals[0]
		case "Warn":
			return fmt.Errorf("'fieldValidation=Warn' is not supported yet: apiserver warnings are not passed through; use Ignore or Strict")
		default:
			return fmt.Errorf("invalid 'fieldValidation' value %q: accepted values are \"Ignore\" and \"Strict\"", vals[0])
		}
	}

	if vals, present := q["raw"]; present {
		if opts.verb != http.MethodGet {
			return fmt.Errorf("'raw' applies only to reads, not %s", opts.verb)
		}
		if len(vals) != 1 || vals[0] != "true" {
			return fmt.Errorf("invalid 'raw' value %q: the only accepted value is \"true\"", strings.Join(vals, ","))
		}
		opts.raw = true
	}
	return nil
}

// Echo response headers (#443). A client that relies on one of these
// behaviours must treat a missing echo as a failure: an older snowplow sends
// none of them.
const (
	// HeaderDryRun is "All" when the apiserver call carried dryRun=All.
	HeaderDryRun = util.HeaderDryRun
	// HeaderFieldValidation is the fieldValidation value the apiserver call
	// carried.
	HeaderFieldValidation = util.HeaderFieldValidation
	// HeaderRaw is "true" when the stored object was read without resolving.
	HeaderRaw = util.HeaderRaw
	// HeaderResolveSource is "request-body" on an inline dry-run resolve (#443
	// part 2, set by the dispatchers package). Re-exported here so the CORS
	// exposure and the header name have one source.
	HeaderResolveSource = util.HeaderResolveSource
	// HeaderStageOutcomes is the inline resolve's per-stage outcome header
	// (#443 part 2, set by the dispatchers package).
	HeaderStageOutcomes = util.HeaderStageOutcomes
)

// setCallEchoHeaders sets the #443 echo headers from the BUILT outbound URI,
// so an echo can only appear when the parameter really goes to the apiserver.
// raw has no apiserver parameter: it is echoed from the validated options,
// because reaching this handler with raw set is what skips the resolver.
func setCallEchoHeaders(h http.Header, uri string, opts callOptions) {
	if u, err := url.Parse(uri); err == nil {
		q := u.Query()
		if v := q.Get("dryRun"); v != "" {
			h.Set(HeaderDryRun, v)
		}
		if v := q.Get("fieldValidation"); v != "" {
			h.Set(HeaderFieldValidation, v)
		}
	}
	if opts.raw {
		h.Set(HeaderRaw, "true")
	}
}

// resolveScope reports whether opts.gvr is namespace-scoped. It is called
// ONLY on the namespace-absent branch of validateRequest (Issue #156). A
// nil scopeResolver (a mis-constructed handler) is treated as scope-unknown
// and fails-closed, never as "cluster" — so a wiring bug cannot silently
// widen the URI.
func (r *callHandler) resolveScope(gvr schema.GroupVersionResource) (namespaced bool, err error) {
	if r.scopeResolver == nil {
		return false, fmt.Errorf("scope resolver not configured")
	}
	return r.scopeResolver(gvr)
}

type callOptions struct {
	gvr         schema.GroupVersionResource
	nsn         types.NamespacedName
	verb        string
	contentType string
	perPage     int
	page        int
	dat         []byte
	// namespaced records the resolved cluster scope of gvr (Issue #156).
	// true  → emit the namespaces/<ns> URI segment (byte-identical to
	//         pre-#156 behaviour; always the case when a namespace was
	//         supplied).
	// false → cluster-scoped: OMIT the namespaces/<ns> segment.
	namespaced bool
	// subresource is the optional subresource segment (#282), e.g. "status"
	// or "scale", appended as .../{name}/<subresource>. Empty for the
	// historical whole-object addressing (byte-identical path). validateRequest
	// enforces that it only rides a by-name verb.
	subresource string
	// dryRun (#443) adds dryRun=All to the outbound URI. Set only by the
	// /call/dry-run handler.
	dryRun bool
	// fieldValidation (#443) is "", "Ignore" or "Strict"; when set it is
	// forwarded as the fieldValidation query parameter.
	fieldValidation string
	// raw (#443) records a validated raw=true read. It changes no outbound
	// parameter; it selects the fallthrough reason and the X-Snowplow-Raw echo.
	raw bool
}

func buildURIPath(opts callOptions) (string, error) {
	base := path.Join("/apis", opts.gvr.Group, opts.gvr.Version)
	if len(opts.gvr.Group) == 0 {
		base = path.Join("/api", opts.gvr.Version)
	}

	// Issue #156 — the ONLY scope-dependent difference is the
	// namespaces/<ns> segment. namespaced==true reproduces today's path
	// byte-for-byte; namespaced==false (a POSITIVE cluster scope resolved
	// upstream) omits the segment → base/resource. The name-append block
	// below is scope-independent and unchanged.
	var uriPath string
	if opts.namespaced {
		uriPath = path.Join(base, "namespaces", opts.nsn.Namespace, opts.gvr.Resource)
	} else {
		uriPath = path.Join(base, opts.gvr.Resource)
	}
	if strings.EqualFold("namespaces", opts.gvr.Resource) {
		// namespaces is itself cluster-scoped; either branch above yields
		// base/resource for it, but keep this explicit special case for
		// zero churn / backward-compat (harmless with the new branch).
		uriPath = path.Join(base, opts.gvr.Resource)
	}

	if has([]string{
		http.MethodDelete,
		http.MethodGet,
		http.MethodPut,
		http.MethodPatch,
	}, opts.verb) {
		uriPath = path.Join(uriPath, opts.nsn.Name)
	}

	// #282 — append the subresource segment (.../{name}/<subresource>, e.g.
	// /status). validateRequest guarantees a subresource only rides a by-name
	// verb, so the name was just appended above; hang the subresource off it.
	// Empty subresource → byte-identical to the pre-#282 whole-object path.
	if opts.subresource != "" {
		uriPath = path.Join(uriPath, opts.subresource)
	}

	// Aggiunta dei query parametri, se necessario
	query := url.Values{}
	if opts.perPage > 0 {
		query.Set("perPage", strconv.Itoa(opts.perPage))
	}
	if opts.page > 0 {
		query.Set("page", strconv.Itoa(opts.page))
	}
	// #443 — the apiserver's own dry-run and field validation, carried on the
	// caller's request. These are what the echo headers are read back from.
	if opts.dryRun {
		query.Set("dryRun", "All")
	}
	if opts.fieldValidation != "" {
		query.Set("fieldValidation", opts.fieldValidation)
	}

	if len(query) > 0 {
		uriPath += "?" + query.Encode()
	}

	return uriPath, nil
}

func has(s []string, e string) bool {
	for _, a := range s {
		if strings.EqualFold(a, e) {
			return true
		}
	}

	return false
}

func callResponseHandler(out map[string]any) func(io.ReadCloser) error {
	return func(in io.ReadCloser) error {
		dat, err := io.ReadAll(in)
		if err != nil {
			return err
		}

		x := bytes.TrimSpace(dat)
		isArray := len(x) > 0 && x[0] == '['

		if isArray {
			v := []any{}
			err := json.Unmarshal(dat, &v)
			if err != nil {
				return err
			}
			out["items"] = v
			return nil
		}

		return json.Unmarshal(dat, &out)
	}
}
