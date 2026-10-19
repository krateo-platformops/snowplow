package api

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"

	xcontext "github.com/krateo-platformops/plumbing/context"
	"github.com/krateo-platformops/plumbing/ptr"
	templates "github.com/krateo-platformops/snowplow/apis/templates/v1"
	"github.com/krateo-platformops/snowplow/internal/listenvelope"
	jqsupport "github.com/krateo-platformops/snowplow/internal/support/jq"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// jsonHandlerOptions plumbs per-stage knobs into jsonHandlerCore.
//
// Ship 0.30.235 — `uaf` and `apiCallName` plumb the UserAccessFilter
// spec into jsonHandlerCore so the refilter runs on the RAW envelope
// BEFORE the stage filter applies its projection. The pre-0.30.235
// post-g.Wait() applyUserAccessFilter call ran on the projected shape,
// which broke NamespaceFrom for any filter that dropped .metadata. See
// refilter_layering_test.go for the permanent regression gate.
//
// Ship K / 0.30.245 — `dict` plumbs the resolver's accumulated
// stage-output dict so resolveUAFResources can evaluate
// `userAccessFilter.resourcesFrom` against UPSTREAM stage outputs
// (e.g. `.crds` populated by a prior stage). Pre-Ship-K passed `pig`
// (the per-stage scope) to resolveUAFResources, which never contains
// upstream stage keys → ResourcesFrom always returned []  → fail-closed
// drop every item → 0-count piechart regression on the live
// compositions-list RA. Permanent regression gate at
// refilter_layering_test.go::TestUAFRefilter_ResourcesFromMultiStage_ResolvesAgainstDictNotPig.
// The dict reference is the SAME map jsonHandlerCore writes to as
// `out`; refilter reads it BEFORE the merge, so it sees upstream
// stages but NOT the current stage being filtered.
type jsonHandlerOptions struct {
	key         string
	out         map[string]any
	dict        map[string]any
	filter      *string
	uaf         *templates.UserAccessFilterSpec
	apiCallName string
	// extras is the PURE per-request extras map (r.opts.Extras), exposed to
	// the per-stage filter as the reserved sibling key pig["extras"] so the
	// step filter can read `.extras.*` like every other jq surface. This is
	// NOT opts.dict: dict starts as a DeepCopy of Extras (resolve.go) but then
	// accumulates stage outputs + a synthetic `slice`, so at later stages it
	// is no longer the pure extras. The reference is SHARED, not deep-copied:
	// gojq is the krateo-platformops COW fork, so the filter cannot mutate the input
	// (mirrors the existing pig["slice"] shared-map pattern).
	extras map[string]any
	// raNamespace/raName name the owning RESTAction on the #638 stage-filter
	// failure line (log attribution only; never part of the filter input).
	raNamespace string
	raName      string
	// k8sList: this call is a Kubernetes collection LIST (a GET on an
	// apiserver path with no object name) of listGVR. Its response is a list
	// envelope: jsonHandlerCore normalises it to E6 (normaliseListEnvelope)
	// and stageEnvelopeCheck checks what the stage receives.
	k8sList bool
	listGVR schema.GroupVersionResource
	// stageFailed records a failure of this stage that is still SERVED (#651:
	// a userAccessFilter whose resource set did not resolve keeps its
	// fail-closed empty answer): the stage-error sink bump every L1 Put gate
	// declines on, and the Error stage outcome. Set by the resolver for its
	// own stage; nil-safe.
	stageFailed func(msg string)
}

// stageEnvelopeCheck is the E6 behavioural chokepoint (#603 §1, #674): every
// Kubernetes LIST envelope reaches a stage through jsonHandlerCore, whatever
// built it (informer, content cache, cluster-list collapse, branch C, or the
// httpcall.Do apiserver fall-through). It is called on the envelope the
// stage RECEIVES, after normaliseListEnvelope, so E6 holds on every path,
// fall-through included (#676 C2(a)). Nil in production. This package's tests
// set it (envelope_e6_chokepoint_test.go) to assert the keys are a subset of
// listenvelope.Keys(). A key normaliseListEnvelope does not recognise (any
// beyond apiVersion/kind/metadata/items) is left in place and caught here.
var stageEnvelopeCheck func(opts jsonHandlerOptions, tmp any)

// normaliseListEnvelope gives a Kubernetes LIST response the E6 envelope
// (#603 §1, #674, reviewer-603's #676 C2(a)): exactly {apiVersion, kind,
// items}, with apiVersion/kind synthesized from the GVR exactly as the
// informer and content paths build them (listenvelope.For). The list-level
// metadata (resourceVersion, continue, remainingItemCount) of a live
// apiserver List, which the httpcall.Do fall-through hands back, is dropped.
// The informer and content paths never carried it, so no RESTAction can rely
// on a list-level .metadata read.
//
// Only an envelope of exactly the Kubernetes List shape is rewritten: keys
// within {apiVersion, kind, metadata, items}, with items an array. Anything
// else (a Status error, a non-Kubernetes response on an apiserver-shaped
// path) is returned unchanged. An envelope that is already E6 for this GVR is
// returned as is, with no copy.
func normaliseListEnvelope(gvr schema.GroupVersionResource, tmp any) any {
	m, ok := tmp.(map[string]any)
	if !ok {
		return tmp
	}
	items, ok := m[listenvelope.Items].([]any)
	if !ok {
		return tmp
	}
	for k := range m {
		switch k {
		case listenvelope.APIVersion, listenvelope.Kind, listenvelope.Items, "metadata":
		default:
			return tmp
		}
	}
	av := listenvelope.APIVersionFor(gvr.Group, gvr.Version)
	kind := listenvelope.KindFor(gvr.Resource)
	if _, hasMeta := m["metadata"]; !hasMeta && m[listenvelope.APIVersion] == av && m[listenvelope.Kind] == kind {
		return tmp
	}
	return listenvelope.New(av, kind, items)
}

// jsonHandler is the HTTP-body-shaped entry point — the form
// httpcall.Do's ResponseHandler contract requires. It io.ReadAll's the
// response body, json.Unmarshal's it, and hands the decoded value to
// jsonHandlerCore. Use this ONLY for a genuine HTTP-body stream
// (httpcall.Do); for an in-memory dispatch result use jsonHandlerBytes
// (raw []byte) or jsonHandlerValue (already-decoded value) — Ship
// 0.30.128 P-CORE-1: those skip the redundant io.ReadAll copy that every
// cache hit otherwise pays.
func jsonHandler(ctx context.Context, opts jsonHandlerOptions) func(io.ReadCloser) error {
	return func(in io.ReadCloser) error {
		dat, err := io.ReadAll(in)
		if err != nil {
			return err
		}
		return jsonHandlerBytesApply(ctx, opts, dat)
	}
}

// jsonHandlerBytes is the []byte-direct entry point — Ship 0.30.128
// P-CORE-1. For an in-memory dispatch result (informer-served bytes, an
// in-process nested /call's Status.Raw, the internal-rest-config
// dispatch) the bytes are ALREADY in memory; wrapping them in an
// io.ReadCloser only so jsonHandler can io.ReadAll them straight back
// out is a redundant full copy paid on every call. jsonHandlerBytes
// json.Unmarshal's the bytes directly and skips that copy.
func jsonHandlerBytes(ctx context.Context, opts jsonHandlerOptions) func([]byte) error {
	return func(dat []byte) error {
		return jsonHandlerBytesApply(ctx, opts, dat)
	}
}

// jsonHandlerValue is the decoded-value-direct entry point — Ship
// 0.30.128 P-CORE-2. For an apistage content-cache hit the gated
// envelope is already a structured value (map[string]any); marshalling
// it to []byte only for jsonHandler to unmarshal it right back is a
// redundant decode paid on every cache hit. jsonHandlerValue feeds the
// already-decoded value straight to jsonHandlerCore — no marshal, no
// unmarshal.
func jsonHandlerValue(ctx context.Context, opts jsonHandlerOptions) func(any) error {
	return func(decoded any) error {
		return jsonHandlerCore(ctx, opts, decoded)
	}
}

// jsonHandlerBytesApply json.Unmarshal's dat then runs jsonHandlerCore.
// The shared body behind jsonHandler (HTTP path) and jsonHandlerBytes
// (in-memory []byte path).
func jsonHandlerBytesApply(ctx context.Context, opts jsonHandlerOptions, dat []byte) error {
	var tmp any
	// #709 — an empty successful body is "no content": the stage's value is
	// JSON null, on every dispatch path (the external fetch says the same).
	if len(bytes.TrimSpace(dat)) == 0 {
		return jsonHandlerCore(ctx, opts, nil)
	}
	if err := json.Unmarshal(dat, &tmp); err != nil {
		return safeDecodeError(err)
	}
	return jsonHandlerCore(ctx, opts, tmp)
}

// safeDecodeError renders a response-decode failure without response bytes
// (#638). encoding/json's SyntaxError quotes the offending character of the
// BODY ("invalid character 's' after top-level value"); the error is logged and
// written to the stage's errorKey. Keep the class and the byte offset only.
func safeDecodeError(err error) error {
	var se *json.SyntaxError
	if errors.As(err, &se) {
		return fmt.Errorf("response is not valid JSON: syntax error at byte offset %d", se.Offset)
	}
	// Anything else (decoding into `any` cannot raise a type error) is
	// reported by class only — UnmarshalTypeError.Value would quote the body.
	return fmt.Errorf("response is not valid JSON (%T)", err)
}

// jsonHandlerCore is the post-decode logic: wrap the decoded value under
// opts.key, apply the optional stage filter, and merge into opts.out.
// Byte-identical behaviour to the pre-0.30.128 jsonHandler from the
// decoded value onward (AC-128.4) — P-CORE-1/2 only change WHEN/WHERE
// the decode happens (once at entry-load, not once per cache-hit), never
// the merge/filter result.
func jsonHandlerCore(ctx context.Context, opts jsonHandlerOptions, tmp any) error {
	log := xcontext.Logger(ctx)

	pig := map[string]any{}
	// pig["extras"] is written BEFORE pig[opts.key] so a stage literally named
	// "extras" keeps its OWN response (response wins; collision Option A). The
	// guard mirrors the `slice` guard below: write nothing when extras is
	// absent so the no-extras envelope stays byte-identical to pre-feature.
	// The map is SHARED, not copied — the gojq COW fork can't mutate
	// the filter input (same contract as pig["slice"]).
	if len(opts.extras) > 0 {
		pig["extras"] = opts.extras
	}
	if opts.k8sList {
		// E6 on every path, the apiserver fall-through included (#676 C2(a)).
		tmp = normaliseListEnvelope(opts.listGVR, tmp)
		if stageEnvelopeCheck != nil {
			stageEnvelopeCheck(opts, tmp)
		}
	}
	pig[opts.key] = tmp
	if si, ok := opts.out["slice"]; ok {
		pig["slice"] = si
	}

	// Ship 0.30.235 — UAF refilter runs on the RAW envelope BEFORE the
	// stage filter projects items. The projection can omit .metadata;
	// running UAF first uses the K8s-canonical shape the spec documents.
	// nil-UAF is a single early-return — pre-0.30.235 byte-identical.
	//
	// Ship K / 0.30.245 — opts.dict is threaded into the refilter so
	// resolveUAFResources evaluates `resourcesFrom` against the FULL
	// resolver dict (upstream stage outputs), not the per-stage pig
	// scope. Fixes the 0-count compositions-list regression preserved
	// from 0.30.235.
	if opts.uaf != nil {
		rf := applyUserAccessFilterOnPig(ctx, pig, opts.dict, opts.key, opts.uaf)
		emitRefilterFalsifierFromHandler(ctx, log, opts.apiCallName, opts.uaf, rf)
		// #759 (SECURITY) — the refilter REPLACES pig[key] on its fail-closed
		// branches (the empty list; {} for a denied single object) and for a
		// narrowed bare array; only a kept {items} list is narrowed in place.
		// With no stage filter the merge below writes tmp, so tmp must be what
		// the refilter left — never the raw service-account response.
		tmp = pig[opts.key]
		// #651 / #760 — an unresolved resource set or a missing requester
		// keeps that fail-closed empty answer, but it is a stage failure, not
		// "no resources".
		if opts.stageFailed != nil {
			switch {
			case rf.UserInfoMissing:
				opts.stageFailed(uafUserInfoMissingMsg)
			case rf.ResourcesUnresolved:
				opts.stageFailed(uafResourcesUnresolvedMsg)
			}
		}
	}

	if opts.filter != nil {
		q := ptr.Deref(opts.filter, "")
		log.Debug("found local filter on api result", slog.String("filter", q))
		// Ship A (0.30.137): EvalValue returns gojq's result value
		// directly — no jqutil encode-to-string + json.Unmarshal-back
		// round-trip (design §3.4.2). Behaviour byte-identical per the
		// §3.4.2 truth table.
		v, ok, err := EvalValue(context.TODO(), q, pig, jqsupport.ModuleLoader())
		switch {
		case errors.Is(err, ErrMultiYield):
			// Current: multi-yield -> invalid concatenated JSON ->
			// json.Unmarshal errors -> return err -> stage fails.
			return err
		case err != nil:
			// #638 — a parse/compile/runtime gojq error FAILS the stage. It
			// used to be logged and the stage continued with tmp = the RAW,
			// unfiltered response: wrong data (the projection or narrowing
			// the filter performs was skipped), served AND cacheable (no
			// stage-error sink bump, so every L1 Put accepted it). Returning
			// the error routes it through the caller's recordItemError at
			// every feed site — the stage-error sink is bumped (the Put
			// declines), the stage outcome is Error, dict[errorKey] carries
			// the message, ContinueOnError is honoured — and dict[key] is
			// never written, so the raw body cannot reach the output.
			log.Error("stage filter failed; stage response not merged",
				slog.String("stage", opts.key),
				slog.String("restaction_namespace", opts.raNamespace),
				slog.String("restaction_name", opts.raName),
				slog.String("filter", q), slog.Any("error", err))
			return fmt.Errorf("jq filter %q failed: %w", q, err)
		case !ok:
			// Zero-yield (jq `empty`). Current: jqutil.Eval returns "",
			// json.Unmarshal("") errors -> return err -> stage fails.
			return fmt.Errorf("jq filter %q yielded no value", q)
		default:
			// Single value. Current: json.Unmarshal(s) -> tmp. Ship A:
			// tmp = v directly (the §3.1-3.3 equivalence proof).
			tmp = v
		}
	}

	got, ok := opts.out[opts.key]
	if !ok {
		opts.out[opts.key] = tmp
		return nil
	}

	// #74 Class 3 R1' — PROVENANCE-DISCRIMINATED flatten. A bare []any element
	// `tmp` is SPLICED into the accumulator (its members appended element-wise)
	// ONLY when it is FILTER-PRODUCED — `opts.filter != nil`, i.e. the per-stage
	// gojq filter constructed the array (e.g. composition-schema's
	// `getCompositionDefinitionNamespace … | map(.metadata.namespace)`, where
	// element-wise concat is intended). A RAW-RESPONSE []any (`opts.filter ==
	// nil`) is appended as a SINGLE element, never spliced.
	//
	// Why: the dispatch never returns a bare []any for a no-filter step — a
	// GET-by-name is an object, a LIST GET is the {items} envelope OBJECT
	// (listEnvelopeValue, apistage.go), discovery is an APIResourceList OBJECT.
	// A no-filter bare []any arises ONLY when a resolve:true nested element
	// resolves to an RA/widget whose top-level value is an array — the Class 3
	// `allCompositionResources` self/nested resolve returning ["v1", …].
	// Splicing THAT poisons the parent list with bare scalars ("v1"), which the
	// downstream portal filter then indexes as `.metadata` → "expected an object
	// but got: string". Corpus-proven safe (docs/errzero-class3-decision §7.2):
	// getCompositionDefinitionNamespace = 10/10 filter-bearing (still flattens),
	// allCompositionResources = 0/34 (stops splicing), no identity-passthrough
	// filter exists, no no-filter LIST-GET returns a bare []any.
	//
	// STRUCTURAL predicate (feedback_no_special_cases) — keyed on
	// `opts.filter != nil`, never on a resource/name/GVR literal. Global, no
	// per-RA config, no CRD field.
	flatten := opts.filter != nil
	switch existingSlice := got.(type) {
	case []any:
		if arr, isArr := tmp.([]any); isArr {
			if flatten && len(arr) > 0 {
				// (A) filter-produced array → splice members (preserves the
				// pre-R1' len>0 empty-slice skip).
				opts.out[opts.key] = append(existingSlice, arr...)
			} else if !flatten {
				// (B) raw-response array → single element, never spliced.
				opts.out[opts.key] = append(existingSlice, tmp)
			}
			// flatten && len==0: empty filtered array contributes nothing
			// (unchanged from the pre-R1' len(v) > 0 guard).
		} else {
			// scalar/object element → single element (wrapAsSlice's old
			// default branch).
			opts.out[opts.key] = append(existingSlice, tmp)
		}
	default:
		// Second element arriving (existing accumulator value is not yet a
		// slice). Same A/B discrimination as above.
		if arr, isArr := tmp.([]any); isArr && flatten {
			opts.out[opts.key] = append([]any{got}, arr...)
		} else {
			opts.out[opts.key] = []any{got, tmp}
		}
	}

	return nil
}
