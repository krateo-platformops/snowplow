// Package restactions is the top-level RESTAction resolver. Resolve runs the
// CR's ordered API stages (delegating to the api subpackage to execute each
// stage's HTTP/Kubernetes calls and jq), then applies the RESTAction's
// optional jq filter to produce the resolved status. It emits unordered
// data only; widget-shaping logic lives in the widget resolvers, never here.
package restactions

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"

	xcontext "github.com/krateo-platformops/plumbing/context"
	"github.com/krateo-platformops/plumbing/jqutil"
	"github.com/krateo-platformops/plumbing/ptr"
	templates "github.com/krateo-platformops/snowplow/apis/templates/v1"
	"github.com/krateo-platformops/snowplow/internal/redact"
	"github.com/krateo-platformops/snowplow/internal/resolvers/restactions/api"
	"github.com/krateo-platformops/snowplow/internal/slowcall"
	jqsupport "github.com/krateo-platformops/snowplow/internal/support/jq"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/rest"
)

const (
	annotationKeyLastAppliedConfiguration = "kubectl.kubernetes.io/last-applied-configuration"
	annotationKeyVerboseAPI               = "krateo.io/verbose"
)

type ResolveOptions struct {
	In      *templates.RESTAction
	SArc    *rest.Config
	AuthnNS string
	PerPage int
	Page    int
	// Cursor (#874) — the data source's continuation token, handed to the
	// RESTAction's jq as .slice.cursor (api.ResolveOptions.Cursor).
	Cursor string
	Extras map[string]any
	// Provenance (#443 part 2) — who vouches for In. Zero value: a stored CR.
	// Passed straight through to api.ResolveOptions.
	Provenance api.Provenance
}

func Resolve(ctx context.Context, opts ResolveOptions) (*templates.RESTAction, error) {
	ra, _, _, err := resolve(ctx, opts)
	return ra, err
}

// ResolveStatusTree is Resolve plus the RESTAction's status as the Go tree a
// json.Unmarshal of Status.Raw into a map[string]any yields — the form the
// widget apiRef path hands to the widget's jq (#1079).
//
// When exact is true, tree IS that decode, built from the filter's output
// values instead of by re-parsing the bytes this same call just encoded from
// them (jqsupport.DecodedObjectOf names what it reproduces). When exact is
// false the caller decodes Status.Raw itself; nothing else differs. Status.Raw
// is set exactly as Resolve sets it either way: the memo stores those bytes.
func ResolveStatusTree(ctx context.Context, opts ResolveOptions) (ra *templates.RESTAction, tree map[string]any, exact bool, err error) {
	ra, outputs, filtered, err := resolve(ctx, opts)
	if err != nil || !filtered {
		return ra, nil, false, err
	}
	tree, exact = jqsupport.DecodedObjectOf(outputs)
	return ra, tree, exact, nil
}

// resolve is Resolve plus, when the spec.filter ran (filtered), its output
// values: Status.Raw is their encoding.
func resolve(ctx context.Context, opts ResolveOptions) (ra *templates.RESTAction, outputs []any, filtered bool, err error) {
	// #1116 (SECURITY) — extras that name a key the resolver writes (a stage
	// name, an errorKey, slice) are refused before anything runs. The error
	// is an *api.ReservedExtrasError; the dispatchers render it as a 400
	// naming the keys. api.ResolveWithFailure holds the same rule for any
	// other caller of the api package.
	if err := api.RefuseReservedExtras(ctx, opts.In.Spec.API, opts.Extras, opts.In.GetNamespace(), opts.In.GetName()); err != nil {
		return opts.In, nil, false, err
	}
	dict, failure := api.ResolveWithFailure(ctx, api.ResolveOptions{
		RC:      opts.SArc,
		AuthnNS: opts.AuthnNS,
		Verbose: isVerbose(opts.In),
		Items:   opts.In.Spec.API,
		PerPage: opts.PerPage,
		Page:    opts.Page,
		Cursor:  opts.Cursor,
		Extras:  opts.Extras,
		// Ship E (0.30.116): the owning RESTAction's identity, folded
		// into the per-api-stage L1 key so a stage id is scoped to its
		// RESTAction. opts.In is the typed RESTAction CR.
		RESTActionNamespace: opts.In.GetNamespace(),
		RESTActionName:      opts.In.GetName(),
		Provenance:          opts.Provenance,
		// #603 PR-2 SHADOW — the read-set analysis of the RESTAction chain.
		SpecFilter: opts.In.Spec.Filter,
	})
	if dict == nil {
		dict = map[string]any{}
	}

	log := xcontext.Logger(ctx)
	// #487: never the dict itself. It holds every stage's response body
	// (Secret data, per-user rows). The dict is summarised per entry under a
	// key label, and "stages" maps each spec-defined stage id to its label.
	// #490: both attrs are lazy pointer-shaped LogValuers, so at a disabled
	// level this line costs nothing (no encoding, no digest, no allocation).
	log.LogAttrs(ctx, slog.LevelDebug, "resolved api",
		redact.DictAttr("dict", dict),
		slog.Any("stages", stageIDsLog{api: &opts.In.Spec.API}),
	)

	var raw []byte
	if opts.In.Spec.Filter != nil {
		q := ptr.Deref(opts.In.Spec.Filter, "")
		// #1054 — the RESTAction filter's JQ, timed on the request. The eval
		// is not cancellable and never was (it ran on context.TODO(), whose
		// Done is nil): the filter is in-memory work over stages already
		// fetched, and its result is the cell every requester of the key
		// shares. It runs on context.Background() because that is the one
		// context the gojq fork treats as non-cancellable (execute.go:24): on
		// any other it polls ctx.Done() before every opcode — ~2.7% of all
		// CPU on run 23 for a channel that could never close.
		jq := slowcall.Start(ctx, slowcall.PhaseJQ)
		vals, s, err := evalFilter(context.Background(), q, dict)
		jq.End()
		if err != nil {
			// #709 — when a stage without continueOnError failed, the filter
			// error is its consequence (it read the stage that has no
			// value): report the stage's upstream status, not the jq error.
			if failure != nil {
				log.Debug("restaction filter failed after a stage failure",
					slog.String("stage", failure.Stage), slog.Any("err", jqsupport.SanitizeError(err)))
				return opts.In, nil, false, failure.StatusError(opts.In.GetNamespace(), opts.In.GetName())
			}
			return opts.In, nil, false, fmt.Errorf("unable to resolve filter: %w", jqsupport.SanitizeError(err))
		}
		if s == "" {
			// #709 — the filter produced no value (jq `empty`). jqutil.Eval
			// returns "" only then, and "" is not JSON: it used to become the
			// status and fail every reader with `unexpected end of JSON input`.
			//   - a stage without continueOnError failed: that failure is why
			//     there is no value, so it is the answer — the upstream code,
			//     reason and message (a 404 stays a 404);
			//   - otherwise no value is a legitimate result: the status is
			//     JSON null.
			if failure != nil {
				return opts.In, nil, false, failure.StatusError(opts.In.GetNamespace(), opts.In.GetName())
			}
			s = "null"
		}

		raw = []byte(s)
		outputs, filtered = vals, true
	} else {
		var err error
		raw, err = json.Marshal(dict)
		if err != nil {
			return opts.In, nil, false, err
		}
	}

	opts.In.Status = &runtime.RawExtension{
		Raw: raw,
	}

	if opts.In.Annotations != nil {
		delete(opts.In.Annotations, annotationKeyLastAppliedConfiguration)
	}
	if opts.In.ManagedFields != nil {
		opts.In.ManagedFields = nil
	}

	return opts.In, outputs, filtered, nil
}

// evalFilter is jqutil.Eval for the RESTAction filter, keeping the output
// values it encodes (#1079): the same parse, compile and run, the same errors
// in the same order, and the same text, written by jqutil's own encoder.
// jqutil.Eval encodes each output as the iterator yields it, through one
// encoder, with no separator; the identity program yields its input
// unchanged, so jqutil.Eval(".") of one output is that encoder applied to it,
// and the concatenation is jqutil.Eval's text for the whole stream.
//
// The program comes from jqsupport.Compile (run 23: the per-resolve parse +
// compile was ≈0.56% of all CPU): the program gojq.Parse + gojq.Compile build,
// reused while it is alive. A reused program's constants are shared across
// resolves, so its outputs must stay read-only, and they do: they reach only
// jqutil's encoder (below) and jqsupport.DecodedObjectOf (ResolveStatusTree),
// which reads them into a fresh tree.
func evalFilter(ctx context.Context, query string, data any) ([]any, string, error) {
	code, err := jqsupport.Compile(query)
	if err != nil {
		return nil, "", err
	}
	var (
		vals  []any
		first string
		rest  strings.Builder // the text once there are several outputs
	)
	iter := code.RunWithContext(ctx, data)
	for {
		v, ok := iter.Next()
		if !ok {
			break
		}
		if verr, isErr := v.(error); isErr {
			return nil, "", verr
		}
		s, err := jqutil.Eval(ctx, jqutil.EvalOptions{Query: ".", Data: v})
		if err != nil {
			return nil, "", err
		}
		vals = append(vals, v)
		switch len(vals) {
		case 1:
			first = s
		case 2:
			rest.WriteString(first)
			rest.WriteString(s)
		default:
			rest.WriteString(s)
		}
	}
	if len(vals) > 1 {
		return vals, rest.String(), nil
	}
	return vals, first, nil
}

// IsVerbose returns true if the object has the AnnotationKeyConnectorVerbose
// annotation set to `true`.
func isVerbose(o metav1.Object) bool {
	return o.GetAnnotations()[annotationKeyVerboseAPI] == "true"
}

// stageIDsLog renders the RESTAction's spec-defined stage ids, each mapped to
// the redact.KeyLabel its dict entry is logged under (#487/#490). A stage id
// is a spec name, not data. The type holds one pointer into the CR, so the
// attr is pointer-shaped and the work runs only when the record is rendered.
type stageIDsLog struct{ api *[]*templates.API }

// LogValue renders only the stage ids and their labels.
func (s stageIDsLog) LogValue() slog.Value {
	if s.api == nil {
		return slog.GroupValue()
	}
	attrs := make([]slog.Attr, 0, len(*s.api))
	for _, a := range *s.api {
		if a != nil {
			attrs = append(attrs, slog.String(a.Name, redact.KeyLabel(a.Name)))
		}
	}
	return slog.GroupValue(attrs...)
}
