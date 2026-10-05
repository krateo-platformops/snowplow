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

	xcontext "github.com/krateo-platformops/plumbing/context"
	"github.com/krateo-platformops/plumbing/jqutil"
	"github.com/krateo-platformops/plumbing/ptr"
	templates "github.com/krateo-platformops/snowplow/apis/templates/v1"
	"github.com/krateo-platformops/snowplow/internal/redact"
	"github.com/krateo-platformops/snowplow/internal/resolvers/restactions/api"
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
	Extras  map[string]any
	// Provenance (#443 part 2) — who vouches for In. Zero value: a stored CR.
	// Passed straight through to api.ResolveOptions.
	Provenance api.Provenance
}

func Resolve(ctx context.Context, opts ResolveOptions) (*templates.RESTAction, error) {
	dict := api.Resolve(ctx, api.ResolveOptions{
		RC:      opts.SArc,
		AuthnNS: opts.AuthnNS,
		Verbose: isVerbose(opts.In),
		Items:   opts.In.Spec.API,
		PerPage: opts.PerPage,
		Page:    opts.Page,
		Extras:  opts.Extras,
		// Ship E (0.30.116): the owning RESTAction's identity, folded
		// into the per-api-stage L1 key so a stage id is scoped to its
		// RESTAction. opts.In is the typed RESTAction CR.
		RESTActionNamespace: opts.In.GetNamespace(),
		RESTActionName:      opts.In.GetName(),
		Provenance:          opts.Provenance,
	})
	if dict == nil {
		dict = map[string]any{}
	}

	log := xcontext.Logger(ctx)
	// #487: never the dict itself. It holds every stage's response body
	// (Secret data, per-user rows). Log the spec-defined stage ids, each
	// stage's size and keyed digest, and the dict's totals.
	stageIDs := make([]string, 0, len(opts.In.Spec.API))
	for _, a := range opts.In.Spec.API {
		if a != nil {
			stageIDs = append(stageIDs, a.Name)
		}
	}
	log.Debug("resolved api", redact.DictAttr("dict", dict, stageIDs))

	var raw []byte
	if opts.In.Spec.Filter != nil {
		q := ptr.Deref(opts.In.Spec.Filter, "")
		s, err := jqutil.Eval(context.TODO(), jqutil.EvalOptions{
			Query: q, Data: dict,
			ModuleLoader: jqsupport.ModuleLoader(),
		})
		if err != nil {
			return opts.In, fmt.Errorf("unable to resolve filter: %w", err)
		}

		raw = []byte(s)
	} else {
		var err error
		raw, err = json.Marshal(dict)
		if err != nil {
			return opts.In, err
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

	return opts.In, nil
}

// IsVerbose returns true if the object has the AnnotationKeyConnectorVerbose
// annotation set to `true`.
func isVerbose(o metav1.Object) bool {
	return o.GetAnnotations()[annotationKeyVerboseAPI] == "true"
}
