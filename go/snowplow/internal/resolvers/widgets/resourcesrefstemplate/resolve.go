// Package resourcesrefstemplate expands a widget's resourcesRefsTemplate into
// concrete resource references. It evaluates each template's jq against the
// resolved data dictionary to produce dynamic ResourceRef values, which the
// resourcesrefs resolver then fetches.
package resourcesrefstemplate

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	xcontext "github.com/krateo-platformops/plumbing/context"
	"github.com/krateo-platformops/plumbing/jqutil"
	templatesv1 "github.com/krateo-platformops/snowplow/apis/templates/v1"
	jqsupport "github.com/krateo-platformops/snowplow/internal/support/jq"
	"k8s.io/utils/ptr"
)

type ResolveOptions struct {
	Items []templatesv1.ResourceRefTemplate
	Dict  map[string]any
}

func Resolve(ctx context.Context, items []templatesv1.ResourceRefTemplate, ds map[string]any) ([]templatesv1.ResourceRef, error) {
	all := []templatesv1.ResourceRef{}

	var errs []error
	for _, el := range items {
		tmp, err := createResourceReferencesFromTemplate(ctx, &el, ds)
		if err != nil {
			errs = append(errs, err)
			continue
		}
		if len(tmp) > 0 {
			all = append(all, tmp...)
		}
	}

	return all, errors.Join(errs...)
}

func createResourceReferencesFromTemplate(ctx context.Context, in *templatesv1.ResourceRefTemplate, ds map[string]any) (all []templatesv1.ResourceRef, err error) {
	it := ptr.Deref(in.Iterator, "")
	q, ok := jqutil.MaybeQuery(it)
	if !ok || q == "" {
		log := xcontext.Logger(ctx)
		// Iterator is optional (omitempty): an absent/non-jq value is the
		// supported non-iterated mode — produce exactly one ResourceRef from
		// the template. Not a fault; fires per-template — DEBUG, not WARN.
		log.Debug("bad or empty iterator", slog.String("iterator", it))

		el, err := createResourceRef(ctx, in, ds)
		if err != nil {
			return nil, err
		}
		return []templatesv1.ResourceRef{el}, nil
	}

	all = []templatesv1.ResourceRef{}

	// #638 — the shared iterator contract (jqsupport.IterateValues): the
	// pre-#638 element semantics, except that a single null or no value is an
	// empty expansion, and a runtime error carries no value preview (the log
	// line below used to print response bytes). #651 — with the module loader,
	// like every other resolve-path jq surface.
	var elems []any
	elems, err = jqsupport.IterateValues(ctx, q, ds, jqsupport.ModuleLoader())
	for _, sa := range elems {
		el, ferr := createResourceRef(ctx, in, sa)
		if ferr != nil {
			// #651 — a failing field fails the WHOLE template: no partial
			// expansion is ever handed to resourcesrefs.Resolve.
			return nil, ferr
		}
		all = append(all, el)
	}
	if err != nil {
		log := xcontext.Logger(ctx)
		if jqsupport.IsBenignNilIteration(err) {
			// Iterator walked a null/absent upstream value → zero refs, exactly
			// like an empty iterator. Data-dependent and benign — DEBUG, not the
			// ERROR that would flood the WARN-floor firehose on a healthy cluster.
			log.Debug("iterator yielded no items (nil upstream)", slog.String("iterator", it), slog.Any("err", err))
		} else {
			log.Error("unable to execute iterator", slog.String("iterator", it), slog.Any("err", err))
		}
	}

	return
}

// createResourceRef renders one ref from the template. #651 (a) — a field
// expression that fails returns the error: the template fails, never a ref
// carrying error text in its fields (the pre-#651 swallow, the same one
// #293/#302/#341 removed from the RESTAction path render).
func createResourceRef(ctx context.Context, in *templatesv1.ResourceRefTemplate, ds any) (out templatesv1.ResourceRef, err error) {
	out.Verb = in.Template.Verb
	for _, f := range []struct {
		name string
		q    string
		dst  *string
	}{
		{"id", in.Template.ID, &out.ID},
		{"apiVersion", in.Template.APIVersion, &out.APIVersion},
		{"name", in.Template.Name, &out.Name},
		{"namespace", in.Template.Namespace, &out.Namespace},
		{"resource", in.Template.Resource, &out.Resource},
	} {
		if *f.dst, err = evalJQE(ctx, f.q, ds); err != nil {
			return templatesv1.ResourceRef{}, fmt.Errorf("resourcesRefsTemplate %s: %w", f.name, err)
		}
	}
	return out, nil
}

// evalJQE renders one template field: a non-jq literal as is, a jq expression
// as its unquoted result. #638 — the error carries the class, never the value
// preview gojq renders (it is served as status.error and logged).
func evalJQE(ctx context.Context, q string, ds any) (string, error) {
	q, ok := jqutil.MaybeQuery(q)
	if !ok {
		return q, nil
	}

	out, err := jqutil.Eval(ctx,
		jqutil.EvalOptions{
			Query:        q,
			Unquote:      true,
			Data:         ds,
			ModuleLoader: jqsupport.ModuleLoader(),
		})
	if err != nil {
		return "", jqsupport.SanitizeError(err)
	}
	return out, nil
}
