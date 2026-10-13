// Package widgetdatatemplate evaluates a widget's widgetDataTemplate entries.
// Each entry's jq expression is run against the resolved data dictionary to
// produce a path/value pair, which the widget resolver merges into the
// widget's widgetData status.
package widgetdatatemplate

import (
	"context"
	"strings"

	"github.com/krateo-platformops/plumbing/jqutil"
	templatesv1 "github.com/krateo-platformops/snowplow/apis/templates/v1"
	jqsupport "github.com/krateo-platformops/snowplow/internal/support/jq"
)

type EvalResult struct {
	Path  string
	Value any
}

type ResolveOptions struct {
	Items      []templatesv1.WidgetDataTemplate
	DataSource map[string]any
}

func Resolve(ctx context.Context, opts ResolveOptions) (res []EvalResult, err error) {
	if len(opts.Items) == 0 {
		return []EvalResult{}, nil
	}

	res = make([]EvalResult, 0, len(opts.Items))

	for _, el := range opts.Items {
		expression := el.Expression
		if expression == "" {
			continue
		}

		path := el.ForPath
		if path == "" {
			continue
		}
		path = strings.TrimSpace(path)

		if path[0] == '.' {
			path = path[1:]
		}

		s := expression
		if exp, ok := jqutil.MaybeQuery(expression); ok {

			s, err = jqutil.Eval(ctx, jqutil.EvalOptions{
				Query: exp, Data: opts.DataSource, Unquote: false,
				ModuleLoader: jqsupport.ModuleLoader(),
			})
			if err != nil {
				// #638: logged by the widget resolver — no value preview.
				return res, jqsupport.SanitizeError(err)
			}
		}

		res = append(res, EvalResult{
			Path:  path,
			Value: jqutil.InferType(s),
		})
	}

	return res, nil
}
