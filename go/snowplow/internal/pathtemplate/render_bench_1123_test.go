package pathtemplate

import (
	"context"
	"testing"

	"github.com/krateo-platformops/plumbing/jqutil"
	jqsupport "github.com/krateo-platformops/snowplow/internal/support/jq"
)

// The per-call cost of the path render against the plain jq render it
// replaces (setup.go evalJQE). Both parse and compile per call, as before.
var benchTmpl = `${ "/apis/observability.krateo.io/v1alpha1/namespaces/" + .namespace + "/alerts/" + .name }`
var benchData = map[string]any{"namespace": "demo-system", "name": "high-cpu"}

func BenchmarkIssue1123_PathRender(b *testing.B) {
	ctx := context.Background()
	for i := 0; i < b.N; i++ {
		if _, err := Render(ctx, benchTmpl, benchData); err != nil {
			b.Fatal(err)
		}
	}
}

// An iterator stage compiles once and renders per element.
func BenchmarkIssue1123_CompiledPathRender(b *testing.B) {
	pt := Compile(benchTmpl)
	ctx := context.Background()
	for i := 0; i < b.N; i++ {
		if _, err := pt.Render(ctx, benchData); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkIssue1123_PlainJQRender(b *testing.B) {
	q, _ := jqutil.MaybeQuery(benchTmpl)
	ctx := context.Background()
	for i := 0; i < b.N; i++ {
		_, err := jqutil.Eval(ctx, jqutil.EvalOptions{Query: q, Unquote: true, Data: benchData, ModuleLoader: jqsupport.ModuleLoader()})
		if err != nil {
			b.Fatal(err)
		}
	}
}
