package api

// #1116 — a READ item a dial guard refused (the #288 shape guard, a #302 jq
// error, a #293 unrendered template) is a stage failure, the same one #1112 D2
// records for a write: the reason, the errorKey record, the stage-error sink
// (no Put of the body), the StageFailure without continueOnError, and one
// count in snowplow_refused_read_total{class}. Before, it was silent: no
// outcome, no errorKey, so whatever stood in the stage's key looked like its
// output.

import (
	"context"
	"net/http"
	"strings"
	"testing"

	xcontext "github.com/krateo-platformops/plumbing/context"
	"github.com/krateo-platformops/plumbing/jwtutil"
	"github.com/krateo-platformops/plumbing/ptr"
	templates "github.com/krateo-platformops/snowplow/apis/templates/v1"
	"github.com/krateo-platformops/snowplow/internal/cache"
	"github.com/krateo-platformops/snowplow/internal/workclass"
	"k8s.io/client-go/rest"
)

type rr16Out struct {
	dict     map[string]any
	failure  *StageFailure
	outcomes string
	sinkN    int64
}

func rr16Resolve(t *testing.T, class workclass.Class, extras map[string]any, stages ...*templates.API) rr16Out {
	t.Helper()
	ctx := xcontext.BuildContext(context.Background(),
		xcontext.WithUserInfo(jwtutil.UserInfo{Username: x1106User, Groups: []string{"tenants"}}))
	if class != workclass.None {
		var end func()
		ctx, end = workclass.Begin(ctx, class)
		defer end()
	}
	ctx, sink := cache.WithStageErrorSink(ctx)
	ctx, oc := WithStageOutcomes(ctx)
	dict, f := ResolveWithFailure(ctx, ResolveOptions{
		RC: &rest.Config{Host: "http://127.0.0.1:1"}, AuthnNS: x1106NS, Items: stages, Extras: extras,
		RESTActionNamespace: x1106NS, RESTActionName: "reads",
	})
	return rr16Out{dict: dict, failure: f, outcomes: oc.HeaderValue(), sinkN: sink.Count()}
}

// rr16Read is a GET of a ConfigMap in this cluster, its namespace from extras.
func rr16Read(name string, coe bool) *templates.API {
	return &templates.API{Name: name, Verb: ptr.To(http.MethodGet), ErrorKey: ptr.To(name + "Err"),
		ContinueOnError: ptr.To(coe), Path: `${ "/api/v1/namespaces/" + (.namespace // "") + "/configmaps/cm" }`}
}

func rr16ErrRecord(t *testing.T, o rr16Out, key, reason string) {
	t.Helper()
	errs, _ := o.dict[key].([]any)
	if len(errs) != 1 {
		t.Fatalf("want one record under %s, got %v", key, o.dict[key])
	}
	m, _ := errs[0].(map[string]any)
	if m["reason"] != reason || statusCodeOf(m["code"]) != http.StatusBadRequest ||
		!strings.HasPrefix(m["message"].(string), "read stage ") {
		t.Errorf("%s record = %v, want reason %s, code 400, a read-stage message", key, m, reason)
	}
}

func TestRefusedRead1116_ShapeGuardRefusalFailsTheRead(t *testing.T) {
	api := newX1106Srv(t, false, x1106OK)
	x1106Env(t, api.srv.URL, map[string]map[string]string{})
	before := RefusedReadSnapshot()

	// Missing namespace: the path collapses to /namespaces//, the #288 guard
	// refuses it. continueOnError=false: the resolve's StageFailure is set.
	o := rr16Resolve(t, workclass.None, map[string]any{}, rr16Read("cm", false))
	if n := len(api.all()); n != 0 {
		t.Fatalf("the refused read was dialled %d time(s)", n)
	}
	x1106WantReason(t, o.outcomes, "cm", StageReasonInvalidPath)
	rr16ErrRecord(t, o, "cmErr", StageReasonInvalidPath)
	if o.sinkN == 0 {
		t.Error("the stage-error sink was not bumped: a Put would store the body around the refused read")
	}
	if o.failure == nil || o.failure.Stage != "cm" || o.failure.Code != http.StatusBadRequest {
		t.Errorf("without continueOnError the refused read must be the resolve's 400 StageFailure, got %+v", o.failure)
	}
	if _, set := o.dict["cm"]; set {
		t.Errorf("the refused read wrote its key: %v", o.dict["cm"])
	}

	// continueOnError=true: the same failure, no StageFailure.
	o = rr16Resolve(t, workclass.None, map[string]any{}, rr16Read("cm", true))
	x1106WantReason(t, o.outcomes, "cm", StageReasonInvalidPath)
	if o.failure != nil {
		t.Errorf("continueOnError: no StageFailure expected, got %+v", o.failure)
	}

	after := RefusedReadSnapshot()
	if after[refusedReadNone]-before[refusedReadNone] != 2 {
		t.Errorf("refused reads counted %d under %q, want 2", after[refusedReadNone]-before[refusedReadNone], refusedReadNone)
	}

	// A valid path dials and succeeds: nothing recorded.
	o = rr16Resolve(t, workclass.None, map[string]any{"namespace": "team-a"}, rr16Read("cm", false))
	if o.outcomes != "[]" || o.failure != nil || len(api.all()) != 1 {
		t.Errorf("a valid read must dial once and succeed: header %s failure %+v sent %d", o.outcomes, o.failure, len(api.all()))
	}
}

func TestRefusedRead1116_TemplateErrorAndPartialIterator(t *testing.T) {
	api := newX1106Srv(t, false, x1106OK)
	x1106Env(t, api.srv.URL, map[string]map[string]string{})

	// A jq error in the path (a number + a string).
	read := rr16Read("cm", true)
	read.Path = `${ "/api/v1/namespaces/" + .namespace + "/configmaps/cm" }`
	o := rr16Resolve(t, workclass.None, map[string]any{"namespace": float64(1)}, read)
	x1106WantReason(t, o.outcomes, "cm", StageReasonTemplateError)
	rr16ErrRecord(t, o, "cmErr", StageReasonTemplateError)

	// An iterator read: one element of three refused; the other two dial and
	// the stage fails (a partial read is not the stage's output).
	it := &templates.API{Name: "each", Verb: ptr.To(http.MethodGet), ErrorKey: ptr.To("eachErr"), ContinueOnError: ptr.To(true),
		DependsOn: &templates.Dependency{Name: "", Iterator: ptr.To(`[ .nss[] | {ns: .} ]`)},
		Path:      `${ "/api/v1/namespaces/" + .ns + "/configmaps/cm" }`}
	n0 := len(api.all())
	o = rr16Resolve(t, workclass.None, map[string]any{"nss": []any{"team-a", "", "team-b"}}, it)
	if sent := len(api.all()) - n0; sent != 2 {
		t.Errorf("the two valid items must still dial, sent %d", sent)
	}
	x1106WantReason(t, o.outcomes, "each", StageReasonInvalidPath)
	if o.sinkN == 0 {
		t.Error("a partial iterator read must bump the stage-error sink")
	}
}

func TestRefusedRead1116_CountedByWorkClass(t *testing.T) {
	api := newX1106Srv(t, false, x1106OK)
	x1106Env(t, api.srv.URL, map[string]map[string]string{})
	before := RefusedReadSnapshot()
	rr16Resolve(t, workclass.Prewarm, map[string]any{}, rr16Read("cm", true))
	rr16Resolve(t, workclass.Customer, map[string]any{}, rr16Read("cm", true))
	after := RefusedReadSnapshot()
	for _, c := range []workclass.Class{workclass.Prewarm, workclass.Customer} {
		if d := after[c.String()] - before[c.String()]; d != 1 {
			t.Errorf("class %s counted %d, want 1", c, d)
		}
	}
	if len(after) != len(workclass.Classes)+1 {
		t.Errorf("every class cell must be present (a 0 is a reading): %v", after)
	}
}
