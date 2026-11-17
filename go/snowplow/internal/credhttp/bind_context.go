package credhttp

// bind_context.go — #1318 review F1: a client built for ONE unit of work ends
// every exchange with that unit.
//
// client-go sends some requests on a context of its own: the discovery client
// and the RESTMapper behind a dynamic client use context.TODO(), so the unit's
// ctx never reaches them and nothing bounds them. BindContext puts the unit's
// end on the transport, the one place every such exchange passes through. It is
// BindRequestBudget's counterpart for work with a deadline and no customer
// budget (a detached CRD-schema fill, schema_fill.go).

import (
	"context"
	"net/http"

	"k8s.io/client-go/rest"
)

// BindContext returns a copy of rc whose every round trip also ends when ctx
// does (its deadline or its cancellation), including those client-go sends on
// context.TODO(). A round trip ends at the earlier of its own ctx and ctx. When
// ctx can never end (no Done channel), rc is returned unchanged.
func BindContext(ctx context.Context, rc *rest.Config) *rest.Config {
	if rc == nil || ctx.Done() == nil {
		return rc
	}
	cp := rest.CopyConfig(rc)
	noClientRateLimit(cp) // #920: a config credhttp returns never carries a client-side limiter
	cp.Wrap(func(rt http.RoundTripper) http.RoundTripper { return &boundContext{next: rt, ctx: ctx} })
	return cp
}

type boundContext struct {
	next http.RoundTripper
	ctx  context.Context
}

func (t *boundContext) WrappedRoundTripper() http.RoundTripper { return t.next }

func (t *boundContext) RoundTrip(req *http.Request) (*http.Response, error) {
	rctx, cancel := context.WithCancelCause(req.Context())
	stop := context.AfterFunc(t.ctx, func() { cancel(context.Cause(t.ctx)) })
	end := func() { stop(); cancel(nil) }
	resp, err := t.next.RoundTrip(req.WithContext(rctx))
	if err != nil || resp == nil || resp.Body == nil {
		end()
		return resp, err
	}
	// The unit's end bounds the body read too; closing the body releases it.
	resp.Body = &endOnClose{ReadCloser: resp.Body, end: end}
	return resp, nil
}
