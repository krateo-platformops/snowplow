// Package loopbacktoken carries, on a prewarm (service-account) context, the
// SOURCE of the authn-issued token a bearer-carrying stage presents: a
// self-loopback stage at snowplow's own JWT-gated /call, or a stage that opts in
// with exportJwt (#57). A customer /call carries its requester's own token
// instead (xcontext.WithAccessToken) and never a source.
//
// #1530: the prewarm context used to carry the token itself, fetched while the
// context was built: every Phase 1 SA context (the navigation-root read, each
// root's walk, the pagination drain, the engine's boot re-walk) waited for authn,
// as long as its context lasted, although only a bearer-carrying stage uses the
// token. Now the stage asks the source, under its own context, once the bearer
// decision (restactions/api bearerAppendForStage) has said it carries one; work
// that never makes such a call never waits for authn.
package loopbacktoken

import "context"

// Source returns the token a bearer-carrying stage presents, or an error when
// it cannot be had now (the stage then runs token-less).
type Source func(ctx context.Context) (string, error)

type sourceKey struct{}

// WithSource returns ctx carrying src. A nil src leaves ctx unchanged.
func WithSource(ctx context.Context, src Source) context.Context {
	if src == nil {
		return ctx
	}
	return context.WithValue(ctx, sourceKey{}, src)
}

// SourceFrom returns the source ctx carries, or nil.
func SourceFrom(ctx context.Context) Source {
	src, _ := ctx.Value(sourceKey{}).(Source)
	return src
}
