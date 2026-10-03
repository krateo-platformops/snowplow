// learned_identity_privacy.go — #262 privacy rail for LEARNED identity seeds.
//
// A learned class is a real username plus its full group set (read from
// authn's `<user>-clientconfig` certificates or live traffic). The hard rule:
// no CN, no Secret name (it embeds the username) and no group string may reach
// a log line or a debug surface. A learned seed is a REAL resolve under that
// identity, and the resolve path below the seed (EvaluateRBAC, the refilter,
// the informer serve, the key diagnostic) logs its requester's username and
// groups through the ctx logger. So every seed ctx built for a learned class
// carries a REDACTING logger: any occurrence of the class's tokens in a
// message, an attribute or a WithAttrs/WithGroup prefix is replaced by the
// class's sha256 label (cache.LearnedClassLabel) before the record reaches the
// real handler. Base-cohort seeds are untouched (the ctx logger stays
// slog.Default(), byte-identical to before).

package dispatchers

import (
	"context"
	"fmt"
	"log/slog"
	"sort"
	"strings"

	xcontext "github.com/krateo-platformops/plumbing/context"
	"github.com/krateo-platformops/plumbing/kubeutil"
	"github.com/krateo-platformops/snowplow/internal/cache"
)

// learnedRedactionTokens returns the strings a learned class's logs must never
// carry: the username, its clientconfig Secret name (the DNS-1123 form authn
// writes) and every group except system:authenticated (a constant every
// identity carries, not an identity fact). Longest first, so a token that
// contains another is replaced whole.
func learnedRedactionTokens(username string, groups []string) []string {
	seen := map[string]struct{}{}
	var out []string
	add := func(s string) {
		if s == "" {
			return
		}
		if _, dup := seen[s]; dup {
			return
		}
		seen[s] = struct{}{}
		out = append(out, s)
	}
	add(username)
	if username != "" {
		add(kubeutil.MakeDNS1123Compatible(username) + "-clientconfig")
		add(kubeutil.MakeDNS1123Compatible(username))
	}
	for _, g := range groups {
		if g == "system:authenticated" {
			continue
		}
		add(g)
	}
	sort.SliceStable(out, func(i, j int) bool { return len(out[i]) > len(out[j]) })
	return out
}

// redactingHandler scrubs a learned class's tokens out of every record.
type redactingHandler struct {
	inner  slog.Handler
	tokens []string
	repl   string
}

func (h *redactingHandler) scrub(s string) string {
	for _, t := range h.tokens {
		if strings.Contains(s, t) {
			s = strings.ReplaceAll(s, t, h.repl)
		}
	}
	return s
}

func (h *redactingHandler) carries(s string) bool {
	for _, t := range h.tokens {
		if strings.Contains(s, t) {
			return true
		}
	}
	return false
}

func (h *redactingHandler) scrubAttr(a slog.Attr) slog.Attr {
	a.Key = h.scrub(a.Key)
	v := a.Value.Resolve()
	switch v.Kind() {
	case slog.KindString:
		a.Value = slog.StringValue(h.scrub(v.String()))
	case slog.KindGroup:
		in := v.Group()
		out := make([]slog.Attr, 0, len(in))
		for _, ga := range in {
			out = append(out, h.scrubAttr(ga))
		}
		a.Value = slog.GroupValue(out...)
	case slog.KindAny:
		switch x := v.Any().(type) {
		case []string:
			cp := make([]string, len(x))
			for i, s := range x {
				cp[i] = h.scrub(s)
			}
			a.Value = slog.AnyValue(cp)
		case []byte:
			// slog's JSON handler base64-encodes a []byte, which a text scan
			// would miss; scrub the decoded content.
			if h.carries(string(x)) {
				a.Value = slog.StringValue(h.scrub(string(x)))
			}
		case error:
			if h.carries(x.Error()) {
				a.Value = slog.StringValue(h.scrub(x.Error()))
			}
		default:
			if s := fmt.Sprint(x); h.carries(s) {
				a.Value = slog.StringValue(h.scrub(s))
			}
		}
	default:
		a.Value = v
	}
	return a
}

func (h *redactingHandler) Enabled(ctx context.Context, l slog.Level) bool {
	return h.inner.Enabled(ctx, l)
}

func (h *redactingHandler) Handle(ctx context.Context, r slog.Record) error {
	nr := slog.NewRecord(r.Time, r.Level, h.scrub(r.Message), r.PC)
	r.Attrs(func(a slog.Attr) bool {
		nr.AddAttrs(h.scrubAttr(a))
		return true
	})
	return h.inner.Handle(ctx, nr)
}

func (h *redactingHandler) WithAttrs(as []slog.Attr) slog.Handler {
	scrubbed := make([]slog.Attr, 0, len(as))
	for _, a := range as {
		scrubbed = append(scrubbed, h.scrubAttr(a))
	}
	return &redactingHandler{inner: h.inner.WithAttrs(scrubbed), tokens: h.tokens, repl: h.repl}
}

func (h *redactingHandler) WithGroup(name string) slog.Handler {
	return &redactingHandler{inner: h.inner.WithGroup(h.scrub(name)), tokens: h.tokens, repl: h.repl}
}

// learnedRedactingLogger wraps base so the class's tokens never leave it.
func learnedRedactingLogger(base *slog.Logger, username string, groups []string) *slog.Logger {
	if base == nil {
		base = slog.Default()
	}
	return slog.New(&redactingHandler{
		inner:  base.Handler(),
		tokens: learnedRedactionTokens(username, groups),
		repl:   cache.LearnedClassLabel(username, groups),
	})
}

// learnedSeedLabelKey marks a seed ctx built for a learned class; the value is
// the class's sha256 label, the only form a log line may carry.
type learnedSeedLabelKey struct{}

func withLearnedSeedLabel(ctx context.Context, label string) context.Context {
	return context.WithValue(ctx, learnedSeedLabelKey{}, label)
}

func learnedSeedLabelFromCtx(ctx context.Context) (string, bool) {
	l, ok := ctx.Value(learnedSeedLabelKey{}).(string)
	return l, ok && l != ""
}

// seedDiagLogger is the logger the seed primitives' key diagnostic uses: the
// ctx logger of a learned seed (redacting), else slog.Default() exactly as
// before.
func seedDiagLogger(ctx context.Context) *slog.Logger {
	if _, learned := learnedSeedLabelFromCtx(ctx); learned {
		return xcontext.Logger(ctx)
	}
	return slog.Default()
}

// isLearnedSeedTarget reports whether a seed target is (or coincides with) a
// learned identity — the predicate every privacy-relevant seed site keys on. A
// re-mint (#378) or a reseed rebuilds its identity from a cell's representative
// without the flag, and that representative may be a class since replaced by a
// re-login or removed with its Secret, so the predicate is "this username has
// been a learned class in this process" (cache.WasLearnedUsername).
func isLearnedSeedTarget(c seedTarget) bool {
	return c.Learned || isLearnedIdentity(c.Username)
}

// isLearnedIdentity is the privacy predicate for an identity outside the seed
// loop (the refresher's representative).
func isLearnedIdentity(username string) bool {
	return cache.WasLearnedUsername(username)
}
