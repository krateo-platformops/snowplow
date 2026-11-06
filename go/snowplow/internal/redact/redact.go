// Package redact keeps identity and data values off snowplow's log lines.
//
// THE RULE (#1192): no username, certificate CN, `<user>-clientconfig[-stepup]`
// Secret name or group string reaches a log line, in clear or as a label. The
// audit record is the one place that stores identity on purpose, and a log line
// reaches it through the traceId. That join is also why a per-identity label is
// not allowed on a line (#453 allowed one): a stable label on a request's line
// names everything else carrying that label as soon as one traceId is joined.
//
// Identity reaches a line by three routes, each closed once:
//   - a site's own attribute: the structural guard (log_guard_test.go) fails on
//     an identity key, or a label, on any slog site;
//   - the request logger: middleware.RequestLogger replaces plumbing's
//     use.Logger, which the guard also refuses;
//   - the text of an error, which the apiserver or authn wrote: ScrubLogAttr, in
//     buildLogHandler, so every record passes through it.
//
// User is the keyed label of a username, for the in-process uses that need one
// (cache.StaleSubject). It is
//
//	user:<12 hex of HMAC-SHA256(processKey, username)>
//
// processKey is 32 random bytes drawn once per process, so the label cannot be
// reversed by hashing a dictionary of tenant usernames, and it is not stable
// across pods or restarts. The same digest serves labels of opaque non-identity
// values (Digest, Prefixed).
//
// DATA VALUES (#487): a resolved stage body, a JQ result or a template dict
// may hold Secret data or per-user rows. It is never logged. ValueAttr /
// DictAttr log its type, JSON size and keyed digest, and DictAttr adds the
// caller's spec-defined stage ids.
//
// SCOPE: a label is never a cache key, a memo key, or a map key that must
// survive a restart, because it changes on every restart.
//
// The package is a stdlib-only leaf, so rbac, cache, objects, the resolvers and
// the handlers can all import it.
package redact

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log/slog"
	"regexp"
	"sort"
	"strings"
)

// Anonymous is the label of an empty identity. It is a constant, not an
// identity fact.
const Anonymous = "anonymous"

// Redacted stands where ErrorText removed an identity. It is one constant for
// every identity, so two lines cannot be told apart by who they were about.
const Redacted = "[redacted]"

// labelHexLen is the hex length of a label's digest (6 bytes).
const labelHexLen = 12

var processKey = func() []byte {
	k := make([]byte, 32)
	if _, err := rand.Read(k); err != nil {
		// crypto/rand.Read never fails on supported platforms (Go 1.24+ it
		// panics internally instead). Never fall back to an unkeyed digest.
		panic("redact: no entropy for the process label key: " + err.Error())
	}
	return k
}()

// Digest is the keyed digest every label is built from: 12 hex of
// HMAC-SHA256(processKey, s). It is exported for labels of non-identity
// opaque values that must not be logged in clear either.
func Digest(s string) string {
	m := hmac.New(sha256.New, processKey)
	m.Write([]byte(s))
	return hex.EncodeToString(m.Sum(nil))[:labelHexLen]
}

// User labels a username (a JWT subject, a cert CN, a binding subject name).
func User(username string) string {
	if username == "" {
		return Anonymous
	}
	return "user:" + Digest("u\x00"+username)
}

// Prefixed labels an opaque string under a caller-chosen prefix, for a label
// family whose prefix carries meaning on its own (cache.LearnedClassLabel's
// "learned:"). The input is the caller's exact class key.
func Prefixed(prefix, s string) string {
	return prefix + ":" + Digest("p\x00"+prefix+"\x00"+s)
}

// apiserverSubject matches the identity the apiserver names in an error: the
// requester of a Forbidden (`pods is forbidden: User "alice" cannot list ...`)
// and the subject of a refused impersonation (`users "alice" is forbidden`,
// `groups "devs" is forbidden`). clientconfig matches an authn per-user
// credential Secret name quoted in a NotFound: the session `<user>-clientconfig`
// (`secrets "alice-clientconfig" not found`) and, #994, the step-up
// `<user>-clientconfig-stepup`. It matches any quoted name carrying
// `-clientconfig` rather than the two suffixes, because this package stays a
// stdlib-only leaf and cannot import credsecret (which owns the suffixes); a
// scrubber that over-matches costs only a word of the message.
//
// Both accept a backslash before each quote: a Status body folded into an error
// keeps the JSON escaping of its message.
var (
	apiserverSubject = regexp.MustCompile(`\b(User|user|users|groups) (\\?")[^"\\]*\\?"`)
	clientconfig     = regexp.MustCompile(`(\\?")[^"]*-clientconfig[^"]*"`)
)

// ErrorText replaces the identity an apiserver or authn error string carries
// (see apiserverSubject, clientconfig) with Redacted. It keeps the rest of the
// text, and it is idempotent.
func ErrorText(s string) string {
	s = apiserverSubject.ReplaceAllString(s, "$1 ${2}"+Redacted+"$2")
	return clientconfig.ReplaceAllString(s, "${1}"+Redacted+"$1")
}

// Err is ErrorText of err ("" for a nil err).
func Err(err error) string {
	if err == nil {
		return ""
	}
	return ErrorText(err.Error())
}

// ScrubLogAttr is the slog.HandlerOptions.ReplaceAttr of snowplow's log
// handlers: ErrorText over every string and error attribute, and the message.
// A handler calls it only for a record it renders, so a disabled level costs
// nothing, and only a string holding a double quote pays for the regexps.
func ScrubLogAttr(_ []string, a slog.Attr) slog.Attr {
	var text string
	switch a.Value.Kind() {
	case slog.KindString:
		text = a.Value.String()
	case slog.KindAny:
		err, ok := a.Value.Any().(error)
		if !ok {
			return a
		}
		if text, ok = errorString(err); !ok {
			return a
		}
	default:
		return a
	}
	if strings.IndexByte(text, '"') < 0 {
		return a
	}
	if scrubbed := ErrorText(text); scrubbed != text {
		a.Value = slog.StringValue(scrubbed)
	}
	return a
}

// errorString is err.Error(), false when it panics: a typed nil pointer in an
// error, which the handler itself renders as <nil>. A log line never takes the
// process down.
func errorString(err error) (s string, ok bool) {
	defer func() {
		if recover() != nil {
			s, ok = "", false
		}
	}()
	return err.Error(), true
}

// ValueDigest summarises a data value (a stage body, a JQ result, a template
// dict) as its JSON size and keyed digest. It is the only form of a value
// that may reach a log line (#487): the value can hold Secret data or per-user
// rows. A value JSON cannot encode reports size -1 and a digest of its Go
// type.
func ValueDigest(v any) (size int, digest string) {
	b, err := json.Marshal(v)
	if err != nil {
		return -1, Digest("unencodable\x00" + fmt.Sprintf("%T", v))
	}
	return len(b), Digest("v\x00" + string(b))
}

// LAZY BY CONSTRUCTION (#490 review). ValueAttr and DictAttr return
// slog.Any(key, ref), where ref is a POINTER-SHAPED slog.LogValuer that holds
// only the reference. slog resolves a LogValuer only when a handler renders
// the record, so at a disabled level building the attr costs nothing: no JSON
// encoding, no HMAC, and no allocation. Boxing a single-pointer value into an
// interface does not allocate. All of the work runs inside LogValue, and
// LogValue renders only {type, bytes, sha256} and key labels, never a value.
// The pre-#490 eager form cost 144 ms / 95 MB per op on a 50K-item dict at
// LOG_LEVEL=warn.

// ValueAttr logs *v as {type, bytes, sha256}: never the value. v is a
// pointer, typically into the slice the value already lives in, so the attr
// is pointer-shaped.
func ValueAttr(key string, v *any) slog.Attr {
	return slog.Any(key, valueRef{p: v})
}

type valueRef struct{ p *any }

// LogValue renders only the redacted summary.
func (r valueRef) LogValue() slog.Value {
	var v any
	if r.p != nil {
		v = *r.p
	}
	return valueSummary(v)
}

func valueSummary(v any) slog.Value {
	n, d := ValueDigest(v)
	return slog.GroupValue(
		slog.String("type", fmt.Sprintf("%T", v)),
		slog.Int("bytes", n),
		slog.String("sha256", d),
	)
}

// DictAttr logs a resolve dict as its key count, total size and digest, plus
// {type, bytes, sha256} for each entry under KeyLabel(key). The key name is
// not logged, because a dict mixes spec-defined stage ids with request-supplied
// extras keys. A caller that knows which keys are spec-defined can log the
// mapping id -> KeyLabel(id) beside it (restactions does). Never a value.
func DictAttr(key string, dict map[string]any) slog.Attr {
	return slog.Any(key, dictRef(dict))
}

type dictRef map[string]any

// LogValue renders only the redacted summary.
func (d dictRef) LogValue() slog.Value {
	n, dg := ValueDigest(map[string]any(d))
	keys := make([]string, 0, len(d))
	for k := range d {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	entries := make([]slog.Attr, 0, len(keys))
	for _, k := range keys {
		entries = append(entries, slog.Attr{Key: KeyLabel(k), Value: valueSummary(d[k])})
	}
	return slog.GroupValue(
		slog.Int("keys", len(d)),
		slog.Int("bytes", n),
		slog.String("sha256", dg),
		slog.Attr{Key: "entries", Value: slog.GroupValue(entries...)},
	)
}

// KeyLabel is the log label of a dict key ("k:<hex>").
func KeyLabel(k string) string { return Prefixed("k", k) }
