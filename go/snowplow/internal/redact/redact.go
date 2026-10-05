// Package redact is the single source of every identity label that may reach
// a log line, a debug surface or an expvar (#453).
//
// THE RULE: no username, certificate CN, `<user>-clientconfig` Secret name or
// group string is ever logged in clear. A site that wants to say WHICH identity
// it acted for renders it through this package. The label is
//
//	<prefix>:<12 hex of HMAC-SHA256(processKey, input)>
//
// processKey is 32 random bytes drawn once per process. The label is stable
// for the process lifetime, so every line one pod writes about one identity
// carries the same label and can be correlated, and the label cannot be
// reversed by hashing a dictionary of tenant usernames (a bare sha256 of a
// username can be). It is NOT stable across pods or restarts, by design. The
// cross-pod correlator is the cache key_hash, which is deterministic and
// carries no identity in clear.
//
// DATA VALUES (#487): a resolved stage body, a JQ result or a template dict
// may hold Secret data or per-user rows. It is never logged. ValueAttr /
// DictAttr log its type, JSON size and keyed digest, and DictAttr adds the
// caller's spec-defined stage ids.
//
// SCOPE: LOG AND DEBUG LABELS ONLY. A label is never a cache key, a memo key, a
// map key that must survive a restart, or an SSE key, because it changes on
// every restart. The structural guard (log_guard_test.go) fails when a
// slog site emits an identity field without going through this package.
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

// Anonymous is the label of an empty identity (no username, no groups). It is
// a constant, not an identity fact.
const Anonymous = "anonymous"

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

// Group labels one group name.
func Group(group string) string {
	if group == "" {
		return Anonymous
	}
	return "group:" + Digest("g\x00"+group)
}

// Groups labels each group of a set. The order of the input is kept, so the
// count and positions stay visible while the names do not.
func Groups(groups []string) []string {
	out := make([]string, len(groups))
	for i, g := range groups {
		out[i] = Group(g)
	}
	return out
}

// Identity labels a whole identity: the username plus its group set, order
// independent (the groups are sorted). The empty identity is Anonymous.
func Identity(username string, groups []string) string {
	if username == "" && len(groups) == 0 {
		return Anonymous
	}
	g := append([]string(nil), groups...)
	sort.Strings(g)
	return "id:" + Digest("i\x00"+username+"\x00"+strings.Join(g, "\x00"))
}

// Prefixed labels an opaque string under a caller-chosen prefix, for a label
// family whose prefix carries meaning on its own (cache.LearnedClassLabel's
// "learned:"). The input is the caller's exact class key.
func Prefixed(prefix, s string) string {
	return prefix + ":" + Digest("p\x00"+prefix+"\x00"+s)
}

// apiserverUser matches the requester the apiserver names in a Forbidden
// message (`pods is forbidden: User "alice" cannot list ...`); clientconfig
// matches an authn `<user>-clientconfig` Secret name quoted in a NotFound
// (`secrets "alice-clientconfig" not found`).
var (
	apiserverUser = regexp.MustCompile(`\b(User|user) "[^"]*"`)
	clientconfig  = regexp.MustCompile(`"[^"]*-clientconfig"`)
)

// ErrorText scrubs the identity an apiserver or authn error string carries
// (the Forbidden requester, a clientconfig Secret name) and replaces each
// with its redact label. It is a cheap, partial mitigation for an err that a
// snowplow site logs. The full treatment of error text is its own issue
// (#453 residual).
func ErrorText(s string) string {
	s = apiserverUser.ReplaceAllStringFunc(s, func(m string) string {
		i := strings.IndexByte(m, '"')
		return m[:i] + `"` + User(strings.Trim(m[i:], `"`)) + `"`
	})
	return clientconfig.ReplaceAllStringFunc(s, func(m string) string {
		return `"` + Prefixed("secret", strings.Trim(m, `"`)) + `"`
	})
}

// Err is ErrorText of err ("" for a nil err).
func Err(err error) string {
	if err == nil {
		return ""
	}
	return ErrorText(err.Error())
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

// Label is a value that is already a redact label. A parameter or variable of
// this type may be logged as is: the structural guard accepts it, and it
// rejects a Label(x) conversion of anything that is not itself redact output.
// Use it to carry a label through a call chain (the seed primitives' cohort
// label) without re-deriving it from the identity at each log site.
type Label string

// String returns the label text.
func (l Label) String() string { return string(l) }
