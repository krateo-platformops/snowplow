// Package viewercontext is the one definition of the viewer-context vocabulary:
// the closed set of request-extras keys that describe WHO IS LOOKING rather
// than WHAT they are looking at (username, groups, displayName, tz), and, per
// key, whether the value is acceptable at all.
//
// A widget that does not declare a viewer-context key has it stripped before
// resolution (widgets.sanitizeUndeclaredViewerContextExtras), so its body cannot
// depend on it and the key does not move; a widget that declares it in
// spec.keyExtras receives it and it partitions the key. This package adds the
// third rule: a value that fails its key's validator is treated exactly as if
// it had not been sent — dropped at the request edge (util.ParseExtras), before
// the key, the Put decision and the resolve see it.
//
// It is a stdlib-only leaf so the request parser, the widget resolver and the
// dispatchers can all depend on it without importing each other.
package viewercontext

import (
	_ "embed"
	"expvar"
	"strings"
	"sync"
	"sync/atomic"
	// The IANA database travels with the binary (a distroless image has no
	// /usr/share/zoneinfo): zones.txt describes this database, and every name
	// in it is resolvable by time.LoadLocation (TestEveryListedZoneLoads).
	_ "time/tzdata"
)

// TZ is the viewer's IANA time zone, client-asserted (the SPA reads it from
// Intl and sends it as an extra). Unlike username/groups it cannot be injected
// from the JWT: the server does not know it.
const TZ = "tz"

// MaxTZLen bounds a zone name. The longest IANA names are ~30 bytes.
const MaxTZLen = 64

// entry is one vocabulary key.
type entry struct {
	// valid reports whether a client-supplied value is acceptable. nil = any
	// value (identity keys are only ever a spoofed claim; they are stripped or
	// partition the key, never trusted).
	valid func(v any) bool
	// clientAsserted marks a key whose value only the client knows: it can be
	// neither injected nor verified server-side, so its only protections are
	// strip-when-undeclared and partition-when-declared.
	clientAsserted bool
}

var vocabulary = map[string]entry{
	"username":    {},
	"groups":      {},
	"displayName": {},
	TZ:            {valid: validTZ, clientAsserted: true},
}

// Keys returns the vocabulary's key names (unordered).
func Keys() []string {
	out := make([]string, 0, len(vocabulary))
	for k := range vocabulary {
		out = append(out, k)
	}
	return out
}

// IsKey reports whether k is a viewer-context key.
func IsKey(k string) bool { _, ok := vocabulary[k]; return ok }

// ClientAsserted reports whether k is a viewer-context key whose value only the
// client can vouch for.
func ClientAsserted(k string) bool { return vocabulary[k].clientAsserted }

// Valid reports whether v is an acceptable value for k. A key outside the
// vocabulary, or one without a validator, accepts anything.
func Valid(k string, v any) bool {
	e, ok := vocabulary[k]
	if !ok || e.valid == nil {
		return true
	}
	return e.valid(v)
}

// validTZ accepts a value only if it is byte-for-byte a zone name the embedded
// tzdata carries (zones.txt). time.LoadLocation is NOT the test: it joins the
// name onto a zoneinfo directory, so it also accepts spellings of a zone that
// are not its name (`Europe//Rome`, `Europe/./Rome`, and on a case-insensitive
// filesystem `europe/rome`), and non-zone entries such as `Factory`. Each
// spelling would be its own cache cell for one zone, and what is accepted would
// depend on the OS. Exact membership has neither problem. Zone aliases that
// tzdata itself names (`UTC`, `Etc/UTC`, `Zulu`) are accepted as they are, not
// folded together. Empty and `Local` are not zone names, so they are refused
// without a special case.
func validTZ(v any) bool {
	s, ok := v.(string)
	if !ok || !shapeOK(s) {
		return false
	}
	_, member := zoneNames()[s]
	return member
}

// shapeOK is the cheap first filter: a non-empty string of at most MaxTZLen
// bytes made only of ASCII letters, digits and / _ + - . Nothing quote-, space-,
// newline-, control- or non-ASCII-shaped goes further, so it never reaches the
// set lookup, a log line or, downstream, an SQL literal. Every real zone name
// passes it (pinned by a test), so on its own it is only a filter; it is tested
// directly because the set lookup would hide a mistake in it.
func shapeOK(s string) bool {
	if s == "" || len(s) > MaxTZLen {
		return false
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z', c >= '0' && c <= '9',
			c == '/', c == '_', c == '+', c == '-':
		default:
			return false
		}
	}
	return true
}

// zonesTxt is the zone-name list of the IANA database the binary embeds
// (time/tzdata is the Go toolchain's lib/time/zoneinfo.zip), one name per line,
// without the non-zone entry `Factory`. The embedded database cannot be
// enumerated at run time (its archive is unexported), so the list is generated
// from that same zip and a test fails if a newer toolchain's zip names a zone
// the list lacks or the list keeps one the toolchain dropped (regenerate:
// go generate, see TestZonesListMatchesToolchainZip).
//
//go:generate go run gen_zones.go
//go:embed zones.txt
var zonesTxt string

var (
	zonesOnce sync.Once
	zones     map[string]struct{}
)

// zoneNames builds the set once, on first use.
func zoneNames() map[string]struct{} {
	zonesOnce.Do(func() {
		lines := strings.Split(zonesTxt, "\n")
		zones = make(map[string]struct{}, len(lines))
		for _, l := range lines {
			if l != "" {
				zones[l] = struct{}{}
			}
		}
	})
	return zones
}

var droppedTotal atomic.Uint64

func init() {
	expvar.Publish("snowplow_viewer_context_dropped_total", expvar.Func(func() any { return DroppedTotal() }))
}

// DroppedTotal counts viewer-context values dropped for failing their
// validator. Values are never recorded, only the count.
func DroppedTotal() uint64 { return droppedTotal.Load() }

// DropInvalid returns extras without the viewer-context entries whose value
// fails its validator. It returns the input map itself (no copy) when nothing
// is dropped; it never mutates its input.
func DropInvalid(extras map[string]any) map[string]any {
	var out map[string]any
	for k, v := range extras {
		if Valid(k, v) {
			continue
		}
		if out == nil {
			out = make(map[string]any, len(extras))
			for k2, v2 := range extras {
				out[k2] = v2
			}
		}
		delete(out, k)
		droppedTotal.Add(1)
	}
	if out == nil {
		return extras
	}
	return out
}
