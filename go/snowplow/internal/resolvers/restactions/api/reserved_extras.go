// reserved_extras.go — #1116 (SECURITY): a resolve's extras never name a key
// the resolver itself writes.
//
// THE DEFECT. The resolve dict starts as a deep copy of the request extras
// (newResolveRun), and the resolver then writes its own keys into the same
// map: each stage's output under the stage's name, each stage's errors under
// its errorKey, and the pagination block under "slice". A stage that runs
// APPENDS to a key already present (jsonHandlerCore splices a list seed into
// the real list); a stage that is denied, skipped or never dialled leaves the
// caller's value where its output would be. A later stage that builds its
// path, payload, headers, iterator, endpointRef or userAccessFilter
// resourcesFrom from that key then dials with the caller's value, and with a
// credential the caller does not hold (the snowplow ServiceAccount of a
// userAccessFilter stage, a named endpointRef's Secret). Measured on the
// portal corpus: compositions-list and composition-detail return a kind the
// caller cannot read, blueprint-detail has the ServiceAccount list a path the
// caller chose (#1116).
//
// THE RULE. These keys are reserved for one RESTAction:
//   - every stage's name;
//   - every stage's effective errorKey: its errorKey, or "error" for a stage
//     that declares none (createRequestOption and the iterator-error path
//     both write there);
//   - "slice", the pagination block resolve.go writes.
//
// Extras that name any of them are REFUSED, never dropped: the resolve does
// not start, nothing is dialled or read, and the caller gets a 400 whose
// details.causes name every reserved key it sent (the dispatchers render
// *ReservedExtrasError, reserved_extras_status.go). Dropping the key would
// hand the caller a different answer than the one it asked for, silently.
//
// WHERE IT HOLDS. ResolveWithFailure is the one function that seeds the dict
// from extras, so the rule is checked there, before anything else: every
// RESTAction resolve passes it (a /call GET or POST /call/read, a widget's
// apiRef, a nested resolve:true step, the refresher's re-resolve from its
// stored extras, the boot seeds, /export). restactions.Resolve checks the same
// rule first so the refusal reaches its caller as an error, not as an empty
// dict through the RESTAction's filter.
//
// THE KEYS ARE NAMES THE CALLER CAN ALREADY READ. The 400 names stage names
// and errorKeys. Every path reaches the check only after the RESTAction CR
// was read under the caller's authorisation (the dispatch RBAC gate, or the
// caller's own apiserver read with the cache off), so the cause tells the
// caller nothing its own GET of the CR would not.
package api

import (
	"context"
	"errors"
	"expvar"
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"sync"
	"sync/atomic"

	xcontext "github.com/krateo-platformops/plumbing/context"
	"github.com/krateo-platformops/plumbing/ptr"
	templates "github.com/krateo-platformops/snowplow/apis/templates/v1"
)

// Owners of a reserved key (the closed set the 400's causes and the
// snowplow_extras_reserved_key_refused_total cells use).
const (
	ReservedByStage    = "stage"
	ReservedByErrorKey = "errorKey"
	ReservedBySlice    = "slice"
)

// sliceDictKey is the pagination block resolve.go writes (newResolveRun).
const sliceDictKey = "slice"

// defaultErrorKey is the errorKey of a stage that declares none
// (createRequestOption, the iterator-error path).
const defaultErrorKey = "error"

// ReservedKey is one reserved dict key and who writes it.
type ReservedKey struct {
	Key   string
	Owner string // ReservedByStage | ReservedByErrorKey | ReservedBySlice
	Stage string // the stage that owns it ("" for slice)
}

// ReservedExtrasError is the refusal of extras that name reserved keys. Keys
// is sorted by Key and holds every offending key.
type ReservedExtrasError struct {
	Keys []ReservedKey
}

func (e *ReservedExtrasError) Error() string {
	parts := make([]string, 0, len(e.Keys))
	for _, k := range e.Keys {
		parts = append(parts, reservedKeyMessage(k))
	}
	return "extras name keys the RESTAction resolver writes itself: " + strings.Join(parts, "; ")
}

// reservedKeyMessage is the human text of one cause: the key, and what owns it.
func reservedKeyMessage(k ReservedKey) string {
	switch k.Owner {
	case ReservedByStage:
		return fmt.Sprintf("%q is the name of stage %q (its output)", k.Key, k.Stage)
	case ReservedByErrorKey:
		return fmt.Sprintf("%q is the errorKey of stage %q", k.Key, k.Stage)
	default:
		return fmt.Sprintf("%q is the pagination block (use the page and perPage query parameters)", k.Key)
	}
}

// Message is reservedKeyMessage for a dispatcher's cause.
func (k ReservedKey) Message() string { return reservedKeyMessage(k) }

// ReservedDictKeys returns every dict key the resolver writes for items.
// A key owned twice (a stage named like another stage's errorKey) reports the
// stage: the stage's own output is the stronger claim.
func ReservedDictKeys(items []*templates.API) map[string]ReservedKey {
	out := map[string]ReservedKey{sliceDictKey: {Key: sliceDictKey, Owner: ReservedBySlice}}
	for _, it := range items {
		if it == nil {
			continue
		}
		ek := ptr.Deref(it.ErrorKey, defaultErrorKey)
		if prev, ok := out[ek]; !ok || prev.Owner == ReservedBySlice {
			out[ek] = ReservedKey{Key: ek, Owner: ReservedByErrorKey, Stage: it.Name}
		}
	}
	for _, it := range items {
		if it == nil {
			continue
		}
		out[it.Name] = ReservedKey{Key: it.Name, Owner: ReservedByStage, Stage: it.Name}
	}
	return out
}

// CheckReservedExtras is THE #1116 rule: nil, or a *ReservedExtrasError naming
// every key of extras the resolver writes for items.
func CheckReservedExtras(items []*templates.API, extras map[string]any) error {
	if len(extras) == 0 || len(items) == 0 {
		return nil
	}
	reserved := ReservedDictKeys(items)
	var bad []ReservedKey
	for k := range extras {
		if r, ok := reserved[k]; ok {
			bad = append(bad, r)
		}
	}
	if len(bad) == 0 {
		return nil
	}
	sort.Slice(bad, func(i, j int) bool { return bad[i].Key < bad[j].Key })
	return &ReservedExtrasError{Keys: bad}
}

// RefuseReservedExtras enforces the rule for one resolve: nil when extras name
// no reserved key; otherwise it counts and logs the refusal, records the
// resolve as Aborted (every stage NotRun) on the outcome collector this
// resolve claims, and returns the *ReservedExtrasError. Nothing has been read
// or dialled when it returns. raNamespace/raName name the RESTAction on the
// log line only.
func RefuseReservedExtras(ctx context.Context, items []*templates.API, extras map[string]any, raNamespace, raName string) error {
	re, ok := AsReservedExtrasError(CheckReservedExtras(items, extras))
	if !ok {
		return nil
	}
	recordReservedExtrasRefused(re)
	keys := make([]string, 0, len(re.Keys))
	for _, k := range re.Keys {
		keys = append(keys, k.Owner+":"+k.Key)
	}
	xcontext.Logger(ctx).WarnContext(ctx, "resolve refused: extras name keys the resolver writes (#1116)",
		slog.String("restaction_namespace", raNamespace),
		slog.String("restaction_name", raName),
		slog.Any("reserved_keys", keys))
	abortedResolve(ctx, items)
	return re
}

// AsReservedExtrasError unwraps err to the #1116 refusal, if it is one.
func AsReservedExtrasError(err error) (*ReservedExtrasError, bool) {
	var re *ReservedExtrasError
	if errors.As(err, &re) {
		return re, true
	}
	return nil, false
}

// ---- observability: snowplow_extras_reserved_key_refused_total{owner} ----

var reservedKeyRefused struct {
	stage, errorKey, slice atomic.Uint64
}

// recordReservedExtrasRefused counts one refused resolve per reserved key it
// named, by owner. Ungated on the cache mode: the rule holds on every resolve.
func recordReservedExtrasRefused(e *ReservedExtrasError) {
	for _, k := range e.Keys {
		switch k.Owner {
		case ReservedByStage:
			reservedKeyRefused.stage.Add(1)
		case ReservedByErrorKey:
			reservedKeyRefused.errorKey.Add(1)
		default:
			reservedKeyRefused.slice.Add(1)
		}
	}
}

// ReservedExtrasRefusedSnapshot returns the {owner} refusal counts; every
// cell is present, so a 0 is a reading.
func ReservedExtrasRefusedSnapshot() map[string]uint64 {
	return map[string]uint64{
		ReservedByStage:    reservedKeyRefused.stage.Load(),
		ReservedByErrorKey: reservedKeyRefused.errorKey.Load(),
		ReservedBySlice:    reservedKeyRefused.slice.Load(),
	}
}

var reservedKeyExpvarOnce sync.Once

func init() { registerReservedKeyExpvar() }

// registerReservedKeyExpvar publishes snowplow_extras_reserved_key_refused_total.
// Ungated on the cache mode (listed in the CFG-1 nonCacheInitPublishers
// exception): the refusal happens on every RESTAction resolve.
func registerReservedKeyExpvar() {
	reservedKeyExpvarOnce.Do(func() {
		expvar.Publish("snowplow_extras_reserved_key_refused_total", expvar.Func(func() any {
			return ReservedExtrasRefusedSnapshot()
		}))
	})
}
