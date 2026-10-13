// declined_external_mark.go — #643 PR-B: take a declined-external mark with
// the facts it holds under, and re-check those facts before every skip.
//
// THE SKIP (brief §1(b)). seedSkipDecision consults the engine-wide set BEFORE
// its per-mode switch, for every mode: a key whose earlier resolve touched an
// external endpoint is never Put (I-2), so re-resolving it in a background pass
// warms nothing and only re-dials the endpoint, once per cohort per pass.
//
// THE MARK (brief §1(c), amended #643 comment 6150618043). Only an OBSERVED
// external touch marks (the ExternalTouchedSink bump at resolve.go's external
// fall-through), in BOTH decline branches: a fetch abandoned at the
// boot-readiness bound bumps the stage-error sink as well, and those abandoned
// fetches are the bulk of the load. A stage error with NO external touch never
// marks. apiref.DeclaresExternalEndpoint is never the criterion (I-5): an
// endpointRef can point at the apiserver and dispatch internally.
//
// THE RE-CHECK (I-1, zero cold navigation). Before a mark suppresses a resolve
// every recorded fact is re-read; any change, and any failed read, retires the
// mark and the cell is resolved:
//   - the RESTAction's uid + resourceVersion (informer-only objects.Get — it
//     never dials);
//   - each static endpointRef Secret's resourceVersion (Secrets snapshot);
//   - the deps the resolve captured, against its entry stamp (d4: external
//     dispatch can be data-dependent).
// Put-unmark (a) and GONE (c) retire marks on the cache side
// (cache/declined_external_engine.go). Every retire is counted by reason on
// snowplow_prewarm_declined_external_retired_total.
//
// KNOWN, DOCUMENTED LIMITS (architect-ch review, #656):
//   - A widget unit's mark does not fold in the WIDGET's own uid/RV. Moot today:
//     the seed resolves the harvester's first-write-wins copy, so a widget edit
//     is not seen by the seed at all, and a customer /call Put of the edited
//     widget retires the mark via Put-unmark. Revisit if the harvester ever
//     refreshes its copies. The DISCOVERY WALK's marks (#643 PR-B2,
//     walk_declined_external.go) do fold the widget's uid/RV: the walk re-reads
//     the live widget every walk and replays its recorded children.
//   - endpointRef Secrets outside the Secrets informer's namespace
//     (cache.SecretsCacheNamespace), and templated endpointRef names, are not
//     tracked: a change to one does not retire the mark through rule (b).
//   - The served-cell coverage map is exact DepKey equality. A mismatch between
//     what a cell covers and what the resolve recorded only leaves that dep to
//     d4 — re-dials during a stale window at worst, never a cold cell.

package dispatchers

import (
	"context"
	"log/slog"

	"github.com/krateo-platformops/plumbing/jqutil"
	templatesv1 "github.com/krateo-platformops/snowplow/apis/templates/v1"
	"github.com/krateo-platformops/snowplow/internal/cache"
	"github.com/krateo-platformops/snowplow/internal/objects"
	"github.com/krateo-platformops/snowplow/internal/redact"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

// declineUnit names the seed unit a resolve belongs to, so a decline can record
// the coordinates its mark retires on. Installed on the resolve ctx by
// seedOneWidget / seedOneRestaction.
type declineUnit struct {
	widget cache.DepKey // zero for a RESTAction unit
	ra     templatesv1.ObjectReference
}

type ctxKeyDeclineUnitType struct{}

var ctxKeyDeclineUnit = ctxKeyDeclineUnitType{}

func withDeclineUnit(ctx context.Context, u declineUnit) context.Context {
	if ctx == nil {
		return ctx
	}
	return context.WithValue(ctx, ctxKeyDeclineUnit, u)
}

func declineUnitFromContext(ctx context.Context) (declineUnit, bool) {
	if ctx == nil {
		return declineUnit{}, false
	}
	u, ok := ctx.Value(ctxKeyDeclineUnit).(declineUnit)
	return u, ok
}

// raObjectRef is the objects.Get reference for a RESTAction coordinate.
func raObjectRef(namespace, name string) templatesv1.ObjectReference {
	return templatesv1.ObjectReference{
		Reference:  templatesv1.Reference{Name: name, Namespace: namespace},
		APIVersion: restActionGVR.Group + "/" + restActionGVR.Version,
		Resource:   restActionGVR.Resource,
	}
}

// readRAForMark reads the RESTAction informer-only (never a dial).
func readRAForMark(ctx context.Context, ref templatesv1.ObjectReference) (*unstructured.Unstructured, bool) {
	if ref.Name == "" || ref.Namespace == "" {
		return nil, false
	}
	got := objects.Get(cache.WithInformerOnlyReads(ctx), raObjectRef(ref.Namespace, ref.Name))
	if got.Err != nil || got.Unstructured == nil {
		return nil, false
	}
	return got.Unstructured, true
}

// staticEndpointSecrets returns the literal endpointRef names the RESTAction's
// stages dispatch through, in the namespace the Secrets informer serves. A
// templated name (#113) is not trackable and is left out; the seed skips a
// templated-endpointRef RESTAction anyway.
func staticEndpointSecrets(ra *unstructured.Unstructured) []string {
	api, _, _ := unstructured.NestedSlice(ra.Object, "spec", "api")
	var out []string
	seen := map[string]struct{}{}
	for _, st := range api {
		m, ok := st.(map[string]any)
		if !ok {
			continue
		}
		name, _, _ := unstructured.NestedString(m, "endpointRef", "name")
		ns, _, _ := unstructured.NestedString(m, "endpointRef", "namespace")
		if name == "" || ns != cache.SecretsCacheNamespace() {
			continue
		}
		if _, templated := jqutil.MaybeQuery(name); templated {
			continue
		}
		if _, dup := seen[name]; dup {
			continue
		}
		seen[name] = struct{}{}
		out = append(out, name)
	}
	return out
}

func secretResourceVersion(name string) (string, bool) {
	if !cache.SecretsCacheServable() {
		return "", false
	}
	snap := cache.SecretsSnapshotLoad()
	if snap == nil {
		return "", false
	}
	sec, ok := snap.ByName[name]
	if !ok || sec == nil {
		return "", false
	}
	return sec.ResourceVersion, true
}

// buildDeclinedExternalMark records the facts a mark taken under the resolve
// ctx holds under. resCtx is the resolve's ctx: it carries the dep-gen sink
// (deps + entry stamp) and the decline unit.
func buildDeclinedExternalMark(resCtx context.Context) *cache.DeclinedExternalMark {
	m := &cache.DeclinedExternalMark{}
	if deps, stamp, ok := cache.DepGenDepsFromContext(resCtx); ok {
		m.Deps, m.Stamp = deps, stamp
	}
	m.Served = cache.ServedCellsFromContext(resCtx)
	u, ok := declineUnitFromContext(resCtx)
	if !ok {
		return m
	}
	m.Widget = u.widget
	if u.ra.Name == "" {
		return m
	}
	m.RA = cache.DepKey{GVR: restActionGVR, Namespace: u.ra.Namespace, Name: u.ra.Name}
	ra, ok := readRAForMark(resCtx, u.ra)
	if !ok {
		// Unreadable now: record the coordinate only. The re-check re-reads it
		// and an unreadable RESTAction then retires the mark (doubt → resolve).
		return m
	}
	m.RAUID, m.RAResourceVersion = string(ra.GetUID()), ra.GetResourceVersion()
	for _, name := range staticEndpointSecrets(ra) {
		if rv, ok := secretResourceVersion(name); ok {
			m.Secrets = append(m.Secrets, cache.SecretVersion{Name: name, ResourceVersion: rv})
		}
	}
	return m
}

// markDeclinedExternal takes the mark for key under the resolve ctx (the decline
// site). No set on ctx (a /call, the refresher, a seed run outside the engine)
// → no mark.
func markDeclinedExternal(resCtx context.Context, key string) {
	set := cache.SeedDeclinedExternalSetFromContext(resCtx)
	if set == nil {
		return
	}
	set.MarkWith(key, buildDeclinedExternalMark(resCtx))
}

// declinedExternalMarkHolds re-checks every fact the mark was taken under.
// False on any change or failed read: the caller retires the mark and resolves.
func declinedExternalMarkHolds(ctx context.Context, m *cache.DeclinedExternalMark) (bool, string) {
	if m == nil {
		return false, cache.DeclinedExternalRetiredNilMark
	}
	// A cached input re-Put or gone: the resolve decided from a body that is no
	// longer the cell's (the refresh of a stale cell lands here).
	if servedCellsChangedFn(m.Served) {
		return false, cache.DeclinedExternalRetiredServedCellChanged
	}
	// d4 for the deps read directly (not through a served cell — those are
	// judged by the cell above, so a stale cell does not re-dial every pass).
	if len(m.Deps) > 0 && depsMovedFn(m.Deps, m.Stamp, m.Served) {
		return false, cache.DeclinedExternalRetiredDepsMoved
	}
	if m.RA.Name != "" {
		if m.RAUID == "" && m.RAResourceVersion == "" {
			return false, cache.DeclinedExternalRetiredRAUnreadAtMark
		}
		ra, ok := readRAForMark(ctx, raObjectRef(m.RA.Namespace, m.RA.Name))
		if !ok {
			return false, cache.DeclinedExternalRetiredRAUnreadable
		}
		if string(ra.GetUID()) != m.RAUID || ra.GetResourceVersion() != m.RAResourceVersion {
			return false, cache.DeclinedExternalRetiredRAChanged
		}
	}
	for _, s := range m.Secrets {
		rv, ok := secretResourceVersion(s.Name)
		if !ok || rv != s.ResourceVersion {
			return false, cache.DeclinedExternalRetiredEndpointSecretChanged
		}
	}
	return true, ""
}

// declinedExternalSkip is the consult (brief §1(b)): true iff key is marked and
// every fact behind the mark still holds. A mark that no longer holds is
// retired here and the cell is resolved.
func declinedExternalSkip(ctx context.Context, key, class, target string, cohortLabel redact.Label) bool {
	set := cache.SeedDeclinedExternalSetFromContext(ctx)
	m, ok := set.Lookup(key)
	if !ok {
		return false
	}
	if holds, reason := declinedExternalMarkHolds(ctx, m); !holds {
		if set.Unmark(key) {
			cache.RecordDeclinedExternalRetired(reason, 1)
		}
		slog.Default().Debug("phase1.seed.declined_external.retired",
			slog.String("subsystem", "cache"),
			slog.String("class", class),
			slog.String("target", target),
			slog.String("cohort", cohortLabel.String()),
			slog.String("reason", reason),
			slog.String("effect", "a fact the declined-external mark was taken under changed; the mark is "+
				"retired and the cell is resolved (it may now be cacheable)"),
		)
		return false
	}
	return true
}

// servedCellsChangedFn / depsMovedFn are the two data-retire checks, as seams
// ONLY so the F3d mutation arms can remove one at a time and prove each is
// load-bearing. Production never reassigns them.
var (
	servedCellsChangedFn = cache.ServedCellsChanged
	depsMovedFn          = cache.DepsMovedSinceUncovered
)

// declinedExternalSkipFn is the consult seam. Production never reassigns it; the
// F2 differential forces it off (and F2-m swaps in a mutant) to prove the skip
// changes no Put.
var declinedExternalSkipFn = declinedExternalSkip
