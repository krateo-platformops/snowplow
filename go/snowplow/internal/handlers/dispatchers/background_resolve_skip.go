// background_resolve_skip.go — #1477 (generalises #113): every background
// resolve site skips a resolve that needs request extras it does not carry.
//
// #113 skipped, in the RESTAction seed only, a no-extras resolve of a
// RESTAction whose api-step endpointRef.name is a template: rendered with no
// extras, the name selects a Secret no customer's request would, the endpoint
// read misses, and the stage truncates. No customer key can match the cell such
// a resolve produces (the customer's request carries the key the template
// reads, and every cache key folds the extras), so it is work nobody can use,
// plus a possibly truncated body. The discovery walk, the widget cohort seed
// and the boot content prewarm had no such guard: on 057 they re-resolved, as
// the ServiceAccount and with no extras, widgets whose RESTAction builds its
// endpointRef from `.name` — 359 ERROR lines a day, one burst per keepwarm and
// per gvr-discovered pass.
//
// THE ONE PREDICATE is restactionsapi.NeedsRequestExtras: a templated
// endpointRef.name that reads a dict key neither carried by the resolve nor
// written by the resolver itself. Each site asks it with the extras its
// resolve carries:
//   - the RESTAction seed and the content prewarm resolve the RESTAction with
//     none (restactionNeedsRequestExtras(cr, nil), which is #113's
//     hasTemplatedEndpointRef);
//   - the walk and the widget seed resolve a widget, whose apiRef RESTAction
//     receives the widget's spec.apiRef.extras and declared identity
//     (widgets.ApiRefExtras, the derivation widgets.Resolve itself uses):
//     widgetNeedsRequestExtras. A widget whose inline extras supply the key is
//     resolved.
// Decided by the declared template and the carried extras only, never by a
// resource, RESTAction or widget name.
//
// THE COUNTER. snowplow_prewarm_resolve_skipped_total{site,reason}: one count
// per skipped resolve. site is the closed set below, reason the closed set
// {needs_request_extras}; every cell is published, so a 0 is a reading.

package dispatchers

import (
	"context"
	"expvar"
	"log/slog"
	"sync"
	"sync/atomic"

	"github.com/krateo-platformops/plumbing/jqutil"
	templatesv1 "github.com/krateo-platformops/snowplow/apis/templates/v1"
	"github.com/krateo-platformops/snowplow/internal/cache"
	"github.com/krateo-platformops/snowplow/internal/objects"
	restactionsapi "github.com/krateo-platformops/snowplow/internal/resolvers/restactions/api"
	"github.com/krateo-platformops/snowplow/internal/resolvers/widgets"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

// Background resolve sites (closed set).
const (
	bgSkipSiteRestactionSeed = "restaction_seed"
	bgSkipSiteWidgetSeed     = "widget_seed"
	bgSkipSiteWalk           = "walk"
	bgSkipSiteContentPrewarm = "content_prewarm"
)

// Skip reasons (closed set).
const bgSkipReasonNeedsRequestExtras = "needs_request_extras"

var backgroundResolveSkipSites = []string{
	bgSkipSiteRestactionSeed, bgSkipSiteWidgetSeed, bgSkipSiteWalk, bgSkipSiteContentPrewarm,
}

var backgroundResolveSkipReasons = []string{bgSkipReasonNeedsRequestExtras}

// backgroundResolveSkipped holds every {site, reason} cell, pre-populated;
// the maps are never written after init, only the counters.
var backgroundResolveSkipped = func() map[string]map[string]*atomic.Uint64 {
	m := make(map[string]map[string]*atomic.Uint64, len(backgroundResolveSkipSites))
	for _, s := range backgroundResolveSkipSites {
		m[s] = make(map[string]*atomic.Uint64, len(backgroundResolveSkipReasons))
		for _, r := range backgroundResolveSkipReasons {
			m[s][r] = &atomic.Uint64{}
		}
	}
	return m
}()

// bumpBackgroundResolveSkip counts one skipped resolve. A site or reason
// outside the closed sets is not recorded, so the label set cannot grow.
func bumpBackgroundResolveSkip(site, reason string) {
	if c := backgroundResolveSkipped[site][reason]; c != nil {
		c.Add(1)
	}
}

// BackgroundResolveSkipCount is one {site, reason} cell.
type BackgroundResolveSkipCount struct {
	Site, Reason string
	Count        uint64
}

// BackgroundResolveSkipSnapshot returns every {site, reason} cell, zeros
// included, in the closed-set order.
func BackgroundResolveSkipSnapshot() []BackgroundResolveSkipCount {
	out := make([]BackgroundResolveSkipCount, 0, len(backgroundResolveSkipSites)*len(backgroundResolveSkipReasons))
	for _, s := range backgroundResolveSkipSites {
		for _, r := range backgroundResolveSkipReasons {
			out = append(out, BackgroundResolveSkipCount{Site: s, Reason: r, Count: backgroundResolveSkipped[s][r].Load()})
		}
	}
	return out
}

var backgroundResolveSkipExpvarOnce sync.Once

// CFG-1: published only with the cache on, like every prewarm counter.
func init() {
	if cache.Disabled() {
		return
	}
	backgroundResolveSkipExpvarOnce.Do(func() {
		expvar.Publish("snowplow_prewarm_resolve_skipped_total", expvar.Func(func() any {
			m := map[string]map[string]uint64{}
			for _, c := range BackgroundResolveSkipSnapshot() {
				if m[c.Site] == nil {
					m[c.Site] = map[string]uint64{}
				}
				m[c.Site][c.Reason] = c.Count
			}
			return m
		}))
	})
}

// backgroundRESTActionGetFn reads a RESTAction CR for a background resolve
// site: the walk's and the widget seed's read of a widget's apiRef RESTAction
// (widgetNeedsRequestExtras) and the content prewarm's fetch. Production wires
// it to objects.Get; only the #1477 _test.go arms reassign it, to serve a
// templated- or literal-endpointRef RESTAction without a cluster. Its own seam,
// not seedObjectsGetFn: tests read a seedObjectsGetFn call as "the RESTAction
// seed ran".
var backgroundRESTActionGetFn = objects.Get

// restactionNeedsRequestExtras is the one predicate (#1477) over a RESTAction
// CR: a resolve of cr carrying `carried` would render a templated endpointRef
// with a key nobody supplied. nil-safe.
func restactionNeedsRequestExtras(cr *templatesv1.RESTAction, carried map[string]any) bool {
	if cr == nil {
		return false
	}
	return restactionsapi.NeedsRequestExtras(cr.Spec.API, carried)
}

// widgetNeedsRequestExtras applies the predicate to a background resolve of
// the widget w (no request extras): the RESTAction its spec.apiRef names,
// with the extras widgets.Resolve threads into that RESTAction
// (widgets.ApiRefExtras). A widget with no apiRef, or whose RESTAction cannot
// be read, is not skipped: its resolve reports what it reports today. It runs
// for every walked widget and every seed unit, so it decodes only the stage
// fields the predicate reads (predicateStepsOf), and only for a RESTAction
// that has a templated endpointRef at all (#1477 review N3). The RESTAction is read through backgroundRESTActionGetFn (objects.Get)
// from the informer only (cache.WithInformerOnlyReads, as
// apiref.DeclaresExternalEndpoint reads it): the predicate never costs an
// apiserver round trip, and a RESTAction the informer does not hold is
// resolved as before.
func widgetNeedsRequestExtras(ctx context.Context, w *unstructured.Unstructured) bool {
	if w == nil {
		return false
	}
	ref, err := widgets.GetApiRef(w.Object)
	if err != nil || ref.Name == "" || ref.Namespace == "" {
		return false
	}
	got := backgroundRESTActionGetFn(cache.WithInformerOnlyReads(ctx), ref)
	if got.Err != nil || got.Unstructured == nil {
		return false
	}
	steps, templated := predicateStepsOf(got.Unstructured.Object)
	if !templated {
		return false
	}
	return restactionsapi.NeedsRequestExtras(steps, widgets.ApiRefExtras(ctx, w.Object, nil))
}

// predicateStepsOf reads, from a RESTAction's unstructured object, exactly the
// api-step fields restactionsapi.NeedsRequestExtras reads (name, path, payload,
// headers, errorKey, filter, endpointRef, dependsOn.iterator, userAccessFilter
// resourcesFrom), without converting the whole object. templated is false, and
// nothing is decoded, when no step's endpointRef.name is a `${...}` template:
// the predicate is then false whatever the rest says. A field of the wrong
// type is read as absent, as the predicate would see a step without it.
func predicateStepsOf(obj map[string]any) (steps []*templatesv1.API, templated bool) {
	raw, found, err := unstructured.NestedFieldNoCopy(obj, "spec", "api")
	if err != nil || !found {
		return nil, false
	}
	list, ok := raw.([]any)
	if !ok {
		return nil, false
	}
	str := func(m map[string]any, k string) string {
		v, _ := m[k].(string)
		return v
	}
	sub := func(m map[string]any, k string) map[string]any {
		v, _ := m[k].(map[string]any)
		return v
	}
	for _, it := range list {
		m, ok := it.(map[string]any)
		if !ok {
			continue
		}
		if _, isTemplate := jqutil.MaybeQuery(str(sub(m, "endpointRef"), "name")); isTemplate {
			templated = true
			break
		}
	}
	if !templated {
		return nil, false
	}
	steps = make([]*templatesv1.API, 0, len(list))
	for _, it := range list {
		m, ok := it.(map[string]any)
		if !ok {
			steps = append(steps, nil)
			continue
		}
		st := &templatesv1.API{Name: str(m, "name"), Path: str(m, "path")}
		if v, ok := m["payload"].(string); ok {
			st.Payload = &v
		}
		if v, ok := m["errorKey"].(string); ok {
			st.ErrorKey = &v
		}
		if v, ok := m["filter"].(string); ok {
			st.Filter = &v
		}
		if hs, ok := m["headers"].([]any); ok {
			for _, h := range hs {
				if hv, ok := h.(string); ok {
					st.Headers = append(st.Headers, hv)
				}
			}
		}
		if ep := sub(m, "endpointRef"); ep != nil {
			st.EndpointRef = &templatesv1.Reference{Name: str(ep, "name"), Namespace: str(ep, "namespace")}
		}
		if dep := sub(m, "dependsOn"); dep != nil {
			st.DependsOn = &templatesv1.Dependency{Name: str(dep, "name")}
			if v, ok := dep["iterator"].(string); ok {
				st.DependsOn.Iterator = &v
			}
		}
		if uaf := sub(m, "userAccessFilter"); uaf != nil {
			st.UserAccessFilter = &templatesv1.UserAccessFilterSpec{ResourcesFrom: str(uaf, "resourcesFrom")}
		}
		steps = append(steps, st)
	}
	return steps, true
}

// logBackgroundResolveSkip counts the skip and writes its DEBUG line: the
// site and the object (namespace/name of the widget or RESTAction; never an
// identity).
func logBackgroundResolveSkip(site, kind, target string) {
	bumpBackgroundResolveSkip(site, bgSkipReasonNeedsRequestExtras)
	slog.Default().Debug("prewarm.resolve.skip.needs_request_extras",
		slog.String("subsystem", "cache"),
		slog.String("site", site),
		slog.String(kind, target),
		slog.String("effect", "a templated endpointRef reads a request extra this background resolve does not "+
			"carry: no customer key can match its cell (#1477); not resolved"),
	)
}
