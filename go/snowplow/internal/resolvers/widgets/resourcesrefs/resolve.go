// Package resourcesrefs resolves a widget's static resourcesRefs. For each
// reference it fetches the named resource (or lists it, per the ref's verb),
// applies RBAC filtering, and returns the per-reference results the widget
// embeds in its status.
package resourcesrefs

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"net/url"
	"strconv"

	xcontext "github.com/krateo-platformops/plumbing/context"
	templatesv1 "github.com/krateo-platformops/snowplow/apis/templates/v1"
	"github.com/krateo-platformops/snowplow/internal/cache"
	"github.com/krateo-platformops/snowplow/internal/rbac"
	"github.com/krateo-platformops/snowplow/internal/redact"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/rest"
)

func Resolve(ctx context.Context, items []templatesv1.ResourceRef) ([]templatesv1.ResourceRefResult, error) {
	ep, err := xcontext.UserConfig(ctx)
	if err != nil {
		return nil, err
	}

	// 0.30.103: ClientConfigFor returns the context-injected
	// internal-dispatch *rest.Config when an internal/startup driver
	// (Phase 1's SA-credentialed walk) is in flight, else delegates to
	// the unchanged kubeconfig.NewClientConfig per-user path — see
	// cache.WithInternalRESTConfig.
	rc, err := cache.ClientConfigFor(ctx, ep)
	if err != nil {
		return nil, err
	}

	results := []templatesv1.ResourceRefResult{}
	for _, el := range items {
		res, err2 := resolveOne(ctx, rc, &el)
		if err2 != nil {
			err = errors.Join(err, err2)
			continue
		}

		results = append(results, res...)
	}

	return results, nil
}

func resolveOne(ctx context.Context, rc *rest.Config, in *templatesv1.ResourceRef) ([]templatesv1.ResourceRefResult, error) {
	all := []templatesv1.ResourceRefResult{}
	if in == nil {
		return all, nil
	}

	log := xcontext.Logger(ctx)

	gv, err := schema.ParseGroupVersion(in.APIVersion)
	if err != nil {
		return all, err
	}
	gvr := gv.WithResource(in.Resource)

	// Ship 2 (production-aim cleanup 2026-06-01) — resolve the Kind
	// through cache.KindForGVR. Built-in scheme arm + permanent
	// pluralsKindReverseStore serve the vast majority of widget
	// resourceRefs without an apiserver hop; CRD-backed kinds fall
	// through to one discovery hop per process lifetime (already
	// counted by ReasonPluralsDiscoveryHop inside KindForGVR).
	//
	// Counter attribution: ReasonResolverPluralsHit fires when the
	// in-process arms serve the lookup; ReasonResolverPluralsMiss
	// fires when discovery is required. We detect the hit/miss arm by
	// consulting the permanent reverse store BEFORE the call — the
	// builtin map is constant + the reverse store is monotonically
	// populated, so the pre-check is race-free w.r.t. attribution
	// (worst case: a concurrent KindForGVR populates the store
	// between our check and our call, and we record a "miss" the
	// next caller would record as "hit" — the per-cell counter still
	// rises monotonically and the attribution stabilises after the
	// first miss per (gvr) tuple). Replaces Ship D's
	// ReasonRestmapperKindFor + the dynamic.KindFor cold-restmapper
	// build per call (the helper was deleted in Ship 2).
	if cache.IsResolverPluralsHit(gvr) {
		cache.RecordResolverPluralsHit(ctx, gvr.String())
	} else {
		cache.RecordResolverPluralsMiss(ctx, gvr.String())
	}
	kindStr, err := cache.KindForGVR(ctx, gvr, rc)
	if err != nil {
		return all, err
	}
	gvk := gvr.GroupVersion().WithKind(kindStr)

	log.Info("resolving resource ref",
		slog.String("id", in.ID),
		slog.String("group", gvr.Group),
		slog.String("name", in.Name),
		slog.String("namespace", in.Namespace),
	)

	verbs := mapVerbs(in.Verb)
	for _, verb := range verbs {
		el := templatesv1.ResourceRefResult{
			ID:   in.ID,
			Verb: kubeToREST[verb],
			// #72: carry the source ref's Inline flag through to the dispatcher
			// inline-walk (it consumes it post-resolve; not re-read off spec).
			Inline: in.Inline,
		}

		el.Allowed = rbac.UserCan(ctx, rbac.UserCanOptions{
			Verb:          verb,
			GroupResource: gvr.GroupResource(),
			Namespace:     in.Namespace,
		})
		if !el.Allowed {
			// #214: a resourceRef RBAC denial is EXPECTED on the prewarm walk
			// (the per-user login-cohort seed resolves write-verb refs the user
			// may not hold) — Debug + count it there instead of WARN-spamming
			// (546 lines/12h on 057). A SERVE-path denial STILL WARNs: it may be
			// a real access problem and must stay visible. Log-only — el.Allowed
			// and the emitted result are untouched, so prewarm and serve stay
			// byte-identical.
			//
			// #563: #214 recognised ONE of the two markers that mean "not a
			// customer", so the refresher — which stamps WithBackgroundResolve
			// and never WithPrewarmPath — kept WARNing: 17,578 of 21,283 log
			// lines (83%) in 161 min on 057, flat at ~109/min, all write verbs.
			// cache.RefDenialOrigin answers "who is resolving" ONCE: the counter
			// cell and the log level both come from it, so a background driver
			// can no longer be de-WARNed without being attributed, nor
			// attributed without being de-WARNed. Every non-customer origin
			// Debugs; only the serve origin WARNs.
			origin := cache.RefDenialOrigin(ctx)
			cache.RecordRefDenied(origin)
			if origin == cache.RefDenialOriginServe {
				// #563 — the one denial that may be a REAL access problem is
				// also the one that was unactionable: it named the verb, the
				// resource and the namespace but never WHO was denied. The
				// identity goes in as its redact label, never in clear (#453);
				// a ctx with no UserInfo (the fail-closed denial) labels as
				// anonymous.
				ui, _ := xcontext.UserInfo(ctx)
				log.Warn("resource ref action not allowed",
					slog.String("id", in.ID),
					slog.String("verb", verb),
					slog.String("group", gvr.Group),
					slog.String("resource", gvr.Resource),
					slog.String("namespace", in.Namespace),
					slog.String("user", redact.User(ui.Username)))
			} else {
				log.Debug("resource ref action not allowed",
					slog.String("id", in.ID),
					slog.String("verb", verb),
					slog.String("group", gvr.Group),
					slog.String("resource", gvr.Resource),
					slog.String("namespace", in.Namespace),
					slog.String("origin", origin))
			}
		}

		el.Path = buildPath(gvr, in)

		if el.Verb == http.MethodPost || el.Verb == http.MethodPut || el.Verb == http.MethodPatch {
			el.Payload = &templatesv1.ResourceRefPayload{
				Kind:       gvk.Kind,
				APIVersion: in.APIVersion,
				MetaData: &templatesv1.Reference{
					Name:      in.Name,
					Namespace: in.Namespace,
				},
			}
		}

		all = append(all, el)

		log.Info("resource ref successfully resolved",
			slog.String("id", in.ID),
			slog.String("group", gvr.Group),
			slog.String("name", in.Name),
			slog.String("namespace", in.Namespace),
			slog.String("verb", verb),
			slog.String("path", el.Path),
			slog.Bool("allowed", el.Allowed),
		)
	}

	return all, nil
}

func buildPath(gvr schema.GroupVersionResource, in *templatesv1.ResourceRef) string {
	u := url.URL{
		Path: "/call",
	}

	q := url.Values{}
	q.Set("resource", gvr.Resource)
	q.Set("apiVersion", gvr.GroupVersion().String())
	q.Set("namespace", in.Namespace)

	if in.Name != "" {
		q.Set("name", in.Name)
	}

	if slice := in.Slice; slice != nil {
		q.Set("page", strconv.Itoa(slice.Page))
		q.Set("perPage", strconv.Itoa(slice.PerPage))
	}

	u.RawQuery = q.Encode()
	return u.String()
}
