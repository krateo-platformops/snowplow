// reseed_engine.go — #258/#378 SHARED reseed engine: the composition over the
// seed primitives (seedOneWidgetFn/seedOneRestactionFn) that re-resolves a target
// under its CURRENT sub-generation key. Two entrypoints differ ONLY in how the
// unit is acquired:
//
//   - reseedTargets   (#258): units IN HAND from the harvester — NEVER re-fetched
//     (the #258 amplification bound depends on not walking/fetching).
//   - reseedFromInputs (#378): the reaper holds only each aging cell's
//     ResolvedKeyInputs (coords + representative identity), so this FETCHES the
//     unit by coords before re-minting the SAME key.
//
// Both funnel reseedUnderCurrentIdentity, which stamps the current sub-gen under
// the subject/representative identity (NEVER the SA), customer-priority-yielded
// and per-target-timeout-bounded. The MODE selects the terminal write through the
// ONE #394 mechanism (seedTerminalGuardFor / seedTerminalPut):
// seedModeRBACShift = PutIfGen INSERT, seedModeReMint = ReplaceIfGenReMint.

package dispatchers

import (
	"context"
	"errors"
	"log/slog"

	templatesv1 "github.com/krateo-platformops/snowplow/apis/templates/v1"
	"github.com/krateo-platformops/snowplow/internal/cache"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// reseedRequest is one reseed unit: a resident target (widget OR restaction)
// paired with the cohort identity to re-resolve it under. For #258 the unit is in
// hand (widget CR / RA ObjectReference from the harvester); for #378 it is
// reconstructed from fetched coords.
type reseedRequest struct {
	identity seedTarget
	isWidget bool
	widget   navWidgetEntry              // valid when isWidget
	ra       templatesv1.ObjectReference // valid when !isWidget
}

// reseedCohortCtx builds the per-target reseed context: a timeout-bounded cohort
// seed context under the subject/representative identity (NEVER the SA). Mirrors
// the boot engine's seedOneTarget wrapper. The returned cancel MUST be called.
func reseedCohortCtx(ctx context.Context, deps rePrewarmDeps, identity seedTarget) (context.Context, context.CancelFunc) {
	cctx, cancel := context.WithTimeout(ctx, pipCohortTimeout)
	return withCohortSeedContext(cctx, identity, deps.saEP, deps.saRC), cancel
}

// reseedUnderCurrentIdentity re-resolves ONE in-hand target under the given cohort
// ctx and Puts it under the CURRENT sub-generation key via the shared seed
// primitive, in the chosen reseed MODE. Returns (wrapped) errSeedTerminalPutRefused
// when the gen-guarded terminal Put was refused (#394: the cell was removed during
// the resolve); any other non-nil error is a per-target resolve/transport failure.
func reseedUnderCurrentIdentity(cohortCtx context.Context, deps rePrewarmDeps, req reseedRequest, mode seedScopeMode) error {
	if req.isWidget {
		return seedOneWidgetFn(cohortCtx, req.widget, deps.authnNS, mode)
	}
	return seedOneRestactionFn(cohortCtx, cohortLogLabel(req.identity), req.ra, deps.authnNS, mode)
}

// reseedOnce runs one target under a fresh per-target cohort ctx.
func reseedOnce(ctx context.Context, deps rePrewarmDeps, req reseedRequest, mode seedScopeMode) error {
	cohortCtx, cancel := reseedCohortCtx(ctx, deps, req.identity)
	defer cancel()
	return reseedUnderCurrentIdentity(cohortCtx, deps, req, mode)
}

// reseedWithRefusalPolicy reseeds one target and applies the per-mode refusal
// policy of the ONE #394 mechanism (seed_terminal_put_guard.go):
//   - seedModeRBACShift: the #394 one-shot inline re-seed in the SAME mode
//     (reseedAfterTerminalPutRefusal — fresh capture, PutIfGen may INSERT); a
//     second refusal is logged and swallowed (nil), exactly like keepwarm /
//     gvr-discovered.
//   - seedModeReMint: NO retry (retriesTerminalPutRefusal) — a refused
//     ReplaceIfGenReMint means the cell was removed; it is logged and swallowed.
//
// A refusal is never re-enqueued and never classified as a failure: the removal
// is authoritative. Re-arming the whole rotated set for one refused cell would
// re-resolve every target of the flush (amplification) to heal one cell.
func reseedWithRefusalPolicy(ctx context.Context, deps rePrewarmDeps, req reseedRequest, mode seedScopeMode) error {
	err := reseedOnce(ctx, deps, req, mode)
	if !errors.Is(err, errSeedTerminalPutRefused) || ctx.Err() != nil {
		return err
	}
	if !retriesTerminalPutRefusal(mode) {
		slog.Default().Info("prewarm.engine.reseed.remint_refused",
			slog.String("subsystem", "cache"),
			slog.String("class", reseedClass(req)),
			slog.String("target", reseedTargetLabel(req)),
			slog.String("effect", "#378 the cell was removed during the re-mint resolve; the removal is "+
				"authoritative and the next fill is a fresh birth — not retried, not re-enqueued"),
		)
		return nil
	}
	return reseedAfterTerminalPutRefusal(reseedClass(req), reseedTargetLabel(req), cohortLogLabel(req.identity),
		func() error { return reseedOnce(ctx, deps, req, mode) })
}

// reseedTargets reseeds a batch of #258 units-in-hand under their current
// sub-generation keys, yielding to customers between targets. Returns the subset
// to RE-ENQUEUE: on a ctx cancel, the as-yet-unprocessed tail (never dropped). A
// refused terminal Put is handled inline by reseedWithRefusalPolicy (one-shot
// re-seed) and is never part of the returned set.
func reseedTargets(ctx context.Context, deps rePrewarmDeps, reqs []reseedRequest) (reEnqueue []reseedRequest) {
	for i := range reqs {
		if ctx.Err() != nil {
			return append(reEnqueue, reqs[i:]...)
		}
		engineYieldCheckpoint(ctx)
		err := reseedWithRefusalPolicy(ctx, deps, reqs[i], seedModeRBACShift)
		switch {
		case err == nil:
		case ctx.Err() != nil:
			return append(reEnqueue, reqs[i:]...)
		default:
			slog.Warn("prewarm.engine.reseed.target_error",
				slog.String("subsystem", "cache"),
				slog.String("mode", seedModeRBACShift.String()),
				slog.String("class", reseedClass(reqs[i])),
				slog.String("target", reseedTargetLabel(reqs[i])),
				slog.String("err", err.Error()),
			)
		}
	}
	return reEnqueue
}

// reseedFromInputs is #378's entrypoint over the shared core: the reaper holds
// aging cells' ResolvedKeyInputs (coords + representative identity, NO unit). This
// reconstructs each unit (restactions self-fetch inside seedOneRestaction; widgets
// fetched by coords here) and re-mints the SAME key (seedModeReMint). Returns the
// inputs to RE-ENQUEUE (the unprocessed tail on a ctx cancel). A refused re-mint
// is terminal (see reseedWithRefusalPolicy) and is not re-enqueued.
//
// #378 OWNERSHIP (dev-1218): the reaper selection + the per-fetch C5 sizing land
// with #378; this seam + core are ready now. An input whose unit is GONE is
// skipped (the DELETE path reclaims it — not a re-mint).
func reseedFromInputs(ctx context.Context, deps rePrewarmDeps, inputs []*cache.ResolvedKeyInputs) (reEnqueue []*cache.ResolvedKeyInputs) {
	for i, in := range inputs {
		if in == nil {
			continue
		}
		if ctx.Err() != nil {
			return append(reEnqueue, inputs[i:]...)
		}
		engineYieldCheckpoint(ctx)
		identity := seedTarget{Username: in.RepresentativeUsername, Groups: in.RepresentativeGroups}
		fetchCtx, cancel := reseedCohortCtx(ctx, deps, identity)
		req, ok := reseedRequestFromInputs(fetchCtx, identity, in)
		cancel()
		if !ok {
			continue
		}
		err := reseedWithRefusalPolicy(ctx, deps, req, seedModeReMint)
		switch {
		case err == nil:
		case ctx.Err() != nil:
			return append(reEnqueue, inputs[i:]...)
		default:
			slog.Warn("prewarm.engine.reseed.target_error",
				slog.String("subsystem", "cache"),
				slog.String("mode", seedModeReMint.String()),
				slog.String("class", in.CacheEntryClass),
				slog.String("target", in.Namespace+"/"+in.Name),
				slog.String("err", err.Error()),
			)
		}
	}
	return reEnqueue
}

// reseedRequestFromInputs reconstructs a reseed unit from a cell's inputs. A
// restaction is passed by ObjectReference (seedOneRestaction self-fetches); a
// widget CR is fetched by coords here (seedOneWidget needs the object in hand).
// ok=false when the widget unit is gone/unfetchable.
func reseedRequestFromInputs(cohortCtx context.Context, identity seedTarget, in *cache.ResolvedKeyInputs) (reseedRequest, bool) {
	gvr := schema.GroupVersionResource{Group: in.Group, Version: in.Version, Resource: in.Resource}
	apiVersion := in.Version
	if in.Group != "" {
		apiVersion = in.Group + "/" + in.Version
	}
	ref := templatesv1.ObjectReference{
		Reference:  templatesv1.Reference{Name: in.Name, Namespace: in.Namespace},
		APIVersion: apiVersion,
		Resource:   in.Resource,
	}
	if in.CacheEntryClass != "widgets" {
		return reseedRequest{identity: identity, isWidget: false, ra: ref}, true
	}
	got := seedObjectsGetFn(cohortCtx, ref)
	if got.Err != nil || got.Unstructured == nil {
		return reseedRequest{}, false
	}
	return reseedRequest{
		identity: identity,
		isWidget: true,
		widget: navWidgetEntry{
			W:          got.Unstructured,
			GVR:        gvr,
			PerPage:    in.PerPage,
			Page:       in.Page,
			KeyPerPage: in.PerPage,
			KeyPage:    in.Page,
		},
	}, true
}

func reseedClass(req reseedRequest) string {
	if req.isWidget {
		return "widgets"
	}
	return "restactions"
}

func reseedTargetLabel(req reseedRequest) string {
	if req.isWidget {
		if req.widget.W != nil {
			return req.widget.W.GetNamespace() + "/" + req.widget.W.GetName()
		}
		return "<nil-widget>"
	}
	return req.ra.Namespace + "/" + req.ra.Name
}

// reseedIdentityTargets resolves the per-binding identity target set for a target
// GVR — the SAME enumeration the boot seed uses (enumeratePrewarmTargetsForGVRFn),
// so the reseed's (unit × identity) set is a faithful subset of the boot's, never
// a parallel derivation (feedback_no_special_cases).
func reseedIdentityTargets(gvr schema.GroupVersionResource) []seedTarget {
	raw := enumeratePrewarmTargetsForGVRFn(gvr, "list")
	out := make([]seedTarget, 0, len(raw))
	for _, t := range raw {
		out = append(out, seedTarget{
			BindingUID:        t.BindingUID,
			Username:          t.Subject.Username,
			Groups:            append([]string(nil), t.Subject.Groups...),
			CollapsedBindings: t.CollapsedBindings,
		})
	}
	return out
}

// enumerateRotatedResidentTargets is the #258 Q1 TARGET-FILTER: it walks the
// RESIDENT units (the harvester snapshots — NO fresh nav walk) and keeps only the
// (unit × identity) pairs whose folded subject ROTATED in this flush
// (RotatedSubjectSet.Rotated, mirroring the RBACSubGenForSubject key fold). The
// identity side is the LIVE binding index (reseedIdentityTargets) read now, so a
// WIDENING subject newly authorized for a resident unit is included with no extra
// walk (TestS258_WideningSubjectAppearsInReseedSet). Units the harvester never
// saw (a runtime-new GVR) belong to scopeKindGVRDiscovered.
func enumerateRotatedResidentTargets(ctx context.Context, deps rePrewarmDeps, rotated cache.RotatedSubjectSet) []reseedRequest {
	var reqs []reseedRequest
	for _, e := range deps.navHarv.snapshot() {
		if ctx.Err() != nil {
			return reqs
		}
		engineYieldCheckpoint(ctx)
		for _, c := range reseedIdentityTargets(e.GVR) {
			if rotated.Rotated(c.Username, c.Groups) {
				reqs = append(reqs, reseedRequest{identity: c, isWidget: true, widget: e})
			}
		}
	}
	for _, ref := range deps.harvester.snapshot() {
		if ctx.Err() != nil {
			return reqs
		}
		engineYieldCheckpoint(ctx)
		targetGVR, haveTarget := restActionTargetGVRFn(ctx, ref)
		if !haveTarget {
			continue
		}
		for _, c := range reseedIdentityTargets(targetGVR) {
			if rotated.Rotated(c.Username, c.Groups) {
				reqs = append(reqs, reseedRequest{identity: c, isWidget: false, ra: ref})
			}
		}
	}
	return reqs
}
