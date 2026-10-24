// demand_revalidate.go — #547 item 2: revalidate a KNOWN-STALE identity-bound
// cell on demand, under the requester.
//
// A customer hit on a known-stale cell (its latest dirty mark ended without an
// accepted re-Put — cache/known_stale.go) still SERVES the cell: zero cold
// navigations. After the body is written, the handler hands the request to
// revalidateKnownStale, which claims the key's single revalidation slot for the
// stale generation (key, t0) and re-runs THIS handler's cold path on a detached
// copy of the request:
//
//   - IDENTITY AND CREDENTIAL: the requester's. The copy keeps every value of the
//     request ctx (identity, its own UserConfig endpoint) — exactly what the
//     customer's cold read uses, which is the read known to work where the
//     background refresh fails (#547 trace row 7). The requester derived THIS key,
//     so their resolve is a valid body for it, and it becomes the cell's
//     representative (the #444 re-pick with repSourceHitter = the requester): the
//     cold path's Put records cacheInputs, which carry the requester.
//   - DETACHED AND BOUNDED: context.WithoutCancel (the response is already
//     written and the request may end), under DemandRevalidationDeadline — the
//     server's own write deadline, the longest any customer resolve may run. It
//     runs on its own goroutine and never blocks the response.
//   - TERMINAL: the cold path's own generation-guarded PutIfGen, which reports
//     its outcome to the revalidation's sink; an accepted write ends the
//     known-stale state and settles the revalidation (#884, owner ruling F5:
//     known-stale means the last refresh FAILED). If the data moved during the
//     resolve, the cell stays an ordinary open window the refresher replaces;
//     otherwise the window closes (a dirty→fresh sample). The cold path's
//     publishIfSubscribed announces the write over SSE.
//   - FAILURE (owner ruling 1): no Put → the slot is released, the cell stays
//     resident, served and counted; the next read or change retries. Never an
//     eviction.
//   - ABSORBED READ (#740): a read that finds the slot held earns the attempt
//     in flight ONE rerun if it settles still known-stale with no accepted Put
//     (it may have read its inputs before the state that reader needs; #884: an
//     accepted Put never reruns). The slot stays claimed across
//     the rerun; with no absorbed read the first settle releases it.
//   - SUPPRESSED (team ruling (b)): a key suppressed for a structurally-
//     permanent reason (external endpoint, UAF, unsupported kind, sensitive
//     resource) is NOT revalidated — the cold path would decline its Put by the
//     same gate — and the skip is counted (ClaimDemandRevalidation).
//
// The re-entered handler skips both L1 lookups (it must resolve, not hit), the
// customer-in-flight mark (no customer is waiting on it) and the live-caller
// observation (the real request already made it); its per-call record carries
// l1_hit="revalidate".
package dispatchers

import (
	"context"
	"log/slog"
	"net/http"
	"sync"
	"time"

	xcontext "github.com/krateo-platformops/plumbing/context"
	"github.com/krateo-platformops/snowplow/internal/cache"
)

// isDemandRevalidation reports whether req is a #547 demand revalidation.
func isDemandRevalidation(req *http.Request) bool {
	_, ok := cache.DemandRevalidationFromContext(req.Context())
	return ok
}

// revalidateKnownStale starts the single demand revalidation of key when key is
// known-stale and none is in flight. h is the handler that served the hit; req
// the customer request. Never blocks.
func revalidateKnownStale(h http.Handler, req *http.Request, key string) {
	if key == "" || isDemandRevalidation(req) || cache.Inert(req.Context()) {
		return
	}
	t0, reason, ok := cache.KnownStale(key)
	if !ok {
		return
	}
	if !cache.ClaimDemandRevalidation(key, t0) {
		// Absorbed into the attempt in flight (or skipped as suppressed): the
		// newest same-key request becomes the template its rerun and any
		// viewer-gated retry run (#740 Q3).
		if _, held := cache.DemandSlotDone(key); held {
			setDemandTemplate(key, req)
		}
		return
	}
	tpl := setDemandTemplate(key, req)
	go func() {
		res := runDemandRevalidation(h, tpl, key, t0, reason)
		demandAfterSettle(h, key, reason, res)
	}()
}

// demandRun is one demand revalidation's final settle and its last attempt's
// duration.
type demandRun struct {
	cache.DemandSettle
	dur time.Duration
}

// runDemandRevalidation runs key's demand revalidation on its claimed slot: one
// attempt, then — #740 — one more for each read absorbed into the attempt in
// flight while the cell is still known-stale (the slot stays claimed across
// them). Driven by reads: with no absorbed read the first settle releases the
// slot. Each attempt runs the NEWEST same-key request (Q3).
func runDemandRevalidation(h http.Handler, tpl *demandTemplate, key string, t0 time.Time, reason string) demandRun {
	for {
		start := time.Now()
		s := demandRevalidateOnce(h, currentDemandTemplate(key, tpl), key, t0, reason)
		if !s.Rerun {
			return demandRun{DemandSettle: s, dur: time.Since(start)}
		}
	}
}

// demandAfterSettle opens or joins key's viewer-gated chain when its demand
// revalidation settled still known-stale (#740): refused ⇒ transient, anything
// else ⇒ genuine (Q4); a suppressed key opens no chain. Otherwise it drops the
// request template, unless a chain still holds it.
func demandAfterSettle(h http.Handler, key, reason string, res demandRun) {
	if stale, resident := cache.KnownStaleRetryState(key); !res.Fresh && stale && resident {
		kind := cache.StaleRetryGenuine
		if res.Refused {
			kind = cache.StaleRetryTransient
		}
		cache.StaleRetryFailed(key, kind, res.dur, demandRetrySpec(h, key, reason))
		return
	}
	if !cache.StaleRetryChainOpen(key) {
		demandTemplates.Delete(key)
	}
}

// demandRetrySpec is key's viewer-gated chain on the demand slot. Each attempt
// claims the slot and runs the newest template; if an attempt is already in
// flight it waits for that one (it was absorbed, so that attempt reruns). With
// no template left (abandoned, then nobody read the cell again) there is
// nothing to retry under: the chain ends, and the next read starts over.
func demandRetrySpec(h http.Handler, key, reason string) cache.StaleRetrySpec {
	return cache.StaleRetrySpec{
		Source: cache.StaleRetryDemand,
		Attempt: func(ctx context.Context) cache.StaleRetryOutcome {
			t0, _, ok := cache.KnownStale(key)
			tpl := loadDemandTemplate(key)
			if !ok || tpl == nil {
				return cache.StaleRetryFresh
			}
			if !cache.ClaimDemandRevalidation(key, t0) {
				if done, held := cache.DemandSlotDone(key); held {
					ctx, cancel := context.WithTimeout(ctx, cache.DemandRevalidationDeadline())
					defer cancel()
					select {
					case <-done:
					case <-ctx.Done():
					}
				}
				if stale, _ := cache.KnownStaleRetryState(key); stale {
					return cache.StaleRetryGenuine
				}
				return cache.StaleRetryFresh
			}
			res := runDemandRevalidation(h, tpl, key, t0, reason)
			switch {
			case res.Fresh:
				return cache.StaleRetryFresh
			case res.Refused:
				return cache.StaleRetryTransient
			}
			return cache.StaleRetryGenuine
		},
		Dependents: func() []string { return []string{key} },
		StillStale: func() (bool, bool) {
			stale, resident := cache.KnownStaleRetryState(key)
			return stale && loadDemandTemplate(key) != nil, resident
		},
		Release: func() { demandTemplates.Delete(key) },
	}
}

// demandTemplate is the newest same-key request a demand revalidation of key
// runs (#740 Q3): a detached clone (the response it belonged to is long
// written), replaced on each claim and absorbed read, dropped when the slot is
// released with no chain left or when the chain stops holding it. Same key only
// by construction: the map is keyed by the cell's L1 key, which folds the
// identity inputs. An expired credential in it surfaces as a genuine failure;
// it is never minted or extended — the next read replaces it.
type demandTemplate struct {
	mu  sync.Mutex
	req *http.Request
}

var demandTemplates sync.Map // L1 key → *demandTemplate

func setDemandTemplate(key string, req *http.Request) *demandTemplate {
	clone := req.Clone(context.WithoutCancel(req.Context()))
	v, _ := demandTemplates.LoadOrStore(key, &demandTemplate{})
	t := v.(*demandTemplate)
	t.mu.Lock()
	t.req = clone
	t.mu.Unlock()
	return t
}

func loadDemandTemplate(key string) *demandTemplate {
	v, ok := demandTemplates.Load(key)
	if !ok {
		return nil
	}
	return v.(*demandTemplate)
}

// currentDemandTemplate is the newest request for key: the shared template if
// one is registered, else own's.
func currentDemandTemplate(key string, own *demandTemplate) *http.Request {
	t := loadDemandTemplate(key)
	if t == nil {
		t = own
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.req
}

// demandRevalidateOnce runs one demand revalidation attempt of key on a
// detached, bounded copy of req and settles it (#740: the settle reports a
// rerun and a refused Put).
func demandRevalidateOnce(h http.Handler, req *http.Request, key string, t0 time.Time, reason string) (s cache.DemandSettle) {
	// #710 PR-2b — a customer started it and is looking at the stale body: its
	// content and nested fills bypass like a debt's (top priority, never queued
	// behind background work), ahead of any origin rule. #740 — stamped on EVERY
	// attempt, the absorbed-read reruns included.
	ctx, cancel := context.WithTimeout(cache.WithCustomerTriggeredRefresh(context.WithoutCancel(req.Context())), cache.DemandRevalidationDeadline())
	defer cancel()
	ctx, settle := cache.BeginDemandRevalidation(ctx, key, t0)
	defer func() {
		if p := recover(); p != nil {
			xcontext.Logger(ctx).Error("demand revalidation panicked; cell stays known-stale",
				slog.String("subsystem", "cache"),
				slog.String("key_hash", key),
				slog.Any("panic", p),
			)
		}
		s = settle()
		xcontext.Logger(ctx).Debug("demand revalidation settled",
			slog.String("subsystem", "cache"),
			slog.String("key_hash", key),
			slog.String("known_stale_reason", reason),
			slog.Bool("fresh", s.Fresh),
			slog.Bool("rerun", s.Rerun),
		)
	}()
	h.ServeHTTP(&discardResponseWriter{}, req.Clone(ctx))
	return s
}

// discardResponseWriter swallows the re-entered handler's response: the
// customer already has theirs.
type discardResponseWriter struct{ h http.Header }

func (w *discardResponseWriter) Header() http.Header {
	if w.h == nil {
		w.h = http.Header{}
	}
	return w.h
}
func (w *discardResponseWriter) Write(b []byte) (int, error) { return len(b), nil }
func (w *discardResponseWriter) WriteHeader(int)             {}
