package metrics

// resolve_memo_metrics.go — the refresher's resolve memo lookups and its
// measurement-only would-join count on OTLP, hand-wired (no expvar→OTLP
// bridge), read through the accessors the snowplow_resolve_memo expvar reads
// (dispatchers/refresh_resolve_memo.go). Every series is present at 0 before
// the first memo exists. The one attribute is a closed set; never a
// RESTAction, a key or an identity. Registered only when the cache is on
// (CFG-1: the expvar key is published from a Disabled()-gated init).

import (
	"context"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/krateo-platformops/snowplow/internal/handlers/dispatchers"
)

func registerResolveMemoInstruments(m metric.Meter) error {
	lookups, err := m.Int64ObservableCounter("snowplow_resolve_memo_lookups_total",
		metric.WithDescription("Lookups of the refresher's resolve memo, by outcome {hit; miss: no entry; stale_miss: an entry whose captured deps had moved (#411), deleted}. Disjoint: the expvar misses_total is miss + stale_miss."))
	if err != nil {
		return err
	}
	join, err := m.Int64ObservableCounter("snowplow_resolve_memo_would_join_total",
		metric.WithDescription("MEASUREMENT ONLY: refresher memo misses an in-flight join would have saved: produces that started while a produce of the same memo key (their leader) was in flight, counted only when that leader handed over a body the memo would serve them (it reached the memo Store and passed the memo's #411 freshness rule when it ended; a dependency that moved after the leader started refuses it). Nothing joins."))
	if err != nil {
		return err
	}
	_, err = m.RegisterCallback(func(_ context.Context, o metric.Observer) error {
		hits, misses, stale := dispatchers.RefreshMemoLookups()
		o.ObserveInt64(lookups, int64(hits), metric.WithAttributes(attribute.String("outcome", "hit")))
		o.ObserveInt64(lookups, int64(misses-stale), metric.WithAttributes(attribute.String("outcome", "miss")))
		o.ObserveInt64(lookups, int64(stale), metric.WithAttributes(attribute.String("outcome", "stale_miss")))
		o.ObserveInt64(join, int64(dispatchers.RefreshMemoWouldJoin()))
		return nil
	}, lookups, join)
	return err
}
