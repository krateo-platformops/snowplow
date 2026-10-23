package cache

import "expvar"

// #854 — snowplow_list_incremental on /debug/vars: the incremental list cell's
// counters. divergence_total must stay 0 (any non-zero value is a bug: the cell
// is dropped and re-filled); rawjson_built stays 0 in steady state (nothing
// needs the envelope); refresh_skipped counts the content-key re-dispatches the
// cells made unnecessary; gen_alloc_* reports the bytes each generation copies.
func init() {
	if Disabled() {
		return
	}
	expvar.Publish("snowplow_list_incremental", expvar.Func(func() any {
		s := ListCellStatsSnapshot()
		return map[string]any{
			"live":                  s.Live,
			"anchors":               s.Anchors,
			"anchor_skipped":        s.AnchorSkipped,
			"reads":                 s.Reads,
			"restamps":              s.Restamps,
			"generations":           s.Generations,
			"applied_keys":          s.AppliedKeys,
			"decodes":               s.Decodes,
			"rebinds":               s.Rebinds,
			"items_built":           s.ItemsBuilt,
			"rawjson_built":         s.RawJSONBuilt,
			"pending_recorded":      s.PendingRecorded,
			"passes":                s.Passes,
			"suspects":              s.Suspects,
			"refresh_skipped":       s.RefreshSkipped,
			"gen_alloc_bytes_total": s.GenAllocBytesTotal,
			"gen_alloc_bytes_max":   s.GenAllocBytesMax,
			"gen_alloc_count":       s.GenAllocCount,
			"unanchors":             s.Unanchors,
			"divergences":           s.Divergences,
			"divergence_total":      s.DivergenceTotal,
		}
	}))
}
