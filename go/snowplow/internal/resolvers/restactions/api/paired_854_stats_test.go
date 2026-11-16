//go:build paired854

package api

import "github.com/krateo-platformops/snowplow/internal/cache"

// pairedListStats: the branch reads the #854 incremental list cell counters.
func pairedListStats() any { return cache.ListCellStatsSnapshot() }
