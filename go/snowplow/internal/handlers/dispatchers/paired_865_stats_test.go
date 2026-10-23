//go:build paired854

package dispatchers

import "github.com/krateo-platformops/snowplow/internal/cache"

// pairedListStats865 is the branch side of the paired customer arm's list-cell
// stats (main's copy of this file returns nil: the cell does not exist there).
func pairedListStats865() any { return cache.ListCellStatsSnapshot() }
