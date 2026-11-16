//go:build unit || integration

package api

// issue796_serve_miss_reason_test.go — #796: the serve-miss reason mapping. A
// refusal is named only for an UNSYNCED informer; a synced one that later broke
// is a watch_broken miss, never list_refused.

import (
	"testing"

	"github.com/krateo-platformops/snowplow/internal/cache"
)

func TestIssue796_ServeMissReason(t *testing.T) {
	all := cache.ServabilitySnapshot{Registered: true, HasSynced: true, WatchHealthy: true, TypeConfirmed: true}
	with := func(f func(*cache.ServabilitySnapshot)) cache.ServabilitySnapshot {
		s := all
		f(&s)
		return s
	}
	cases := []struct {
		snap cache.ServabilitySnapshot
		want string
	}{
		{cache.ServabilitySnapshot{}, "unregistered"},
		{with(func(s *cache.ServabilitySnapshot) { s.HasSynced, s.ListRefused, s.WatchHealthy = false, true, false }), "list_refused"},
		{with(func(s *cache.ServabilitySnapshot) { s.HasSynced = false }), "sync_timeout"},
		{with(func(s *cache.ServabilitySnapshot) { s.HasSynced, s.WatchHealthy = false, false }), "sync_timeout"},
		{with(func(s *cache.ServabilitySnapshot) { s.WatchHealthy, s.ListRefused = false, true }), "watch_broken"},
		{with(func(s *cache.ServabilitySnapshot) { s.TypeConfirmed = false }), "type_unconfirmed"},
		{all, "not_servable"},
	}
	for i, c := range cases {
		if got := serveMissReason(c.snap); got != c.want {
			t.Errorf("case %d %+v: reason %q, want %q", i, c.snap, got, c.want)
		}
	}
	for _, c := range cases {
		found := false
		for _, r := range ServeMissReasons {
			found = found || r == c.want
		}
		if !found {
			t.Fatalf("reason %q is not in the closed ServeMissReasons set", c.want)
		}
	}
}
