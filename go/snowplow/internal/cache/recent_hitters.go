// recent_hitters.go — #444: a small per-cell pool of identities the cell was
// served to, so the refresher can replace a representative that drifted out of
// the cell's RBAC class instead of leaving the cell un-refreshable until its TTL.
//
// PRIVACY: identity tuples (username + groups) only, in memory only. No String
// method, never logged, never exported on /debug/vars, never copied into
// ResolvedEntryMeta (#262 redaction rules).

package cache

import "slices"

// recentHittersCap bounds the pool per cell. It is a structural constant, not a
// knob: the pool only has to outlive ONE representative's drift (every candidate
// is re-verified against the key's class before use), and a cell's class
// members are interchangeable by construction.
const recentHittersCap = 4

// HitterIdentity is one identity a cell was served to.
type HitterIdentity struct {
	Username string
	Groups   []string
}

// NoteHitter records (username, groups) as the most recent hitter of e, moving
// it to the front if already present and dropping the oldest beyond the cap.
// Lock-free (copy-on-write CAS). A repeat hit by the identity already at the
// front allocates nothing. Nil-safe. A note racing a replace-in-place may land
// on the superseded entry and be lost — benign: the pool is a candidate list.
func (e *ResolvedEntry) NoteHitter(username string, groups []string) {
	if e == nil {
		return
	}
	for {
		cur := e.recentHitters.Load()
		var old []HitterIdentity
		if cur != nil {
			old = *cur
		}
		if len(old) > 0 && old[0].Username == username && slices.Equal(old[0].Groups, groups) {
			return
		}
		next := make([]HitterIdentity, 0, recentHittersCap)
		next = append(next, HitterIdentity{Username: username, Groups: slices.Clone(groups)})
		for _, h := range old {
			if len(next) == recentHittersCap {
				break
			}
			if h.Username == username && slices.Equal(h.Groups, groups) {
				continue
			}
			next = append(next, h)
		}
		if e.recentHitters.CompareAndSwap(cur, &next) {
			return
		}
	}
}

// RecentHitters returns e's hitter pool, most recent first. The slice is shared
// and must not be mutated. Nil-safe.
func (e *ResolvedEntry) RecentHitters() []HitterIdentity {
	if e == nil {
		return nil
	}
	if cur := e.recentHitters.Load(); cur != nil {
		return *cur
	}
	return nil
}
