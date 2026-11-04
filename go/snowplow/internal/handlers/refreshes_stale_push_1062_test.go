package handlers

// refreshes_stale_push_1062_test.go — #1062 (re-review P4) at the REAL
// /refreshes handler: the stream subscribes AS its authenticated user
// (cache.StaleSubject of the JWT's username), so the frame the handler writes
// closes that user's stale serve and never another user's on the same key.
//
// Two users are served stale on one identity-free widgetContent key (it is one
// cell for everyone). Only userA's /refreshes stream exists (RefreshAuth +
// Refreshes over HTTP). One accepted Put replaces the cell for both and
// publishes; the handler writes the frame on userA's stream. Exactly userA's
// serve closes pushed; userB's stays open.
//
// Mutation → RED: the handler subscribes with no subject
// (cache.SubscribeRefreshStreamAs("", …)), or with another user's.

import (
	"context"
	"testing"
	"time"

	"github.com/krateo-platformops/snowplow/internal/cache"
)

func k1062Apistage() cache.StalePushLayerStats {
	for _, s := range cache.StalePushStatsSnapshot() {
		if s.Layer == cache.StaleLayerAPIStage {
			return s
		}
	}
	return cache.StalePushLayerStats{}
}

func TestRefreshes1062_TheHandlersFrameClosesOnlyItsUsersStaleServe(t *testing.T) {
	names := []string{"sp1062-0"}
	seedPanels(t, names)
	var keys []string
	store := evictionWiring(t, &keys)
	k := expectedKey(t, names[0])
	keys = append(keys, k)
	base := refreshServer(t)
	cache.ResetStalePushForTest()
	t.Cleanup(cache.ResetStalePushForTest)

	// Both users were served the stale-derived body of the shared key.
	derived := &cache.ResolvedEntry{RawJSON: []byte(`{"stale":true}`), StaleDerived: true}
	for _, user := range []string{"userA", "userB"} {
		cache.NoteStaleHit(context.Background(), k, derived, func(context.Context) string { return cache.StaleSubject(user) })
	}
	if s := k1062Apistage(); s.Open != 2 {
		t.Fatalf("PRECONDITION: open=%d, want the two users' serves", s.Open)
	}

	stream, cancel := openSPAStream(t, base, store, names, nil) // userA's tab
	t.Cleanup(cancel)
	awaitArmed(t, keys, 1)
	w484Commit(store, k, names[0], w484Body(k, 1)) // one accepted Put, then the publish

	deadline := time.Now().Add(10 * time.Second)
	for k1062Apistage().Closes["pushed"] == 0 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if stream.eventCount("refresh") == 0 {
		t.Fatalf("PRECONDITION: the handler wrote no refresh frame on userA's stream")
	}
	s := k1062Apistage()
	if s.Closes["pushed"] != 1 || s.Open != 1 || s.ToPush.Count != 1 {
		t.Fatalf("P4 RED: pushed=%d open=%d samples=%d, want 1/1/1 — the handler's frame on userA's stream must close userA's "+
			"serve (the stream subscribes AS its user) and leave userB's open", s.Closes["pushed"], s.Open, s.ToPush.Count)
	}
}
