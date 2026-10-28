package api

import (
	"context"

	"github.com/krateo-platformops/snowplow/internal/cache"
)

// fixtureStoreVerdict is the Put-gate verdict of a resolve that recorded nothing
// on its three sinks: it allows the Put. Fixtures write cells through the
// verdict-gated store methods with it — the ungated ones are unexported, so the
// compiler keeps every write behind a verdict (#912 delta-3 item 3).
var fixtureStoreVerdict = func() cache.PutVerdict {
	_, g := cache.WithChildPutGateSinks(context.Background())
	return cache.PutGateVerdict(g, "restactions")
}()
