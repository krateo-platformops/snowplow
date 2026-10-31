package api

import (
	"context"

	"github.com/krateo-platformops/snowplow/internal/credhttp"
)

// targetsOwnCluster reports whether the stage dispatched on ctx targets
// snowplow's own cluster (#1017). It is the one predicate every serve and
// dispatch site that answers from THIS cluster consults:
//   - the content cell and the informer dispatch;
//   - the cluster-list collapse;
//   - the internal-rest-config dispatch, which reads with the ServiceAccount;
//   - the discovery dispatch;
//   - the in-process resolve;
//   - the lazy informer register and its discovery hop.
//
// A stage targets this cluster when its endpoint is one snowplow holds
// (credhttp.HeldCredential, set once per stage by runStage from
// resolveStageEndpoint's provenance): the ServiceAccount endpoint, or the
// requester's own `<user>-clientconfig`, the credential snowplow resolves to
// this cluster's apiserver on the user's behalf. A named endpointRef names
// another endpoint and carries its own credential. It may be another cluster
// entirely, and an apiserver-shaped path proves nothing about which one, so
// such a stage is never answered from this cluster's cells, informers or
// ServiceAccount: it is sent to the endpoint it names.
//
// Precedence follows where the stage's calls actually go: a userAccessFilter
// stage always dials the ServiceAccount endpoint (resolveStageEndpoint checks
// it before the endpointRef), so it targets this cluster even when it names an
// endpointRef.
//
// PROVENANCE, never endpoint shape: a named ref whose Secret happens to point
// at this cluster's apiserver is not recognised as this cluster. That costs it
// the cache, never a correct answer.
//
// The mark is per stage and set to the stage's own value, true or false
// (credhttp.WithHeldCredentialValue): a nested resolve (resolve: true) runs on
// a ctx derived from the outer stage's, and must not inherit it (#1019 review).
func targetsOwnCluster(ctx context.Context) bool {
	return credhttp.HeldCredential(ctx)
}
