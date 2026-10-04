package api

import (
	"context"
	"fmt"
	"strings"

	xcontext "github.com/krateo-platformops/plumbing/context"
	"github.com/krateo-platformops/plumbing/endpoints"
	templates "github.com/krateo-platformops/snowplow/apis/templates/v1"
	"github.com/krateo-platformops/snowplow/internal/cache"
)

// fromSecretAsCallerFn reads a named endpointRef Secret with the caller's
// credentials. A package var so a hermetic test can observe the identity used.
var fromSecretAsCallerFn = endpoints.FromSecret

// resolveOneAsCaller resolves a NAMED endpointRef for a caller-supplied
// (inline draft) spec. The Secret is read with the CALLER's own rest config,
// built from the caller's clientconfig endpoint on the request ctx, so the
// apiserver decides whether the caller may read it (a 403 or 404 becomes the
// stage's error). It never consults the SA-backed Secrets snapshot or the
// ServiceAccount rest config, and refuses a `-clientconfig` name outright.
func (m *endpointReferenceMapper) resolveOneAsCaller(ctx context.Context, ref *templates.Reference) (endpoints.Endpoint, bool, error) {
	if strings.HasSuffix(ref.Name, clientConfigSuffix) {
		return endpoints.Endpoint{}, false, fmt.Errorf(
			"endpointRef %q names a reserved per-user credential Secret (suffix %q); a caller-supplied RESTAction may not select one",
			ref.Name, clientConfigSuffix)
	}
	// An inline request never carries an internal (ServiceAccount) rest
	// config: only seeds and the refresher set one. Refuse rather than let
	// ClientConfigFor hand back the SA's config.
	if _, ok := cache.InternalRESTConfigFromContext(ctx); ok {
		return endpoints.Endpoint{}, false, fmt.Errorf(
			"caller-supplied RESTAction resolved on a context carrying an internal rest config; refusing to read endpointRef %q", ref.Name)
	}
	callerEP, err := xcontext.UserConfig(ctx)
	if err != nil {
		return endpoints.Endpoint{}, false, fmt.Errorf("caller-supplied endpointRef %q: no caller endpoint: %w", ref.Name, err)
	}
	rc, err := cache.ClientConfigFor(ctx, callerEP)
	if err != nil {
		return endpoints.Endpoint{}, false, fmt.Errorf("caller-supplied endpointRef %q: %w", ref.Name, err)
	}
	ep, err := fromSecretAsCallerFn(ctx, rc, ref.Name, ref.Namespace)
	if err != nil {
		return ep, false, err
	}
	ep.CertificateAuthorityData = string(normalizeCAData([]byte(ep.CertificateAuthorityData)))
	return ep, false, nil
}

// Provenance says who vouches for the RESTAction spec being resolved (#443
// part 2). It is a per-resolve OPTION, deliberately not a context value: a
// nested resolve builds its own ResolveOptions and so gets ProvenanceStored,
// which means a STORED RESTAction nested inside an inline one keeps its stored
// (author-vouched) semantics.
type Provenance uint8

const (
	// ProvenanceStored (the zero value) is today's behaviour: the spec is a
	// stored CR, vouched for by whoever was allowed to write it. UAF stages
	// dial the ServiceAccount (and are refiltered), and endpointRef Secrets
	// are read through snowplow's ServiceAccount.
	ProvenanceStored Provenance = iota
	// ProvenanceCallerSupplied marks a spec taken from the request body: an
	// inline dry-run draft, vouched for only by the caller. Exactly two
	// reads switch to the caller's own identity:
	//   - a userAccessFilter stage dials the caller's clientconfig instead of
	//     the ServiceAccount (the refilter still runs);
	//   - a named endpointRef Secret is read with the caller's credentials,
	//     never through the ServiceAccount or the informer snapshot, and a
	//     literal "-clientconfig" ref is refused, so a draft can never dial
	//     as another user.
	ProvenanceCallerSupplied
)
