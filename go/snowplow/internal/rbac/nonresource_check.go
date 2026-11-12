// nonresource_check.go — #1362: the ONE construction of an authorization check
// about a non-resource request (a path outside /api and /apis, such as
// /debug/vars), and the named errors a caller needs to tell "denied" from
// "could not be decided".
//
// WHAT UPSTREAM DOES. For a request whose path is not under an API prefix, the
// apiserver's RequestInfoFactory.NewRequestInfo (k8s.io/apiserver
// pkg/endpoints/request/requestinfo.go) returns IsResourceRequest=false,
// Path=req.URL.Path and Verb=strings.ToLower(req.Method). The RBAC authorizer
// then visits ClusterRoleBindings only (the namespace is ""), and a rule
// permits the request iff VerbMatches && NonResourceURLMatches
// (plugin/pkg/auth/authorizer/rbac RuleAllows). NonResourceCheck builds exactly
// those attributes; EvaluateRBAC and the cache=off SelfSubjectAccessReview
// evaluate them (evaluate.go rulesPermit, rbac.go selfSubjectAccessReview).
// The parity test drives the vendored NewRequestInfo on the same requests.

package rbac

import (
	"errors"
	"strings"
)

// The errors EvaluateRBAC returns when it cannot decide a check. Each one is
// fail-closed for every caller. The /debug gate maps each to a named 503
// reason, so they are exported values and not ad hoc strings. The texts are
// the pre-#1362 fmt.Errorf texts, so a log reader sees no change.
var (
	// ErrWatcherNotWired: cache=on, but no ResourceWatcher is published.
	ErrWatcherNotWired = errors.New("rbac: cache=on but ResourceWatcher not wired")
	// ErrSnapshotNotBuilt: cache=on, but the typed RBAC snapshot has not been
	// published yet (the boot window before the RBAC informers sync).
	ErrSnapshotNotBuilt = errors.New("rbac: snapshot not yet built")
	// ErrReviewUnavailable: cache=off, and the SelfSubjectAccessReview could
	// not be asked or answered (no user endpoint, no clientset, a failed
	// Create). The cause is wrapped.
	ErrReviewUnavailable = errors.New("rbac: SelfSubjectAccessReview unavailable")
)

// NonResourceCheck builds the EvaluateOptions for an HTTP request with the
// given method and path (req.Method, req.URL.Path) by the requester (username,
// groups). The verb is the lowercase method, as the apiserver derives it for a
// non-resource request: GET is "get", POST is "post", HEAD is "head". The path
// is passed through unchanged. path must not be empty: an empty
// NonResourcePath is a resource check.
func NonResourceCheck(username string, groups []string, method, path string) EvaluateOptions {
	return EvaluateOptions{
		Username:        username,
		Groups:          groups,
		Verb:            strings.ToLower(method),
		NonResourcePath: path,
	}
}
