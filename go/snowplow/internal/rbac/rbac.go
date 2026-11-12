package rbac

import (
	"context"
	"fmt"
	"log/slog"

	xcontext "github.com/krateo-platformops/plumbing/context"
	"github.com/krateo-platformops/plumbing/endpoints"
	"github.com/krateo-platformops/snowplow/internal/cache"
	"github.com/krateo-platformops/snowplow/internal/credhttp"
	authv1 "k8s.io/api/authorization/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/kubernetes"
)

type UserCanOptions struct {
	Verb          string
	GroupResource schema.GroupResource
	Namespace     string
	// Name is the object the check is about (#1046): a resourceNames-scoped
	// grant only matches a request that names its object. "" when the check
	// is about no single object. Both paths apply it through ObjectCheck, so
	// it is carried exactly when the verb names a single object.
	Name string
}

// target is the object these options are about.
func (o UserCanOptions) target() ObjectTarget {
	return ObjectTarget{Group: o.GroupResource.Group, Resource: o.GroupResource.Resource,
		Namespace: o.Namespace, Name: o.Name}
}

// UserCan reports whether the user identified by ctx is permitted to
// perform opts.Verb on opts.GroupResource in opts.Namespace.
//
// Cache=on (CACHE_ENABLED=true): routes through EvaluateRBAC →
// informer-cached RBAC types. Zero SubjectAccessReview calls (0.30.4
// Revision 1 binding).
//
// Cache=off (default): falls through to the upstream
// SelfSubjectAccessReview path — preserves the CACHE_ENABLED toggle's
// removability contract (project_redis_removal.md).
func UserCan(ctx context.Context, opts UserCanOptions) (ok bool) {
	log := xcontext.Logger(ctx)

	if !cache.Disabled() {
		ui, err := xcontext.UserInfo(ctx)
		if err != nil {
			log.ErrorContext(ctx, "rbac.UserCan: unable to extract UserInfo", slog.Any("err", err))
			return false
		}
		// Ship 0.30.242 H.c-layered Phase 2 step 2a — EvaluateRBAC returns
		// (allowed, matchedBindingUID, err). UserCan is a per-item caller;
		// matchedBindingUID is ignored.
		allowed, _, evalErr := EvaluateRBAC(ctx, ObjectCheck(ui.Username, ui.Groups, opts.Verb, opts.target()))
		if evalErr != nil {
			log.ErrorContext(ctx, "rbac.UserCan: EvaluateRBAC failed", slog.Any("err", evalErr))
			return false
		}
		return allowed
	}

	// #1046 — the review asks about the request the apiserver would see,
	// built by the same ObjectCheck as the in-process path: the object's
	// name rides along iff the verb names a single object.
	// A review that could not be answered is a deny here: UserCan answers a
	// bool, and its callers have no "unavailable" to give.
	ok, _ = selfSubjectAccessReview(ctx, ObjectCheck("", nil, opts.Verb, opts.target()))
	return ok
}

// sarClientsetForEndpoint builds the per-user kubernetes clientset the
// SAR baseline issues its SelfSubjectAccessReview against. It is a
// package var (not an inline call) SOLELY so the hermetic evaltest can
// inject a fake authorization clientset — production NEVER reassigns it,
// so the default body is byte-for-byte the pre-seam path (build a
// rest.Config from the user endpoint, then a real clientset). Adds zero
// production surface: the indirection is a single func-var deref.
var sarClientsetForEndpoint = func(ctx context.Context, ep endpoints.Endpoint) (kubernetes.Interface, error) {
	rc, err := credhttp.NewClientConfig(ctx, ep)
	if err != nil {
		return nil, err
	}
	return credhttp.Kubernetes(rc)
}

// selfSubjectAccessReview is the upstream cache=off correctness baseline:
// it asks the apiserver, with the requester's own credential, about exactly
// the attributes of check (its Username and Groups are not sent — the
// credential names the requester). It MUST be reachable only when
// cache.Disabled() == true — any cache=on call here is a Revision 1 binding
// violation (rollback trigger).
//
// A check with a NonResourcePath (#1362) is sent as NonResourceAttributes
// {Path, Verb}: the attributes the apiserver authorizes a non-resource request
// with. Every other check is sent as ResourceAttributes.
//
// (false, nil) is the apiserver's deny. When the review cannot be ASKED or
// cannot be ANSWERED (no user endpoint on ctx, no clientset, a failed Create),
// the result wraps ErrReviewUnavailable and is never a bare deny. A caller that
// can answer "unavailable" (the /debug gate's 503) must be able to tell the
// two apart. Every resource caller already fails closed on an error.
func selfSubjectAccessReview(ctx context.Context, check EvaluateOptions) (ok bool, err error) {
	log := xcontext.Logger(ctx)

	ep, err := xcontext.UserConfig(ctx)
	if err != nil {
		log.ErrorContext(ctx, "unable to get user endpoint", slog.Any("err", err))
		return false, fmt.Errorf("%w: no user endpoint: %w", ErrReviewUnavailable, err)
	}

	clientset, err := sarClientsetForEndpoint(ctx, ep)
	if err != nil {
		log.ErrorContext(ctx, "unable to create kubernetes clientset for SAR", slog.Any("err", err))
		return false, fmt.Errorf("%w: no clientset: %w", ErrReviewUnavailable, err)
	}

	selfCheck := authv1.SelfSubjectAccessReview{}
	if check.NonResourcePath != "" {
		selfCheck.Spec.NonResourceAttributes = &authv1.NonResourceAttributes{
			Path: check.NonResourcePath,
			Verb: check.Verb,
		}
	} else {
		selfCheck.Spec.ResourceAttributes = &authv1.ResourceAttributes{
			Group:     check.Group,
			Resource:  check.Resource,
			Namespace: check.Namespace,
			Verb:      check.Verb,
			Name:      check.Name,
		}
	}

	resp, err := clientset.AuthorizationV1().SelfSubjectAccessReviews().
		Create(ctx, &selfCheck, metav1.CreateOptions{}) // #1115: the request's page budget
	if err != nil {
		log.ErrorContext(ctx, "unable to perform SelfSubjectAccessReviews",
			slog.Any("selfCheck", selfCheck), slog.Any("err", err))
		return false, fmt.Errorf("%w: %w", ErrReviewUnavailable, err)
	}

	log.Debug("SelfSubjectAccessReviews result", slog.Any("response", resp))

	return resp.Status.Allowed, nil
}
