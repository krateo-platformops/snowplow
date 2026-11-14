package dynamic

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/krateo-platformops/snowplow/internal/apipath"
	"github.com/krateo-platformops/snowplow/internal/credhttp"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/discovery"
	cacheddiscovery "k8s.io/client-go/discovery/cached/memory"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/restmapper"
)

func NewClient(rc *rest.Config) (Client, error) {
	dynamicClient, err := credhttp.Dynamic(rc)
	if err != nil {
		return nil, err
	}

	discoveryClient, err := credhttp.Discovery(rc)
	if err != nil {
		return nil, err
	}

	mapper := restmapper.NewDeferredDiscoveryRESTMapper(
		cacheddiscovery.NewMemCacheClient(discoveryClient),
	)

	return &unstructuredClient{
		dynamicClient:   dynamicClient,
		discoveryClient: discoveryClient,
		mapper:          mapper,
		converter:       runtime.DefaultUnstructuredConverter,
	}, nil
}

type Options struct {
	Namespace string
	GVK       schema.GroupVersionKind
	GVR       schema.GroupVersionResource
}

type Client interface {
	Get(ctx context.Context, name string, opts Options) (*unstructured.Unstructured, error)
	List(ctx context.Context, opts Options) (*unstructured.UnstructuredList, error)
	Create(ctx context.Context, obj *unstructured.Unstructured, opts Options) (*unstructured.Unstructured, error)
	Delete(ctx context.Context, name string, opts Options) error
	FromUnstructured(in map[string]any, out any) error
	ToUnstructured(in any) (map[string]any, error)
	// Discover returns the GroupVersionResources in `category`.
	//
	// #517 — ON A PARTIAL DISCOVERY IT RETURNS BOTH: the resources of every
	// HEALTHY group AND a *PartialDiscoveryError naming the group/versions that
	// failed. That is a DEGRADED result, not a failure: a caller that maps it to
	// a 5xx makes one stale aggregated APIService break every category again.
	// Discriminate with AsPartialDiscovery, serve what came back, and report the
	// degradation. Any other non-nil error is genuinely fatal and the resource
	// slice is nil.
	Discover(ctx context.Context, category string) ([]schema.GroupVersionResource, error)
}

var _ Client = (*unstructuredClient)(nil)

type unstructuredClient struct {
	dynamicClient   *dynamic.DynamicClient
	discoveryClient discovery.DiscoveryInterface
	mapper          *restmapper.DeferredDiscoveryRESTMapper
	converter       runtime.UnstructuredConverter
}

func (uc *unstructuredClient) Create(ctx context.Context, obj *unstructured.Unstructured, opts Options) (*unstructured.Unstructured, error) {
	ri, err := uc.resourceInterfaceFor(opts)
	if err != nil {
		return nil, err
	}

	return ri.Create(ctx, obj, metav1.CreateOptions{})
}

func (uc *unstructuredClient) Get(ctx context.Context, name string, opts Options) (*unstructured.Unstructured, error) {
	ri, err := uc.readInterfaceFor(opts)
	if err != nil {
		return nil, err
	}

	return ri.Get(ctx, name, metav1.GetOptions{})
}

func (uc *unstructuredClient) List(ctx context.Context, opts Options) (*unstructured.UnstructuredList, error) {
	ri, err := uc.readInterfaceFor(opts)
	if err != nil {
		return nil, err
	}

	return ri.List(ctx, metav1.ListOptions{})
}

func (uc *unstructuredClient) Delete(ctx context.Context, name string, opts Options) error {
	ri, err := uc.resourceInterfaceFor(opts)
	if err != nil {
		return err
	}

	return ri.Delete(ctx, name, metav1.DeleteOptions{})
}

func (uc *unstructuredClient) FromUnstructured(in map[string]any, out any) error {
	return uc.converter.FromUnstructured(in, out)
}

func (uc *unstructuredClient) ToUnstructured(in any) (map[string]any, error) {
	return uc.converter.ToUnstructured(in)
}

func (uc *unstructuredClient) Discover(ctx context.Context, category string) ([]schema.GroupVersionResource, error) {
	recordDiscoveryCall()

	lists, err := uc.discoveryClient.ServerPreferredResources()

	// #517 — SERVERPREFERREDRESOURCES IS A PARTIAL-RESULT API.
	// When one group fails it returns every HEALTHY group's resources
	// ALONGSIDE *discovery.ErrGroupDiscoveryFailed (client-go
	// discovery/discovery_client.go:597-601; withRetries at :706-720 returns
	// that pair unchanged after its retries). The previous `if err != nil {
	// return }` discarded `lists` wholesale, and handlers/list.go turned it
	// into a 500 — so ONE stale aggregated APIService broke GET /list for
	// EVERY category. Honour the partial result instead and report the
	// degradation; see partial_discovery.go for the full reasoning.
	var degraded *PartialDiscoveryError
	if err != nil {
		failed, isGroupFailure := discovery.GroupDiscoveryFailedErrorGroups(err)
		switch {
		case !isGroupFailure:
			// A genuine transport/auth failure. client-go returns no result at
			// all for it (withRetries: `return nil, nil, err` at :715-717), so
			// there is no healthy subset to honour — STILL FATAL, which is
			// precisely why the discrimination above has to be exact.
			recordFatalDiscovery(nil)
			return nil, err
		case len(lists) == 0:
			// Every group failed. "Partial" needs a part: honouring this would
			// serve an EMPTY list with a 200 and call a total discovery outage
			// healthy — the same under-report shape as the defect, inverted.
			recordFatalDiscovery(failed)
			return nil, err
		default:
			recordPartialDiscovery(failed)
			degraded = &PartialDiscoveryError{Groups: failed}
		}
	}

	var all []schema.GroupVersionResource
	for _, list := range lists {
		if len(list.APIResources) == 0 {
			continue
		}

		// #508 — THE GROUP AND VERSION LIVE ON THE LIST, NOT ON THE RESOURCE.
		// apimachinery documents APIResource.Group as "the preferred group of
		// the resource. Empty implies the group of the containing resource
		// list", and Version identically (types.go:1180-1184). They are
		// populated only where a resource differs from its containing list —
		// subresources, e.g. a v1 Scale inside a v1beta1 list. For every
		// ordinary resource both are EMPTY.
		//
		// So reading them directly produced GroupVersionResource{"", "",
		// <plural>} for essentially every discovered resource, and the one
		// consumer (handlers/list.go, GET /list?category=…) passes that to
		// ListObjects → resourceInterfaceFor → mapper.KindFor, which then
		// resolves by PLURAL ALONE across every group. That is correct only
		// while a plural is unique cluster-wide: once two CRDs in different
		// groups share one — which gets MORE likely as a cluster accumulates
		// CRDs — the mapper resolves by priority or ambiguity rather than by
		// what was asked for, and /list answers 200 with the wrong kind or
		// silently without it. No error, no counter, no log says so.
		gv, gvErr := schema.ParseGroupVersion(list.GroupVersion)
		if gvErr != nil {
			// A list whose own GroupVersion does not parse cannot be attributed
			// to a group: skip it rather than emit a coordinate we cannot stand
			// behind. (Unreachable from a conformant apiserver.)
			continue
		}

		for _, el := range list.APIResources {
			if !found(el, category) {
				continue
			}

			// The per-resource fields WIN when set, which is what they are for
			// (a subresource whose group/version differs from its list).
			group, version := el.Group, el.Version
			if group == "" {
				group = gv.Group
			}
			if version == "" {
				version = gv.Version
			}

			all = append(all, schema.GroupVersionResource{
				Group:    group,
				Version:  version,
				Resource: el.Name,
			})
		}
	}

	if degraded != nil {
		// The healthy resources AND the degradation: both, on purpose.
		return all, degraded
	}
	return all, nil
}

// resourceInterfaceFor returns the interface for opts' resource, as the mapper
// resolves it. A resource the mapper has no mapping for is the mapper's error:
// this is the route of the writes (Create, Delete), which never act on a
// resource the client's copy of discovery does not know. The reads go through
// readInterfaceFor.
func (uc *unstructuredClient) resourceInterfaceFor(opts Options) (dynamic.ResourceInterface, error) {
	if err := checkPathSegments(opts); err != nil {
		return nil, err
	}
	if opts.GVK.Empty() && !opts.GVR.Empty() {
		gvk, err := uc.mapper.KindFor(opts.GVR)
		if err != nil {
			return nil, err
		}
		opts.GVK = gvk
	}

	restMapping, err := uc.mapper.RESTMapping(opts.GVK.GroupKind(), opts.GVK.Version)
	if err != nil {
		return nil, err
	}

	return uc.resourceInterface(restMapping.Resource, opts.Namespace), nil
}

// readInterfaceFor is resourceInterfaceFor for a read (Get, List), with one
// difference (#1414): when the mapper has no mapping for a fully named
// resource, the apiserver is asked instead of failing on the miss.
//
// NO MAPPING IS NOT AN ANSWER. The mapper is a copy of discovery, and it has
// no mapping in three situations it cannot tell apart: the resource is not
// served (no CRD), its group's discovery failed or is stale (client-go drops
// such a group from the mapper without an error), or the copy predates the
// CRD. Returning the miss made every one of them a non-Status error, which
// the callers answer 500. servedResource reads the one document that says
// which it is. One rule for every resource.
func (uc *unstructuredClient) readInterfaceFor(opts Options) (dynamic.ResourceInterface, error) {
	ri, err := uc.resourceInterfaceFor(opts)
	if err == nil || !meta.IsNoMatchError(err) || !opts.GVK.Empty() ||
		opts.GVR.Version == "" || opts.GVR.Resource == "" {
		return ri, err
	}
	gvr, err := uc.servedResource(opts.GVR)
	if err != nil {
		return nil, err
	}
	return uc.resourceInterface(gvr, opts.Namespace), nil
}

func (uc *unstructuredClient) resourceInterface(gvr schema.GroupVersionResource, namespace string) dynamic.ResourceInterface {
	if len(namespace) == 0 {
		return uc.dynamicClient.Resource(gvr)
	}
	return uc.dynamicClient.Resource(gvr).Namespace(namespace)
}

// checkPathSegments refuses options whose group, version or resource is not
// one apiserver path segment (#1257, apipath.Segment). The client is the last
// place before the wire: client-go joins these values into the request path as
// they are, so a "/" would add segments and a ".." would leave the resource.
// Nothing is discovered or dialled for such a value.
func checkPathSegments(opts Options) error {
	// Each part the caller named. A part left empty never becomes a path as
	// given: the mapper resolves it, or the read refuses it.
	for _, part := range []struct{ field, v string }{
		{"group", opts.GVR.Group}, {"version", opts.GVR.Version}, {"resource", opts.GVR.Resource},
	} {
		if part.v == "" {
			continue
		}
		if err := apipath.Segment(apipath.ClientOption(part.field), part.v); err != nil {
			return err
		}
	}
	return nil
}

// NotServedError says the apiserver does not serve a resource: its type is not
// installed. It is never inferred from an error's text or media type; see
// servedResource for the evidence it stands on.
type NotServedError struct {
	GVR schema.GroupVersionResource
}

func (e *NotServedError) Error() string {
	return fmt.Sprintf("the apiserver does not serve %s in %s", e.GVR.Resource, e.GVR.GroupVersion())
}

// IsNotServed reports whether err says the requested resource's type is not
// served by the apiserver, as opposed to an object of a served type missing.
func IsNotServed(err error) bool {
	var nse *NotServedError
	return errors.As(err, &nse)
}

// servedResource answers, for a resource the mapper has no mapping for,
// whether the apiserver serves it, from the one place that says so: the
// discovery document of the resource's own group/version, read now from the
// apiserver and not from the mapper's copy.
//
//   - the document is a 404 (the group/version is not served) or lists no such
//     resource: *NotServedError. Nothing else is dialled;
//   - the document lists it: the resource as the document names it. The name
//     is matched as the mapper matches one (lower-cased, plural or singular),
//     so a read resolves to the same resource whether the mapper or this
//     document resolved it, and the path is built from the apiserver's own
//     name, never from the caller's;
//   - the document cannot be read (the group's backend is down, the apiserver
//     is unreachable): that error. An unknown is never "not served".
func (uc *unstructuredClient) servedResource(gvr schema.GroupVersionResource) (schema.GroupVersionResource, error) {
	list, err := uc.discoveryClient.ServerResourcesForGroupVersion(gvr.GroupVersion().String())
	if apierrors.IsNotFound(err) {
		return schema.GroupVersionResource{}, &NotServedError{GVR: gvr}
	}
	if err != nil {
		return schema.GroupVersionResource{}, err
	}
	want := strings.ToLower(gvr.Resource)
	if list != nil {
		for _, r := range list.APIResources {
			if strings.Contains(r.Name, "/") {
				continue // a subresource is not a resource
			}
			if r.Name == want || (r.SingularName != "" && r.SingularName == want) {
				return gvr.GroupVersion().WithResource(r.Name), nil
			}
		}
	}
	return schema.GroupVersionResource{}, &NotServedError{GVR: gvr}
}

func found(el metav1.APIResource, str string) bool {
	if strings.EqualFold(el.Name, str) {
		return true
	}

	if strings.EqualFold(el.SingularName, str) {
		return true
	}

	if contains(el.ShortNames, str) {
		return true
	}

	return contains(el.Categories, str)
}

func contains(s []string, e string) bool {
	for _, a := range s {
		if a == e {
			return true
		}
	}
	return false
}
