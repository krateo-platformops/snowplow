package v1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

// RESTActionSpec defines the api handler specifications.
type RESTActionSpec struct {
	//+listType=atomic
	API    []*API  `json:"api,omitempty"`
	Filter *string `json:"filter,omitempty"`
}

// +kubebuilder:object:root=true
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="AGE",type="date",JSONPath=".metadata.creationTimestamp"
// +kubebuilder:resource:scope=Namespaced,shortName=ra,categories={krateo,rest,actions}

// RESTAction allows users to declaratively define calls to APIs that may in turn depend on other calls.
type RESTAction struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata"`

	Spec   RESTActionSpec        `json:"spec"`
	Status *runtime.RawExtension `json:"status,omitempty"`
}

// HasUserAccessFilterStage reports whether ANY api-step of this RESTAction
// declares a userAccessFilter — i.e. whether the RA's resolved output is
// PER-REQUESTER NARROWED by the per-object RBAC refilter.
//
// SINGLE SOURCE OF TRUTH (#118 / A-1). The resolved-cache key folds only the
// dispatch-authorizing BindingUID (+ the per-subject RBACSubGen); it does NOT
// fold the per-object UAF narrowing scope. So two users who share the
// first-match binding for `get restactions` derive the SAME key while their
// UAF-narrowed bodies legitimately DIFFER — one user's rows would be served to
// the other from the shared cell. The 1.12.3 A-1 mitigation declines the L1 Put
// (and bypasses the raFullList cell) for any RA this predicate reports true for.
//
// The predicate lives on the API type — not in a consumer package — because it
// is consulted from THREE packages (internal/handlers/dispatchers for the three
// restactions Put sites, internal/resolvers/widgets/apiref for the raFullList
// bypass, and the tests): one derivation, so the sites cannot drift (#64
// anti-shadow-drift). It keys on the presence of the UAF CONTRACT itself,
// uniform across every RA — never on a resource/name/path literal
// (feedback_no_special_cases). Nil receiver, nil api-step elements and a nil
// UserAccessFilter are all guarded.
func (in *RESTAction) HasUserAccessFilterStage() bool {
	if in == nil {
		return false
	}
	for _, step := range in.Spec.API {
		if step != nil && step.UserAccessFilter != nil {
			return true
		}
	}
	return false
}

// HasExternalEndpointStage reports that at least one api[] stage dispatches
// against a named EndpointRef — i.e. an endpoint OUTSIDE the apiserver, reached
// over HTTP (#594). It is the structural cost class of a resolve, not a
// property of any particular endpoint: no host, name or resource literal
// appears here (feedback_no_special_cases).
//
// WHY THE SEED CARES. An external stage is the expensive half of the boot seed
// and the least valuable half to have warm, and both halves of that follow from
// the same fact — the data does not live in an informer:
//
//   - COST. The fetch is a real HTTP round trip, bounded on the readiness path
//     by externalSeedFetchCtx (RESOLVER_SEED_EXTERNAL_FETCH_TIMEOUT_MS, 5s
//     default). A slow endpoint spends that whole budget per (widget × cohort)
//     unit, and seedFailOperational RE-ENQUEUES, so a timeout is paid twice.
//   - VALUE. An external-backed cell has no dep edges, so nothing can ever
//     dirty it and no publish can ever fire for it — which is exactly why
//     setRefreshKeyHeaderUnlessExternal already withholds its refresh-key
//     header. It also carries the bounded external TTL, so a cell warmed at
//     boot expires long before most of the units behind it are navigated to.
//
// The same "has an endpointRef ⇒ not apiserver-backed" reading is already made
// structurally in cache/inventory.go, which skips these stages when collecting
// GVRs to register informers for. This accessor is the typed one derivation the
// seed consults, so the two cannot drift (#64 anti-shadow-drift).
//
// Nil receiver and nil api-step elements are guarded, exactly as the sibling
// predicate above guards them.
func (in *RESTAction) HasExternalEndpointStage() bool {
	if in == nil {
		return false
	}
	for _, step := range in.Spec.API {
		if step != nil && step.EndpointRef != nil {
			return true
		}
	}
	return false
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// RESTActionList contains a list of RESTAction
type RESTActionList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata"`
	Items           []RESTAction `json:"items"`
}
