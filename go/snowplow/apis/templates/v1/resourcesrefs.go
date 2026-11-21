package v1

import "github.com/krateo-platformops/plumbing/http/response"

type Slice struct {
	Continue bool
	Offset   int
	Page     int
	PerPage  int
}

// ResourceRef defines a template for an action.
type ResourceRef struct {
	// ID for the action.
	ID string `json:"id,omitempty"`
	// Name of the related resource.
	Name string `json:"name,omitempty"`
	// Namespace of the related resource.
	Namespace string `json:"namespace,omitempty"`
	// Resource on which the action will act.
	Resource string `json:"resource,omitempty"`
	// APIVersion for the related resource
	APIVersion string `json:"apiVersion,omitempty"`
	// Verb is the HTTP request verb.
	Verb string `json:"verb,omitempty"`
	// Slice is used for pagination
	Slice *Slice `json:"slice,omitempty"`
	// Inline, when true on a GET ref, asks snowplow to resolve this child
	// server-side under the requesting user's identity and embed the resolved
	// envelope into the result's Rendered field (#72, inline-rendered-children).
	// Additive + opt-in: a ref without Inline is byte-identical to today. A
	// non-GET / not-allowed inline ref is NOT embedded.
	Inline bool `json:"inline,omitempty"`
}

// ResourceRefResult defines the action result after evaluating a template.
//
// +k8s:deepcopy-gen=false
// #72: ResourceRefResult is RESOLVED OUTPUT (built per-request from a template;
// written into a widget's status.resourcesRefs at resolve time), NOT a stored
// CRD spec/status type — it is referenced by no apis spec/status field, and
// nothing calls its DeepCopy. The package default (+k8s:deepcopy-gen=package,
// doc.go) speculatively generates deepcopy for every type; that GEN PANICS on
// the additive Rendered map[string]any field (controller-tools v0.17.3 cannot
// deepcopy an interface{}-valued field). deepcopy-gen=false skips this type
// (it needs no generated deepcopy) and lets `make generate` pass while the
// runtime carrier stays the deep-copy-safe map (NOT json.RawMessage, which
// would panic the cache's runtime.DeepCopyJSONValue — feedback_no_rawmessage_
// in_unstructured_status). The embedded child lives inside the resolved
// *unstructured.Unstructured, which has its own (working) deep-copy path.
type ResourceRefResult struct {
	// ID of this action.
	ID string `json:"id,omitempty"`
	// Path is the HTTP request path.
	Path string `json:"path,omitempty"`
	// Verb is the HTTP request verb.
	Verb string `json:"verb,omitempty"`
	// Payload the payload for the action result
	Payload *ResourceRefPayload `json:"payload,omitempty"`
	// Allowed is this resource reference allowed (or not) for the user
	Allowed bool `json:"allowed"`
	// Inline carries the source ref's Inline flag through to the dispatcher,
	// which (post-resolve) decides whether to embed the child (#72). Carried,
	// not re-read — the dispatcher inline-walk consumes it.
	Inline bool `json:"inline,omitempty"`
	// Rendered is the resolved child envelope, embedded server-side when this
	// ref is Inline+Allowed+GET (#72). Empty/omitted for every non-inline ref —
	// so the served shape is byte-identical to today until a widget authors
	// inline:true. map[string]any (NOT json.RawMessage): the dispatcher embeds
	// it into the resolved *unstructured.Unstructured, which is deep-copied on
	// the cache Put / refresher path — runtime.DeepCopyJSONValue PANICS on a
	// json.RawMessage but handles a standard map. So the child is decoded once
	// into a map and re-encoded with the parent's single encode (A1 path i, the
	// design's documented fallback; §6-bench-gated). NOTE: path ii
	// (RawMessage-verbatim) encodes fine but is unsound here — it panics the
	// unstructured deep-copy.
	Rendered map[string]any `json:"rendered,omitempty"`
	// Error is set, and only set, on a FAILURE entry (#1564): the reference
	// could not be resolved (its apiVersion does not parse, discovery says its
	// type is not served, or discovery could not answer). It is the same
	// k8s-style Status a widget /call answers for a failed dependency
	// (docs/howto-frontend-widget-dependency-status.md): code, reason, an
	// identity-free message and, for a type that is not installed, the
	// TypeNotInstalled details. A failure entry carries the ref's id and verb,
	// allowed:false (no action is possible) and NO path, so nothing can fetch
	// it as if it were a working child.
	Error *response.Status `json:"error,omitempty"`
}

// ResourceRefPayload is the template action result payload.
type ResourceRefPayload struct {
	Kind       string     `json:"kind,omitempty"`
	APIVersion string     `json:"apiVersion,omitempty"`
	MetaData   *Reference `json:"metadata,omitempty"`
}
