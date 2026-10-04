package util

import (
	"context"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

// #443 echo response headers. They live in this leaf package so the handlers
// package (/call) and the dispatchers package (the inline resolve) share one
// definition without an import cycle; handlers re-exports them.
const (
	// HeaderDryRun is "All" when the apiserver call carried dryRun=All, or
	// when an inline RESTAction body was resolved without persisting anything.
	HeaderDryRun = "X-Snowplow-Dry-Run"
	// HeaderFieldValidation is the fieldValidation value the apiserver call
	// carried.
	HeaderFieldValidation = "X-Snowplow-Field-Validation"
	// HeaderRaw is "true" when the stored object was read without resolving.
	HeaderRaw = "X-Snowplow-Raw"
	// HeaderResolveSource is ResolveSourceRequestBody on an inline resolve.
	HeaderResolveSource = "X-Snowplow-Resolve-Source"
	// HeaderStageOutcomes carries an inline resolve's per-stage outcomes as
	// compact JSON ([{"name","ok","reason"}], reason codes only), or
	// {"truncated":true,"failed":N} above 4 KiB. Never on a stored resolve.
	HeaderStageOutcomes = "X-Snowplow-Stage-Outcomes"

	// ResolveSourceRequestBody is the HeaderResolveSource value of a resolve
	// whose RESTAction came from the request body, not from the cluster.
	ResolveSourceRequestBody = "request-body"
)

type inlineObjectKey struct{}

// WithInlineObject stashes a validated inline RESTAction (POST /call/read
// body "object", #443 part 2) on ctx. Only middleware.BodyExtrasDecode sets
// it, after validating the object against the query's addressing.
func WithInlineObject(ctx context.Context, obj *unstructured.Unstructured) context.Context {
	return context.WithValue(ctx, inlineObjectKey{}, obj)
}

// InlineObject returns the inline RESTAction stashed by WithInlineObject.
func InlineObject(ctx context.Context) (*unstructured.Unstructured, bool) {
	if ctx == nil {
		return nil, false
	}
	obj, ok := ctx.Value(inlineObjectKey{}).(*unstructured.Unstructured)
	return obj, ok && obj != nil
}
