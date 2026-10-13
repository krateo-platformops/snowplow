// Package chartcrd holds the #657 installability guard for helm/snowplow.
//
// On a Krateo install the chart is not consumed by helm alone: core-provider
// compiles helm/snowplow/values.schema.json into the CompositionDefinition's CRD
// with plumbing/crdgen, and the apiserver must accept that CRD. Two checks that
// passed for 1.12.45 (JSON-Schema validity and `helm template`) say nothing
// about that step. 1.12.45 shipped a schema whose crdgen output was over the
// apiserver's CEL cost budget, so the chart could not be installed.
//
// The package has no production code. Its tests run the same crdgen version
// core-provider pins over the chart schema, and then submit the result to a
// real apiserver.
package chartcrd
