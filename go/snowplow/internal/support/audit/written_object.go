// written_object.go — #957 (krateo-acmp#59 Option B): the identity of the
// object a successful Kubernetes write wrote, read from the apiserver's answer.
//
// WHY. A user write through snowplow names the object by its coordinates. When
// a controller later reconciles that object and performs the real external
// change, nothing tied its audit record back to the user's write. Both sides
// now record the object's identity: snowplow the uid, generation and
// resourceVersion the apiserver returned for the write, the controller the uid
// and the generation it acted on. The trail joins on (uid, generation). The
// user's object is never annotated or otherwise changed for this.
//
// THE RULES, all here so every emit site applies the same ones:
//   - the values come from the apiserver's RESPONSE, never from the request
//     body: the request carries what the caller sent (often no uid, and a
//     resourceVersion that the write replaces), the response what was stored;
//   - a write that did not succeed records none: there is no written object;
//   - a DELETE records only the uid: of the returned object when deletion is
//     pending (finalizers), or the one a Status answer names in details.uid
//     when the object is gone. A deleted object has no generation to reconcile;
//   - a response that carries no such field records none of it, which is not
//     an error.
//
// The emit site decides whether the write went to the Kubernetes apiserver at
// all: a RESTAction stage to a named endpoint never calls this.
package audit

import (
	"encoding/json"
	"net/http"
)

// writtenObjectResponse is the part of an apiserver write answer that names
// the object written. details is decoded only from a Status answer, so an
// object that happens to have a top-level "details" field cannot break it.
type writtenObjectResponse struct {
	APIVersion string `json:"apiVersion"`
	Kind       string `json:"kind"`
	Metadata   struct {
		UID             string `json:"uid"`
		Generation      int64  `json:"generation"`
		ResourceVersion string `json:"resourceVersion"`
	} `json:"metadata"`
	Details json.RawMessage `json:"details"`
}

// RecordWrittenObject sets ev's object identity from response, the body the
// apiserver answered ev's write with. ev.Verb and ev.Outcome must already be
// set: only a successful POST, PUT, PATCH or DELETE records anything. Every
// other case leaves the three fields empty.
func (ev *Event) RecordWrittenObject(response []byte) {
	if ev.Outcome != "success" || len(response) == 0 {
		return
	}
	switch ev.Verb {
	case http.MethodPost, http.MethodPut, http.MethodPatch, http.MethodDelete:
	default:
		return
	}
	var r writtenObjectResponse
	if err := json.Unmarshal(response, &r); err != nil {
		return // not a single JSON object: nothing it names can be trusted
	}
	if ev.Verb == http.MethodDelete {
		ev.ObjectUID = r.Metadata.UID
		if ev.ObjectUID == "" && r.Kind == "Status" && r.APIVersion == "v1" && len(r.Details) > 0 {
			var d struct {
				UID string `json:"uid"`
			}
			if json.Unmarshal(r.Details, &d) == nil {
				ev.ObjectUID = d.UID
			}
		}
		return
	}
	ev.ObjectUID = r.Metadata.UID
	ev.ObjectGeneration = r.Metadata.Generation
	ev.ObjectResourceVersion = r.Metadata.ResourceVersion
}
