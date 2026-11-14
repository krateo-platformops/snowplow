//go:build js && wasm

package fakeapiserver

import (
	"encoding/json"
	"fmt"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// Snapshot is snapshot.json (SNAPSHOT.md §3), as far as the apiserver reads it. bridge.js has
// already validated it with snapshot-rules.js before the program runs.
type Snapshot struct {
	Schema       int `json:"schema"`
	CapturedFrom struct {
		Server string `json:"server"`
		// The reference cluster's rules-review answer (§4, §11.1), replayed on every
		// SelfSubjectRulesReview. Absent, the answer is complete.
		SelfSubjectRulesReview *struct {
			Incomplete      bool   `json:"incomplete"`
			EvaluationError string `json:"evaluationError,omitempty"`
		} `json:"selfSubjectRulesReview,omitempty"`
	} `json:"capturedFrom"`
	ResourceVersion string `json:"resourceVersion"`
	Discovery       struct {
		API       json.RawMessage                   `json:"api"`
		APIs      json.RawMessage                   `json:"apis"`
		Resources map[string]metav1.APIResourceList `json:"resources"`
	} `json:"discovery"`
	Identity struct {
		AuthnNamespace string `json:"authnNamespace"`
		ServiceAccount struct {
			Namespace string `json:"namespace"`
			Name      string `json:"name"`
		} `json:"serviceAccount"`
		Visitor struct {
			Username string   `json:"username"`
			Groups   []string `json:"groups"`
			Name     string   `json:"name"`
		} `json:"visitor"`
	} `json:"identity"`
	Scripted struct {
		ApplyStatus []Rule `json:"applyStatus"`
	} `json:"scripted"`
}

// Rule is one scripted.applyStatus rule (§6).
type Rule struct {
	ID string `json:"id"`
	On struct {
		Verb      string         `json:"verb"`
		Group     string         `json:"group"`
		Resource  string         `json:"resource"`
		Namespace string         `json:"namespace"`
		Name      *string        `json:"name,omitempty"`
		Match     map[string]any `json:"match,omitempty"`
	} `json:"on"`
	AfterMs int64           `json:"afterMs"`
	Target  json.RawMessage `json:"target"`
	Status  map[string]any  `json:"status"`
	Stamp   []string        `json:"stamp,omitempty"`
}

// target is a rule's resolved target; trigger means "the written object", which fireRules resolves
// to that object: its name and its UID, so the write is for the object the rule fired on and not for
// another one that has taken its name since.
type target struct {
	Trigger                          bool
	Group, Resource, Namespace, Name string
	UID                              string
}

func (r Rule) target() (target, error) {
	var s string
	if err := json.Unmarshal(r.Target, &s); err == nil {
		if s != "trigger" {
			return target{}, fmt.Errorf("rule %s: target %q", r.ID, s)
		}
		return target{Trigger: true}, nil
	}
	var t struct{ Group, Resource, Namespace, Name string }
	if err := json.Unmarshal(r.Target, &t); err != nil {
		return target{}, fmt.Errorf("rule %s: target: %v", r.ID, err)
	}
	return target{Group: t.Group, Resource: t.Resource, Namespace: t.Namespace, Name: t.Name}, nil
}

// page is one object page, objects/<n>.json.
type page struct {
	Items []map[string]any `json:"items"`
}
