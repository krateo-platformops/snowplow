//go:build js && wasm

package fakeapiserver

// The sandbox's Raise (portal #424, Zendesk requests): the visitor creates a ZendeskRequest through
// /call, and a scripted create-rule (§6) files it as the Zendesk provider would, after a delay. The
// CRDs are 057's own (testdata/zendesk, read-only from 057 on 2026-11-13); the POST body and the
// "filed" status are the shapes portal #424 sends and renders; the ServiceAccount's grant is 057's
// (get/list/watch on */*, create on self/subjectaccessreviews, impersonate users).

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"
)

const (
	zrGroup    = "zendesk.ogen.krateo.io"
	zrPath     = "/apis/zendesk.ogen.krateo.io/v2/namespaces/krateo-system/zendeskrequests"
	zrAllPath  = "/apis/zendesk.ogen.krateo.io/v2/zendeskrequests"
	zrcPath    = "/apis/zendesk.ogen.krateo.io/v1alpha1/namespaces/krateo-system/zendeskrequestconfigurations"
	zrFiledID  = 7300001
	zrFileWait = 2500 * time.Millisecond
)

func zrCRD(t *testing.T, name string) map[string]any {
	t.Helper()
	var m map[string]any
	if err := json.Unmarshal(readFixture(t, "zendesk/crd-"+name+".json"), &m); err != nil {
		t.Fatal(err)
	}
	m["metadata"].(map[string]any)["uid"] = "zendesk-crd-" + name
	m["metadata"].(map[string]any)["resourceVersion"] = "40"
	return m
}

func zrRBAC() []map[string]any {
	rule := func(groups, resources, verbs []any) map[string]any {
		return map[string]any{"apiGroups": groups, "resources": resources, "verbs": verbs}
	}
	return []map[string]any{
		// 057's snowplow ServiceAccount grant, exactly.
		{"apiVersion": "rbac.authorization.k8s.io/v1", "kind": "ClusterRole", "metadata": map[string]any{"name": "snowplow-krateo-system", "resourceVersion": "41"}, "rules": []any{
			rule([]any{"*"}, []any{"*"}, []any{"get", "list", "watch"}),
			rule([]any{"authorization.k8s.io"}, []any{"selfsubjectaccessreviews", "subjectaccessreviews"}, []any{"create"}),
			rule([]any{""}, []any{"users"}, []any{"impersonate"}),
		}},
		{"apiVersion": "rbac.authorization.k8s.io/v1", "kind": "ClusterRoleBinding", "metadata": map[string]any{"name": "snowplow-krateo-system", "resourceVersion": "42"},
			"roleRef":  map[string]any{"apiGroup": "rbac.authorization.k8s.io", "kind": "ClusterRole", "name": "snowplow-krateo-system"},
			"subjects": []any{map[string]any{"kind": "ServiceAccount", "name": "snowplow", "namespace": "krateo-system"}}},
		// §6: the scripted status write, as the ServiceAccount.
		{"apiVersion": "rbac.authorization.k8s.io/v1", "kind": "Role", "metadata": map[string]any{"name": "sandbox-zendesk-filed", "namespace": "krateo-system", "resourceVersion": "43"}, "rules": []any{
			rule([]any{zrGroup}, []any{"zendeskrequests/status"}, []any{"get", "patch", "update"}),
		}},
		{"apiVersion": "rbac.authorization.k8s.io/v1", "kind": "RoleBinding", "metadata": map[string]any{"name": "sandbox-zendesk-filed", "namespace": "krateo-system", "resourceVersion": "44"},
			"roleRef":  map[string]any{"apiGroup": "rbac.authorization.k8s.io", "kind": "Role", "name": "sandbox-zendesk-filed"},
			"subjects": []any{map[string]any{"kind": "ServiceAccount", "name": "snowplow", "namespace": "krateo-system"}}},
		// The visitor's Raise, in the configuration's namespace only: create, get and list
		// requests; list configurations.
		{"apiVersion": "rbac.authorization.k8s.io/v1", "kind": "Role", "metadata": map[string]any{"name": "sandbox-visitor-raise", "namespace": "krateo-system", "resourceVersion": "45"}, "rules": []any{
			rule([]any{zrGroup}, []any{"zendeskrequests"}, []any{"create", "get", "list"}),
			rule([]any{zrGroup}, []any{"zendeskrequestconfigurations"}, []any{"list"}),
		}},
		{"apiVersion": "rbac.authorization.k8s.io/v1", "kind": "RoleBinding", "metadata": map[string]any{"name": "sandbox-visitor-raise", "namespace": "krateo-system", "resourceVersion": "46"},
			"roleRef":  map[string]any{"apiGroup": "rbac.authorization.k8s.io", "kind": "Role", "name": "sandbox-visitor-raise"},
			"subjects": []any{map[string]any{"apiGroup": "rbac.authorization.k8s.io", "kind": "User", "name": "sandbox-visitor"}}},
	}
}

// The seeds: one configuration and one already-filed request, as marketing's story seeds hold them.
func zrSeeds(incidentUID string) []map[string]any {
	return []map[string]any{
		{"apiVersion": zrGroup + "/v1alpha1", "kind": "ZendeskRequestConfiguration",
			"metadata": map[string]any{"name": "demo-support", "namespace": "krateo-system", "uid": "zrc-1", "resourceVersion": "47", "generation": 1},
			"spec":     map[string]any{"authentication": map[string]any{"bearer": map[string]any{"tokenRef": map[string]any{"name": "demo-support-token", "namespace": "krateo-system", "key": "token"}}}}},
		{"apiVersion": zrGroup + "/v2", "kind": "ZendeskRequest",
			"metadata": map[string]any{"name": "zr-demo-user-1794500000", "namespace": "krateo-system", "uid": "zr-seed-1", "resourceVersion": "48", "generation": 1,
				"labels":      map[string]any{"krateo.io/incident-uid": incidentUID, "krateo.io/raised-by": "demo-user"},
				"annotations": map[string]any{"krateo.io/raised-by": "demo-user"}},
			"spec": map[string]any{"subject": "Volume attach failures on the payments cluster", "description": "Raised from the incident.", "kind": "Bug", "priority": "high",
				"krateo_reference": "zr-demo-user-1794500000", "configurationRef": map[string]any{"name": "demo-support", "namespace": "krateo-system"}},
			"status": map[string]any{"id": json.Number("7300000"), "status": "new", "priority": "high", "created_at": "2026-11-12T09:00:00Z", "updated_at": "2026-11-12T09:00:00Z",
				"url": "https://support.example.invalid/api/v2/requests/7300000.json", "html_url": "https://support.example.invalid/hc/requests/7300000",
				"conditions": []any{
					map[string]any{"type": "Ready", "status": "True", "reason": "Available", "lastTransitionTime": "2026-11-12T09:00:00Z"},
					map[string]any{"type": "Synced", "status": "True", "reason": "ReconcileSuccess", "lastTransitionTime": "2026-11-12T09:00:00Z"},
				}}},
	}
}

// The "filed" rule: a create of a ZendeskRequest carrying the incident's uid label is filed after
// the delay, with the status fields the seeded request has, the times stamped.
func zrRule(incidentUID string) map[string]any {
	return map[string]any{
		"id": "file-raised-request",
		"on": map[string]any{"verb": "create", "group": zrGroup, "resource": "zendeskrequests", "namespace": "krateo-system",
			"match": map[string]any{"metadata": map[string]any{"labels": map[string]any{"krateo.io/incident-uid": incidentUID}}}},
		"afterMs": zrFileWait.Milliseconds(),
		"target":  "trigger",
		"status": map[string]any{"id": zrFiledID, "status": "new", "priority": "high",
			"url": fmt.Sprintf("https://support.example.invalid/api/v2/requests/%d.json", zrFiledID), "html_url": fmt.Sprintf("https://support.example.invalid/hc/requests/%d", zrFiledID),
			"conditions": []any{
				map[string]any{"type": "Ready", "status": "True", "reason": "Available"},
				map[string]any{"type": "Synced", "status": "True", "reason": "ReconcileSuccess"},
			}},
		"stamp": []any{"/status/created_at", "/status/updated_at", "/status/conditions/0/lastTransitionTime", "/status/conditions/1/lastTransitionTime"},
	}
}

func zrHarness(t *testing.T) (*harness, string) {
	t.Helper()
	// The incident's uid, from the fixture.
	var pg struct {
		Items []map[string]any `json:"items"`
	}
	if err := json.Unmarshal(readFixture(t, "objects/000.json"), &pg); err != nil {
		t.Fatal(err)
	}
	var uid string
	for _, o := range pg.Items {
		if o["kind"] == "Incident" {
			uid = fmt.Sprint(get(o, "metadata", "uid"))
		}
	}
	if uid == "" {
		t.Fatal("no incident in the fixture")
	}
	extra := append([]map[string]any{zrCRD(t, "zendeskrequests"), zrCRD(t, "zendeskrequestconfigurations")}, zrRBAC()...)
	extra = append(extra, zrSeeds(uid)...)
	h := newHarness(t, opts{extra: extra, edit: func(s map[string]any) {
		d := s["discovery"].(map[string]any)
		groups := d["apis"].(map[string]any)
		groups["groups"] = append(groups["groups"].([]any), map[string]any{"name": zrGroup,
			"versions":         []any{map[string]any{"groupVersion": zrGroup + "/v2", "version": "v2"}, map[string]any{"groupVersion": zrGroup + "/v1alpha1", "version": "v1alpha1"}},
			"preferredVersion": map[string]any{"groupVersion": zrGroup + "/v2", "version": "v2"}})
		verbs := []any{"delete", "deletecollection", "get", "list", "patch", "create", "update", "watch"}
		res := d["resources"].(map[string]any)
		res[zrGroup+"/v2"] = map[string]any{"kind": "APIResourceList", "groupVersion": zrGroup + "/v2", "resources": []any{
			map[string]any{"name": "zendeskrequests", "singularName": "zendeskrequest", "namespaced": true, "kind": "ZendeskRequest", "verbs": verbs, "categories": []any{"zendeskrequest", "restresources", "rr"}},
			map[string]any{"name": "zendeskrequests/status", "singularName": "", "namespaced": true, "kind": "ZendeskRequest", "verbs": []any{"get", "patch", "update"}},
		}}
		res[zrGroup+"/v1alpha1"] = map[string]any{"kind": "APIResourceList", "groupVersion": zrGroup + "/v1alpha1", "resources": []any{
			map[string]any{"name": "zendeskrequestconfigurations", "singularName": "zendeskrequestconfiguration", "namespaced": true, "kind": "ZendeskRequestConfiguration", "verbs": verbs},
			map[string]any{"name": "zendeskrequestconfigurations/status", "singularName": "", "namespaced": true, "kind": "ZendeskRequestConfiguration", "verbs": []any{"get", "patch", "update"}},
		}}
		sc := s["scripted"].(map[string]any)
		sc["applyStatus"] = append(sc["applyStatus"].([]any), zrRule(uid))
	}})
	return h, uid
}

// raiseBody is the Raise form's POST body (portal #424 form.request-new.yaml): the name fixed at
// resolve, krateo_reference the same, the raised-by label (slug) and annotation (verbatim), the
// incident's uid label, and the create-only spec.
func raiseBody(name, incidentUID string) map[string]any {
	labels := map[string]any{"krateo.io/raised-by": "sandbox-visitor"}
	if incidentUID != "" {
		labels["krateo.io/incident-uid"] = incidentUID
	}
	return map[string]any{
		"apiVersion": zrGroup + "/v2", "kind": "ZendeskRequest",
		"metadata": map[string]any{"name": name, "namespace": "krateo-system", "labels": labels, "annotations": map[string]any{"krateo.io/raised-by": "sandbox-visitor"}},
		"spec": map[string]any{"configurationRef": map[string]any{"name": "demo-support", "namespace": "krateo-system"},
			"subject": "Volume attach failures again", "description": "Raised from the sandbox.", "kind": "Bug", "priority": "high", "krateo_reference": name},
	}
}

// cardState is what the incident card shows (portal #424): "filed" once Ready is True; "filing"
// while there is no Ready, or Ready is False with Synced not False.
func cardState(o map[string]any) string {
	conds, _ := get(o, "status", "conditions").([]any)
	var ready, synced string
	for _, c := range conds {
		m := c.(map[string]any)
		switch m["type"] {
		case "Ready":
			ready = fmt.Sprint(m["status"])
		case "Synced":
			synced = fmt.Sprint(m["status"])
		}
	}
	switch {
	case ready == "True":
		return "filed"
	case ready == "" || (ready == "False" && synced != "False"):
		return "filing"
	}
	return "failed"
}

// (c) and (a): the visitor's create is a 201 with RV+1, one ADDED event and its 10-key request
// line; the card shows "filing"; after the delay the rule files it, as the ServiceAccount, with one
// MODIFIED event, the seeded request's status fields and the times stamped; the card shows "filed".
// Both states, in order, on one watch, without a reload.
func TestZendesk_RaiseIsFilingThenFiledInOrder(t *testing.T) {
	h, uid := zrHarness(t)
	list := h.do("GET", zrAllPath, req{as: "sa"})
	eq(t, list.code, 200, "the ServiceAccount lists requests cluster-wide")
	listRV := rv(list)
	w := h.watch(zrAllPath+"?watch=1&resourceVersion="+listRV, req{as: "sa"})
	// The watch's request line is logged once it is registered: from then on the ADDED event is a
	// live delivery, not a replay from the watch cache.
	for deadline := time.Now().Add(10 * time.Second); !logged(h, func(l map[string]any) bool {
		return l["verb"] == "watch" && l["resource"] == "zendeskrequests" && l["identity"] == "sa"
	}); {
		if time.Now().After(deadline) {
			t.Fatal("the watch was not registered within 10s")
		}
		time.Sleep(5 * time.Millisecond)
	}

	name := "zr-sandbox-visitor-1794600000"
	before := len(h.log())
	created := h.do("POST", zrPath, req{as: "visitor", ct: "application/json", body: raiseBody(name, uid)})
	eq(t, created.code, 201, "the visitor's create")
	var listRVn, createdRVn int
	fmt.Sscan(listRV, &listRVn)
	fmt.Sscan(rv(created), &createdRVn)
	eq(t, createdRVn, listRVn+1, "the create's resourceVersion is the list's + 1")
	eq(t, get(created.body, "metadata", "name"), name, "the name the form fixed")
	eq(t, get(created.body, "spec", "krateo_reference"), name, "krateo_reference")
	eq(t, get(created.body, "metadata", "labels", "krateo.io/incident-uid"), uid, "the incident's uid label")
	eq(t, cardState(created.body), "filing", "the card right after the 201")
	lines := h.log()[before:]
	for _, b := range lineShapeErrors(lines) {
		t.Error(b)
	}
	if len(lines) != 1 {
		t.Fatalf("the create wrote %d lines, want its one request line: %v", len(lines), lines)
	}
	eq(t, lines[0], map[string]any{"verb": "create", "group": zrGroup, "version": "v2", "resource": "zendeskrequests", "namespace": "krateo-system", "name": name, "query": "", "status": 201, "items": 0, "identity": "visitor"}, "the create's request line")

	added := w.next()
	eq(t, added["type"], "ADDED", "the first event")
	eq(t, get(added, "object", "metadata", "name"), name, "the ADDED object")
	eq(t, cardState(added["object"].(map[string]any)), "filing", "the card on the ADDED event")

	// Not before the delay.
	h.advance(zrFileWait - time.Millisecond)
	if s := get(h.do("GET", zrPath+"/"+name, req{as: "sa"}).body, "status"); s != nil {
		t.Fatalf("filed before the delay: %v", s)
	}
	before = len(h.log())
	h.advance(time.Millisecond)
	modified := w.next()
	filing := h.log()[before:] // before any read this test makes below
	eq(t, modified["type"], "MODIFIED", "the second event")
	obj := modified["object"].(map[string]any)
	eq(t, cardState(obj), "filed", "the card on the MODIFIED event")
	var modRVn int
	fmt.Sscan(rv(resp{body: obj}), &modRVn)
	eq(t, modRVn, createdRVn+1, "the filing's resourceVersion is the create's + 1")
	at := h.now.UTC().Format(time.RFC3339)
	st := obj["status"].(map[string]any)
	idJSON, _ := json.Marshal(st["id"])
	eq(t, string(idJSON), fmt.Sprint(zrFiledID), "status.id, an integer as the CRD's int64 says")
	eq(t, st["status"], "new", "status.status")
	eq(t, st["created_at"], at, "status.created_at, stamped")
	eq(t, st["updated_at"], at, "status.updated_at, stamped")
	conds := st["conditions"].([]any)
	eq(t, conds[0], map[string]any{"type": "Ready", "status": "True", "reason": "Available", "lastTransitionTime": at}, "the Ready condition")
	eq(t, conds[1], map[string]any{"type": "Synced", "status": "True", "reason": "ReconcileSuccess", "lastTransitionTime": at}, "the Synced condition")
	// The same status fields as the seeded request, so a raised request renders as the seeded one does.
	seed := h.do("GET", zrPath+"/zr-demo-user-1794500000", req{as: "sa"})
	if a, b := keysOf(seed.body["status"].(map[string]any)), keysOf(st); strings.Join(a, ",") != strings.Join(b, ",") {
		t.Errorf("the filed status's fields %v differ from the seeded request's %v", b, a)
	}
	for _, b := range lineShapeErrors(filing) {
		t.Error(b)
	}
	if len(filing) != 1 {
		t.Fatalf("the filing wrote %d lines, want its one request line: %v", len(filing), filing)
	}
	eq(t, filing[0], map[string]any{"verb": "patch", "group": zrGroup, "version": "v2", "resource": "zendeskrequests/status", "namespace": "krateo-system", "name": name, "query": "", "status": 200, "items": 0, "identity": "sa"}, "the filing's request line")

	// Once: nothing more for this object.
	h.advance(time.Minute)
	eq(t, rv(h.do("GET", zrPath+"/"+name, req{as: "sa"})), fmt.Sprint(modRVn), "no second filing")
	w.cancel()
}

// The rule fires once per created object that carries the incident's uid, and never for another.
func TestZendesk_FiledOncePerCreatedObjectWithTheIncidentLabel(t *testing.T) {
	h, uid := zrHarness(t)
	a, b, other := "zr-sandbox-visitor-1794600001", "zr-sandbox-visitor-1794600002", "zr-sandbox-visitor-1794600003"
	eq(t, h.do("POST", zrPath, req{as: "visitor", ct: "application/json", body: raiseBody(a, uid)}).code, 201, "create a")
	eq(t, h.do("POST", zrPath, req{as: "visitor", ct: "application/json", body: raiseBody(b, uid)}).code, 201, "create b")
	eq(t, h.do("POST", zrPath, req{as: "visitor", ct: "application/json", body: raiseBody(other, "")}).code, 201, "create a request with no incident label")
	h.advance(zrFileWait + time.Minute)
	writes := map[string]int{}
	for _, l := range h.log() {
		if l["resource"] == "zendeskrequests/status" {
			writes[fmt.Sprint(l["name"])]++
		}
	}
	eq(t, writes, map[string]int{a: 1, b: 1}, "one filing per created object with the label, none for the other")
	eq(t, cardState(h.do("GET", zrPath+"/"+a, req{as: "sa"}).body), "filed", "a")
	eq(t, cardState(h.do("GET", zrPath+"/"+b, req{as: "sa"}).body), "filed", "b")
	eq(t, cardState(h.do("GET", zrPath+"/"+other, req{as: "sa"}).body), "filing", "the request with no incident label")
	if l := h.log(); logged(h, func(l map[string]any) bool { return l["event"] == "applyStatus.failed" }) {
		t.Fatalf("an applyStatus.failed line: %v", l)
	}
}

// (d): label-filtered lists serve the seeded objects and the raised ones; the visitor's reads stay in
// the configuration's namespace; the ServiceAccount lists cluster-wide (057's */*).
func TestZendesk_LabelFilteredListsAndTheVisitorsReads(t *testing.T) {
	h, uid := zrHarness(t)
	name := "zr-sandbox-visitor-1794600004"
	eq(t, h.do("POST", zrPath, req{as: "visitor", ct: "application/json", body: raiseBody(name, uid)}).code, 201, "create")
	names := func(r resp) []string {
		var ns []string
		for _, it := range items(r) {
			ns = append(ns, fmt.Sprint(get(it, "metadata", "name")))
		}
		return ns
	}
	byIncident := h.do("GET", zrPath+"?labelSelector=krateo.io%2Fincident-uid%3D"+uid, req{as: "visitor"})
	eq(t, byIncident.code, 200, "the visitor lists by the incident's uid")
	eq(t, strings.Join(names(byIncident), ","), "zr-demo-user-1794500000,"+name, "the seeded request and the raised one")
	mine := h.do("GET", zrAllPath+"?labelSelector=krateo.io%2Fraised-by%3Dsandbox-visitor", req{as: "sa"})
	eq(t, mine.code, 200, "the ServiceAccount lists cluster-wide by raised-by")
	eq(t, strings.Join(names(mine), ","), name, "only the visitor's")
	none := h.do("GET", zrPath+"?labelSelector=krateo.io%2Fincident-uid%3Dno-such-uid", req{as: "visitor"})
	eq(t, len(items(none)), 0, "a selector nothing matches")
	eq(t, h.do("GET", zrPath+"/zr-demo-user-1794500000", req{as: "visitor"}).code, 200, "the detail page's GET")
	eq(t, len(items(h.do("GET", zrcPath, req{as: "visitor"}))), 1, "the form lists configurations")
	eq(t, h.do("GET", zrAllPath, req{as: "visitor"}).code, 403, "the visitor lists cluster-wide")
	eq(t, h.do("DELETE", zrPath+"/"+name, req{as: "visitor"}).code, 403, "the visitor deletes")
	eq(t, h.do("PATCH", zrPath+"/"+name, req{as: "visitor", ct: merge, body: map[string]any{"spec": map[string]any{"priority": "low"}}}).code, 403, "the visitor patches")
	other := raiseBody("zr-sandbox-visitor-1794600005", uid)
	other["metadata"].(map[string]any)["namespace"] = "default"
	eq(t, h.do("POST", "/apis/zendesk.ogen.krateo.io/v2/namespaces/default/zendeskrequests", req{as: "visitor", ct: "application/json", body: other}).code, 403, "the visitor creates in another namespace")
}

// The CRD's schema holds: a create without a required spec field, or with a kind outside its enum, is
// refused as the apiserver refuses it.
func TestZendesk_CreateIsValidatedAgainstTheCRD(t *testing.T) {
	h, uid := zrHarness(t)
	noSubject := raiseBody("zr-sandbox-visitor-1794600006", uid)
	delete(noSubject["spec"].(map[string]any), "subject")
	eq(t, h.do("POST", zrPath, req{as: "visitor", ct: "application/json", body: noSubject}).code, 422, "no spec.subject")
	badKind := raiseBody("zr-sandbox-visitor-1794600007", uid)
	badKind["spec"].(map[string]any)["kind"] = "Question"
	eq(t, h.do("POST", zrPath, req{as: "visitor", ct: "application/json", body: badKind}).code, 422, "kind outside Bug|FeatureRequest")
	dup := raiseBody("zr-demo-user-1794500000", uid)
	eq(t, h.do("POST", zrPath, req{as: "visitor", ct: "application/json", body: dup}).code, 409, "AlreadyExists: two tabs in the same second")
}
