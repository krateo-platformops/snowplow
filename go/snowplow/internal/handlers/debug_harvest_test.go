// debug_harvest_test.go — 1.12.7 observability: /debug/harvest must show a
// forgotten coordinate disappearing, and must never carry a harvested object.
package handlers

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/krateo-platformops/snowplow/internal/handlers/dispatchers"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

func harvestTestGVR() schema.GroupVersionResource {
	return schema.GroupVersionResource{Group: "widgets.templates.krateo.io", Version: "v1beta1", Resource: "flexes"}
}

// getHarvest drives the real handler and returns the decoded body plus the raw
// bytes (the raw form is what the leak assertion inspects).
func getHarvest(t *testing.T, query string) (map[string]any, string) {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, "/debug/harvest"+query, nil)
	rec := httptest.NewRecorder()
	DebugHarvest().ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("/debug/harvest%s returned %d, want 200", query, rec.Code)
	}
	raw := rec.Body.String()
	var body map[string]any
	if err := json.Unmarshal([]byte(raw), &body); err != nil {
		t.Fatalf("decoding /debug/harvest%s: %v — body %q", query, err, raw)
	}
	return body, raw
}

// TestIssue1127Obs_DebugHarvest_ForgottenCoordinateDisappears — THE acceptance
// arm. The revised acceptance step has to prove the harvester FORGOT a
// coordinate, and this is the surface that answers it. If the endpoint cannot
// show the transition, the acceptance cannot be run.
func TestIssue1127Obs_DebugHarvest_ForgottenCoordinateDisappears(t *testing.T) {
	dispatchers.ResetHarvestInspectionForTest()
	t.Cleanup(dispatchers.ResetHarvestInspectionForTest)

	const ns, name = "krateo-system", "doomed-flex"
	gvr := harvestTestGVR()

	// Held.
	dispatchers.PublishHarvestersForInspectionForTest(
		[]dispatchers.HarvestCoordinateForTest{
			{GVR: gvr, Namespace: ns, Name: name},
			{GVR: gvr, Namespace: ns, Name: "live-flex"},
		}, nil)

	q := "?group=" + gvr.Group + "&version=" + gvr.Version + "&resource=" + gvr.Resource +
		"&namespace=" + ns + "&name=" + name
	body, _ := getHarvest(t, q)
	if body["available"] != true {
		t.Fatalf("premise: available=false with harvesters published: %v", body)
	}
	if body["heldByNavHarvester"] != true {
		t.Fatalf("premise: %s/%s is not reported as held before the forget: %v", ns, name, body)
	}
	if got := body["navEntries"]; got != float64(2) {
		t.Fatalf("premise: navEntries=%v, want 2", got)
	}

	// Forgotten — republish without the doomed coordinate, which is what the
	// gone-verdict forget leaves behind.
	dispatchers.PublishHarvestersForInspectionForTest(
		[]dispatchers.HarvestCoordinateForTest{
			{GVR: gvr, Namespace: ns, Name: "live-flex"},
		}, nil)

	body, _ = getHarvest(t, q)
	if body["heldByNavHarvester"] == true {
		t.Fatalf("RED (1.12.7 obs): %s/%s is STILL reported as held after being forgotten. The "+
			"acceptance step cannot prove the harvester dropped a confirmed-gone coordinate, which "+
			"is the whole reason this surface exists: %v", ns, name, body)
	}
	if got := body["navEntries"]; got != float64(1) {
		t.Fatalf("RED (1.12.7 obs): navEntries=%v after one forget, want 1", got)
	}
	// The surviving widget must be untouched — a surface that reports
	// everything gone would pass the assertion above for the wrong reason.
	liveQ := "?group=" + gvr.Group + "&version=" + gvr.Version + "&resource=" + gvr.Resource +
		"&namespace=" + ns + "&name=live-flex"
	if live, _ := getHarvest(t, liveQ); live["heldByNavHarvester"] != true {
		t.Fatalf("RED (1.12.7 obs): the LIVE widget is no longer reported as held: %v", live)
	}
}

// TestIssue1127Obs_DebugHarvest_UnavailableIsNotEmpty — "no harvester" and "an
// empty harvester" are different answers. Conflating them would let a
// misconfigured pod (prewarm off) read as a successful forget.
func TestIssue1127Obs_DebugHarvest_UnavailableIsNotEmpty(t *testing.T) {
	dispatchers.ResetHarvestInspectionForTest()
	t.Cleanup(dispatchers.ResetHarvestInspectionForTest)

	body, _ := getHarvest(t, "")
	if body["available"] != false {
		t.Fatalf("RED (1.12.7 obs): available=%v with NO harvester published. An operator cannot "+
			"tell a pod with prewarm off from a pod that legitimately holds nothing, so a "+
			"misconfiguration reads as a successful forget", body["available"])
	}
}

// TestIssue1127Obs_DebugHarvest_NeverCarriesHarvestedContent — the standing
// rule: the debug surface returns metadata, never per-identity content. The
// harvested value is a whole widget CR, so a regression here is a content leak.
func TestIssue1127Obs_DebugHarvest_NeverCarriesHarvestedContent(t *testing.T) {
	dispatchers.ResetHarvestInspectionForTest()
	t.Cleanup(dispatchers.ResetHarvestInspectionForTest)

	gvr := harvestTestGVR()
	dispatchers.PublishHarvestersForInspectionForTest(
		[]dispatchers.HarvestCoordinateForTest{{GVR: gvr, Namespace: "krateo-system", Name: "secretive-flex"}},
		[]dispatchers.HarvestCoordinateForTest{{GVR: gvr, Namespace: "krateo-system", Name: "secretive-ra"}})

	_, raw := getHarvest(t, "")

	// Nothing from a captured object may appear: no CR envelope, no spec, no
	// status, no resolved rows.
	// NOTE: "apiRefs" is a legitimate COUNT field on this response, so the
	// forbidden list tests for captured-object CONTENT — CR envelope keys, the
	// resolved payload, and the harvested objects' own names — rather than for
	// any substring that happens to occur inside a field name.
	for _, forbidden := range []string{
		"apiVersion", `"kind"`, "metadata", `"spec"`, "status", "widgetData",
		`"items"`, "resourcesRefs", "secretive-flex", "secretive-ra",
	} {
		if strings.Contains(raw, forbidden) {
			t.Fatalf("RED (1.12.7 obs): the count response carries %q. This surface must return "+
				"counts and booleans only — the harvested value is a whole widget CR and what the "+
				"seed makes of it is per-identity resolved output, so any of it leaving the pod is "+
				"the cross-identity read the debug surface is forbidden from doing. Body: %s",
				forbidden, raw)
		}
	}
}
