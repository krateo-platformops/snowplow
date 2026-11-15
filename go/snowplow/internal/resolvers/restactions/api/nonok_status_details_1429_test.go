package api

// nonok_status_details_1429_test.go (#1429): nonOKStatus reads an UPSTREAM's error body (a
// RESTAction stage's endpoint), and what it returns never includes that body's "details".
//
// plumbing's response.Status gains a Details field (plumbing#173). nonOKStatus decodes an
// upstream body into a Status, so without care the field would change its meaning with no line
// here changing: an upstream's details would pass through into the stage's failure envelope (an
// external endpoint's can carry anything, including a TypeNotInstalled cause snowplow did not
// produce, which the sandbox capture acts on), and a body whose "details" is not an object would
// stop decoding as a Status and drop to the raw-text fallback.
//
// Every row gives the Status it gave on plumbing v1.74.0, before the field existed: these
// expectations were recorded there, with this test green and no shadow field in nonOKStatus.

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestNonOKStatus_AnUpstreamsDetailsNeverReachTheStageFailure(t *testing.T) {
	const want = `{"kind":"Status","apiVersion":"v1","status":"Failure","message":"things \"x\" not found","reason":"NotFound","code":404}`
	for name, details := range map[string]string{
		"absent":                   ``,
		"apiserver_object":         `,"details":{"name":"x","group":"g.example.org","kind":"things","uid":"0000-1111"}`,
		"type_not_installed_cause": `,"details":{"group":"g.example.org","kind":"things","causes":[{"reason":"TypeNotInstalled","message":"g.example.org/v1","field":"apiVersion"}]}`,
		"causes_and_retry":         `,"details":{"causes":[{"reason":"FieldValueInvalid","message":"bad","field":"spec.x"}],"retryAfterSeconds":3}`,
		"empty_object":             `,"details":{}`,
		"string":                   `,"details":"see the log"`,
		"number":                   `,"details":7`,
		"array":                    `,"details":[1,2]`,
		"bool":                     `,"details":true`,
		"null":                     `,"details":null`,
	} {
		t.Run(name, func(t *testing.T) {
			body := `{"kind":"Status","apiVersion":"v1","metadata":{},"status":"Failure","message":"things \"x\" not found","reason":"NotFound"` + details + `,"code":404}`
			got, err := json.Marshal(nonOKStatus(404, []byte(body)))
			if err != nil {
				t.Fatal(err)
			}
			if string(got) != want {
				t.Fatalf("nonOKStatus returned\n got: %s\nwant: %s", got, want)
			}
			for _, leak := range []string{"details", "TypeNotInstalled", "0000-1111", "see the log"} {
				if strings.Contains(string(got), leak) {
					t.Errorf("the stage failure carries the upstream's %q: %s", leak, got)
				}
			}
		})
	}
}

// An upstream that answers 424 with snowplow's own cause in its body: the stage failure is a
// plain 424 with the upstream's message, and no details. (Its claimed reason does not survive
// either: #714 keeps a reason only when it is the one the code implies.)
func TestNonOKStatus_AnUpstream424CannotPresentTheTypeNotInstalledCause(t *testing.T) {
	body := `{"kind":"Status","apiVersion":"v1","status":"Failure","message":"an upstream's own 424","reason":"FailedDependency",` +
		`"details":{"group":"g.example.org","kind":"things","causes":[{"reason":"TypeNotInstalled","message":"g.example.org/v1","field":"apiVersion"}]},"code":424}`
	st := nonOKStatus(424, []byte(body))
	got, err := json.Marshal(st)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("stage failure: %s", got)
	if st.Details != nil || strings.Contains(string(got), "details") || strings.Contains(string(got), "TypeNotInstalled") {
		t.Fatalf("an upstream's cause reached the stage failure: %s", got)
	}
	if st.Code != 424 || st.Message != "an upstream's own 424" {
		t.Fatalf("want a plain 424 with the upstream's message, got %s", got)
	}
}
