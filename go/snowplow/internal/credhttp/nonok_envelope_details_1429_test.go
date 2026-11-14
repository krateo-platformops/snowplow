package credhttp

// nonok_envelope_details_1429_test.go (#1429): nonOKEnvelope reads an UPSTREAM's error body, and
// what it returns never includes that body's "details".
//
// plumbing's response.Status gains a Details field (plumbing#173). nonOKEnvelope decodes an
// upstream body into a Status, so without care the field would change its meaning with no line
// here changing: an upstream's details would pass through snowplow's answer (an apiserver's carry
// the object's name and uid; an upstream could present a TypeNotInstalled cause snowplow did not
// produce, and the sandbox capture acts on that cause), and a body whose "details" is not an
// object would stop decoding as a Status and drop to the raw-text fallback.
//
// Every row gives the Status it gave on plumbing v1.74.0, before the field existed: these
// expectations were recorded there, with this test green and no shadow field in nonOKEnvelope.

import (
	"encoding/json"
	"strings"
	"testing"
)

// upstreamDetailsVariants are a Status body's "details" member in every JSON shape, keyed by
// name. "" is the member left out.
var upstreamDetailsVariants = map[string]string{
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
}

func TestNonOKEnvelope_AnUpstreamsDetailsNeverReachTheEnvelope(t *testing.T) {
	const want = `{"kind":"Status","apiVersion":"v1","status":"Failure","message":"things \"x\" not found","reason":"NotFound","code":404}`
	for name, details := range upstreamDetailsVariants {
		t.Run(name, func(t *testing.T) {
			body := `{"kind":"Status","apiVersion":"v1","metadata":{},"status":"Failure","message":"things \"x\" not found","reason":"NotFound"` + details + `,"code":404}`
			got, err := json.Marshal(nonOKEnvelope(404, []byte(body)))
			if err != nil {
				t.Fatal(err)
			}
			if string(got) != want {
				t.Fatalf("nonOKEnvelope returned\n got: %s\nwant: %s", got, want)
			}
			for _, leak := range []string{"details", "TypeNotInstalled", "0000-1111", "see the log"} {
				if strings.Contains(string(got), leak) {
					t.Errorf("the envelope carries the upstream's %q: %s", leak, got)
				}
			}
		})
	}
}
