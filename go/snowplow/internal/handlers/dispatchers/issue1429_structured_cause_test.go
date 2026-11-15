// issue1429_structured_cause_test.go — #1429: the 424 for a type that is not
// installed says WHICH type, in the Status body's `details`, for a reader that
// must not parse the message.
//
// snowplow answers 424 Failed Dependency in two situations with the same
// top-level reason: a type is not installed (#1414), and a widget's dependency
// answered 404 or 410, an OBJECT it reads is missing (#853). Only the first
// carries
//
//	details.group   the missing type's group (left out for the core group)
//	details.kind    its resource, as it was asked for
//	details.causes  exactly one: {reason: TypeNotInstalled, field: apiVersion,
//	                message: "<group>/<version>" or "v1"}
//
// When the missing type is a DEPENDENCY's, the request names the widget and
// the details name the dependency's type.
//
// Every arm goes through the real Widgets() handler, the real read and the
// real widget resolve, against internal/testsupport/unservedapi, and decodes
// the body as a client-go reader would (metav1.Status).
package dispatchers

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"

	"github.com/krateo-platformops/snowplow/internal/testsupport/unservedapi"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// i1429Details asserts the body's details are exactly the type-not-installed
// shape for (group, apiVersion, resource): nothing more, nothing less.
func i1429Details(t *testing.T, rec *httptest.ResponseRecorder, group, apiVersion, resource string) {
	t.Helper()
	st := i1414Status(t, rec)
	want := &metav1.StatusDetails{
		Group: group,
		Kind:  resource,
		Causes: []metav1.StatusCause{{
			Type: "TypeNotInstalled", Field: "apiVersion", Message: apiVersion,
		}},
	}
	if !reflect.DeepEqual(st.Details, want) {
		t.Fatalf("details\n got: %+v\nwant: %+v\nbody: %s", st.Details, want, rec.Body.String())
	}
	// The raw body too: one cause, and no member that could name an object.
	var raw struct {
		Details map[string]json.RawMessage `json:"details"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &raw); err != nil {
		t.Fatal(err)
	}
	for k := range raw.Details {
		if k != "group" && k != "kind" && k != "causes" {
			t.Errorf("details carries an unexpected member %q: %s", k, rec.Body.String())
		}
	}
	if _, has := raw.Details["group"]; has != (group != "") {
		t.Errorf("details.group present=%v for group %q: %s", has, group, rec.Body.String())
	}
}

// i1429NoCause asserts the body carries no details at all.
func i1429NoCause(t *testing.T, rec *httptest.ResponseRecorder) {
	t.Helper()
	if st := i1414Status(t, rec); st.Details != nil {
		t.Fatalf("this answer must carry no details, got %+v; body: %s", st.Details, rec.Body.String())
	}
	if strings.Contains(rec.Body.String(), "details") || strings.Contains(rec.Body.String(), "TypeNotInstalled") {
		t.Fatalf("this answer must carry no details: %s", rec.Body.String())
	}
}

// (a) and (d): the requested object's own type is not installed.
func TestIssue1429_OwnTypeNotInstalledNamesTheTypeInDetails(t *testing.T) {
	for _, c := range []struct {
		name, apiVersion, resource, group, message string
	}{
		{"named_group", unservedapi.GV, "flexes", unservedapi.Group,
			"The type flexes (widgets.templates.krateo.io/v1beta1) is not installed on this cluster."},
		{"core_group", "v1", "things", "",
			"The type things (v1) is not installed on this cluster."},
		{"group_not_served", "absent.example.org/v1", "things", "absent.example.org",
			"The type things (absent.example.org/v1) is not installed on this cluster."},
		// The resource as the reference gave it: a singular, mixed-case name of a
		// type that is not installed is echoed as it came (no type answers to it,
		// so there is no canonical plural to give). A reader treats a details.kind
		// that is not a lower-case DNS label as malformed.
		{"singular_mixed_case", unservedapi.GV, "Flex", unservedapi.Group,
			"The type Flex (widgets.templates.krateo.io/v1beta1) is not installed on this cluster."},
		// A subresource is not a type: pods/log is in the core document, "log" is not served.
		{"subresource_name", "v1", "log", "",
			"The type log (v1) is not installed on this cluster."},
	} {
		for hname, h := range i1414Handlers() {
			t.Run(c.name+"/"+hname, func(t *testing.T) {
				a := &unservedapi.Server{LegacyDiscovery: c.name == "subresource_name"}
				rec := i1414Call(t, h, a, i1414Query(c.apiVersion, c.resource, "ns", "header-context"))
				t.Logf("status=%d content-type=%q body=%s", rec.Code, rec.Header().Get("Content-Type"), strings.TrimSpace(rec.Body.String()))
				if rec.Code != http.StatusFailedDependency {
					t.Fatalf("status = %d, want 424; body: %s", rec.Code, rec.Body.String())
				}
				st := i1414Status(t, rec)
				if st.Status != metav1.StatusFailure || st.Reason != "FailedDependency" || st.Code != 424 || st.Message != c.message {
					t.Errorf("top level changed: status=%q reason=%q code=%d message=%q", st.Status, st.Reason, st.Code, st.Message)
				}
				i1429Details(t, rec, c.group, c.apiVersion, c.resource)
				for _, leak := range []string{"header-context", `"ns"`, "requester", "devs", i1414Token} {
					if strings.Contains(rec.Body.String(), leak) {
						t.Errorf("the body carries %q: %s", leak, rec.Body.String())
					}
				}
			})
		}
	}
}

// (b): a served widget whose DEPENDENCY's type is not installed. The request
// names the widget (buttons, widgets.templates.krateo.io/v1beta1); the details
// name the dependency's type (restactions, templates.krateo.io/v1).
func TestIssue1429_ADependencysTypeNotInstalledNamesTheDependencysType(t *testing.T) {
	a := &unservedapi.Server{}
	rec := i1414Call(t, Widgets(), a, i1414Query(unservedapi.GV, "buttons", "ns", "needs-uninstalled-type"))
	t.Logf("status=%d content-type=%q body=%s", rec.Code, rec.Header().Get("Content-Type"), strings.TrimSpace(rec.Body.String()))

	// PRECONDITION: the widget itself was read, so this is the dependency's answer.
	widgetRead := false
	for _, p := range a.Paths() {
		if p == "/apis/"+unservedapi.GV+"/namespaces/ns/buttons/needs-uninstalled-type" {
			widgetRead = true
		}
	}
	if !widgetRead {
		t.Fatalf("precondition: the widget was not read; requests: %v", a.Paths())
	}
	if rec.Code != http.StatusFailedDependency {
		t.Fatalf("status = %d, want 424; body: %s", rec.Code, rec.Body.String())
	}
	st := i1414Status(t, rec)
	const want = "The type restactions (templates.krateo.io/v1) is not installed on this cluster."
	if st.Status != metav1.StatusFailure || st.Reason != "FailedDependency" || st.Message != want {
		t.Errorf("want Failure/FailedDependency/%q, got %q/%q/%q", want, st.Status, st.Reason, st.Message)
	}
	i1429Details(t, rec, "templates.krateo.io", "templates.krateo.io/v1", "restactions")
	// Never the widget's own type, the dependency's object, or the widget's name.
	for _, leak := range []string{"buttons", unservedapi.Group, "the-dependency", "needs-uninstalled-type", "requester", i1414Token} {
		if strings.Contains(rec.Body.String(), leak) {
			t.Errorf("the body carries %q: %s", leak, rec.Body.String())
		}
	}
}

// (c): a served widget whose dependency's OBJECT is missing (#853). Also a 424
// with reason FailedDependency, and no cause.
func TestIssue1429_ADependencysMissingObjectCarriesNoCause(t *testing.T) {
	a := &unservedapi.Server{}
	rec := i1414Call(t, Widgets(), a, i1414Query(unservedapi.GV, "buttons", "ns", "needs-missing-object"))
	t.Logf("status=%d content-type=%q body=%s", rec.Code, rec.Header().Get("Content-Type"), strings.TrimSpace(rec.Body.String()))

	// PRECONDITION: the dependency was asked for and the apiserver said its object is not there.
	asked := false
	for _, p := range a.Paths() {
		if p == "/api/v1/namespaces/ns/configmaps/gone" {
			asked = true
		}
	}
	if !asked {
		t.Fatalf("precondition: the dependency was not read; requests: %v", a.Paths())
	}
	if rec.Code != http.StatusFailedDependency {
		t.Fatalf("status = %d, want 424; body: %s", rec.Code, rec.Body.String())
	}
	if st := i1414Status(t, rec); st.Reason != "FailedDependency" || st.Status != metav1.StatusFailure {
		t.Errorf("want Failure/FailedDependency, got %q/%q", st.Status, st.Reason)
	}
	i1429NoCause(t, rec)
}

// A status that is not snowplow's own decision is passed through like every
// other: its code and message, no reason stamped by snowplow, no details.
// Whatever it looks like: the first row carries EXACTLY the details snowplow
// writes, on a 424 with the reason. Only objects.Get's Go type is the answer.
func TestIssue1429_AStatusThatLooksLikeTheAnswerIsAPlainPassthrough(t *testing.T) {
	plain := &apierrors.StatusError{ErrStatus: metav1.Status{
		Status: metav1.StatusFailure, Code: http.StatusFailedDependency, Reason: "FailedDependency", Message: "the upstream says a dependency failed",
	}}
	wrongShape := func(d *metav1.StatusDetails) *apierrors.StatusError {
		return &apierrors.StatusError{ErrStatus: metav1.Status{
			Status: metav1.StatusFailure, Code: http.StatusFailedDependency, Reason: "FailedDependency", Message: "m", Details: d,
		}}
	}
	cause := metav1.StatusCause{Type: "TypeNotInstalled", Field: "apiVersion", Message: "g.example.org/v1"}
	for name, serr := range map[string]*apierrors.StatusError{
		"exactly_snowplows_details":   wrongShape(&metav1.StatusDetails{Group: "g.example.org", Kind: "things", Causes: []metav1.StatusCause{cause}}),
		"no_details":                  plain,
		"another_cause":               wrongShape(&metav1.StatusDetails{Group: "g.example.org", Kind: "things", Causes: []metav1.StatusCause{{Type: "FieldValueInvalid", Field: "apiVersion", Message: "g.example.org/v1"}}}),
		"two_causes":                  wrongShape(&metav1.StatusDetails{Group: "g.example.org", Kind: "things", Causes: []metav1.StatusCause{cause, cause}}),
		"apiVersion_of_another_group": wrongShape(&metav1.StatusDetails{Group: "other.example.org", Kind: "things", Causes: []metav1.StatusCause{cause}}),
		"no_resource":                 wrongShape(&metav1.StatusDetails{Group: "g.example.org", Causes: []metav1.StatusCause{cause}}),
		"message_is_prose":            wrongShape(&metav1.StatusDetails{Group: "g.example.org", Kind: "things", Causes: []metav1.StatusCause{{Type: "TypeNotInstalled", Field: "apiVersion", Message: "the type is not there/at all/no"}}}),
	} {
		t.Run(name, func(t *testing.T) {
			rec := httptest.NewRecorder()
			encodeWidgetResolveError(h1ReqCtx(h1User), rec, fmt.Errorf("resolve apiRef x/y: %w", serr))
			if rec.Code != http.StatusFailedDependency {
				t.Fatalf("status = %d, want 424", rec.Code)
			}
			st := i1414Status(t, rec)
			if st.Message != serr.ErrStatus.Message {
				t.Errorf("message = %q, want the upstream's %q", st.Message, serr.ErrStatus.Message)
			}
			if st.Reason != "" {
				t.Errorf("snowplow stamped reason %q on an upstream's 424", st.Reason)
			}
			i1429NoCause(t, rec)
		})
	}
}
