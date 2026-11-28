package widgets

import (
	"reflect"
	"testing"
)

// tz is a viewer-context key: stripped from the resolve input unless the widget
// declares it in keyExtras; unrelated keys are untouched.
func TestViewerContext_TZ_StripUnlessDeclaredInKeyExtras(t *testing.T) {
	in := func() map[string]any { return map[string]any{"tz": "Europe/Rome", "foo": "bar"} }

	got := sanitizeUndeclaredViewerContextExtras(a3CR(nil, nil), in())
	if !reflect.DeepEqual(got, map[string]any{"foo": "bar"}) {
		t.Fatalf("undeclared tz must be stripped (body cannot depend on it) and foo left alone; got %#v", got)
	}

	got = sanitizeUndeclaredViewerContextExtras(a3CR(nil, []string{"tz"}), in())
	if !reflect.DeepEqual(got, in()) {
		t.Fatalf("a tz declared in keyExtras must reach the resolve input; got %#v", got)
	}

	got = sanitizeUndeclaredViewerContextExtras(a3CR(nil, []string{"range"}), in())
	if _, ok := got["tz"]; ok {
		t.Fatalf("declaring some OTHER key must not keep tz; got %#v", got)
	}
}

// identityContext is server-injected from the JWT; the server does not know the
// viewer's time zone, so tz must never be an enum value (and so is never
// "authoritative" through that axis).
func TestViewerContext_TZ_IsNotAnIdentityContextValue(t *testing.T) {
	cr := a3CR([]string{"tz", "username"}, nil)
	if got := GetIdentityContext(cr); !reflect.DeepEqual(got, []string{"username"}) {
		t.Fatalf("GetIdentityContext must drop tz (not injectable); got %v", got)
	}
	got := sanitizeUndeclaredViewerContextExtras(cr, map[string]any{"tz": "UTC"})
	if len(got) != 0 {
		t.Fatalf("an identityContext entry named tz must not keep a client-supplied tz; got %#v", got)
	}
}
