package viewercontext

import (
	"reflect"
	"sort"
	"strings"
	"testing"
)

func TestVocabularyIsExactlyTheFourViewerContextKeys(t *testing.T) {
	got := Keys()
	sort.Strings(got)
	want := []string{"displayName", "groups", "tz", "username"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("viewer-context vocabulary = %v, want %v", got, want)
	}
	if !ClientAsserted("tz") {
		t.Fatal("tz is client-asserted (not in the JWT): ClientAsserted(tz) must be true")
	}
	for _, k := range []string{"username", "groups", "displayName", "foo"} {
		if ClientAsserted(k) {
			t.Fatalf("%q must not be flagged client-asserted", k)
		}
	}
}

func TestTZValidator_AcceptsRealIANANames(t *testing.T) {
	for _, z := range []string{
		"Europe/Rome", "UTC", "America/Argentina/Buenos_Aires", "Etc/GMT+5",
		"Etc/GMT-14", "America/Port-au-Prince", "Asia/Kolkata", "GMT",
	} {
		if !Valid(TZ, z) {
			t.Errorf("a real IANA zone %q was rejected: a viewer in it would lose their local midnight", z)
		}
	}
}

func TestTZValidator_RejectsEverythingElse(t *testing.T) {
	cases := map[string]any{
		"sql injection":        "UTC'); DROP TABLE otel_logs; --",
		"single quote":         "Europe/Rome'",
		"double quote":         `Europe/"Rome`,
		"newline":              "Europe/Rome\n",
		"embedded newline":     "Europe/\nRome",
		"space":                "Europe/ Rome",
		"overlong":             strings.Repeat("A", MaxTZLen+1),
		"overlong valid chars": "America/" + strings.Repeat("a", MaxTZLen),
		"Local":                "Local",
		"empty":                "",
		"dot-dot path":         "../etc/passwd",
		// time.LoadLocation resolves these to a real zone (it joins the name
		// onto the zoneinfo directory), so ONLY the allow-list and the length
		// bound keep them out: they pin those two checks.
		"dot segment":        "Europe/./Rome",
		"dot prefix":         "./UTC",
		"overlong real zone": "Europe/" + strings.Repeat("/", MaxTZLen+6) + "Rome",
		"backslash":          `Europe\Rome`,
		"semicolon":          "UTC;",
		"unknown zone":       "Mars/Olympus_Mons",
		"nul":                "UTC\x00",
		"unicode":            "Europe/Rómé",
		"non-string number":  float64(1),
		"non-string nil":     nil,
		"non-string list":    []any{"UTC"},
	}
	for name, v := range cases {
		if Valid(TZ, v) {
			t.Errorf("%s: %#v was accepted as a tz; it would reach resolution and the key", name, v)
		}
	}
}

// The boundary itself: exactly MaxTZLen bytes of a resolvable name is not an
// "overlong" rejection, one more is. Pins the length check against an off-by-one
// in either direction, which the cases above (far from the boundary) cannot.
func TestTZValidator_LengthBoundIsExact(t *testing.T) {
	if validTZ(strings.Repeat("a", MaxTZLen)) {
		t.Fatal("a MaxTZLen-byte unknown zone must still fail LoadLocation (control for the boundary test)")
	}
	long := "America/Argentina/" + strings.Repeat("x", MaxTZLen-len("America/Argentina/")+1)
	if len(long) != MaxTZLen+1 || Valid(TZ, long) {
		t.Fatalf("a %d-byte name must be rejected by the length bound", len(long))
	}
}

func TestOtherKeysAcceptAnyValue(t *testing.T) {
	for _, k := range []string{"username", "groups", "displayName", "foo"} {
		for _, v := range []any{"x'", "", float64(3), []any{"g"}, nil} {
			if !Valid(k, v) {
				t.Errorf("%q has no validator; %#v must be accepted (identity values are stripped or partition the key, never filtered here)", k, v)
			}
		}
	}
}

func TestDropInvalid_DropsOnlyInvalidAndCounts(t *testing.T) {
	before := DroppedTotal()
	in := map[string]any{"tz": "UTC'); DROP", "name": "n", "username": "u"}
	snapshot := map[string]any{"tz": "UTC'); DROP", "name": "n", "username": "u"}
	out := DropInvalid(in)
	if _, ok := out["tz"]; ok {
		t.Fatal("an invalid tz survived DropInvalid")
	}
	if out["name"] != "n" || out["username"] != "u" {
		t.Fatalf("DropInvalid removed more than the invalid value: %#v", out)
	}
	if !reflect.DeepEqual(in, snapshot) {
		t.Fatalf("DropInvalid mutated its input: %#v", in)
	}
	if d := DroppedTotal() - before; d != 1 {
		t.Fatalf("dropped counter moved by %d, want 1", d)
	}
}

func TestDropInvalid_NothingToDropReturnsSameMapAndDoesNotCount(t *testing.T) {
	before := DroppedTotal()
	in := map[string]any{"tz": "Europe/Rome", "name": "n"}
	out := DropInvalid(in)
	if reflect.ValueOf(out).Pointer() != reflect.ValueOf(in).Pointer() {
		t.Fatal("a clean request must get its own map back (no copy on the hot path)")
	}
	if DroppedTotal() != before {
		t.Fatal("the dropped counter moved on a clean request")
	}
	if DropInvalid(nil) != nil {
		t.Fatal("nil in, nil out")
	}
}
