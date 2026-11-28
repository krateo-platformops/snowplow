package viewercontext

import (
	"archive/zip"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"sort"
	"strings"
	"testing"
	"time"
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

// Canonical names and the aliases tzdata itself names are accepted exactly as
// spelled; aliases are not folded together.
func TestTZValidator_AcceptsExactTzdataSpellings(t *testing.T) {
	for _, z := range []string{
		"Europe/Rome", "America/Argentina/Buenos_Aires", "America/Port-au-Prince",
		"Asia/Kolkata", "Etc/GMT+5", "Etc/GMT-14",
		"UTC", "Etc/UTC", "Zulu", "GMT", "Etc/GMT", "Universal",
	} {
		if !Valid(TZ, z) {
			t.Errorf("a real tzdata zone name %q was rejected: a viewer in it would lose their local midnight", z)
		}
	}
}

func TestTZValidator_RejectsEverythingElse(t *testing.T) {
	cases := map[string]any{
		"sql injection":     "UTC'); DROP TABLE otel_logs; --",
		"single quote":      "Europe/Rome'",
		"double quote":      `Europe/"Rome`,
		"newline":           "Europe/Rome\n",
		"embedded newline":  "Europe/\nRome",
		"space":             "Europe/ Rome",
		"overlong":          strings.Repeat("A", MaxTZLen+1),
		"Local":             "Local",
		"empty":             "",
		"dot-dot path":      "../etc/passwd",
		"backslash":         `Europe\Rome`,
		"semicolon":         "UTC;",
		"unknown zone":      "Mars/Olympus_Mons",
		"nul":               "UTC\x00",
		"unicode":           "Europe/Rómé",
		"non-string number": float64(1),
		"non-string nil":    nil,
		"non-string list":   []any{"UTC"},
		// One zone, many spellings: time.LoadLocation accepts these (it joins
		// the name onto a zoneinfo directory), each would be its own cache
		// cell, and on a case-insensitive filesystem the case variants load too.
		"double slash":           "Europe//Rome",
		"triple slash":           "America//Argentina///Buenos_Aires",
		"dot segment":            "Europe/./Rome",
		"dot prefix":             "./UTC",
		"lower case":             "europe/rome",
		"upper case":             "EUROPE/ROME",
		"mixed case alias":       "utc",
		"overlong real zone":     "Europe/" + strings.Repeat("/", MaxTZLen+6) + "Rome",
		"trailing slash":         "Europe/Rome/",
		"leading slash":          "/Europe/Rome",
		"non-zone entry Factory": "Factory",
		"non-zone posixrules":    "posixrules",
		"non-zone localtime":     "localtime",
		"directory, not a zone":  "Europe",
		"directory with slash":   "America/Argentina",
	}
	for name, v := range cases {
		if Valid(TZ, v) {
			t.Errorf("%s: %#v was accepted as a tz; it would reach resolution and the key", name, v)
		}
	}
}

// shapeOK is the cheap first filter. The membership test behind it refuses
// every non-zone string anyway, so a mistake in the filter cannot show in
// Valid(): it is tested on its own. Boundary, space and non-ASCII are the cases
// a regression would turn into a different, wider filter.
func TestShapeOK(t *testing.T) {
	ok := func(s string, want bool) {
		t.Helper()
		if got := shapeOK(s); got != want {
			t.Errorf("shapeOK(%q) = %v, want %v", s, got, want)
		}
	}
	ok(strings.Repeat("a", MaxTZLen-1), true)
	ok(strings.Repeat("a", MaxTZLen), true)
	ok(strings.Repeat("a", MaxTZLen+1), false)
	ok("", false)
	ok("Europe/Rome", true)
	ok("Etc/GMT+5", true)
	ok("a-b_c/D9", true)
	ok("Europe/ Rome", false)
	ok(" ", false)
	ok("Europe/Rome ", false)
	ok("Europe/Rómé", false)
	ok("é", false)
	ok("Europe/Rome\xff", false)
	ok("Europe/Rome\n", false)
	ok("Europe/Rome'", false)
	ok("Europe/Rome.", false)
	ok("Europe/Rome\x00", false)
	// each character class at both ends of its range
	for _, c := range []string{"a", "z", "A", "Z", "0", "9", "/", "_", "+", "-"} {
		ok(c, true)
	}
	// the characters just outside each range, and the control / high bytes
	for _, c := range []string{"`", "{", "@", "[", "/\x00", ":", ".", "\x7f", "\x80", "\xc3\xa9"} {
		ok(c, false)
	}
}

// The filter must never exclude a real zone, and no real zone is longer than
// the bound: otherwise a legitimate viewer would be dropped.
func TestEveryZoneNamePassesTheShapeFilter(t *testing.T) {
	zs := zoneNames()
	if len(zs) < 400 {
		t.Fatalf("zone list has %d names; expected the full IANA set", len(zs))
	}
	longest := 0
	for z := range zs {
		if !shapeOK(z) {
			t.Errorf("real zone %q fails the shape filter", z)
		}
		if len(z) > longest {
			longest = len(z)
		}
	}
	if longest > MaxTZLen {
		t.Fatalf("longest zone name is %d bytes, above MaxTZLen %d", longest, MaxTZLen)
	}
}

// Every listed name loads: the list describes the database the binary embeds
// (and the OS database, where one exists), so accepting a name never leads to
// a zone Go cannot resolve.
func TestEveryListedZoneLoads(t *testing.T) {
	for z := range zoneNames() {
		if _, err := time.LoadLocation(z); err != nil {
			t.Errorf("listed zone %q does not load: %v", z, err)
		}
	}
	for _, bad := range []string{"Factory", "posixrules", "localtime", ""} {
		if _, in := zoneNames()[bad]; in {
			t.Errorf("non-zone entry %q is in the list", bad)
		}
	}
}

// The list is generated from the Go toolchain's lib/time/zoneinfo.zip, the very
// archive time/tzdata embeds. If a toolchain bump brings a zone the list lacks,
// that zone would be refused: fail here so the list is regenerated.
//
//	python3 -I -c "import zipfile,subprocess,sys;g=subprocess.check_output(['go','env','GOROOT'],text=True).strip();z=zipfile.ZipFile(g+'/lib/time/zoneinfo.zip');open('zones.txt','w').write('\n'.join(sorted(i.filename for i in z.infolist() if i.filename!='Factory'))+'\n')"
func TestZonesListMatchesToolchainZip(t *testing.T) {
	zr, err := zip.OpenReader(filepath.Join(runtime.GOROOT(), "lib", "time", "zoneinfo.zip"))
	if err != nil {
		if os.IsNotExist(err) {
			t.Skipf("toolchain zoneinfo.zip not present (%v)", err)
		}
		t.Fatal(err)
	}
	defer zr.Close()
	for _, f := range zr.File {
		if f.Name == "Factory" || strings.HasSuffix(f.Name, "/") {
			continue
		}
		if _, ok := zoneNames()[f.Name]; !ok {
			t.Errorf("toolchain tzdata has zone %q that zones.txt lacks; regenerate zones.txt (command above)", f.Name)
		}
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
