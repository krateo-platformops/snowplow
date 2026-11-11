package audit

// provenance_1333_test.go — #1333: the X-Krateo-Provenance header decoder.
//
//   A  a header that keeps every rule is read field by field, exactly.
//   R  every rejection reason, each by every rule it covers: the result is
//      the reason alone (a rejected header asserts nothing).
//   B  the 2 KiB bound: a value of exactly MaxProvenanceBytes is read, one
//      byte more is oversize, decided before anything is decoded.
//   N  no header asserts nothing; several field lines are one (rejected) value.
//   G  the golden vector of the #1333 comment: the canonical confirmed set's
//      bytes and sha256, which the portal and snowplow both prove.
//   F  FuzzDecodeProvenance (the header value) and FuzzProvenanceJSON (the
//      JSON in it): for ANY value, the result is a closed-set rejection
//      alone, or an assertion that re-encodes to itself and that the shared
//      library's Check accepts on a write record.

import (
	"bytes"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"net/http"
	"slices"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	krateoaudit "github.com/krateo-platformops/audit"
	"github.com/krateo-platformops/audit/audittest"
	"go.opentelemetry.io/otel/attribute"
)

// encode1333 is the header value of a JSON text: "v1." + base64url, no padding.
func encode1333(js string) string {
	return provenancePrefix + base64.RawURLEncoding.EncodeToString([]byte(js))
}

// bs1333 is a backslash, so a JSON escape in a raw string reads as one.
const bs1333 = `\`

const (
	sha1333  = "0a08bb91090b111f15a045b6061bccc4d0b8488d85f99ba57d8632e17061c277"
	uuid1333 = "6f1d2c3b-4a5e-4f60-8b7a-9c0d1e2f3a4b"
	// full1333 sets every field.
	full1333 = `{"source":"portal","agentSessionId":"ses_01-AB","confirmationId":"` + uuid1333 + `",` +
		`"confirmedSet":{"count":3,"irreversible":true,"sha256":"` + sha1333 + `"},"op":2,` +
		`"requestedAt":"2026-11-11T10:20:30.456Z"}`
)

func TestProvenance1333_A_ValidHeaderIsReadExactly(t *testing.T) {
	cases := []struct {
		name string
		js   string
		want ClientProvenance
	}{
		{"every field", full1333, ClientProvenance{
			Source: "portal", AgentSessionID: "ses_01-AB", ConfirmationID: uuid1333,
			ConfirmedSet: &krateoaudit.ConfirmedSet{Count: 3, Irreversible: true, SHA256: sha1333, Op: 2},
			RequestedAt:  time.Date(2026, 11, 11, 10, 20, 30, 456e6, time.UTC),
		}},
		{"source only", `{"source":"portal"}`, ClientProvenance{Source: "portal"}},
		{"a human write: no agent session", `{"source":"portal","confirmationId":"` + uuid1333 +
			`","confirmedSet":{"count":1,"irreversible":false,"sha256":"` + sha1333 + `"},"op":0}`,
			ClientProvenance{Source: "portal", ConfirmationID: uuid1333,
				ConfirmedSet: &krateoaudit.ConfirmedSet{Count: 1, SHA256: sha1333}}},
		{"whitespace and key order are JSON's", " {\n \"op\" : 0 , \"confirmedSet\":{\"sha256\":\"" + sha1333 +
			"\",\"irreversible\":false,\"count\":1},\"source\":\"portal\"}\t",
			ClientProvenance{Source: "portal", ConfirmedSet: &krateoaudit.ConfirmedSet{Count: 1, SHA256: sha1333}}},
		{"a JSON escape is its character", `{"source":"` + bs1333 + `u0070ortal"}`, ClientProvenance{Source: "portal"}},
		{"requestedAt at year 0000", `{"source":"portal","requestedAt":"0000-01-01T00:00:00.000-00:01"}`,
			ClientProvenance{Source: "portal", RequestedAt: time.Date(0, 1, 1, 0, 1, 0, 0, time.UTC)}},
		{"requestedAt at year 9999", `{"source":"portal","requestedAt":"9999-12-31T23:59:59.999+23:59"}`,
			ClientProvenance{Source: "portal", RequestedAt: time.Date(9999, 12, 31, 0, 0, 59, 999e6, time.UTC)}},
		{"requestedAt with an offset", `{"source":"portal","requestedAt":"2026-11-11T12:20:30.456+02:00"}`,
			ClientProvenance{Source: "portal", RequestedAt: time.Date(2026, 11, 11, 10, 20, 30, 456e6, time.UTC)}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := decodeProvenance(encode1333(c.js))
			assertProvenance1333(t, got, c.want)
		})
	}
}

// assertProvenance1333 compares two provenances field by field (times by
// instant: an offset is the same instant in UTC on the record).
func assertProvenance1333(t *testing.T, got, want ClientProvenance) {
	t.Helper()
	gt, wt := got.RequestedAt, want.RequestedAt
	got.RequestedAt, want.RequestedAt = time.Time{}, time.Time{}
	gs, ws := got.ConfirmedSet, want.ConfirmedSet
	got.ConfirmedSet, want.ConfirmedSet = nil, nil
	if got != want || !gt.Equal(wt) || (gs == nil) != (ws == nil) || (gs != nil && *gs != *ws) {
		t.Fatalf("decoded %+v set=%+v at=%v, want %+v set=%+v at=%v", got, gs, gt, want, ws, wt)
	}
}

// rejections1333 is every rule of the header, broken one at a time, with the
// reason it must be rejected for.
var rejections1333 = []struct {
	name, value, reason string
}{
	// version: the value must start with "v1.".
	{"another version", "v2." + strings.TrimPrefix(encode1333(full1333), "v1."), "version"},
	{"version case", "V1." + strings.TrimPrefix(encode1333(full1333), "v1."), "version"},
	{"v10 is not v1", "v10." + strings.TrimPrefix(encode1333(full1333), "v1."), "version"},
	{"no version", strings.TrimPrefix(encode1333(full1333), "v1."), "version"},
	{"version without its dot", "v1", "version"},
	{"empty value", "", "version"},
	// encoding: unpadded base64url of a UTF-8 JSON text.
	{"padded", encode1333(`{"source":"portal"}`) + "=", "encoding"},
	{"standard alphabet", provenancePrefix + base64.RawStdEncoding.EncodeToString([]byte(`{"source":"portal","agentSessionId":"a~~~>>>"}`)), "encoding"},
	{"a line break inside", provenancePrefix + "eyJzb3Vy\nY2UiOiJwb3J0YWwifQ", "encoding"},
	{"a space inside", provenancePrefix + "eyJzb3Vy Y2UiOiJwb3J0YWwifQ", "encoding"},
	{"trailing bits set", provenancePrefix + "eyJzb3VyY2UiOiJwb3J0YWwifR", "encoding"},
	{"nothing after the version", provenancePrefix, "encoding"},
	{"not JSON", encode1333(`source=portal`), "encoding"},
	{"two JSON texts", encode1333(`{"source":"portal"}{"source":"portal"}`), "encoding"},
	{"truncated JSON", encode1333(`{"source":"portal"`), "encoding"},
	{"not UTF-8", encode1333("{\"source\":\"portal\",\"agentSessionId\":\"\xff\"}"), "encoding"},
	// schema: the closed schema.
	{"unknown key", encode1333(`{"source":"portal","extra":1}`), "schema"},
	{"identity is never the header's", encode1333(`{"source":"portal","user":"mallory"}`), "schema"},
	{"the actor class is never the header's", encode1333(`{"source":"portal","actorClass":"human"}`), "schema"},
	{"the verb is never the header's", encode1333(`{"source":"portal","verb":"get"}`), "schema"},
	{"the target is never the header's", encode1333(`{"source":"portal","namespace":"kube-system"}`), "schema"},
	{"the outcome is never the header's", encode1333(`{"source":"portal","outcome":"success"}`), "schema"},
	{"a key in another case", encode1333(`{"Source":"portal"}`), "schema"},
	{"a key twice", encode1333(`{"source":"portal","source":"portal"}`), "schema"},
	{"a key twice, once escaped", encode1333(`{"source":"portal","` + bs1333 + `u0073ource":"portal"}`), "schema"},
	{"not an object", encode1333(`["portal"]`), "schema"},
	{"a bare string", encode1333(`"portal"`), "schema"},
	{"null", encode1333(`null`), "schema"},
	{"empty object: no source", encode1333(`{}`), "schema"},
	{"no source", encode1333(`{"confirmationId":"` + uuid1333 + `"}`), "schema"},
	{"another source", encode1333(`{"source":"agentiko"}`), "schema"},
	{"source null", encode1333(`{"source":null}`), "schema"},
	{"source a number", encode1333(`{"source":1}`), "schema"},
	{"agentSessionId empty", encode1333(`{"source":"portal","agentSessionId":""}`), "schema"},
	{"agentSessionId 129", encode1333(`{"source":"portal","agentSessionId":"` + strings.Repeat("a", 129) + `"}`), "schema"},
	{"agentSessionId a dot", encode1333(`{"source":"portal","agentSessionId":"a.b"}`), "schema"},
	{"agentSessionId null", encode1333(`{"source":"portal","agentSessionId":null}`), "schema"},
	{"confirmationId upper case", encode1333(`{"source":"portal","confirmationId":"` + strings.ToUpper(uuid1333) + `"}`), "schema"},
	{"confirmationId no hyphens", encode1333(`{"source":"portal","confirmationId":"` + strings.ReplaceAll(uuid1333, "-", "") + `"}`), "schema"},
	{"confirmationId braces", encode1333(`{"source":"portal","confirmationId":"{` + uuid1333 + `}"}`), "schema"},
	{"confirmedSet without op", encode1333(`{"source":"portal","confirmedSet":{"count":1,"irreversible":false,"sha256":"` + sha1333 + `"}}`), "schema"},
	{"op without confirmedSet", encode1333(`{"source":"portal","op":0}`), "schema"},
	{"op equal to count", encode1333(`{"source":"portal","confirmedSet":{"count":3,"irreversible":false,"sha256":"` + sha1333 + `"},"op":3}`), "schema"},
	{"op negative", encode1333(`{"source":"portal","confirmedSet":{"count":3,"irreversible":false,"sha256":"` + sha1333 + `"},"op":-1}`), "schema"},
	{"op a fraction", encode1333(`{"source":"portal","confirmedSet":{"count":3,"irreversible":false,"sha256":"` + sha1333 + `"},"op":1.0}`), "schema"},
	{"op a string", encode1333(`{"source":"portal","confirmedSet":{"count":3,"irreversible":false,"sha256":"` + sha1333 + `"},"op":"1"}`), "schema"},
	{"count zero", encode1333(`{"source":"portal","confirmedSet":{"count":0,"irreversible":false,"sha256":"` + sha1333 + `"},"op":0}`), "schema"},
	{"count an exponent", encode1333(`{"source":"portal","confirmedSet":{"count":1e1,"irreversible":false,"sha256":"` + sha1333 + `"},"op":0}`), "schema"},
	{"count past int64", encode1333(`{"source":"portal","confirmedSet":{"count":9223372036854775808,"irreversible":false,"sha256":"` + sha1333 + `"},"op":0}`), "schema"},
	{"irreversible a string", encode1333(`{"source":"portal","confirmedSet":{"count":1,"irreversible":"true","sha256":"` + sha1333 + `"},"op":0}`), "schema"},
	{"irreversible null", encode1333(`{"source":"portal","confirmedSet":{"count":1,"irreversible":null,"sha256":"` + sha1333 + `"},"op":0}`), "schema"},
	{"sha256 upper case", encode1333(`{"source":"portal","confirmedSet":{"count":1,"irreversible":false,"sha256":"` + strings.ToUpper(sha1333) + `"},"op":0}`), "schema"},
	{"sha256 short", encode1333(`{"source":"portal","confirmedSet":{"count":1,"irreversible":false,"sha256":"` + sha1333[1:] + `"},"op":0}`), "schema"},
	{"confirmedSet without sha256", encode1333(`{"source":"portal","confirmedSet":{"count":1,"irreversible":false},"op":0}`), "schema"},
	{"confirmedSet with another key", encode1333(`{"source":"portal","confirmedSet":{"count":1,"irreversible":false,"sha256":"` + sha1333 + `","ops":[]},"op":0}`), "schema"},
	{"confirmedSet a key twice", encode1333(`{"source":"portal","confirmedSet":{"count":1,"count":1,"irreversible":false,"sha256":"` + sha1333 + `"},"op":0}`), "schema"},
	{"confirmedSet null", encode1333(`{"source":"portal","confirmedSet":null,"op":0}`), "schema"},
	{"requestedAt seconds only", encode1333(`{"source":"portal","requestedAt":"2026-11-11T10:20:30Z"}`), "schema"},
	{"requestedAt microseconds", encode1333(`{"source":"portal","requestedAt":"2026-11-11T10:20:30.456789Z"}`), "schema"},
	{"requestedAt no zone", encode1333(`{"source":"portal","requestedAt":"2026-11-11T10:20:30.456"}`), "schema"},
	{"requestedAt month 13", encode1333(`{"source":"portal","requestedAt":"2026-13-11T10:20:30.456Z"}`), "schema"},
	{"requestedAt before year 0000 in UTC", encode1333(`{"source":"portal","requestedAt":"0000-01-01T00:00:00.000+00:01"}`), "schema"},
	{"requestedAt past year 9999 in UTC", encode1333(`{"source":"portal","requestedAt":"9999-12-31T23:59:59.999-01:00"}`), "schema"},
	{"requestedAt offset hour 24", encode1333(`{"source":"portal","requestedAt":"2026-11-11T10:20:30.456+24:00"}`), "schema"},
	{"requestedAt offset minute 60", encode1333(`{"source":"portal","requestedAt":"2026-11-11T10:20:30.456+23:60"}`), "schema"},
	{"requestedAt hour 24", encode1333(`{"source":"portal","requestedAt":"2026-11-11T24:00:00.000Z"}`), "schema"},
	{"requestedAt epoch millis", encode1333(`{"source":"portal","requestedAt":1762856430456}`), "schema"},
}

func TestProvenance1333_R_EveryRejectionIsTheReasonAlone(t *testing.T) {
	reasons := map[string]bool{}
	for _, c := range rejections1333 {
		reasons[c.reason] = true
		t.Run(c.reason+"/"+c.name, func(t *testing.T) {
			got := decodeProvenance(c.value)
			if want := (ClientProvenance{Rejected: c.reason}); got != want {
				t.Fatalf("decoded %+v, want %+v", got, want)
			}
		})
	}
	// oversize has its own arm (B); every other reason is covered here.
	for _, r := range []string{krateoaudit.ClientRejectedVersion, krateoaudit.ClientRejectedEncoding, krateoaudit.ClientRejectedSchema} {
		if !reasons[r] {
			t.Errorf("no arm for the reason %q", r)
		}
	}
}

// padded1333 is a valid header of exactly n bytes: full1333 with JSON
// whitespace before its closing brace, so the length is the only variable.
// n - len("v1.") must be a length unpadded base64 can have (not 1 mod 4).
func padded1333(t *testing.T, n int) string {
	t.Helper()
	for pad := 0; pad < n; pad++ {
		v := encode1333(strings.TrimSuffix(full1333, "}") + strings.Repeat(" ", pad) + "}")
		if len(v) == n {
			return v
		}
		if len(v) > n {
			break
		}
	}
	t.Fatalf("SETUP: no valid header of exactly %d bytes", n)
	return ""
}

func TestProvenance1333_B_TheBoundIsTwoKiB(t *testing.T) {
	if MaxProvenanceBytes != 2048 {
		t.Fatalf("MaxProvenanceBytes = %d: #1333's contract is 2 KiB", MaxProvenanceBytes)
	}
	// 2048 - len("v1.") = 2045 is 1 mod 4, a length no unpadded base64 has:
	// the longest valid header is 2047 bytes, the next 2049.
	under := padded1333(t, MaxProvenanceBytes-1)
	if got := decodeProvenance(under); got.Rejected != "" || got.Source != "portal" {
		t.Fatalf("a valid header of %d bytes was rejected: %+v", len(under), got)
	}
	over := padded1333(t, MaxProvenanceBytes+1)
	if got := decodeProvenance(over); got != (ClientProvenance{Rejected: "oversize"}) {
		t.Fatalf("a valid header of %d bytes decoded %+v, want oversize", len(over), got)
	}
	// Exactly the bound is read, not refused for its size: it goes on to the
	// encoding check (which no 2048-byte value passes).
	at := under + "A"
	if got := decodeProvenance(at); got != (ClientProvenance{Rejected: "encoding"}) {
		t.Errorf("a %d-byte value decoded %+v, want encoding (read, then refused for its base64)", len(at), got)
	}
	// Over the bound, oversize is decided before the version, the encoding
	// or the schema.
	for _, v := range []string{under + "AA", strings.Repeat("!", MaxProvenanceBytes+1), "v9." + strings.Repeat("A", MaxProvenanceBytes)} {
		if got := decodeProvenance(v); got != (ClientProvenance{Rejected: "oversize"}) {
			t.Errorf("a %d-byte value decoded %+v, want oversize", len(v), got)
		}
	}
}

func TestProvenance1333_N_HeaderPresence(t *testing.T) {
	if got := ProvenanceOf(http.Header{}); got != (ClientProvenance{}) {
		t.Fatalf("no header: %+v, want nothing asserted", got)
	}
	h := http.Header{}
	h.Set(ProvenanceHeader, encode1333(full1333))
	if got := ProvenanceOf(h); got.Source != "portal" || got.ConfirmedSet == nil {
		t.Fatalf("one header: %+v", got)
	}
	// Two field lines are one value (RFC 9110 §5.3): never one of them.
	h.Add(ProvenanceHeader, encode1333(`{"source":"portal"}`))
	if got := ProvenanceOf(h); got != (ClientProvenance{Rejected: "encoding"}) {
		t.Fatalf("two field lines: %+v, want encoding", got)
	}
	// Present but empty is a header that names no version.
	e := http.Header{ProvenanceHeader: {""}}
	if got := ProvenanceOf(e); got != (ClientProvenance{Rejected: "version"}) {
		t.Fatalf("an empty header: %+v, want version", got)
	}
}

// --- G: the canonical confirmed set (#1333) -------------------------------

// canonicalOp1333 is one write of a confirmed set, as the canonical form
// names it: all seven keys, core group "", absent name "", verb upper-case.
type canonicalOp1333 struct {
	group, name, namespace, resource, verb, version string
	irreversible                                    bool
}

// canonicalKeys1333 is the canonical form's key order: sorted.
var canonicalKeys1333 = []string{"group", "irreversible", "name", "namespace", "resource", "verb", "version"}

// canonical1333 writes the canonical confirmed set of ops, by the #1333 rules
// and independently of encoding/json (which escapes <, >, &, U+2028 and
// U+2029 beyond JSON's minimum): UTF-8 JSON, no whitespace, keys sorted, ops
// in confirmation order, booleans true/false, strings escaped only where JSON
// requires it (raw UTF-8 otherwise), as JavaScript's JSON.stringify writes them.
func canonical1333(ops []canonicalOp1333) []byte {
	var b bytes.Buffer
	b.WriteByte('[')
	for i, op := range ops {
		if i > 0 {
			b.WriteByte(',')
		}
		vals := map[string]string{
			"group": jsonString1333(op.group), "irreversible": strconv.FormatBool(op.irreversible),
			"name": jsonString1333(op.name), "namespace": jsonString1333(op.namespace),
			"resource": jsonString1333(op.resource), "verb": jsonString1333(strings.ToUpper(op.verb)),
			"version": jsonString1333(op.version),
		}
		b.WriteByte('{')
		for j, k := range canonicalKeys1333 {
			if j > 0 {
				b.WriteByte(',')
			}
			b.WriteString(jsonString1333(k) + ":" + vals[k])
		}
		b.WriteByte('}')
	}
	b.WriteByte(']')
	return b.Bytes()
}

// jsonString1333 is s as a JSON string with JSON's minimum escaping: the
// quote, the backslash, and the control characters below U+0020 (\b \f \n \r
// \t by name, any other as \u00xx, lowercase hex).
func jsonString1333(s string) string {
	var b strings.Builder
	b.WriteByte('"')
	for _, r := range s {
		switch r {
		case '"':
			b.WriteString(`\"`)
		case '\\':
			b.WriteString(`\\`)
		case '\b':
			b.WriteString(`\b`)
		case '\f':
			b.WriteString(`\f`)
		case '\n':
			b.WriteString(`\n`)
		case '\r':
			b.WriteString(`\r`)
		case '\t':
			b.WriteString(`\t`)
		default:
			if r < 0x20 {
				fmt.Fprintf(&b, `\u%04x`, r)
			} else {
				b.WriteRune(r)
			}
		}
	}
	b.WriteByte('"')
	return b.String()
}

func TestProvenance1333_G_GoldenVector(t *testing.T) {
	if !sort.StringsAreSorted(canonicalKeys1333) {
		t.Fatalf("SETUP: canonical keys %v are not sorted", canonicalKeys1333)
	}
	// The #1333 golden vector v1, byte for byte as the issue comment gives it.
	const golden = `[{"group":"composition.krateo.io","irreversible":false,"name":"example-a","namespace":"example-ns","resource":"examplecatalogs","verb":"POST","version":"v1-2-0"},` +
		`{"group":"","irreversible":true,"name":"example-b","namespace":"example-ns","resource":"configmaps","verb":"DELETE","version":"v1"},` +
		`{"group":"apps","irreversible":false,"name":"","namespace":"example-ns","resource":"deployments","verb":"POST","version":"v1"}]`
	ops := []canonicalOp1333{
		{group: "composition.krateo.io", name: "example-a", namespace: "example-ns", resource: "examplecatalogs", verb: "post", version: "v1-2-0"},
		{group: "", name: "example-b", namespace: "example-ns", resource: "configmaps", verb: "DELETE", version: "v1", irreversible: true},
		{group: "apps", namespace: "example-ns", resource: "deployments", verb: "POST", version: "v1"},
	}
	got := canonical1333(ops)
	if string(got) != golden {
		t.Fatalf("canonical bytes:\n got %s\nwant %s", got, golden)
	}
	sum := sha256.Sum256(got)
	if h := hex.EncodeToString(sum[:]); h != sha1333 {
		t.Fatalf("sha256 = %s, want the golden %s", h, sha1333)
	}
	irreversible := slices.ContainsFunc(ops, func(o canonicalOp1333) bool { return o.irreversible })
	if len(ops) != 3 || !irreversible {
		t.Fatalf("count %d irreversible %v, want 3 and true (any op irreversible)", len(ops), irreversible)
	}

	// The header carrying it: snowplow records the client's sha256 as sent.
	h := http.Header{}
	h.Set(ProvenanceHeader, encode1333(`{"source":"portal","confirmedSet":{"count":3,"irreversible":true,"sha256":"`+sha1333+`"},"op":1}`))
	c := ProvenanceOf(h)
	if c.ConfirmedSet == nil || *c.ConfirmedSet != (krateoaudit.ConfirmedSet{Count: 3, Irreversible: true, SHA256: sha1333, Op: 1}) {
		t.Fatalf("decoded set %+v", c.ConfirmedSet)
	}

	// "No escaping beyond JSON's minimum; non-ASCII is raw UTF-8": the bytes
	// and sha256 below are JavaScript's JSON.stringify of the same op (node
	// v-any, ES2019 well-formed stringify), the portal's own encoder.
	esc := canonical1333([]canonicalOp1333{{
		name: "café-" + string(rune(0x2028)) + "<&>\"\\\t" + string(rune(0x01)), namespace: "ns-é", resource: "configmaps", verb: "DELETE", version: "v1", irreversible: true,
	}})
	const escHex = "5b7b2267726f7570223a22222c22697272657665727369626c65223a747275652c226e616d65223a22636166c3a92de280a83c263e5c225c5c5c745c7530303031222c226e616d657370616365223a226e732dc3a9222c227265736f75726365223a22636f6e6669676d617073222c2276657262223a2244454c455445222c2276657273696f6e223a227631227d5d"
	if hex.EncodeToString(esc) != escHex {
		t.Fatalf("escaping vector:\n got %s\nwant %s", hex.EncodeToString(esc), escHex)
	}
	if s := sha256.Sum256(esc); hex.EncodeToString(s[:]) != "c7d3aedeac8dd069c50a82a9be6540050c026b8b3230fe702e6a53ac97e6b377" {
		t.Fatalf("escaping vector sha256 = %x", s)
	}
}

// --- F: fuzz ---------------------------------------------------------------

// reencode1333 is the header a client would send for c: what an accepted
// header asserts, written back by the contract.
func reencode1333(c ClientProvenance) string {
	var parts []string
	q := strconv.Quote
	parts = append(parts, `"source":`+q(c.Source))
	if c.AgentSessionID != "" {
		parts = append(parts, `"agentSessionId":`+q(c.AgentSessionID))
	}
	if c.ConfirmationID != "" {
		parts = append(parts, `"confirmationId":`+q(c.ConfirmationID))
	}
	if s := c.ConfirmedSet; s != nil {
		parts = append(parts, fmt.Sprintf(`"confirmedSet":{"count":%d,"irreversible":%t,"sha256":%q},"op":%d`,
			s.Count, s.Irreversible, s.SHA256, s.Op))
	}
	if !c.RequestedAt.IsZero() {
		parts = append(parts, `"requestedAt":`+q(c.RequestedAt.UTC().Format("2006-01-02T15:04:05.000Z07:00")))
	}
	return encode1333("{" + strings.Join(parts, ",") + "}")
}

// fuzz1333 holds what both fuzz targets check every value with.
type fuzz1333 struct {
	rec *audittest.Recorder
	em  *krateoaudit.Emitter
}

func newFuzz1333(f *testing.F) *fuzz1333 {
	rec := audittest.NewRecorder(f)
	return &fuzz1333{rec: rec, em: krateoaudit.New(rec.Logger(Scope), krateoaudit.WithWriter(Writer))}
}

var reasons1333 = []string{krateoaudit.ClientRejectedOversize, krateoaudit.ClientRejectedEncoding,
	krateoaudit.ClientRejectedVersion, krateoaudit.ClientRejectedSchema}

// check is the property: for ANY header value, the result is a closed-set
// reason alone, or an assertion that re-encodes to itself, reaches the record
// and that the shared library's Check accepts on a write record.
func (z *fuzz1333) check(t *testing.T, v string) {
	c := decodeProvenance(v)
	if len(v) > MaxProvenanceBytes && c != (ClientProvenance{Rejected: krateoaudit.ClientRejectedOversize}) {
		t.Fatalf("a %d-byte value decoded %+v, want oversize", len(v), c)
	}
	if c.Rejected != "" {
		if !slices.Contains(reasons1333, c.Rejected) {
			t.Fatalf("rejected %q: not a closed-set reason", c.Rejected)
		}
		if c != (ClientProvenance{Rejected: c.Rejected}) {
			t.Fatalf("a rejected header asserted %+v", c)
		}
		return
	}
	if c.Source != krateoaudit.ClientSourcePortal {
		t.Fatalf("accepted with source %q", c.Source)
	}
	again := decodeProvenance(reencode1333(c))
	assertProvenance1333(t, again, c)

	z.rec.Reset()
	z.em.Emit(t.Context(), krateoaudit.Event{
		Action: "call", Verb: "create", HTTPMethod: http.MethodPost, Outcome: krateoaudit.OutcomeSuccess,
		StatusCode: http.StatusCreated, Version: "v1", Resource: "configmaps", ObjectName: "x", Namespace: "ns",
		Client: c,
	})
	attrs := audittest.Attributes(z.rec.Only(t))
	if !slices.ContainsFunc(attrs, func(kv attribute.KeyValue) bool { return string(kv.Key) == krateoaudit.KeyClientSource }) {
		t.Fatalf("an accepted header put no client key on the record")
	}
	for _, vio := range krateoaudit.Check(Writer, attrs) {
		if strings.HasPrefix(vio.Key, "krateo.audit.client.") {
			t.Fatalf("the library refuses what the decoder accepted: %s (from %q)", vio, v)
		}
	}
}

// FuzzDecodeProvenance fuzzes the header value itself: the bound, the
// version, the base64.
func FuzzDecodeProvenance(f *testing.F) {
	f.Add(encode1333(full1333))
	f.Add(encode1333(`{"source":"portal"}`))
	for _, c := range rejections1333 {
		f.Add(c.value)
	}
	z := newFuzz1333(f)
	f.Fuzz(func(t *testing.T, v string) { z.check(t, v) })
}

// FuzzProvenanceJSON fuzzes the JSON the header carries, encoded as a client
// would, so the mutator works on the schema's own values (a byte of a date,
// an offset, a count) instead of on base64, where almost every change only
// breaks the encoding.
func FuzzProvenanceJSON(f *testing.F) {
	f.Add(full1333)
	f.Add(`{"source":"portal"}`)
	f.Add(`{"source":"portal","requestedAt":"0000-01-01T00:00:00.000-00:01"}`)
	f.Add(`{"source":"portal","requestedAt":"9999-12-31T23:59:59.999+23:59"}`)
	for _, c := range rejections1333 {
		if js, err := base64.RawURLEncoding.DecodeString(strings.TrimPrefix(c.value, provenancePrefix)); err == nil {
			f.Add(string(js))
		}
	}
	z := newFuzz1333(f)
	f.Fuzz(func(t *testing.T, js string) { z.check(t, encode1333(js)) })
}
