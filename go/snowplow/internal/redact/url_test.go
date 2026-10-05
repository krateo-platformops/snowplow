package redact

import (
	"strings"
	"testing"
)

// TestS499_URLStripsCredentialsFromEveryShape — the #499 falsifier.
//
// The shipped 1.12.36 implementation was `url.Parse` + `u.User = nil`, which
// is correct ONLY for a scheme'd URL. The schemeless row below is the one that
// leaked: it goes GREEN here and RED against that implementation, which is the
// whole point of the table. The remaining rows pin the behaviour that must NOT
// change while fixing it — a scheme'd URL is still stripped, a bare host:port
// still renders, a genuinely unparseable URL is still refused.
func TestS499_URLStripsCredentialsFromEveryShape(t *testing.T) {
	const (
		user = "ops-zq499"
		pass = "hunter2-zq499"
		key  = "api-key-zq499"
	)

	for _, tc := range []struct {
		name string
		raw  string
		want string
	}{
		{
			name: "scheme'd userinfo is stripped (already worked)",
			raw:  "https://" + user + ":" + pass + "@api.example:6443/base",
			want: "https://api.example:6443/base",
		},
		{
			name: "SCHEMELESS userinfo is stripped (#499: used to round-trip verbatim)",
			raw:  user + ":" + pass + "@api.example/base",
			want: "api.example/base",
		},
		{
			name: "schemeless with a port",
			raw:  user + ":" + pass + "@api.example:6443/base",
			want: "api.example:6443/base",
		},
		{
			name: "bare host:port still renders (it carries nothing)",
			raw:  "api.example:6443",
			want: "api.example:6443",
		},
		{
			name: "query VALUES are replaced, keys kept",
			raw:  "https://api.example/base?api_key=" + key + "&page=2",
			want: "https://api.example/base?api_key=" + URLRedactedValue + "&page=" + URLRedactedValue,
		},
		{
			name: "userinfo AND query together",
			raw:  "https://" + user + ":" + pass + "@api.example/base?token=" + key,
			want: "https://api.example/base?token=" + URLRedactedValue,
		},
		{
			name: "fragment is dropped",
			raw:  "https://api.example/base#frag",
			want: "https://api.example/base",
		},
		{
			name: "unparseable is refused",
			raw:  "://bad url",
			want: URLUnparseable,
		},
		{
			name: "empty stays empty",
			raw:  "",
			want: "",
		},
		{
			name: "no credentials, nothing to do",
			raw:  "https://api.example:6443/base",
			want: "https://api.example:6443/base",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := URL(tc.raw)
			if got != tc.want {
				t.Errorf("URL(%q)\n got  %q\n want %q", tc.raw, got, tc.want)
			}
			// Whatever the shape, no secret may survive into the rendering.
			for _, secret := range []string{pass, key} {
				if got != tc.raw && strings.Contains(got, secret) {
					t.Errorf("URL(%q) = %q still carries %q", tc.raw, got, secret)
				}
			}
		})
	}
}

// TestS499_URLNeverReturnsAnAuthorityWithUserinfo is the invariant stated
// independently of the table: no input, however shaped, may produce a
// rendering whose authority still carries an `@`.
func TestS499_URLNeverReturnsAnAuthorityWithUserinfo(t *testing.T) {
	for _, raw := range []string{
		"https://u:p@h/x",
		"u:p@h/x",
		"//u:p@h/x",
		"u:p@h",
		"scheme://u:p@h:1/x?q=1#f",
		"u:p@h/x?token=t",
	} {
		got := URL(raw)
		if got == URLUnparseable || got == "" {
			continue // refused outright, which is safe
		}
		authority := got
		if i := strings.IndexByte(authority, '/'); i >= 0 {
			authority = authority[:i]
		}
		if strings.Contains(authority, "@") {
			t.Errorf("URL(%q) = %q — the authority still carries userinfo", raw, got)
		}
		if strings.Contains(got, "p@") || strings.Contains(got, ":p") {
			t.Errorf("URL(%q) = %q — the password survived", raw, got)
		}
	}
}

// TestS499_URLNeverLeaksOverAShapeCorpus is the arm that would have caught the
// FIRST fix for #499, which closed the schemeless hole and left two more open:
// `https:/u:p@h/x` (one slash — a scheme AND no authority) and
// `jdbc:mysql://u:p@h/db` (an opaque URL whose real userinfo lands in Path).
// Both round-tripped the password verbatim past a shape-enumerating table.
//
// So this does not enumerate shapes. It builds a cross-product and asserts the
// two properties that must hold for EVERY input, whatever url.Parse decides to
// do with it:
//
//  1. no credential sentinel appears in the rendering, ever;
//  2. no "@" appears in the rendering, ever — credentials live before one, and
//     query values are already replaced, so a surviving "@" means some shape
//     outwitted the parser.
//
// A new leaking shape then fails here without anyone having thought of it
// first, which is the only kind of coverage that survives the next surprise
// from net/url.
func TestS499_URLNeverLeaksOverAShapeCorpus(t *testing.T) {
	const (
		pw  = "hunter2-zq499-pw"
		tok = "tok-zq499-secret"
	)
	prefixes := []string{"", "//", "https://", "https:/", "http://", "k8s:/", "jdbc:mysql://", "scheme:"}
	userinfos := []string{"", "u@", "u:" + pw + "@", "u:@", "u%3Ap@"}
	hosts := []string{"h", "h:6443", "10.0.0.1:6443", "[::1]:6443", "h."}
	paths := []string{"", "/", "/base", "/a@b", "/base;p=1"}
	queries := []string{"", "?", "?token=" + tok, "?a=1&b=2", "?novalue", "?k=" + tok + "&k=2"}

	checked := 0
	for _, pre := range prefixes {
		for _, ui := range userinfos {
			for _, h := range hosts {
				for _, p := range paths {
					for _, q := range queries {
						raw := pre + ui + h + p + q
						got := URL(raw)
						checked++

						if strings.Contains(got, pw) {
							t.Errorf("PASSWORD LEAK: URL(%q) = %q", raw, got)
						}
						if strings.Contains(got, tok) {
							t.Errorf("QUERY CREDENTIAL LEAK: URL(%q) = %q", raw, got)
						}
						if got != URLUnparseable && strings.Contains(got, "@") {
							t.Errorf("AUTHORITY LEAK: URL(%q) = %q still carries an \"@\"", raw, got)
						}
					}
				}
			}
		}
	}
	if checked < 1000 {
		t.Fatalf("NON-VACUITY: only %d inputs were checked; the corpus is not being built", checked)
	}

	// Non-vacuity of the other kind: the corpus must contain inputs that DO
	// render, otherwise "<unparseable> for everything" would pass the above.
	rendered := 0
	for _, raw := range []string{
		"https://h:6443/base",
		"https://u:" + pw + "@h:6443/base?token=" + tok,
		"u:" + pw + "@h/base",
		"h:6443",
		"https://[::1]:6443/x",
	} {
		if got := URL(raw); got != URLUnparseable && got != "" {
			rendered++
		}
	}
	if rendered != 5 {
		t.Errorf("OVER-REFUSAL: only %d/5 of the ordinary shapes still render; "+
			"refusing everything would satisfy the leak properties vacuously", rendered)
	}
}

// TestS499_CredentialShapesAreSTRIPPEDNotRefused closes a coverage gap the
// #502 gate found in the corpus arm above.
//
// That arm asserts "no credential sentinel, and no surviving @". Both
// properties are satisfied EQUALLY by "the userinfo was stripped and the URL
// rendered" and by "the URL was thrown away as <unparseable>" — so it cannot
// tell the two apart, and removing the stripping while keeping the catch-all
// leaves it GREEN. Its REDs under that mutation are all the corpus's own
// `/a@b` PATH dimension, not one of them a credential.
//
// This arm pins the behaviour the corpus arm cannot see: for a URL that
// carries real userinfo, the credential must be REMOVED and the rest of the
// URL must SURVIVE. Refusing the URL is not an acceptable answer here —
// <unparseable> would hide a stripper that had stopped working, which is
// exactly the state #499 shipped in.
//
// The mutation that demonstrates the gap, from the #502 gate (`u.Path = ""`
// before u.String()): it strips the credential, leaves no "@", and still
// renders — so the corpus arm stays GREEN while this arm reports
//
//	got "https://h:6443"  want "https://h:6443/base"
//
// Removing `u.User = nil` is NOT that demonstration: the corpus arm catches
// that one through its own over-refusal check.
func TestS499_CredentialShapesAreSTRIPPEDNotRefused(t *testing.T) {
	const pw = "hunter2-zq499-pw"
	for _, tc := range []struct{ raw, want string }{
		{"https://u:" + pw + "@h:6443/base", "https://h:6443/base"},
		{"https://u:" + pw + "@h/base", "https://h/base"},
		{"u:" + pw + "@h/base", "h/base"},
		{"u:" + pw + "@h:6443/base", "h:6443/base"},
		{"u:" + pw + "@h", "h"},
		{"u@h", "h"},
		{"u@h/base", "h/base"},
		{"//u:" + pw + "@h/base", "//h/base"},
		{"https://u:" + pw + "@[::1]:6443/x", "https://[::1]:6443/x"},
		{"https://u:" + pw + "@h/base?token=t", "https://h/base?token=" + URLRedactedValue},
	} {
		got := URL(tc.raw)
		if got == URLUnparseable {
			t.Errorf("URL(%q) = <unparseable>: a credential-bearing URL must be STRIPPED and RENDERED, "+
				"not refused — refusing hides whether the stripper still works", tc.raw)
			continue
		}
		if got != tc.want {
			t.Errorf("URL(%q)\n got  %q\n want %q", tc.raw, got, tc.want)
		}
	}
}
