package redact

import "testing"

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
				if got != tc.raw && contains(got, secret) {
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
		if i := indexByte(authority, '/'); i >= 0 {
			authority = authority[:i]
		}
		if contains(authority, "@") {
			t.Errorf("URL(%q) = %q — the authority still carries userinfo", raw, got)
		}
		if contains(got, "p@") || contains(got, ":p") {
			t.Errorf("URL(%q) = %q — the password survived", raw, got)
		}
	}
}

func contains(s, sub string) bool {
	return len(sub) == 0 || indexOf(s, sub) >= 0
}

func indexOf(s, sub string) int {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return i
		}
	}
	return -1
}

func indexByte(s string, b byte) int {
	for i := 0; i < len(s); i++ {
		if s[i] == b {
			return i
		}
	}
	return -1
}
