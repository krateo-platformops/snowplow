package credhttp

import "testing"

// RequestURL's doc says what it joins: the server URL without its trailing "/",
// exactly one "/", and the path without ITS leading "/". Only one: a second
// leading "/" is the caller's, and a named endpoint receives it as written.
func TestRequestURL1137_JoinsWithExactlyOneSlash(t *testing.T) {
	for _, c := range []struct{ server, path, want string }{
		{"http://h", "", "http://h"},
		{"http://h/", "", "http://h"},
		{"http://h", "/", "http://h/"},
		{"http://h", "x", "http://h/x"},
		{"http://h", "/x", "http://h/x"},
		{"http://h/", "/x", "http://h/x"},
		{"http://h", "//x", "http://h//x"},
		{"http://h", "///x", "http://h///x"},
		{"http://h/base", "/x?a=1#f", "http://h/base/x?a=1#f"},
		{"http://h", "?x=1", "http://h/?x=1"},
		{"http://h", "#f", "http://h/#f"},
	} {
		if got := RequestURL(c.server, c.path); got != c.want {
			t.Errorf("RequestURL(%q, %q) = %q, want %q", c.server, c.path, got, c.want)
		}
	}
}
