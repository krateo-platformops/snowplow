package frontendconfig

import "testing"

// ConfigMapName is the env var's value, trimmed, or the default when it is unset, empty or blank
// (mutation: the trim dropped turns the " x " row RED).
func TestConfigMapName(t *testing.T) {
	for _, c := range []struct{ env, want string }{
		{"", Default},
		{"   ", Default},
		{"tenant-frontend-cfg", "tenant-frontend-cfg"},
		{" x ", "x"},
	} {
		t.Setenv(Env, c.env)
		if got := ConfigMapName(); got != c.want {
			t.Fatalf("%s=%q: want %q, got %q", Env, c.env, c.want, got)
		}
	}
}
