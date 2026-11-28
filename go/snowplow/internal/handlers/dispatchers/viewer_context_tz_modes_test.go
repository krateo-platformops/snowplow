package dispatchers

import (
	"bytes"
	"testing"
)

// tz arrives in BOTH identity modes of the frontend. Identity injection OFF:
// username + displayName + tz on the wire. Injection ON: tz alone (the server
// injects identity from the JWT). Nothing server-side branches on the mode, and
// the tz rule must not depend on identity keys being present.
func TestViewerContextTZ_BothIdentityModes(t *testing.T) {
	h1BuildWatcher(t)
	rome := "Europe/Rome"
	modes := map[string]map[string]any{
		"injection-off": {"tz": rome, "username": "evil-user", "displayName": "evil-display"},
		"injection-on":  {"tz": rome},
	}
	for mode, extras := range modes {
		t.Run(mode+"/undeclared", func(t *testing.T) {
			served, persisted, _ := a3Serve(t, a3EchoSpec(nil, nil), extras)
			if len(persisted) == 0 {
				t.Fatal("an undeclared widget must be cached")
			}
			for _, leak := range []string{rome, "evil-user", "evil-display"} {
				if bytes.Contains(served, []byte(leak)) || bytes.Contains(persisted, []byte(leak)) {
					t.Fatalf("%q reached an undeclared widget's body: %s", leak, served)
				}
			}
		})
		t.Run(mode+"/declared tz + identityContext username", func(t *testing.T) {
			spec := a3EchoSpec([]string{"username"}, []string{"tz"})
			served, persisted, _ := a3Serve(t, spec, extras)
			if len(persisted) == 0 {
				t.Fatal("a declared widget must be cached")
			}
			if !bytes.Contains(persisted, []byte(rome)) || !bytes.Contains(persisted, []byte(h1User)) {
				t.Fatalf("body must carry the client tz and the JWT username; persisted=%s", persisted)
			}
			if bytes.Contains(served, []byte("evil-user")) || bytes.Contains(served, []byte("evil-display")) {
				t.Fatalf("a client identity claim reached the body: %s", served)
			}
			if k1, k2 := tzKey(t, spec, extras), tzKey(t, spec, map[string]any{"tz": "UTC"}); k1 == k2 {
				t.Fatal("the declared tz must partition the key in this mode")
			}
		})
	}
}
