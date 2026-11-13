package middleware_test

// public_identity_userconfig_readers_1334_test.go — #1334 MN3, the structural
// half: every production read of the caller's clientconfig endpoint
// (xcontext.UserConfig) is CLASSIFIED by what it does for the public identity,
// whose endpoint carries no credential and whose only credential is the
// impersonating config on its context. A new reader fails this test until it is
// classified, so a dial site that would present the clientconfig endpoint
// directly cannot appear unseen.

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// i1334UserConfigReaders maps each production file (relative to the module
// root) to how many xcontext.UserConfig reads it holds, and why each is safe for
// the public identity.
var i1334UserConfigReaders = map[string]struct {
	n   int
	why string
}{
	"internal/handlers/call.go":                           {1, "dials through cache.ImpersonatingConfigFor → credhttp.DoAs when the context carries one"},
	"internal/handlers/list.go":                           {1, "builds its client through cache.ClientConfigFor (the impersonating config)"},
	"internal/handlers/jq.go":                             {1, "presence check only; dials nothing"},
	"internal/objects/get.go":                             {1, "builds its client through cache.ClientConfigFor (the impersonating config)"},
	"internal/resolvers/widgets/resourcesrefs/resolve.go": {1, "builds its client through cache.ClientConfigFor (the impersonating config)"},
	"internal/resolvers/restactions/api/provenance.go":    {1, "refuses a context carrying an internal rest config before it reads the endpoint"},
	"internal/rbac/rbac.go": {1, "the SelfSubjectAccessReview baseline runs only with the cache off, where the " +
		"impersonation probe never runs, so the public identity is unavailable (503)"},
}

func TestIssue1334_EveryUserConfigReadIsClassified(t *testing.T) {
	root := filepath.Join("..", "..", "..")
	found := map[string]int{}
	err := filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			switch d.Name() {
			case "vendor", "testdata", "e2e", ".git":
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		raw, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		n := 0
		for _, line := range strings.Split(string(raw), "\n") {
			code, _, _ := strings.Cut(line, "//")
			n += strings.Count(code, "xcontext.UserConfig(")
		}
		if n > 0 {
			rel, _ := filepath.Rel(root, path)
			found[filepath.ToSlash(rel)] = n
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(found) == 0 {
		t.Fatal("instrument: no xcontext.UserConfig read found at all; the walk is not looking at the module")
	}
	for f, n := range found {
		c, ok := i1334UserConfigReaders[f]
		if !ok {
			t.Errorf("%s reads xcontext.UserConfig %d time(s) and is not classified for the public identity (#1334): "+
				"a dial of that endpoint carries no credential for it; route it through cache.ClientConfigFor or "+
				"cache.ImpersonatingConfigFor, then classify it here", f, n)
			continue
		}
		if n != c.n {
			t.Errorf("%s reads xcontext.UserConfig %d time(s), classified for %d (%s): re-check the new read", f, n, c.n, c.why)
		}
	}
	for f := range i1334UserConfigReaders {
		if _, ok := found[f]; !ok {
			t.Errorf("classified reader %s no longer reads xcontext.UserConfig: drop it from the list", f)
		}
	}
}
