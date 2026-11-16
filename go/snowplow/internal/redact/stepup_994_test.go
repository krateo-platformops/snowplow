package redact_test

import (
	"errors"
	"strings"
	"testing"

	"github.com/krateo-platformops/snowplow/internal/credsecret"
	"github.com/krateo-platformops/snowplow/internal/redact"
)

// TestErrorText994_ScrubsTheStepUpSecretName — #994: a NotFound naming a
// step-up credential Secret (`<user>-clientconfig-stepup`, the name the auth
// middleware and the resolver now read) is scrubbed like a session one. The
// names come from credsecret, the package that builds them.
func TestErrorText994_ScrubsTheStepUpSecretName(t *testing.T) {
	const u = "carla-zq994"
	for _, name := range []string{credsecret.StepUpNames(u)[0], credsecret.SessionNames(u)[0]} {
		nf := `secrets "` + name + `" not found`
		got := redact.Err(errors.New(nf))
		if strings.Contains(got, u) {
			t.Fatalf("credential Secret name %q survived ErrorText: %s", name, got)
		}
		if got != `secrets "`+redact.Redacted+`" not found` {
			t.Fatalf("the reason must stay readable with the name removed: %s", got)
		}
	}
}
