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
	for _, name := range []string{credsecret.StepUpName(u), credsecret.SessionName(u)} {
		nf := `secrets "` + name + `" not found`
		got := redact.Err(errors.New(nf))
		if strings.Contains(got, u) {
			t.Fatalf("credential Secret name %q survived ErrorText: %s", name, got)
		}
		if !strings.Contains(got, "not found") || !strings.HasPrefix(got, `secrets "secret:`) {
			t.Fatalf("the reason must stay readable with the label in place: %s", got)
		}
	}
}
