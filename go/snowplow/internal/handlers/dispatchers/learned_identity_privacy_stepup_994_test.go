package dispatchers

// learned_identity_privacy_stepup_994_test.go — #994: the learned-class
// redacting logger knows BOTH of a user's credential Secret names. A step-up
// Secret name `<dns1123(user)>-clientconfig-stepup` (authn#49) is replaced
// WHOLE by the class label, never left as `<label>-stepup` (which would still
// say the class holds a step-up credential).

import (
	"bytes"
	"log/slog"
	"slices"
	"strings"
	"testing"

	"github.com/krateo-platformops/snowplow/internal/cache"
	"github.com/krateo-platformops/snowplow/internal/credsecret"
)

func TestLearnedPrivacy994_StepUpSecretNameIsRedactedWhole(t *testing.T) {
	const user = "carla.zq994@tenant.example"
	groups := []string{"grp-zq994-private", credsecret.StepUpGroup}

	tokens := learnedRedactionTokens(user, groups)
	for _, want := range []string{credsecret.StepUpName(user), credsecret.SessionName(user)} {
		if !slices.Contains(tokens, want) {
			t.Fatalf("learnedRedactionTokens lacks the credential Secret name %q: %v", want, tokens)
		}
	}

	var buf bytes.Buffer
	log := learnedRedactingLogger(slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug})), user, groups)
	log.Info(`secrets "`+credsecret.StepUpName(user)+`" not found`,
		slog.String("secret", credsecret.StepUpName(user)),
		slog.String("session", credsecret.SessionName(user)))
	out := buf.String()

	label := cache.LearnedClassLabel(user, groups)
	if !strings.Contains(out, label) {
		t.Fatalf("NON-VACUITY: the class label must replace the tokens; out=%s", out)
	}
	if strings.Contains(out, "-stepup") || strings.Contains(out, "clientconfig") {
		t.Fatalf("a credential Secret name (or its step-up tail) survived the redacting logger: %s", out)
	}
}

// TestDependencyMessage994_StepUpSecretNameIsAnIdentityShape — a widget's
// dependency-failure message naming ANOTHER user's step-up Secret (not one of
// the caller's tokens) is never shown: `-clientconfig-stepup` matches the
// `-clientconfig\b` identity shape.
func TestDependencyMessage994_StepUpSecretNameIsAnIdentityShape(t *testing.T) {
	ctx := psUserCtx(psArm{}, psBob)
	for _, msg := range []string{
		`secrets "` + credsecret.StepUpName("dave-zq994") + `" not found`,
		`GET /api/v1/namespaces/krateo-system/secrets/` + credsecret.StepUpName("dave-zq994"),
	} {
		if got := dependencyFailureMessage(ctx, msg); got != dependencyFailureFallback {
			t.Fatalf("a step-up Secret name reached a widget message: %q -> %q", msg, got)
		}
	}
	if plain := `configmaps "settings" not found`; dependencyFailureMessage(ctx, plain) != plain {
		t.Fatal("control: a message without an identity must be shown")
	}
}
