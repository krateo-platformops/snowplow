package redact

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"log/slog"
	"strings"
	"testing"
)

func TestS453_LabelsNeverCarryTheInput(t *testing.T) {
	const u = "carla.zq453@tenant.example"
	const g = "grp-zq453-private"
	for name, got := range map[string]string{
		"user":     User(u),
		"prefixed": Prefixed("learned", u+"\x00"+g),
	} {
		if strings.Contains(got, u) || strings.Contains(got, g) {
			t.Errorf("%s label %q carries its input", name, got)
		}
	}
	if !strings.HasPrefix(User(u), "user:") || !strings.HasPrefix(Prefixed("learned", u), "learned:") {
		t.Error("each label family keeps its prefix")
	}
	if len(strings.TrimPrefix(User(u), "user:")) != labelHexLen {
		t.Errorf("digest length: %q", User(u))
	}
}

func TestS453_LabelsStableWithinTheProcessAndDistinct(t *testing.T) {
	if User("a") != User("a") {
		t.Error("a label is stable for the process lifetime")
	}
	if User("a") == User("b") {
		t.Error("distinct inputs must render distinct labels")
	}
	if User("") != Anonymous {
		t.Error("the empty identity is Anonymous")
	}
}

// TestS453_LabelIsKeyedNotADictionaryHash: the label must not be the bare
// sha256 of the username (reversible by hashing a tenant-username dictionary).
func TestS453_LabelIsKeyedNotADictionaryHash(t *testing.T) {
	const u = "alice"
	for _, in := range []string{u, "u\x00" + u} {
		sum := sha256.Sum256([]byte(in))
		plain := hex.EncodeToString(sum[:])[:labelHexLen]
		if strings.Contains(User(u), plain) {
			t.Fatalf("the user label is the unkeyed sha256 of %q", in)
		}
	}
}

// TestS487_ValueAndDictAttrsNeverRenderAValue: what each lazy LogValuer's
// LogValue renders, through both slog handlers. It carries {type, bytes,
// sha256} and key labels only. Neither a value nor a dict key name appears
// (#490 review: the guard trusts a depth-0 LogValuer, so this arm is what
// holds its LogValue to redacted fields).
func TestS487_ValueAndDictAttrsNeverRenderAValue(t *testing.T) {
	const secret = "zq487-not-a-real-secret" // stands in for a Secret value
	dict := map[string]any{
		"secret":   map[string]any{"data": map[string]any{"password": secret}},
		"username": "erin.zq487@tenant.example", // a request-extras key
		"slice":    map[string]any{"page": 1},
	}
	for name, mk := range map[string]func(*bytes.Buffer) slog.Handler{
		"json": func(b *bytes.Buffer) slog.Handler { return slog.NewJSONHandler(b, nil) },
		"text": func(b *bytes.Buffer) slog.Handler { return slog.NewTextHandler(b, nil) },
	} {
		var buf bytes.Buffer
		v := dict["secret"]
		var nilRef *any
		slog.New(mk(&buf)).Info("m", DictAttr("dict", dict), ValueAttr("value", &v), ValueAttr("nil", nilRef))
		out := buf.String()
		for _, leak := range []string{secret, "password", "erin.zq487", "username", "slice", `"secret"`, "secret="} {
			if strings.Contains(out, leak) {
				t.Errorf("%s: %q reached the line: %s", name, leak, out)
			}
		}
		for _, want := range []string{"sha256", "bytes", "keys", "entries", KeyLabel("secret"), KeyLabel("username")} {
			if !strings.Contains(out, want) {
				t.Errorf("NON-VACUITY %s: the summary must carry %q: %s", name, want, out)
			}
		}
	}
	n1, d1 := ValueDigest(dict)
	n2, d2 := ValueDigest(map[string]any{"slice": map[string]any{"page": 1}, "username": "erin.zq487@tenant.example",
		"secret": map[string]any{"data": map[string]any{"password": secret}}})
	if n1 != n2 || d1 != d2 || n1 <= 0 {
		t.Errorf("the digest must be stable for equal values (%d %s vs %d %s)", n1, d1, n2, d2)
	}
	if _, d3 := ValueDigest(map[string]any{"secret": "other"}); d3 == d1 {
		t.Error("different values must digest differently")
	}
	if n, _ := ValueDigest(func() {}); n != -1 {
		t.Errorf("an unencodable value reports size -1, got %d", n)
	}
}

func TestS453_ErrorTextScrubsApiserverIdentity(t *testing.T) {
	const u = "carla.zq453@tenant.example"
	const g = "grp-zq453-private"
	for _, c := range []struct{ name, in, stays string }{
		{"forbidden requester", `configmaps is forbidden: User "` + u + `" cannot list resource "configmaps" in API group "" in the namespace "x"`, `User "[redacted]" cannot list resource "configmaps" in API group ""`},
		{"impersonated user", `users "` + u + `" is forbidden: User "sa" cannot impersonate resource "users" in API group ""`, `users "[redacted]" is forbidden`},
		{"impersonated group", `groups "` + g + `" is forbidden: User "sa" cannot impersonate resource "groups" in API group ""`, `groups "[redacted]" is forbidden`},
		{"clientconfig Secret", `secrets "carla.zq453@tenant.example-clientconfig" not found`, `secrets "[redacted]" not found`},
		{"Status body in an error", `{"message":"pods is forbidden: User \"` + u + `\" cannot list"}`, `User \"[redacted]\" cannot list`},
		{"escaped clientconfig", `{"message":"secrets \"` + u + `-clientconfig\" not found"}`, `secrets \"[redacted]\" not found`},
	} {
		got := ErrorText(c.in)
		if strings.Contains(got, u) || strings.Contains(got, g) {
			t.Errorf("%s: the identity survived: %s", c.name, got)
		}
		if !strings.Contains(got, c.stays) {
			t.Errorf("%s: the reason must stay readable: got %s, want it to hold %s", c.name, got, c.stays)
		}
		if again := ErrorText(got); again != got {
			t.Errorf("%s: not idempotent: %s then %s", c.name, got, again)
		}
	}
	if got := Err(errors.New(`secrets "carla-zq453-clientconfig" not found`)); strings.Contains(got, "carla") || !strings.Contains(got, "not found") {
		t.Fatalf("clientconfig Secret name survived: %s", got)
	}
	if Err(nil) != "" {
		t.Fatal("Err(nil) is empty")
	}
	for _, plain := range []string{`templates.krateo.io "x" not found`, `the server could not find the requested resource (get pods.v1 "p")`, `no group "apps" in API group "apps"`} {
		if ErrorText(plain) != plain {
			t.Fatalf("text without an identity is unchanged: %s", plain)
		}
	}
}

// typedNilError has a method that dereferences its receiver.
type typedNilError struct{ msg string }

func (e *typedNilError) Error() string { return e.msg }

// TestS1192_ScrubLogAttr: the handler option scrubs a string attribute, an error
// attribute, a nested group and the message, leaves every other kind alone, and
// survives an error whose Error method panics.
func TestS1192_ScrubLogAttr(t *testing.T) {
	const u = "carla.zq1192@tenant.example"
	forbidden := `pods is forbidden: User "` + u + `" cannot list`
	var buf bytes.Buffer
	log := slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{ReplaceAttr: ScrubLogAttr}))
	log.Info(forbidden,
		slog.String("s", forbidden),
		slog.Any("err", errors.New(forbidden)),
		slog.Group("g", slog.Any("nested", errors.New(forbidden))),
		slog.Int("n", 7))
	out := buf.String()
	if strings.Contains(out, u) {
		t.Fatalf("the identity survived: %s", out)
	}
	if got := strings.Count(out, `User \"[redacted]\" cannot list`); got != 4 {
		t.Fatalf("message, string, error and nested error must each stay readable, %d did: %s", got, out)
	}
	if !strings.Contains(out, `"n":7`) {
		t.Fatalf("a non-text attribute is untouched: %s", out)
	}

	buf.Reset()
	var nilErr *typedNilError
	log.Info("m", slog.Any("err", error(nilErr)))
	if !strings.Contains(buf.String(), `"msg":"m"`) {
		t.Fatalf("a typed nil error must not stop the line: %s", buf.String())
	}
}
