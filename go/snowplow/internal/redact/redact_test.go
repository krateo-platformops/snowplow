package redact

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"strings"
	"testing"
)

func TestS453_LabelsNeverCarryTheInput(t *testing.T) {
	const u = "carla.zq453@tenant.example"
	const g = "grp-zq453-private"
	for name, got := range map[string]string{
		"user":     User(u),
		"group":    Group(g),
		"identity": Identity(u, []string{g}),
		"prefixed": Prefixed("learned", u+"\x00"+g),
		"groups":   strings.Join(Groups([]string{g, "system:authenticated"}), ","),
	} {
		if strings.Contains(got, u) || strings.Contains(got, g) || strings.Contains(got, "system:authenticated") {
			t.Errorf("%s label %q carries its input", name, got)
		}
	}
	if !strings.HasPrefix(User(u), "user:") || !strings.HasPrefix(Group(g), "group:") ||
		!strings.HasPrefix(Identity(u, nil), "id:") || !strings.HasPrefix(Prefixed("learned", u), "learned:") {
		t.Error("each label family keeps its prefix")
	}
	if len(strings.TrimPrefix(User(u), "user:")) != labelHexLen {
		t.Errorf("digest length: %q", User(u))
	}
}

func TestS453_LabelsStableWithinTheProcessAndDistinct(t *testing.T) {
	first, second := User("a"), User("a")
	if first != second || Identity("a", []string{"x", "y"}) != Identity("a", []string{"y", "x"}) {
		t.Error("a label is stable for the process lifetime and the group order does not matter")
	}
	if User("a") == User("b") || Group("a") == Group("b") || Identity("a", []string{"x"}) == Identity("a", []string{"y"}) {
		t.Error("distinct inputs must render distinct labels")
	}
	// Domain separation: the same text as a user and as a group are different facts.
	if strings.TrimPrefix(User("x"), "user:") == strings.TrimPrefix(Group("x"), "group:") {
		t.Error("the user and group digests must be domain-separated")
	}
	if User("") != Anonymous || Group("") != Anonymous || Identity("", nil) != Anonymous {
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

func TestS453_ErrorTextScrubsApiserverIdentity(t *testing.T) {
	const u = "carla.zq453@tenant.example"
	forbidden := `configmaps is forbidden: User "` + u + `" cannot list resource "configmaps" in API group "" in the namespace "x"`
	got := ErrorText(forbidden)
	if strings.Contains(got, u) {
		t.Fatalf("Forbidden requester survived: %s", got)
	}
	if !strings.Contains(got, `User "`+User(u)+`"`) || !strings.Contains(got, `cannot list resource "configmaps"`) {
		t.Fatalf("the reason must stay readable with the label in place: %s", got)
	}
	nf := `secrets "carla-zq453-clientconfig" not found`
	if got := Err(errors.New(nf)); strings.Contains(got, "carla") || !strings.Contains(got, "not found") {
		t.Fatalf("clientconfig Secret name survived: %s", got)
	}
	if Err(nil) != "" {
		t.Fatal("Err(nil) is empty")
	}
	if plain := `templates.krateo.io "x" not found`; ErrorText(plain) != plain {
		t.Fatal("text without an identity is unchanged")
	}
}
