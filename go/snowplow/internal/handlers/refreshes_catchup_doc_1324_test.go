package handlers

// refreshes_catchup_doc_1324_test.go — #1324: the frontend integration doc
// (docs/howto-frontend-refresh-digest-catch-up.md) stays equal to the wire:
// its example digest is ContentDigest of its example body, its header name is
// the one /call stamps, and its arm payload decodes into the subRequest the
// handler reads, with `held` carrying that digest.

import (
	"encoding/json"
	"os"
	"strings"
	"testing"

	"github.com/krateo-platformops/snowplow/internal/cache"
	"github.com/krateo-platformops/snowplow/internal/handlers/dispatchers"
)

func fenced1324(t *testing.T, doc, lang string) string {
	t.Helper()
	blocks := strings.Split(doc, "```"+lang+"\n")
	if len(blocks) != 2 {
		t.Fatalf("the doc must carry exactly one ```%s example, found %d", lang, len(blocks)-1)
	}
	return blocks[1][:strings.Index(blocks[1], "```")]
}

func TestRefreshes1324_DocEqualsWire(t *testing.T) {
	b, err := os.ReadFile("../../docs/howto-frontend-refresh-digest-catch-up.md")
	if err != nil {
		t.Fatal(err)
	}
	doc := string(b)
	var body, digest string
	for _, ln := range strings.Split(fenced1324(t, doc, "http"), "\n") {
		switch {
		case strings.HasPrefix(ln, "{"):
			body = ln
		case strings.HasPrefix(ln, dispatchers.RefreshDigestHeader+": "):
			digest = strings.TrimPrefix(ln, dispatchers.RefreshDigestHeader+": ")
		}
	}
	if body == "" || digest == "" {
		t.Fatalf("the ```http example must show a body line and a %s line", dispatchers.RefreshDigestHeader)
	}
	if want := cache.ContentDigest([]byte(body)); digest != want {
		t.Fatalf("doc digest %s is not the digest of its example body (%s)", digest, want)
	}
	var subs []subRequest
	if err := json.Unmarshal([]byte(strings.TrimSpace(fenced1324(t, doc, "json"))), &subs); err != nil {
		t.Fatalf("the doc's arm payload is not a coordinate array the handler decodes: %v", err)
	}
	if len(subs) != 1 || subs[0].Held != digest || subs[0].Class == "" || subs[0].Name == "" {
		t.Fatalf("the doc's arm payload %+v must carry held=%s on a full coordinate", subs, digest)
	}
	for _, must := range []string{"Access-Control-Expose-Headers", "`held`", `"held": ""`, "JSON.stringify(prev.coords)"} {
		if !strings.Contains(doc, must) {
			t.Fatalf("the doc must state %q (CORS, the field, the empty-held rule, the dedup caveat)", must)
		}
	}
}
