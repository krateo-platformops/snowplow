package handlers

// call_provenance_doc_1333_test.go — #1333: the frontend contract
// (docs/howto-frontend-call-provenance.md) stays equal to the code: its
// capability list is the advertised one, its example header is accepted and
// decodes to its example JSON, its bound is MaxProvenanceBytes, and its
// rejection table is the library's closed set.

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"testing"

	krateoaudit "github.com/krateo-platformops/audit"
	"github.com/krateo-platformops/snowplow/internal/support/audit"
)

func TestCall1333_DocIsTheWire(t *testing.T) {
	b, err := os.ReadFile("../../docs/howto-frontend-call-provenance.md")
	if err != nil {
		t.Fatal(err)
	}
	doc := string(b)

	caps, _ := json.Marshal(map[string][]string{"capabilities": Capabilities()})
	if !strings.Contains(doc, "200 "+string(caps)) {
		t.Errorf("the doc's /capabilities answer is not %s", caps)
	}

	var header, example string
	for _, line := range strings.Split(doc, "\n") {
		if v, ok := strings.CutPrefix(line, audit.ProvenanceHeader+": v1."); ok && !strings.Contains(v, "<") {
			header = "v1." + v
		}
		if strings.HasPrefix(line, `{"source":`) {
			example = line
		}
	}
	if header == "" || example == "" {
		t.Fatalf("SETUP: the doc has no example header (%q) or JSON (%q)", header, example)
	}
	raw, err := base64.RawURLEncoding.DecodeString(strings.TrimPrefix(header, "v1."))
	if err != nil || string(raw) != example {
		t.Errorf("the example header decodes to %q (%v), not the example JSON", raw, err)
	}
	if !strings.Contains(doc, fmt.Sprintf("Example (%d bytes)", len(header))) {
		t.Errorf("the doc does not give the example's length, %d bytes", len(header))
	}
	h := map[string][]string{audit.ProvenanceHeader: {header}}
	if c := audit.ProvenanceOf(h); c.Rejected != "" || c.Source != krateoaudit.ClientSourcePortal || c.ConfirmedSet == nil {
		t.Errorf("the doc's example header is not accepted: %+v", c)
	}

	if !strings.Contains(doc, fmt.Sprintf("**%d bytes**", audit.MaxProvenanceBytes)) {
		t.Errorf("the doc does not state the bound, %d bytes", audit.MaxProvenanceBytes)
	}
	for _, r := range []string{krateoaudit.ClientRejectedOversize, krateoaudit.ClientRejectedVersion,
		krateoaudit.ClientRejectedEncoding, krateoaudit.ClientRejectedSchema} {
		if !strings.Contains(doc, "| `"+r+"` |") {
			t.Errorf("the doc's rejection table has no row for %q", r)
		}
	}
	for _, k := range []string{krateoaudit.KeyClientSource, krateoaudit.KeyClientAgentSessionID, krateoaudit.KeyClientConfirmationID,
		krateoaudit.KeyClientConfirmedSetCount, krateoaudit.KeyClientConfirmedSetIrreversible, krateoaudit.KeyClientConfirmedSetSHA256,
		krateoaudit.KeyClientConfirmedSetOp, krateoaudit.KeyClientRequestedAt, krateoaudit.KeyClientRejected} {
		if !strings.Contains(doc, "`"+k+"`") {
			t.Errorf("the doc does not name %s", k)
		}
	}
}
