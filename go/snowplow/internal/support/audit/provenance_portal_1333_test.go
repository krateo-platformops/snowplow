package audit

// provenance_portal_1333_test.go — #1333: the decoder reads what the portal's
// REAL encoder sends. The fixtures (testdata/provenance_portal_1333/
// samples.json) are four headers produced by frontend#695 at d9b251f
// (ui/src/hooks/callProvenance.ts, confirmedWriteOf(...).headerFor(op)), with
// the JSON each carries, the canonical confirmed-set bytes its hash covers,
// and the client keys each must record, written from its JSON by hand.
//
//   P  each sample is the JSON it says, of the length it says; it decodes;
//      it records exactly its client keys; the record passes the library's
//      Check; and where it names a confirmed set, the sha256 it carries is
//      the SHA-256 of the canonical bytes.
//   S  sample 3, a sandbox-confined preview no confirm gate ran for, carries
//      no confirmationId and no confirmedSet, and is valid: the absence is
//      the truthful record that nothing was confirmed.

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"maps"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"

	krateoaudit "github.com/krateo-platformops/audit"
	"github.com/krateo-platformops/audit/audittest"
)

type portalSample1333 struct {
	Name           string            `json:"name"`
	Header         string            `json:"header"`
	HeaderBytes    int               `json:"headerBytes"`
	DecodedJSON    string            `json:"decodedJSON"`
	CanonicalSet   *string           `json:"canonicalSet"`
	WantClientKeys map[string]string `json:"wantClientKeys"`
}

func portalSamples1333(t *testing.T) []portalSample1333 {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join("testdata", "provenance_portal_1333", "samples.json"))
	if err != nil {
		t.Fatal(err)
	}
	var f struct {
		Samples []portalSample1333 `json:"samples"`
	}
	if err := json.Unmarshal(raw, &f); err != nil {
		t.Fatal(err)
	}
	if len(f.Samples) != 4 {
		t.Fatalf("SETUP: %d samples, want the portal's 4", len(f.Samples))
	}
	return f.Samples
}

func TestProvenance1333_P_ThePortalsEncoderIsRead(t *testing.T) {
	rec := audittest.NewRecorder(t)
	em := krateoaudit.New(rec.Logger(Scope), krateoaudit.WithWriter(Writer))
	for _, s := range portalSamples1333(t) {
		t.Run(s.Name, func(t *testing.T) {
			// The fixture is self-consistent: the header carries the JSON.
			if len(s.Header) != s.HeaderBytes {
				t.Fatalf("SETUP: header is %d bytes, the sample says %d", len(s.Header), s.HeaderBytes)
			}
			js, err := base64.RawURLEncoding.DecodeString(strings.TrimPrefix(s.Header, provenancePrefix))
			if err != nil || string(js) != s.DecodedJSON {
				t.Fatalf("SETUP: the header decodes to %q (%v), the sample says %q", js, err, s.DecodedJSON)
			}

			c := ProvenanceOf(http.Header{ProvenanceHeader: {s.Header}})
			if c.Rejected != "" {
				t.Fatalf("RED #1333: the portal's header is rejected: %s", c.Rejected)
			}

			rec.Reset()
			em.Emit(t.Context(), krateoaudit.Event{
				Action: "call", Verb: "create", HTTPMethod: http.MethodPost, Outcome: krateoaudit.OutcomeSuccess,
				StatusCode: http.StatusCreated, Version: "v1", Resource: "configmaps", ObjectName: "x", Namespace: "ns",
				Client: c,
			})
			attrs := audittest.Attributes(rec.Only(t))
			got := map[string]string{}
			for _, kv := range attrs {
				if k := string(kv.Key); strings.HasPrefix(k, "krateo.audit.client.") {
					got[k] = kv.Value.Emit()
				}
			}
			if !maps.Equal(got, s.WantClientKeys) {
				t.Errorf("RED #1333: client keys\n got %v\nwant %v", got, s.WantClientKeys)
			}
			for _, v := range krateoaudit.Check(Writer, attrs) {
				if strings.HasPrefix(v.Key, "krateo.audit.client.") {
					t.Errorf("RED #1333: the library refuses the portal's provenance: %s", v)
				}
			}

			want, named := s.WantClientKeys[krateoaudit.KeyClientConfirmedSetSHA256]
			if named != (s.CanonicalSet != nil) {
				t.Fatalf("SETUP: the sample names a set %t but has canonical bytes %t", named, s.CanonicalSet != nil)
			}
			if named {
				sum := sha256.Sum256([]byte(*s.CanonicalSet))
				if h := hex.EncodeToString(sum[:]); h != want {
					t.Errorf("RED #1333: sha256 of the canonical bytes is %s, the header carries %s", h, want)
				}
			}
		})
	}
}

func TestProvenance1333_S_AnUnconfirmedPreviewIsValid(t *testing.T) {
	var preview *portalSample1333
	for _, s := range portalSamples1333(t) {
		if s.CanonicalSet == nil {
			preview = &s
		}
	}
	if preview == nil {
		t.Fatal("SETUP: no sample without a confirmed set")
	}
	c := ProvenanceOf(http.Header{ProvenanceHeader: {preview.Header}})
	if c.Rejected != "" || c.Source != krateoaudit.ClientSourcePortal || c.AgentSessionID == "" {
		t.Fatalf("RED #1333: the unconfirmed preview's header decoded %+v", c)
	}
	if c.ConfirmationID != "" || c.ConfirmedSet != nil {
		t.Fatalf("an unconfirmed preview asserted a confirmation: %+v", c)
	}
}
