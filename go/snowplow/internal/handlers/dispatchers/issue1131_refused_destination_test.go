package dispatchers

// issue1131_refused_destination_test.go — #1131 at the HTTP edge, through the real
// /call chain on the #443 harness: what a caller is told about a required stage
// whose destination the policy refused. Nothing is sent: the refusal is in the
// dialer's control hook.

import (
	"net/http"
	"strings"
	"testing"

	"github.com/krateo-platformops/snowplow/internal/handlers/util"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

func TestIssue1131_RequiredStageRefused_CallSaysThePolicyRefused(t *testing.T) {
	ra := in443RA("refused-1131", i729Filter, i729Stage())
	f := in443Setup(t, &unstructured.Unstructured{Object: ra})
	psSeedClientconfigs(t, "http://169.254.169.254")

	before := takeIn443State(t)
	rec := serveStored(t, f, in443Ctx(f, psAlice), ra)
	after := takeIn443State(t)
	body := rec.Body.String()

	if rec.Code != http.StatusBadGateway {
		t.Fatalf("RED: code=%d, want 502: %s", rec.Code, body)
	}
	st := i709Status(t, rec)
	msg, _ := st["message"].(string)
	if st["kind"] != "Status" || st["code"] != float64(http.StatusBadGateway) {
		t.Fatalf("want a 502 Status, got %s", body)
	}
	if strings.Contains(msg, "upstream answered") {
		t.Fatalf("RED: nothing was sent, and the caller is told an upstream answered: %q", msg)
	}
	for _, want := range []string{"restaction " + h1NS + "/refused-1131", `stage "crd"`, "169.254.169.254:80", "destination policy"} {
		if !strings.Contains(msg, want) {
			t.Fatalf("RED: the message must contain %q, got %q", want, msg)
		}
	}
	if got := rec.Header().Get(util.HeaderStageOutcomes); !strings.Contains(got, `"reason":"DestinationRefused"`) {
		t.Fatalf("RED: the outcomes header does not name the reason: %q", got)
	}
	if len(before.l1Keys) != len(after.l1Keys) {
		t.Fatalf("a refused resolve changed the L1 key set: %d → %d keys", len(before.l1Keys), len(after.l1Keys))
	}
}
