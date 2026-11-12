package middleware_test

import (
	"net/http"
	"testing"

	krateoaudit "github.com/krateo-platformops/audit"
	"github.com/krateo-platformops/audit/audittest"

	"github.com/krateo-platformops/snowplow/internal/handlers"
	"github.com/krateo-platformops/snowplow/internal/support/audit"
)

// M (audit22_refused_rows_test.go) — a /call write the handler would refuse as
// malformed is still the route's one row when UserConfig refuses it first:
// denied, the credential reason, its target unresolved (marked target).
func TestAudit22_UserConfig_M_AMalformedWriteRefusedFirstIsOneRow(t *testing.T) {
	code, recs := serve22(t, ptrRoute(handlers.CallRoute()), handlers.Call(), http.MethodPut,
		"/call?apiVersion=v1&resource=configmaps&namespace=default&name=cm-x%2Fstatus", cm22, su994Group)
	if code != http.StatusUnauthorized || len(recs) != 1 {
		t.Fatalf("code %d, %d rows; want 401 and exactly 1", code, len(recs))
	}
	a := map[string]string{}
	for _, kv := range audittest.Attributes(recs[0]) {
		a[string(kv.Key)] = kv.Value.Emit()
	}
	if a[krateoaudit.KeyOutcome] != krateoaudit.OutcomeDenied || a[krateoaudit.KeyDenialReason] != audit.DenialSessionCredentialMissing {
		t.Errorf("outcome %q reason %q", a[krateoaudit.KeyOutcome], a[krateoaudit.KeyDenialReason])
	}
	if v, ok := a[krateoaudit.KeyObjectResource]; ok {
		t.Errorf("an unresolved target is named: resource %q", v)
	}
	opt := audittest.AllowIncomplete(krateoaudit.RuleTarget)
	audittest.AssertConformant(t, audit.Writer, recs[0], opt)
	audittest.AssertRequiredKeysDetected(t, audit.Writer, recs[0], opt)
	audittest.AssertEnumsDetected(t, audit.Writer, recs[0], opt)
}
