package api

// audit_schema_version_test.go — every audit row a RESTAction resolve emits
// carries krateo.audit.schema_version = "1".
//
// The rows reach the collector through one funnel (audit.Emit → the shared
// writer krateo-platformops/audit, which stamps the key on every record it
// writes); this arm reads the records the real resolve hands to the
// LoggerProvider, for every class of stage row: the sent writes (an own-cluster
// write, a named-endpoint write, a write the apiserver refused) and every
// refusal (#1379/#1384: per-user credential ref, malformed endpoint, Secret
// read failure, dependency failed, userAccessFilter on a write verb, invalid
// path, template error, dry run, export). A record without the key, or with a
// value other than "1", fails it.

import (
	"context"
	"net/http"
	"testing"

	krateoaudit "github.com/krateo-platformops/audit"
	"github.com/krateo-platformops/audit/audittest"
	"github.com/krateo-platformops/plumbing/kubeutil"
	"github.com/krateo-platformops/plumbing/ptr"
	templates "github.com/krateo-platformops/snowplow/apis/templates/v1"
	"github.com/krateo-platformops/snowplow/internal/cache"
	sdklog "go.opentelemetry.io/otel/sdk/log"
)

// schemaVersionOf returns every value of krateo.audit.schema_version on rec.
func schemaVersionOf(rec sdklog.Record) []string {
	var out []string
	for _, kv := range audittest.Attributes(rec) {
		if string(kv.Key) == krateoaudit.KeySchemaVersion {
			out = append(out, kv.Value.Emit())
		}
	}
	return out
}

func TestAuditSchemaVersion_EveryStageRowCarriesV1(t *testing.T) {
	own := kubeutil.MakeDNS1123Compatible(user410) + "-clientconfig"
	uaf := stage410("uaf", http.MethodPost, base22)
	uaf.UserAccessFilter = &templates.UserAccessFilterSpec{Verb: "get", Resource: "fireworksapps"}
	dependent := stage410("after", http.MethodPost, base22)
	dependent.ContinueOnError = ptr.To(false)
	dependent.DependsOn = &templates.Dependency{Name: "first"}
	total := 0
	for _, c := range []struct {
		name   string
		mark   func(context.Context) context.Context
		stages []*templates.API
	}{
		{"sent writes: own cluster, named endpoint, refused by the apiserver", nil, []*templates.API{
			extStage410("render", http.MethodPost, "/render"),
			stage410("create", http.MethodPost, base22),
			stage410("denied", http.MethodPost, denyPath410),
		}},
		{"per_user_credential_ref", nil, []*templates.API{refStage22("s", http.MethodPost, "/render", own)}},
		{"endpoint_ref_malformed", nil, []*templates.API{refStage22("s", http.MethodPost, "/render", brokenRef22)}},
		{"the Secret read failed", nil, []*templates.API{refStage22("s", http.MethodPost, "/render", absentRef22)}},
		{"dependency_failed", nil, []*templates.API{stage410("first", http.MethodPost, denyPath410), dependent}},
		{"uaf_on_write_verb", nil, []*templates.API{uaf}},
		{"invalid_path", nil, []*templates.API{stage410("s", http.MethodPost, base22+"/a%2Fb")}},
		{"template_error", nil, []*templates.API{stage410("s", http.MethodPost, `${ error("x22") }`)}},
		{"dry_run_not_executed", cache.WithInert, []*templates.API{stage410("s", http.MethodPost, base22)}},
		{"write_refused (an export)", cache.WithExportRead, []*templates.API{stage410("s", http.MethodPost, base22)}},
	} {
		t.Run(c.name, func(t *testing.T) {
			recs := resolve22(t, c.mark, c.stages...)
			if len(recs) == 0 {
				t.Fatal("PRECONDITION: no row was emitted, so the arm proves nothing")
			}
			for _, r := range recs {
				total++
				if got := schemaVersionOf(r); len(got) != 1 || got[0] != "1" {
					t.Errorf("stage %q row: krateo.audit.schema_version = %v, want exactly [\"1\"]",
						attrs22(r)[krateoaudit.KeyRESTActionStage], got)
				}
			}
		})
	}
	if total < 12 {
		t.Fatalf("PRECONDITION: %d rows checked, want at least one per case", total)
	}
}
