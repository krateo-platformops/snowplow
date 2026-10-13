package resourcesrefstemplate

// issue638_no_body_test.go — #638 (reviewer-628 re-gate, finding 2): the
// resourcesRefsTemplate field render wrote the RAW jq error into the expanded
// ref's id / name / namespace fields, so gojq's preview of the data value (a
// Secret prefix) was served and logged. The fields and the iterator error must
// carry the error class only. RED on 4842cd83.

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"testing"

	xcontext "github.com/krateo-platformops/plumbing/context"
	templatesv1 "github.com/krateo-platformops/snowplow/apis/templates/v1"
	"k8s.io/utils/ptr"
)

const s638Sentinel = "withheld-sentinel-638-zz"

func TestIssue638_TemplateFieldRender_NoResponseBytes(t *testing.T) {
	var logs bytes.Buffer
	ctx := xcontext.BuildContext(context.Background(),
		xcontext.WithLogger(slog.New(slog.NewJSONHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug}))))

	ds := map[string]any{"items": []any{
		map[string]any{"name": "cm-a", "secret": s638Sentinel},
	}}

	// Field render: name/id/namespace expressions fail at runtime on the value.
	items := []templatesv1.ResourceRefTemplate{{
		Iterator: ptr.To("${ .items }"),
		Template: templatesv1.ResourceRef{
			ID:         "${ .secret | keys }",
			Verb:       "get",
			Resource:   "configmaps",
			APIVersion: "v1",
			Name:       "${ .secret + 1 }",
			Namespace:  "${ .secret[] }",
		},
	}}
	refs, err := Resolve(ctx, items, ds)
	if err != nil {
		t.Fatalf("Resolve errored: %v", err)
	}
	if len(refs) != 1 {
		t.Fatalf("expected 1 ref, got %d", len(refs))
	}
	for field, v := range map[string]string{"id": refs[0].ID, "name": refs[0].Name, "namespace": refs[0].Namespace} {
		if strings.Contains(v, s638Sentinel) {
			t.Errorf("ref %s carries response bytes: %q", field, v)
		}
		if !strings.Contains(v, "jq runtime error") {
			t.Errorf("ref %s = %q: the arm did not exercise a runtime error", field, v)
		}
	}

	// Iterator: a runtime error over the value — returned and logged.
	items = []templatesv1.ResourceRefTemplate{{
		Iterator: ptr.To("${ .items[0].secret | keys }"),
		Template: templatesv1.ResourceRef{Verb: "get", Resource: "configmaps", APIVersion: "v1", Name: "x"},
	}}
	_, err = Resolve(ctx, items, ds)
	if err == nil {
		t.Fatal("iterator over a string did not error — the arm cannot see the leak")
	}
	if strings.Contains(err.Error(), s638Sentinel) {
		t.Errorf("iterator error carries response bytes: %v", err)
	}
	if !strings.Contains(logs.String(), "unable to execute iterator") {
		t.Fatalf("the iterator failure was not logged: %s", logs.String())
	}
	if strings.Contains(logs.String(), s638Sentinel) {
		t.Errorf("a log line carries response bytes: %s", logs.String())
	}
}
