package otelresource

import (
	"context"
	"testing"

	"go.opentelemetry.io/otel/attribute"
)

// #462: service.version is the pod's release label when the chart wires it,
// else the build string; the build commit always rides vcs.ref.head.revision.
func TestIssue462_ServiceVersionSourceAndRevision(t *testing.T) {
	const build = "0123456789abcdef0123456789abcdef01234567"
	cases := []struct {
		name, label, build, wantVersion, wantRev string
	}{
		{"label wired", "1.12.99", build, "1.12.99", build},
		{"no label (outside a pod)", "", build, build, build},
		{"unstamped build", "1.12.99", "", "1.12.99", ""},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			t.Setenv(EnvServiceVersion, c.label)
			t.Setenv(EnvPodNamespace, "")
			t.Setenv(EnvPodName, "")
			t.Setenv(EnvPodUID, "")
			res, err := Build(context.Background(), c.build, nil)
			if err != nil {
				t.Fatal(err)
			}
			set := res.Set()
			if v, _ := set.Value(attribute.Key("service.version")); v.AsString() != c.wantVersion {
				t.Errorf("service.version = %q, want %q", v.AsString(), c.wantVersion)
			}
			v, ok := set.Value(AttrVCSRevision)
			if c.wantRev == "" {
				if ok {
					t.Errorf("vcs.ref.head.revision = %q, want absent for an unstamped build", v.AsString())
				}
			} else if v.AsString() != c.wantRev {
				t.Errorf("vcs.ref.head.revision = %q, want %q", v.AsString(), c.wantRev)
			}
		})
	}
}
