package main

import (
	"os"
	"path/filepath"
	"testing"

	"sigs.k8s.io/yaml"
)

// The repo's own file passes the structural checks, and the offline controls
// refuse every malformed shape (the registry is not asked here).
func TestRetractedVersions_FileAndControls(t *testing.T) {
	root := filepath.Join("..", "..", "..", "..")
	known, err := publishedCharts(root)
	if err != nil {
		t.Fatal(err)
	}
	if p := controls(known, false); len(p) > 0 {
		t.Fatalf("controls: %v", p)
	}
	b, err := os.ReadFile(filepath.Join(root, "retracted-versions.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	var doc any
	if err := yaml.Unmarshal(b, &doc); err != nil {
		t.Fatal(err)
	}
	failures, entries := check(doc, known)
	if len(failures) > 0 || len(entries) == 0 {
		t.Fatalf("retracted-versions.yaml: failures %v, %d entries", failures, len(entries))
	}
}
