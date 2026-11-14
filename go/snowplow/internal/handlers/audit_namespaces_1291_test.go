package handlers

import (
	"context"

	"github.com/krateo-platformops/snowplow/internal/support/audit"
)

// unlabelledNamespaces1291 is a cluster in which every Namespace exists and none
// is in the preview sandbox: the preview flag of every record these tests
// emit is read (false), never left unknown. Arms about the flag install their
// own reader.
type unlabelledNamespaces1291 struct{}

func (unlabelledNamespaces1291) NamespaceLabels(context.Context, string) (map[string]string, bool, error) {
	return map[string]string{}, true, nil
}

func init() { audit.SetNamespaceReader(unlabelledNamespaces1291{}) }
