//go:build js && wasm

package fakeapiserver

import (
	"encoding/hex"
	"fmt"
	"strings"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	utilnet "k8s.io/apimachinery/pkg/util/net"
	"k8s.io/apimachinery/pkg/util/validation/field"
	kjson "sigs.k8s.io/json"
)

// fieldValidation is the request option as the write handlers read it (k8s.io/apiserver v0.36.5
// pkg/endpoints/handlers create.go:117-121, update.go:107-111, patch.go:162-166): Warn and Strict
// decode with the strict serializer, which reports unknown and duplicate fields; Ignore decodes
// without it. "" is the default, Warn. A write with no request (a controller's applyStatus) has no
// options and decodes as Ignore.
func fieldValidation(c *reqCtx) (string, error) {
	if c.r == nil {
		return "Ignore", nil
	}
	v := c.r.URL.Query().Get("fieldValidation")
	switch v {
	case "":
		return "Warn", nil
	case "Ignore", "Warn", "Strict":
		return v, nil
	}
	kind := map[string]string{"create": "CreateOptions", "update": "UpdateOptions", "patch": "PatchOptions"}[c.verb]
	return "", apierrors.NewInvalid(schema.GroupKind{Group: "meta.k8s.io", Kind: kind}, "", field.ErrorList{field.NotSupported(field.NewPath("fieldValidation"), v, []string{"Ignore", "Strict", "Warn"})})
}

func strictDecoding(fv string) bool { return fv == "Warn" || fv == "Strict" }

// transformDecodeError is rest.go:245-256: a create or update body that did not decode, named by the
// kind the decoder read from it when it read one, else summarised (summarizeData, rest.go:355-370).
func transformDecodeError(c *reqCtx, gvk *schema.GroupVersionKind, body []byte, err error) error {
	if gvk != nil && len(gvk.Kind) > 0 {
		return apierrors.NewBadRequest(fmt.Sprintf("%s in version %q cannot be handled as a %s: %v", gvk.Kind, gvk.Version, c.api.Kind, err))
	}
	return apierrors.NewBadRequest(fmt.Sprintf("the object provided is unrecognized (must be of type %s): %v (%s)", c.api.Kind, err, summarizeData(body, 30)))
}

// summarizeData is rest.go:355-370.
func summarizeData(data []byte, maxLength int) string {
	switch {
	case len(data) == 0:
		return "<empty>"
	case data[0] == '{':
		if len(data) > maxLength {
			return string(data[:maxLength]) + " ..."
		}
		return string(data)
	default:
		if len(data) > maxLength {
			return hex.EncodeToString(data[:maxLength]) + " ..."
		}
		return hex.EncodeToString(data)
	}
}

// decodeError is transformDecodeError for a decoded custom resource body: its kind is the one in
// the object.
func decodeError(c *reqCtx, obj map[string]any, body []byte, err error) error {
	var gvk *schema.GroupVersionKind
	if v, _ := obj["apiVersion"].(string); v != "" {
		if k, _ := obj["kind"].(string); k != "" {
			gv, _ := schema.ParseGroupVersion(v)
			gvk = &schema.GroupVersionKind{Group: gv.Group, Version: gv.Version, Kind: k}
		}
	}
	return transformDecodeError(c, gvk, body, err)
}

// patchDecodeError is a patched object that did not decode (patch.go:339-347): 422 on "patch".
func patchDecodeError(patched []byte, err error) error {
	return apierrors.NewInvalid(schema.GroupKind{}, "", field.ErrorList{field.Invalid(field.NewPath("patch"), string(patched), err.Error())})
}

// patchStrictErrors are the strict errors in the patch document itself (patch.go applyJSPatch,
// :389-432), read only under Warn and Strict: duplicate fields, and for a JSON patch unknown
// operation fields. A patch that does not parse is 400.
func patchStrictErrors(fv, patchType string, body []byte) ([]error, error) {
	if !strictDecoding(fv) {
		return nil, nil
	}
	if patchType == "application/json-patch+json" {
		var v []struct {
			Op    string `json:"op"`
			Path  string `json:"path"`
			From  string `json:"from"`
			Value any    `json:"value"`
		}
		errs, err := kjson.UnmarshalStrict(body, &v)
		if err != nil {
			return nil, apierrors.NewBadRequest(fmt.Sprintf("error decoding patch: %v", err))
		}
		for i, e := range errs {
			errs[i] = fmt.Errorf("json patch %v", e)
		}
		return errs, nil
	}
	v := map[string]any{}
	errs, err := kjson.UnmarshalStrict(body, &v)
	if err != nil {
		return nil, apierrors.NewBadRequest(fmt.Sprintf("error decoding patch: %v", err))
	}
	return errs, nil
}

// strictOutcome answers a write's strict decoding errors per fieldValidation: Warn as one Warning
// header each, Strict as the handler's error (a create or update body: 400 through
// transformDecodeError; a patch: 422 on "patch", patch.go:348-366). Ignore never has any.
func strictOutcome(c *reqCtx, fv string, obj map[string]any, body, patched []byte, errs []error) error {
	if len(errs) == 0 || !strictDecoding(fv) {
		return nil
	}
	if fv == "Warn" {
		addStrictDecodingWarnings(c, errs)
		return nil
	}
	sde := runtime.NewStrictDecodingError(errs)
	if c.verb == "patch" {
		return patchDecodeError(patched, sde)
	}
	return decodeError(c, obj, body, sde)
}

// addStrictDecodingWarnings is rest.go:419-446 with the warning recorder's header (k8s.io/apiserver
// pkg/endpoints/filters/warning.go): one "299 - <text>" Warning per message, each message once.
func addStrictDecodingWarnings(c *reqCtx, errs []error) {
	if c.w == nil {
		return
	}
	for _, e := range errs {
		for _, w := range parseYAMLWarnings(e.Error()) {
			h, err := utilnet.NewWarningHeader(299, "-", w)
			if err != nil {
				continue
			}
			dup := false
			for _, have := range c.w.Header().Values("Warning") {
				dup = dup || have == h
			}
			if !dup {
				c.w.Header().Add("Warning", h)
			}
		}
	}
}

// parseYAMLWarnings is rest.go:419-435: a YAML strict error lists one problem per line.
func parseYAMLWarnings(errString string) []string {
	const shortPrefix, longPrefix = "yaml: unmarshal errors:\n", "error converting YAML to JSON: yaml: unmarshal errors:\n"
	var trimmed string
	if s := strings.TrimPrefix(errString, shortPrefix); len(s) < len(errString) {
		trimmed = s
	} else if s := strings.TrimPrefix(errString, longPrefix); len(s) < len(errString) {
		trimmed = s
	} else {
		return []string{errString}
	}
	lines := strings.Split(trimmed, "\n")
	for i, s := range lines {
		lines[i] = strings.TrimSpace(s)
	}
	return lines
}
