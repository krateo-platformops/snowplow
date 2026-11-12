//go:build js && wasm

package fakeapiserver

import (
	"net/http"
	"strings"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// bodyTypes are the request media types the apiserver's serializer accepts for this resource:
// JSON, YAML and, for a kind the scheme registers, protobuf. (CBOR is behind an alpha gate, off.)
func (s *Server) bodyTypes(c *reqCtx) []string {
	if s.custom[c.gr] {
		return []string{"application/json", "application/yaml"}
	}
	return []string{"application/json", "application/yaml", "application/vnd.kubernetes.protobuf"}
}

// unsupportedMediaType is the apiserver's 415 (apimachinery negotiation.NewUnsupportedMediaTypeError).
func unsupportedMediaType(accepted []string) error {
	return apierrors.NewGenericServerResponse(http.StatusUnsupportedMediaType, "", schema.GroupResource{}, "",
		"the body of the request was in an unknown format - accepted media types include: "+strings.Join(accepted, ", "), 0, false)
}
