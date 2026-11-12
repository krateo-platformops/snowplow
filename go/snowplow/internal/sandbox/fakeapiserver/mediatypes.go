//go:build js && wasm

package fakeapiserver

import "k8s.io/apiserver/pkg/endpoints/handlers/negotiation"

// bodyTypes are the request media types the apiserver's serializer accepts for this resource:
// JSON, YAML and, for a kind the scheme registers, protobuf. (CBOR is behind an alpha gate, off.)
func (s *Server) bodyTypes(c *reqCtx) []string {
	if s.custom[c.gr] {
		return []string{"application/json", "application/yaml"}
	}
	return []string{"application/json", "application/yaml", "application/vnd.kubernetes.protobuf"}
}

// unsupportedMediaType is the apiserver's own 415 (k8s.io/apiserver pkg/endpoints/handlers/negotiation).
func unsupportedMediaType(accepted []string) error {
	return negotiation.NewUnsupportedMediaTypeError(accepted)
}
