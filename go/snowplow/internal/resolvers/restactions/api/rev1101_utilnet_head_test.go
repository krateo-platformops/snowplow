//go:build unit

package api

import (
	"net/http"

	utilnet "k8s.io/apimachinery/pkg/util/net"
)

func utilnetCloseIdle(rt http.RoundTripper) { utilnet.CloseIdleConnectionsFor(rt) }
