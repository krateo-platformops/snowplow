package credhttp

// endpoint_transport_1509.go — #1509 (b): the transport of the owned branch of
// a stage fetch, built here so its https connections are set up by the raced
// TLS dialer (endpointDialTLS).
//
// WHY NOT transport.New ANY MORE. client-go's transport.New builds the base
// http.Transport in its TLS cache (transport/cache.go) and offers a dial hook
// only for the connect (Config.DialHolder); the TLS handshake then runs inside
// net/http, under client-go's 10 s TLSHandshakeTimeout, out of the race's
// reach. A connection is usable only once its handshake is done, so the race
// must own the handshake: net/http's DialTLSContext does exactly that (the
// returned connection is past its handshake; net/http finds it done and hands
// a negotiated "h2" to the HTTP/2 transport).
//
// WHAT IS client-go's, UNCHANGED. Everything but the dial hooks is the code
// client-go runs for these configs, called rather than copied:
//   - the TLS material: transport.TLSConfigFor(cfg);
//   - the base transport's fields as newTransport sets them (cache.go): the
//     proxy, the 10 s TLSHandshakeTimeout (still what bounds a handshake
//     net/http runs itself, through a proxy), MaxIdleConnsPerHost 25, the dial
//     and DisableCompression; then utilnet.SetTransportDefaults (HTTP/2 with
//     its read-idle health check);
//   - every wrapper above it: transport.HTTPWrappersForConfig (WrapTransport,
//     bearer token and its file refresh, basic auth, user agent, impersonation).
// What client-go's cache adds and the owned branch never asks for is refused,
// not dropped: file-based TLS material (its reloading dialer and CA holder), a
// certificate callback, a caller's own transport or dial hook.
//
// One transport per call, never shared: the owned branch keeps one client per
// endpoint identity (endpoints_tls.go, #1088), which is what client-go's cache
// was kept from sharing by the explicit Proxy.

import (
	"context"
	"errors"
	"net"
	"net/http"
	"time"

	utilnet "k8s.io/apimachinery/pkg/util/net"
	"k8s.io/client-go/transport"
)

// endpointIdleConnsPerHost is client-go's idleConnsPerHost (transport/cache.go),
// unexported there.
const endpointIdleConnsPerHost = 25

// endpointTLSHandshakeTimeout is client-go's TLSHandshakeTimeout for the
// transports it builds (transport/cache.go newTransport).
const endpointTLSHandshakeTimeout = 10 * time.Second

// NewEndpointTransport returns the round tripper of the owned branch of a stage
// fetch for cfg (see the file comment).
func NewEndpointTransport(cfg *transport.Config) (http.RoundTripper, error) {
	if cfg == nil {
		return nil, errors.New("credhttp: no transport config")
	}
	switch {
	case cfg.Transport != nil, cfg.DialHolder != nil:
		return nil, errors.New("credhttp: the endpoint transport dials itself; a caller's transport or dial hook is not taken")
	case cfg.TLS.CAFile != "", cfg.TLS.CertFile != "", cfg.TLS.KeyFile != "", cfg.TLS.ReloadTLSFiles, cfg.HasCertCallback():
		return nil, errors.New("credhttp: the endpoint transport takes TLS material as data only")
	}
	tlsCfg, err := transport.TLSConfigFor(cfg)
	if err != nil {
		return nil, err
	}
	proxy := http.ProxyFromEnvironment
	if cfg.Proxy != nil {
		proxy = cfg.Proxy
	}
	ht := utilnet.SetTransportDefaults(&http.Transport{
		Proxy:               proxy,
		TLSHandshakeTimeout: endpointTLSHandshakeTimeout,
		TLSClientConfig:     tlsCfg,
		MaxIdleConnsPerHost: endpointIdleConnsPerHost,
		DialContext:         endpointDial,
		DisableCompression:  cfg.DisableCompression,
	})
	// After SetTransportDefaults: its HTTP/2 configuration adds "h2" to the
	// TLS config's NextProtos (and creates the config when there was none),
	// which every raced handshake must offer.
	ht.DialTLSContext = func(ctx context.Context, network, addr string) (net.Conn, error) {
		return endpointDialTLS(ctx, network, addr, ht.TLSClientConfig)
	}
	return transport.HTTPWrappersForConfig(cfg, ht)
}
