package rum

// forward_transport_1535_test.go — #1535 review: permanentTransport over the errors the transport returns, shaped as
// it returns them (*url.Error around *net.OpError around *os.SyscallError, *net.DNSError, the TLS verification
// errors, the dial policy's refusal). A failure is permanent only when positively identified as recurring; anything
// else, an error never seen before included, is retried. A scheme that can never be sent is refused by NewForwarder.
// forward_retry_1533_test.go keeps the arms that make real failures.

import (
	"bytes"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"syscall"
	"testing"

	"github.com/krateo-platformops/snowplow/internal/credhttp"
)

// mustForwarder is NewForwarder for a URL the test knows is valid.
func mustForwarder(t *testing.T, collectorURL string) *Forwarder {
	t.Helper()
	f, err := NewForwarder(collectorURL)
	if err != nil {
		t.Fatalf("NewForwarder(%q): %v", collectorURL, err)
	}
	return f
}

// transport1535 is err as http.Client.Do returns it for a POST to the collector.
func transport1535(err error) error {
	return &url.Error{Op: "Post", URL: "http://collector:4318/v1/traces", Err: err}
}

// opError1535 is a syscall failure inside a net.OpError, as the net package returns it.
func opError1535(op, syscallName string, errno syscall.Errno) error {
	return &net.OpError{Op: op, Net: "tcp", Err: os.NewSyscallError(syscallName, errno)}
}

func TestForward1535_PermanentTransportOnlyWhatIsIdentified(t *testing.T) {
	// the dial policy's refusal as the forwarder's own client returns it (no packet is sent)
	deniedURL := "http://169.254.169.254/v1/traces"
	req, _ := http.NewRequest(http.MethodPost, deniedURL, bytes.NewReader([]byte("x")))
	resp, denied := mustForwarder(t, deniedURL).client.Do(req)
	if resp != nil {
		_ = resp.Body.Close()
	}
	if !credhttp.DestinationRefused(denied) {
		t.Fatalf("the metadata address: %v, want the destination policy's refusal", denied)
	}
	for _, tc := range []struct {
		name      string
		err       error
		permanent bool
	}{
		// positively identified: recurs on every attempt
		{"untrusted certificate", transport1535(&tls.CertificateVerificationError{
			Err: x509.UnknownAuthorityError{}}), true},
		{"certificate for another name", transport1535(&tls.CertificateVerificationError{
			Err: x509.HostnameError{Certificate: &x509.Certificate{}, Host: "collector"}}), true},
		{"certificate with an unhandled critical extension", transport1535(&tls.CertificateVerificationError{
			Err: x509.UnhandledCriticalExtension{}}), true},
		{"x509 unknown authority, unwrapped", transport1535(x509.UnknownAuthorityError{}), true},
		{"x509 wrong name, unwrapped", transport1535(x509.HostnameError{Certificate: &x509.Certificate{},
			Host: "collector"}), true},
		{"x509 certificate invalid (expired)", transport1535(x509.CertificateInvalidError{
			Cert: &x509.Certificate{}, Reason: x509.Expired}), true},
		{"denied destination", denied, true},
		{"DNS name not found", transport1535(&net.OpError{Op: "dial", Net: "tcp",
			Err: &net.DNSError{Err: "no such host", Name: "collector", IsNotFound: true}}), true},
		// everything else is retried
		{"an error never seen before", transport1535(errors.New("x")), false},
		{"server closed idle connection (no type, no errno, no EOF)",
			transport1535(errors.New("http: server closed idle connection")), false},
		{"status line cut before its code", transport1535(errors.New(
			`net/http: HTTP/1.x transport connection broken: malformed HTTP status code "20"`)), false},
		{"connection refused", transport1535(opError1535("dial", "connect", syscall.ECONNREFUSED)), false},
		{"host unreachable", transport1535(opError1535("dial", "connect", syscall.EHOSTUNREACH)), false},
		{"network unreachable", transport1535(opError1535("dial", "connect", syscall.ENETUNREACH)), false},
		{"connection reset", transport1535(opError1535("read", "read", syscall.ECONNRESET)), false},
		{"connection aborted", transport1535(opError1535("read", "read", syscall.ECONNABORTED)), false},
		{"broken pipe", transport1535(opError1535("write", "write", syscall.EPIPE)), false},
		{"closed without a byte", transport1535(io.EOF), false},
		{"cut mid-answer", transport1535(io.ErrUnexpectedEOF), false},
		{"DNS temporary failure", transport1535(&net.OpError{Op: "dial", Net: "tcp",
			Err: &net.DNSError{Err: "server misbehaving", Name: "collector", IsTemporary: true}}), false},
		{"DNS timeout", transport1535(&net.OpError{Op: "dial", Net: "tcp",
			Err: &net.DNSError{Err: "i/o timeout", Name: "collector", IsTimeout: true}}), false},
	} {
		if got := permanentTransport(tc.err); got != tc.permanent {
			t.Errorf("%s (%v): permanent %v, want %v", tc.name, tc.err, got, tc.permanent)
		}
	}
}

// TestForward1535_ASchemeThatCanNeverBeSentIsRefusedAtConfiguration: NewForwarder refuses a collector URL that is
// not http or https, or that names no host (#1539), so it never reaches the worker (where it would be retried);
// http and https with a host are accepted (mutations: the scheme check dropped reads a forwarder for ftp://; the host
// check dropped reads one for http:///v1/traces).
func TestForward1535_ASchemeThatCanNeverBeSentIsRefusedAtConfiguration(t *testing.T) {
	for _, bad := range []string{"ftp://collector:4318/v1/traces", "unix:///run/otel.sock", "collector:4318/v1/traces",
		"://collector", "http:///v1/traces", "https://", "http:/v1/traces"} {
		if f, err := NewForwarder(bad); err == nil || f != nil {
			t.Errorf("NewForwarder(%q): forwarder %v, error %v; want refused", bad, f != nil, err)
		}
	}
	for _, good := range []string{"http://10.0.0.1:4318/v1/traces", "https://collector.example/v1/traces",
		"HTTP://10.0.0.1:4318/v1/traces"} {
		if _, err := NewForwarder(good); err != nil {
			t.Errorf("NewForwarder(%q): %v, want accepted", good, err)
		}
	}
}
