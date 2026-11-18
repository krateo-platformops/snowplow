package rum

// forward_transport_1535_test.go — #1535 review: transientTransport over the errors the transport returns, shaped as
// it returns them (*url.Error around *net.OpError around *os.SyscallError, *net.DNSError, the TLS verification
// error, the dial policy's refusal, an unsupported scheme). The failures a unit test cannot make on a real network
// (a temporary DNS failure, an aborted connection, a broken pipe, an unreachable host or network) are pinned here;
// forward_retry_1533_test.go keeps the arms that make real failures.

import (
	"bytes"
	"crypto/tls"
	"crypto/x509"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"syscall"
	"testing"

	"github.com/krateo-platformops/snowplow/internal/credhttp"
)

// transport1535 is err as http.Client.Do returns it for a POST to the collector.
func transport1535(err error) error {
	return &url.Error{Op: "Post", URL: "http://collector:4318/v1/traces", Err: err}
}

// opError1535 is a syscall failure inside a net.OpError, as the net package returns it.
func opError1535(op, syscallName string, errno syscall.Errno) error {
	return &net.OpError{Op: op, Net: "tcp", Err: os.NewSyscallError(syscallName, errno)}
}

// realClientError1535 is the forwarder client's own error for a POST to rawURL (made without a network: the client
// refuses before sending).
func realClientError1535(t *testing.T, rawURL string) error {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, rawURL, bytes.NewReader([]byte("x")))
	if err != nil {
		t.Fatal(err)
	}
	resp, err := NewForwarder(rawURL).client.Do(req)
	if resp != nil {
		_ = resp.Body.Close()
	}
	if err == nil {
		t.Fatalf("POST %s: no error", rawURL)
	}
	return err
}

func TestForward1535_TransientTransportClassifiesWhatCanHeal(t *testing.T) {
	denied := realClientError1535(t, "http://169.254.169.254/v1/traces")
	if !credhttp.DestinationRefused(denied) {
		t.Fatalf("the metadata address: %v, want the destination policy's refusal", denied)
	}
	for _, tc := range []struct {
		name      string
		err       error
		transient bool
	}{
		// "If the client cannot connect to the server, the client SHOULD retry the connection"
		{"connection refused", transport1535(opError1535("dial", "connect", syscall.ECONNREFUSED)), true},
		{"host unreachable", transport1535(opError1535("dial", "connect", syscall.EHOSTUNREACH)), true},
		{"network unreachable", transport1535(opError1535("dial", "connect", syscall.ENETUNREACH)), true},
		{"DNS temporary failure", transport1535(&net.OpError{Op: "dial", Net: "tcp",
			Err: &net.DNSError{Err: "server misbehaving", Name: "collector", IsTemporary: true}}), true},
		{"DNS timeout", transport1535(&net.OpError{Op: "dial", Net: "tcp",
			Err: &net.DNSError{Err: "i/o timeout", Name: "collector", IsTimeout: true}}), true},
		// "If the server disconnects without returning a response, the client SHOULD retry and send the same request."
		{"connection reset", transport1535(opError1535("read", "read", syscall.ECONNRESET)), true},
		{"connection aborted", transport1535(opError1535("read", "read", syscall.ECONNABORTED)), true},
		{"broken pipe", transport1535(opError1535("write", "write", syscall.EPIPE)), true},
		{"closed without a byte", transport1535(io.EOF), true},
		{"cut mid-answer", transport1535(io.ErrUnexpectedEOF), true},
		// the attempt ran out of time
		{"dial timeout", transport1535(&net.OpError{Op: "dial", Net: "tcp", Err: timeout1535{}}), true},
		// recurs identically on every attempt
		{"DNS name not found", transport1535(&net.OpError{Op: "dial", Net: "tcp",
			Err: &net.DNSError{Err: "no such host", Name: "collector", IsNotFound: true}}), false},
		{"untrusted certificate", transport1535(&tls.CertificateVerificationError{
			Err: x509.UnknownAuthorityError{}}), false},
		{"certificate for another name", transport1535(&tls.CertificateVerificationError{
			Err: x509.HostnameError{Certificate: &x509.Certificate{}, Host: "collector"}}), false},
		{"denied destination", denied, false},
		{"unsupported scheme", realClientError1535(t, "ftp://collector/v1/traces"), false},
	} {
		if got := transientTransport(tc.err); got != tc.transient {
			t.Errorf("%s (%v): transient %v, want %v", tc.name, tc.err, got, tc.transient)
		}
	}
}

// timeout1535 is a net.Error that timed out (a dial or read deadline).
type timeout1535 struct{}

func (timeout1535) Error() string   { return "i/o timeout" }
func (timeout1535) Timeout() bool   { return true }
func (timeout1535) Temporary() bool { return true }
