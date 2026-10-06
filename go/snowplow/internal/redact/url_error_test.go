package redact

// url_error_test.go — #523, the unit floor under ErrorURL.
//
// The behavioural arms live in the api package
// (errortext_redaction_523_test.go): they drive a credential-bearing ServerURL
// through the real fetch and assert on what the production logger emitted.
// These arms answer the question a failure there would otherwise leave
// ambiguous — "does the site not redact, or does ErrorURL not work?" — and they
// pin BOTH directions: the credential must go, and the diagnostic must stay.

import (
	"errors"
	"fmt"
	"net/url"
	"strings"
	"testing"
)

const (
	s523User  = "tenant-admin"
	s523Pass  = "pa55w0rd-DO-NOT-LEAK-523"
	s523Token = "qu3ry-t0k3n-DO-NOT-LEAK-523"
)

// parseErrorFor returns the REAL *url.Error net/url produces for raw. It is
// built by calling url.Parse rather than by hand, so the arms below cannot drift
// from what the stdlib actually renders.
func parseErrorFor(t *testing.T, raw string) error {
	t.Helper()
	_, err := url.Parse(raw)
	if err == nil {
		t.Fatalf("SETUP: url.Parse(%q) succeeded; this arm needs a URI it REJECTS", raw)
	}
	var ue *url.Error
	if !errors.As(err, &ue) {
		t.Fatalf("SETUP: url.Parse returned %T, not a *url.Error — the structural fix assumes this type", err)
	}
	return err
}

// malformedCredURI is the issue's own trigger: an ordinary credential-bearing
// endpoint URL with a control character in it, which is an operator typo in a
// tenant Secret rather than an attack.
const malformedCredURI = "https://" + s523User + ":" + s523Pass + "@apiserver.example:6443/base\x7f"

// TestS523_ParseErrorLosesTheCredentialAndKeepsTheReason is the defect, directly.
func TestS523_ParseErrorLosesTheCredentialAndKeepsTheReason(t *testing.T) {
	err := parseErrorFor(t, malformedCredURI)

	// The defect itself, asserted so the arm cannot pass because the premise
	// evaporated: the UNREDACTED text really does carry the password.
	if !strings.Contains(err.Error(), s523Pass) {
		t.Fatalf("PREMISE GONE: url.Error no longer renders its URL verbatim (%q). If the stdlib changed, "+
			"this arm is no longer testing #523 and must be rewritten, not deleted", err)
	}

	got := ErrorURL(err).Error()
	if strings.Contains(got, s523Pass) {
		t.Errorf("#523 CREDENTIAL LEAK: the password survived ErrorURL: %q", got)
	}
	if strings.Contains(got, s523User) {
		t.Errorf("#523: the userinfo USER survived ErrorURL: %q", got)
	}
	// BOTH DIRECTIONS. A sanitiser that returned "" would pass every leak check
	// above and be a different defect: the operator needs to know WHAT failed.
	if !strings.Contains(got, "parse") {
		t.Errorf("#523: the Op was lost, so the diagnostic no longer says which step failed: %q", got)
	}
	if !strings.Contains(got, "invalid control character") {
		t.Errorf("#523: the REASON was lost, so the record no longer says why it failed: %q", got)
	}
}

// TestS523_TransportErrorLosesAQueryTokenTheStdlibKeeps is the second class, and
// the one that shows why reusing redact.URL beats net/http's own measure.
//
// net/http wraps a transport failure in a url.Error whose URL went through its
// stripPassword, which replaces the PASSWORD and keeps the userinfo USERNAME and
// the entire query. A ServerURL that carries its credential as a query parameter
// — a pre-signed URL, an api key — is therefore rendered in clear by the stdlib's
// own redaction. This is the shape the arm pins.
func TestS523_TransportErrorLosesAQueryTokenTheStdlibKeeps(t *testing.T) {
	// EXACTLY the shape net/http's client.go builds: Op from the method, URL
	// already stripPassword'd, Err the transport failure.
	stdlibStripped := "https://" + s523User + ":***@apiserver.example:6443/base?token=" + s523Token
	err := &url.Error{
		Op:  "Get",
		URL: stdlibStripped,
		Err: errors.New("dial tcp 127.0.0.1:1: connect: connection refused"),
	}
	if !strings.Contains(err.Error(), s523Token) {
		t.Fatalf("PREMISE GONE: the fixture does not carry the query token: %q", err)
	}

	got := ErrorURL(err).Error()
	if strings.Contains(got, s523Token) {
		t.Errorf("#523 CREDENTIAL LEAK: the QUERY token survived ErrorURL — the stdlib's stripPassword does not "+
			"touch a query, which is the whole reason this re-renders through redact.URL: %q", got)
	}
	if strings.Contains(got, s523User) {
		t.Errorf("#523: the userinfo USER survived: %q", got)
	}
	// The diagnostic: the host, the key name, the verb and the reason all stay.
	for _, want := range []string{"Get", "apiserver.example:6443", "token=", "connection refused"} {
		if !strings.Contains(got, want) {
			t.Errorf("#523: %q was lost from the diagnostic: %q", want, got)
		}
	}
}

// TestS523_AWrappedURLErrorIsStillRedacted covers the shape that makes the naive
// fix wrong. fmt.Errorf renders its message EAGERLY, so rebuilding the inner
// error does nothing to an outer wrapper's text; the chain has to be walked.
func TestS523_AWrappedURLErrorIsStillRedacted(t *testing.T) {
	inner := parseErrorFor(t, malformedCredURI)

	cases := map[string]error{
		"fmt.Errorf %w":      fmt.Errorf("unable to create HTTP Client for endpoint: %w", inner),
		"doubly wrapped":     fmt.Errorf("outer: %w", fmt.Errorf("inner: %w", inner)),
		"errors.Join":        errors.Join(errors.New("first"), inner),
		"join then wrapped":  fmt.Errorf("stage failed: %w", errors.Join(inner, errors.New("second"))),
		"nested url.Error":   &url.Error{Op: "Get", URL: "https://outer@host/x", Err: inner},
		"inner is the whole": inner,
	}
	for name, err := range cases {
		t.Run(name, func(t *testing.T) {
			if !strings.Contains(err.Error(), s523Pass) {
				t.Fatalf("PREMISE GONE: the %s fixture does not carry the password: %q", name, err)
			}
			got := ErrorURL(err).Error()
			if strings.Contains(got, s523Pass) {
				t.Errorf("#523 CREDENTIAL LEAK through a %s error: %q", name, got)
			}
			if !strings.Contains(got, "invalid control character") {
				t.Errorf("#523: the %s error lost its reason: %q", name, got)
			}
		})
	}
}

// TestS523_AnErrorWithoutAURLIsReturnedUNCHANGED is the other direction, and the
// reason this can be applied at a whole boundary rather than site by site: an
// error that carries no URL must come back as the SAME value, not a copy and not
// a rewrite. Ordinary error text must keep rendering exactly as it did.
func TestS523_AnErrorWithoutAURLIsReturnedUNCHANGED(t *testing.T) {
	for _, err := range []error{
		errors.New("response body is neither JSON nor convertible YAML: yaml: line 3: found character that cannot start any token"),
		fmt.Errorf("api %s item %d failed: %s", "stage", 2, "upstream exploded"),
		errors.Join(errors.New("a"), errors.New("b")),
		// Identity text is ErrorText's job, not this one: it must pass through.
		errors.New(`pods is forbidden: User "alice" cannot list resource "pods"`),
	} {
		got := ErrorURL(err)
		if got != err {
			t.Errorf("#523: an error carrying no URL must be returned unchanged; got a new value %q for %q", got, err)
		}
		if got.Error() != err.Error() {
			t.Errorf("#523: ordinary error text was rewritten: %q → %q", err, got)
		}
	}
	if ErrorURL(nil) != nil {
		t.Error("#523: ErrorURL(nil) must be nil")
	}
}

// TestS523_TheChainSurvivesSoErrorsIsAndAsStillWork pins the deliberate
// trade-off: the TEXT is sanitised, the chain is not broken. Breaking errors.Is
// inside a security fix would be a silent behaviour change.
func TestS523_TheChainSurvivesSoErrorsIsAndAsStillWork(t *testing.T) {
	sentinel := errors.New("sentinel")
	err := &url.Error{Op: "Get", URL: "https://u:" + s523Pass + "@host/x", Err: sentinel}

	red := ErrorURL(err)
	if strings.Contains(red.Error(), s523Pass) {
		t.Fatalf("SETUP: the fixture was not redacted at all: %q", red)
	}
	if !errors.Is(red, sentinel) {
		t.Error("#523: errors.Is no longer reaches the wrapped cause through the redacted error")
	}
	var ue *url.Error
	if !errors.As(red, &ue) {
		t.Error("#523: errors.As no longer finds the *url.Error through the redacted error")
	}
}

// TestS523_NoCredentialSurvivesOverAShapeCorpus is the corpus arm. Enumerating
// shapes is how #487 → #499 → #502 each passed while one shape leaked, so the
// claim is made over a generated set rather than over three hand-picked URLs.
//
// It is non-vacuous by construction: every input is first asserted to carry the
// sentinel BEFORE redaction, and the count of such inputs is checked, so a
// corpus that silently stopped containing credentials cannot read as health.
func TestS523_NoCredentialSurvivesOverAShapeCorpus(t *testing.T) {
	schemes := []string{"https://", "http://", "", "//"}
	hosts := []string{"apiserver.example", "apiserver.example:6443", "127.0.0.1:8443"}
	paths := []string{"", "/", "/base", "/base/v1", "/base\x7f", "/ba se"}
	queries := []string{"", "?page=1", "?token=" + s523Token, "?a=1&token=" + s523Token}
	ops := []string{"parse", "Get", "Post"}

	leakyInputs := 0
	for _, sc := range schemes {
		for _, h := range hosts {
			for _, p := range paths {
				for _, q := range queries {
					raw := sc + s523User + ":" + s523Pass + "@" + h + p + q
					for _, op := range ops {
						// Both routes an error can arrive by: the real one from
						// url.Parse when it rejects the URI, and the constructed
						// one net/http builds for a transport failure.
						errs := []error{
							&url.Error{Op: op, URL: raw, Err: errors.New("dial tcp: connection refused")},
						}
						if _, perr := url.Parse(raw); perr != nil {
							errs = append(errs, perr)
						}
						for _, e := range errs {
							before := e.Error()
							if !strings.Contains(before, s523Pass) {
								continue
							}
							leakyInputs++
							got := ErrorURL(e).Error()
							if strings.Contains(got, s523Pass) {
								t.Errorf("#523 CREDENTIAL LEAK: %q → %q", before, got)
							}
							if strings.Contains(got, s523Token) {
								t.Errorf("#523 QUERY TOKEN LEAK: %q → %q", before, got)
							}
						}
					}
				}
			}
		}
	}
	// 4 schemes x 3 hosts x 6 paths x 4 queries x 3 ops = 864 constructed
	// errors, every one of which carries the password before redaction, plus
	// the url.Parse errors for the shapes net/url rejects. The floor is the
	// constructed count: a corpus that stopped carrying credentials, or a loop
	// that stopped running, would fall below it rather than pass empty.
	const wantLeaky = 864
	if leakyInputs < wantLeaky {
		t.Fatalf("NON-VACUITY: only %d corpus inputs carried the sentinel before redaction, want at least %d",
			leakyInputs, wantLeaky)
	}
}

// TestS523_ACredentialFreeURLStillRendersInClear is the cost side, asserted so a
// future tightening of redact.URL cannot quietly turn every error into
// "<unparseable>" and still pass the leak arms above.
func TestS523_ACredentialFreeURLStillRendersInClear(t *testing.T) {
	err := &url.Error{
		Op:  "Get",
		URL: "https://apiserver.example:6443/base?page=2",
		Err: errors.New("context deadline exceeded"),
	}
	got := ErrorURL(err).Error()
	for _, want := range []string{"Get", "apiserver.example:6443", "/base", "context deadline exceeded"} {
		if !strings.Contains(got, want) {
			t.Errorf("#523: a credential-free URL must still render; %q is missing from %q", want, got)
		}
	}
	if strings.Contains(got, URLUnparseable) {
		t.Errorf("#523: an ordinary URL was REFUSED rather than rendered, so every error log site loses its "+
			"diagnostic: %q", got)
	}
}
