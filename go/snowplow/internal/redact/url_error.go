package redact

import (
	"net/url"
	"strings"
)

// maxErrorChainNodes bounds the walk over an error chain. It is a budget, not a
// visited set: an error type is not required to be comparable (a struct with a
// slice field is not), so keying a map by error value would panic on exactly the
// input this package exists to handle safely. The budget also makes a cyclic
// Unwrap terminate.
const maxErrorChainNodes = 64

// ErrorURL returns err with every URL it carries STRUCTURALLY redacted (#523).
//
// THE DEFECT. #503 routed every log site that reads an endpoint's ServerURL
// FIELD through URL. The same credential still reached the same Error record by
// a second route: net/url.Error renders its URL verbatim.
//
//	url.Parse("https://u:pa ss@host/base\x7f")
//	→ parse "https://u:pa ss@host/base\x7f": net/url: invalid control character in URL
//
// external_fetch.go builds that URI straight off Endpoint.ServerURL and folds
// the *url.Error into the response envelope; resolve.go logs the envelope's
// Message at Error, which is always on in production. URL cannot touch it — it
// is error TEXT, not a URL field.
//
// WHY THIS IS STRUCTURAL AND NOT A PATTERN. net/url.Error is a standard type
// with Op, URL and Err fields, and its Error() method is a pure function of the
// three. So the credential is not "somewhere in free prose": it is one named
// field, and the sanitised text is the SAME type re-rendered with URL(e.URL) in
// place of e.URL. Nothing here matches a credential shape, scans for an "@", or
// guesses where a URL starts and ends in prose — which is the pattern-matching
// #502 was built to avoid and which ErrorText (below) is deliberately limited
// to.
//
// WHY NOT ErrorText. ErrorText takes a STRING. By the time an error has been
// rendered to one, url.Error's fields are gone and only a regex over prose could
// find the URL again — so the input type itself forecloses the structural fix.
// ErrorText is also registered in the #503 guard as explicitly NOT a URL
// sanitiser; teaching it to strip URLs would silently widen that clean set for
// every one of its existing callers. It keeps its #453 identity-pattern job, and
// this is a separate function with a separate guarantee.
//
// PRIOR ART, AND WHY IT IS NOT ENOUGH. net/http does the same thing to its own
// client errors — client.go wraps a transport failure in a url.Error whose URL
// went through stripPassword. That replaces the PASSWORD only: it keeps the
// userinfo USERNAME and the whole query string, so a ServerURL of the shape
// "https://svc@host/base?token=…" still renders its token in clear. URL removes
// the userinfo entirely and replaces every query VALUE, so re-rendering through
// it is strictly stronger than the stdlib's own measure, and it is the same
// single implementation #499/#502 hardened rather than a second one that can
// drift.
//
// WRAPPED ERRORS. fmt.Errorf("%w") renders its message EAGERLY, so rebuilding an
// inner url.Error does not change an outer wrapper's text. The whole chain is
// therefore walked, outermost first, and each url.Error found contributes one
// EXACT substring replacement: its own Error() (the raw rendering) for its
// re-rendered form. The needle is read off the error VALUE, never guessed from a
// pattern, so it cannot silently stop matching the way a regex over arbitrary
// text does. Outermost-first matters: an outer rendering contains the inner one
// verbatim, so replacing the outer first leaves the inner needle intact for the
// next step.
//
// WHAT IT RETURNS. An err carrying no *url.Error comes back UNCHANGED (the same
// error value, not a copy), so applying this at a boundary where most errors
// carry no URL costs nothing and changes nothing. Otherwise the result carries
// the sanitised TEXT and Unwraps to the original, so errors.Is and errors.As
// keep working — breaking those inside a security fix would be a silent
// behaviour change. The consequence is stated rather than implied: a caller that
// errors.As's its way back to the original *url.Error and re-renders THAT gets
// the raw URL, exactly as a caller that reads Endpoint.ServerURL does. This
// function governs the text that reaches a record; it is not a capability
// boundary.
//
// WHAT IT DOES NOT COVER. An error whose text was composed from a URL by
// something that is not a url.Error — plumbing's parseProxyURL does
// `fmt.Errorf("could not parse: %v", proxyURL)` — has no structure left to read,
// so this cannot fix it and does not pretend to. That is the #453 error-text
// residual and the #500 taint-pass gap; the parseProxyURL instance is #536.
func ErrorURL(err error) error {
	if err == nil {
		return nil
	}
	ues := urlErrorsIn(err)
	if len(ues) == 0 {
		return err
	}

	raw := err.Error()
	out := raw
	for _, ue := range ues {
		before := ue.Error()
		after := (&url.Error{Op: ue.Op, URL: URL(ue.URL), Err: ue.Err}).Error()
		if before == after {
			continue
		}
		out = strings.ReplaceAll(out, before, after)
	}
	if out == raw {
		return err
	}
	return &redactedURLError{text: out, cause: err}
}

// urlErrorsIn returns every *url.Error in err's chain, OUTERMOST FIRST. Both
// Unwrap shapes are followed: the single-error one and the errors.Join one.
func urlErrorsIn(err error) []*url.Error {
	var out []*url.Error
	budget := maxErrorChainNodes
	var walk func(error)
	walk = func(e error) {
		for e != nil && budget > 0 {
			budget--
			if ue, ok := e.(*url.Error); ok {
				out = append(out, ue)
			}
			switch x := e.(type) {
			case interface{ Unwrap() []error }:
				for _, sub := range x.Unwrap() {
					walk(sub)
				}
				return
			case interface{ Unwrap() error }:
				e = x.Unwrap()
			default:
				return
			}
		}
	}
	walk(err)
	return out
}

// redactedURLError carries the sanitised TEXT while keeping the original chain
// reachable, so errors.Is and errors.As behave exactly as they did before the
// redaction.
type redactedURLError struct {
	text  string
	cause error
}

func (e *redactedURLError) Error() string { return e.text }

func (e *redactedURLError) Unwrap() error { return e.cause }
