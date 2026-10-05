package redact

import (
	"net/url"
	"strings"
)

// URLUnparseable is the rendering of a URL this package will not vouch for.
// A URL is only ever rendered in clear when every credential-bearing part of
// it has been removed with certainty; when that cannot be guaranteed, the URL
// is not rendered at all.
const URLUnparseable = "<unparseable>"

// URLRedactedValue replaces every query VALUE. The KEYS are kept, because the
// shape of a query ("?token=…&page=…") is the diagnostic value of logging a
// URL at all, while the values are where credentials live (a bearer token, an
// api key, the signature of a pre-signed URL).
const URLRedactedValue = "REDACTED"

// URL renders a URL so it may reach a log line or a span attribute: userinfo
// (`user:password@`) is removed, every query value is replaced, and the
// fragment is dropped. It is the single implementation of "a URL is never
// logged or spanned with its credentials" (#499, #489), shared by the
// endpoint log attr and the outbound-span redactor so the two cannot drift.
//
// THE SCHEMELESS TRAP (#499). The obvious implementation — url.Parse, then
// u.User = nil — is silently wrong for a URL with no scheme:
//
//	url.Parse("u:p@host/x")  →  Scheme "u", Opaque "p@host/x", User NIL
//
// "u:" is read as the SCHEME and the rest as an opaque payload, so there is no
// User to clear, clearing it is a no-op, and u.String() reconstructs the input
// VERBATIM — password included. That shape shipped in 1.12.36 (#499): a
// scheme'd URL was stripped correctly while a schemeless one round-tripped its
// credentials into otel_logs, and the arm covering it only ever tested the
// scheme'd form.
//
// So a schemeless or opaque URL is re-parsed as authority-relative ("//" +
// raw), which makes url.Parse populate Host and User and brings it back under
// the same stripping path, rather than being rendered as-is or thrown away.
// A bare "host:port" survives this and still renders, because it is diagnostic
// and carries nothing.
func URL(raw string) string {
	if raw == "" {
		return ""
	}

	u, err := url.Parse(raw)
	if err != nil {
		return URLUnparseable
	}

	// Opaque != "" is the trap above; Scheme == "" with no Host is a relative
	// URL. Both mean url.Parse did NOT split an authority, so userinfo was
	// never recognised. A protocol-relative "//user:pw@host/x" DOES get its
	// authority split, so it needs no re-parse — only the stripping below.
	schemeless := u.Opaque != "" || (u.Scheme == "" && u.Host == "")
	if schemeless {
		reparsed, rerr := url.Parse("//" + raw)
		if rerr != nil || reparsed.Host == "" {
			return URLUnparseable
		}
		u = reparsed
	}

	u.User = nil
	u.Fragment = ""
	u.RawFragment = ""

	if q := u.Query(); len(q) > 0 {
		for k := range q {
			for i := range q[k] {
				q[k][i] = URLRedactedValue
			}
		}
		u.RawQuery = q.Encode()
	} else {
		// A query that failed to parse into pairs is dropped whole rather than
		// carried through unexamined.
		u.RawQuery = ""
		u.ForceQuery = false
	}

	// Belt and braces: nothing this function returns may carry an authority
	// with userinfo in it, whatever shape produced it.
	if strings.Contains(u.Host, "@") {
		return URLUnparseable
	}

	out := u.String()
	if schemeless {
		out = strings.TrimPrefix(out, "//")
	}
	return out
}
