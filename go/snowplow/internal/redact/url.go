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

	// THE TEST IS "DID url.Parse SPLIT AN AUTHORITY", NOT "IS THERE A SCHEME".
	// Any URL that came back without a Host had no authority parsed out of it,
	// so its userinfo — if any — is sitting unrecognised in Opaque or Path and
	// u.User is nil. Keying on `Scheme == "" && Host == ""` instead was the
	// first fix's own version of the #499 bug: `https:/u:p@host/x` (one slash)
	// has a scheme AND no host, satisfied neither branch, skipped the re-parse,
	// and round-tripped the password verbatim. So: Host == "" is the trigger,
	// whatever the scheme says.
	//
	// A protocol-relative "//user:pw@host/x" DOES get its authority split, so
	// it keeps its Host here and needs only the stripping below.
	schemeless := u.Opaque != "" || u.Host == ""
	if schemeless {
		reparsed, rerr := url.Parse("//" + raw)
		// A re-parsed host ending in ":" means the original's own scheme was
		// swallowed into the authority ("jdbc:mysql://u:p@host/db" →
		// Host "jdbc:mysql:", with the real userinfo left in Path). The result
		// would be neither correct nor safe, so it is refused rather than
		// rendered.
		if rerr != nil || reparsed.Host == "" || strings.HasSuffix(reparsed.Host, ":") {
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

	out := u.String()
	if schemeless {
		out = strings.TrimPrefix(out, "//")
	}

	// THE CATCH-ALL, and the one check that does not depend on having reasoned
	// correctly about url.Parse's shapes. Credentials live before an "@"; query
	// values (where a token would otherwise hide) are already replaced above.
	// So nothing this function returns may contain an "@" AT ALL — if one
	// survived, some shape outwitted the parsing above and the URL is not
	// rendered. Inspecting u.Host alone was not enough: for an opaque URL the
	// real "@" ends up in Path, where the old guard never looked.
	//
	// The cost is that a URL with a literal "@" in its PATH renders as
	// unparseable. That is deliberate: a lost diagnostic is cheaper than a
	// leaked credential, and it keeps the invariant stated as something a test
	// can assert over any corpus rather than over an enumerated shape list.
	// "%40" is "@" that survived encoding: "u:p%40host/x" has no literal "@" for
	// the check above to catch, yet it reads back as the userinfo "u:p".
	if strings.Contains(out, "@") || strings.Contains(strings.ToUpper(out), "%40") {
		return URLUnparseable
	}
	return out
}
