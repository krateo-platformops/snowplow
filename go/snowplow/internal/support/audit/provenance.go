package audit

// provenance.go — #1333 (AUDIT-4): the portal's write provenance, read from the
// X-Krateo-Provenance request header of a /call write or dry run, validated
// against the header's closed schema, and handed to the shared writer as
// krateoaudit.ClientProvenance (Event.Client -> krateo.audit.client.*).
//
// The header is the client's word. Nothing in it is identity, the actor's
// class, the agent's identity, the verb, the target, the namespace, the
// outcome, the status or the resolved time: snowplow establishes those itself,
// and the schema below has no field for any of them, so a header that names
// one is rejected as `schema`.
//
// The header value is never logged and never put on a span: it reaches the
// audit record only, and only as the validated fields (or the rejection
// reason).
//
// Frontend contract: docs/howto-frontend-call-provenance.md.

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	krateoaudit "github.com/krateo-platformops/audit"
)

const (
	// ProvenanceHeader is the request header a client sends its write
	// provenance in (#1333).
	ProvenanceHeader = "X-Krateo-Provenance"

	// MaxProvenanceBytes is the longest header value snowplow reads, in bytes
	// of the encoded value ("v1." included): 2 KiB. It is #1333's contract
	// ("at most 2 KiB encoded"), agreed with the frontend, not a tuning knob;
	// a longer value is rejected as `oversize` without being decoded.
	MaxProvenanceBytes = 2 << 10

	// provenancePrefix opens every value of the one header version snowplow
	// reads: "v1." then the base64url (RFC 4648 §5, no padding) of the JSON.
	provenancePrefix = "v1."
)

// ClientProvenance is the shared writer's client provenance (Event.Client).
type ClientProvenance = krateoaudit.ClientProvenance

// ProvenanceOf is the client provenance h carries: the zero value (no client
// key on the record) when h has no X-Krateo-Provenance, else the header
// decoded and validated (decodeProvenance). Several field lines of the header
// are one value, joined with ", " as HTTP combines them (RFC 9110 §5.3), so
// they are never read as one of them.
func ProvenanceOf(h http.Header) ClientProvenance {
	vs := h.Values(ProvenanceHeader)
	if len(vs) == 0 {
		return ClientProvenance{}
	}
	return decodeProvenance(strings.Join(vs, ", "))
}

// decodeProvenance validates one header value and returns what it asserts,
// or, when any rule fails, a ClientProvenance carrying only the reason
// (Rejected): a rejected header asserts nothing. The checks run in this
// order, and the first that fails names the reason:
//
//   - oversize: the value is longer than MaxProvenanceBytes;
//   - version:  it does not start with "v1.";
//   - encoding: the rest is not unpadded base64url of a UTF-8 JSON text;
//   - schema:   the JSON breaks the closed schema (provenanceSchema).
func decodeProvenance(v string) ClientProvenance {
	if len(v) > MaxProvenanceBytes {
		return rejected(krateoaudit.ClientRejectedOversize)
	}
	payload, ok := strings.CutPrefix(v, provenancePrefix)
	if !ok {
		return rejected(krateoaudit.ClientRejectedVersion)
	}
	raw, ok := decodeBase64URL(payload)
	if !ok || !utf8.Valid(raw) || !json.Valid(raw) {
		return rejected(krateoaudit.ClientRejectedEncoding)
	}
	c, ok := provenanceSchema(raw)
	if !ok {
		return rejected(krateoaudit.ClientRejectedSchema)
	}
	return c
}

func rejected(reason string) ClientProvenance { return ClientProvenance{Rejected: reason} }

// decodeBase64URL decodes s as base64url without padding, strictly: every
// byte must be of the base64url alphabet (the standard decoder skips CR and
// LF, which no encoder emits), and the trailing bits must be zero, so one
// payload has one encoding.
func decodeBase64URL(s string) ([]byte, bool) {
	for i := 0; i < len(s); i++ {
		if !isBase64URL(s[i]) {
			return nil, false
		}
	}
	b, err := base64.RawURLEncoding.Strict().DecodeString(s)
	return b, err == nil
}

func isBase64URL(c byte) bool {
	return c >= 'A' && c <= 'Z' || c >= 'a' && c <= 'z' || c >= '0' && c <= '9' || c == '-' || c == '_'
}

// The header's JSON keys (#1333).
const (
	fieldSource         = "source"
	fieldAgentSessionID = "agentSessionId"
	fieldConfirmationID = "confirmationId"
	fieldConfirmedSet   = "confirmedSet"
	fieldOp             = "op"
	fieldRequestedAt    = "requestedAt"

	fieldSetCount        = "count"
	fieldSetIrreversible = "irreversible"
	fieldSetSHA256       = "sha256"
)

var (
	// opaqueID is agentSessionId: 1 to 128 of [A-Za-z0-9_-] (the library's
	// opaque_id format).
	opaqueID = regexp.MustCompile(`^[A-Za-z0-9_-]{1,128}$`)
	// lowerUUID is confirmationId: a UUID in its canonical textual form,
	// lowercase (the library's uuid format).
	lowerUUID = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`)
	// lowerSHA256 is confirmedSet.sha256: 64 lowercase hex digits.
	lowerSHA256 = regexp.MustCompile(`^[0-9a-f]{64}$`)
	// rfc3339Millis is requestedAt: RFC 3339 with exactly three fractional
	// digits, "Z" or a numeric offset (JavaScript's Date.toISOString form).
	rfc3339Millis = regexp.MustCompile(`^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}(Z|[+-]\d{2}:\d{2})$`)
)

// provenanceSchema reads raw (a JSON text) against the header's closed schema,
// field by field:
//
//	source          string, required, "portal"
//	agentSessionId  string, optional, 1-128 of [A-Za-z0-9_-]
//	confirmationId  string, optional, lowercase UUID
//	confirmedSet    object, with op: {count int >= 1, irreversible bool,
//	                sha256 64 lowercase hex}, all three required, no other key
//	op              int, with confirmedSet: >= 0 and < confirmedSet.count
//	requestedAt     string, optional, RFC 3339 with milliseconds
//
// The top level is an object; a key not listed, a key given twice, a value of
// another JSON type (null included) or a value outside its rule fails it.
// source is required because the library writes no asserted key without it,
// and confirmedSet and op come together because the library writes the four
// confirmed_set keys together.
func provenanceSchema(raw []byte) (ClientProvenance, bool) {
	fields, ok := jsonObject(raw)
	if !ok {
		return ClientProvenance{}, false
	}
	var c ClientProvenance
	var set *krateoaudit.ConfirmedSet
	op, hasOp := int64(0), false
	for k, v := range fields {
		switch k {
		case fieldSource:
			s, ok := jsonString(v)
			if !ok || s != krateoaudit.ClientSourcePortal {
				return ClientProvenance{}, false
			}
			c.Source = s
		case fieldAgentSessionID:
			s, ok := jsonString(v)
			if !ok || !opaqueID.MatchString(s) {
				return ClientProvenance{}, false
			}
			c.AgentSessionID = s
		case fieldConfirmationID:
			s, ok := jsonString(v)
			if !ok || !lowerUUID.MatchString(s) {
				return ClientProvenance{}, false
			}
			c.ConfirmationID = s
		case fieldConfirmedSet:
			if set, ok = confirmedSet(v); !ok {
				return ClientProvenance{}, false
			}
		case fieldOp:
			if op, ok = jsonInt(v); !ok || op < 0 {
				return ClientProvenance{}, false
			}
			hasOp = true
		case fieldRequestedAt:
			s, ok := jsonString(v)
			if !ok || !rfc3339Millis.MatchString(s) {
				return ClientProvenance{}, false
			}
			t, err := time.Parse(time.RFC3339, s)
			if err != nil {
				return ClientProvenance{}, false
			}
			c.RequestedAt = t
		default:
			return ClientProvenance{}, false
		}
	}
	if c.Source == "" || (set != nil) != hasOp {
		return ClientProvenance{}, false
	}
	if set != nil {
		if op >= set.Count {
			return ClientProvenance{}, false
		}
		set.Op = op
		c.ConfirmedSet = set
	}
	return c, true
}

// confirmedSet reads the confirmedSet object: exactly count, irreversible and
// sha256, each by its rule.
func confirmedSet(raw json.RawMessage) (*krateoaudit.ConfirmedSet, bool) {
	fields, ok := jsonObject(raw)
	if !ok || len(fields) != 3 {
		return nil, false
	}
	var s krateoaudit.ConfirmedSet
	for k, v := range fields {
		switch k {
		case fieldSetCount:
			if s.Count, ok = jsonInt(v); !ok || s.Count < 1 {
				return nil, false
			}
		case fieldSetIrreversible:
			if s.Irreversible, ok = jsonBool(v); !ok {
				return nil, false
			}
		case fieldSetSHA256:
			if s.SHA256, ok = jsonString(v); !ok || !lowerSHA256.MatchString(s.SHA256) {
				return nil, false
			}
		default:
			return nil, false
		}
	}
	return &s, true
}

// jsonObject reads raw (a valid JSON text) as an object, refusing anything
// else and a key that appears twice: encoding/json would keep the last of two
// and match a key case-insensitively, and a closed schema does neither.
func jsonObject(raw []byte) (map[string]json.RawMessage, bool) {
	dec := json.NewDecoder(bytes.NewReader(raw))
	if tok, err := dec.Token(); err != nil || tok != json.Delim('{') {
		return nil, false
	}
	out := map[string]json.RawMessage{}
	for dec.More() {
		tok, err := dec.Token()
		if err != nil {
			return nil, false
		}
		k, _ := tok.(string)
		if _, dup := out[k]; dup {
			return nil, false
		}
		var v json.RawMessage
		if err := dec.Decode(&v); err != nil {
			return nil, false
		}
		out[k] = v
	}
	return out, true
}

// jsonString reads a JSON string (never null).
func jsonString(raw json.RawMessage) (string, bool) {
	if len(raw) == 0 || raw[0] != '"' {
		return "", false
	}
	var s string
	return s, json.Unmarshal(raw, &s) == nil
}

// jsonInt reads a JSON number written as an integer (no fraction, no
// exponent) that fits an int64.
func jsonInt(raw json.RawMessage) (int64, bool) {
	n, err := strconv.ParseInt(string(raw), 10, 64)
	return n, err == nil
}

// jsonBool reads a JSON true or false.
func jsonBool(raw json.RawMessage) (bool, bool) {
	switch string(raw) {
	case "true":
		return true, true
	case "false":
		return false, true
	}
	return false, false
}
