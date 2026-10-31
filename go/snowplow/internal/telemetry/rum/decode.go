package rum

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
)

// Structural caps, enforced WHILE decoding (#1020 review F1). The body cap
// (MaxBodyBytes) bounds the bytes; these bound what those bytes may expand
// into. 512 KiB of `{}` is ~174K empty spans, and decoding them into a slice
// before counting allocated ~109 MiB per request. Now the walker counts each
// array element as it reaches it and stops at cap+1, so what a request can
// allocate is set by these caps, not by the shape of the input. Exceeding any
// of them answers 413.
const (
	// MaxSpansPerRequest: the browser exporter's default maxExportBatchSize.
	MaxSpansPerRequest = 512
	// MaxAttributesPerSpan: a krateo.* span carries 3 allowlisted attributes;
	// the browser fetch instrumentation adds about ten more, all dropped.
	MaxAttributesPerSpan = 64
	// MaxResourceAttributes: the browser SDK resource carries about ten.
	MaxResourceAttributes = 64
	// MaxResourceSpans / MaxScopeSpans: the browser sends one resource and
	// one scope per instrumentation; these bound the containers around spans.
	MaxResourceSpans = 16
	MaxScopeSpans    = 64
)

// capError is a structural cap exceeded while decoding (→ 413).
type capError struct{ what string }

func (e capError) Error() string { return "too many " + e.what }

// errMalformed is a body that is not OTLP/JSON (→ 400).
var errMalformed = errors.New("body is not OTLP/JSON")

// decodeCapped walks an OTLP/JSON ExportTraceServiceRequest from r with
// encoding/json's token API. Only the allowlisted fields are kept, every array
// on the way to a span is counted element by element against its cap, and any
// other value is skipped without being materialised (discard). Errors from r
// (e.g. *http.MaxBytesError) are returned unwrapped-compatible.
func decodeCapped(r io.Reader) (*exportRequest, error) {
	d := &walker{dec: json.NewDecoder(r)}
	req := &exportRequest{}
	err := d.object(func(key string) error {
		if key != "resourceSpans" {
			return d.skip()
		}
		return d.array(MaxResourceSpans, &d.resourceSpans, "resourceSpans", func() error {
			res, err := d.resourceSpan()
			if err == nil {
				req.resources = append(req.resources, res)
			}
			return err
		})
	})
	if err != nil {
		return nil, err
	}
	// Nothing but whitespace may follow the request object.
	if _, err := d.dec.Token(); err != io.EOF {
		if err == nil {
			return nil, errMalformed
		}
		return nil, d.wrap(err)
	}
	return req, nil
}

type walker struct {
	dec *json.Decoder
	// running totals across the whole request
	resourceSpans, scopeSpans, spans int
}

// discard is a json.Unmarshaler that keeps nothing: Decode into it scans a
// value without building it (no maps, slices or strings are allocated).
type discard struct{}

func (*discard) UnmarshalJSON([]byte) error { return nil }

func (d *walker) skip() error {
	var x discard
	return d.wrap(d.dec.Decode(&x))
}

// wrap turns syntax, type and truncation errors into errMalformed and passes
// every other error through unchanged, so the body reader's own error (the
// *http.MaxBytesError of the body cap) stays visible to the caller.
func (d *walker) wrap(err error) error {
	if err == nil {
		return nil
	}
	var syn *json.SyntaxError
	var typ *json.UnmarshalTypeError
	if errors.As(err, &syn) || errors.As(err, &typ) || errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, io.EOF) {
		return errMalformed
	}
	return err // e.g. *http.MaxBytesError from the body reader
}

// open consumes the next token and requires the delimiter want. A JSON null
// in its place reports present=false (an absent container).
func (d *walker) open(want json.Delim) (present bool, err error) {
	tok, err := d.dec.Token()
	if err != nil {
		return false, d.wrap(err)
	}
	if tok == nil {
		return false, nil
	}
	if delim, ok := tok.(json.Delim); ok && delim == want {
		return true, nil
	}
	return false, errMalformed
}

// object walks an object, calling field for each key; field must consume the
// value. A key repeated within one object is refused (errMalformed, 400):
// #1020 delta D1 showed that a repeated "attributes" restarted the element
// count, so 100 × "attributes":[64 entries] slipped 6,400 attributes past a
// cap of 64. Refusing the repeat removes the bypass, and with it any question
// of whether the first, the last or every occurrence counts.
func (d *walker) object(field func(key string) error) error {
	present, err := d.open('{')
	if err != nil || !present {
		return err
	}
	seen := make(map[string]struct{}, 4)
	for d.dec.More() {
		tok, err := d.dec.Token()
		if err != nil {
			return d.wrap(err)
		}
		key, ok := tok.(string)
		if !ok {
			return errMalformed
		}
		if _, dup := seen[key]; dup {
			return errMalformed
		}
		seen[key] = struct{}{}
		if err := field(key); err != nil {
			return err
		}
	}
	_, err = d.dec.Token() // '}'
	return d.wrap(err)
}

// array walks an array, counting each element into *count BEFORE decoding it
// and failing at limit+1; elem must consume the element.
func (d *walker) array(limit int, count *int, what string, elem func() error) error {
	present, err := d.open('[')
	if err != nil || !present {
		return err
	}
	for d.dec.More() {
		*count++
		if *count > limit {
			return capError{what: fmt.Sprintf("%s (cap %d)", what, limit)}
		}
		if err := elem(); err != nil {
			return err
		}
	}
	_, err = d.dec.Token() // ']'
	return d.wrap(err)
}

func (d *walker) value(v any) error { return d.wrap(d.dec.Decode(v)) }

func (d *walker) resourceSpan() (decodedResource, error) {
	var res decodedResource
	err := d.object(func(key string) error {
		switch key {
		case "resource":
			return d.object(func(key string) error {
				if key != "attributes" {
					return d.skip()
				}
				n := 0
				return d.array(MaxResourceAttributes, &n, "resource attributes", func() error {
					kv, err := d.keyValue()
					if err != nil {
						return err
					}
					res.attributes = append(res.attributes, kv)
					return nil
				})
			})
		case "scopeSpans":
			return d.array(MaxScopeSpans, &d.scopeSpans, "scopeSpans", func() error {
				return d.object(func(key string) error {
					if key != "spans" {
						return d.skip()
					}
					return d.array(MaxSpansPerRequest, &d.spans, "spans", func() error {
						sp, err := d.span()
						if err == nil {
							res.spans = append(res.spans, sp)
						}
						return err
					})
				})
			})
		default:
			return d.skip()
		}
	})
	return res, err
}

func (d *walker) span() (span, error) {
	var sp span
	err := d.object(func(key string) error {
		switch key {
		case "traceId":
			return d.value(&sp.TraceID)
		case "spanId":
			return d.value(&sp.SpanID)
		case "parentSpanId":
			return d.value(&sp.ParentSpanID)
		case "name":
			return d.value(&sp.Name)
		case "kind":
			return d.value(&sp.Kind)
		case "startTimeUnixNano":
			return d.value(&sp.StartTimeUnixNano)
		case "endTimeUnixNano":
			return d.value(&sp.EndTimeUnixNano)
		case "status":
			return d.object(func(key string) error {
				if key != "code" {
					return d.skip()
				}
				return d.value(&sp.StatusCode)
			})
		case "attributes":
			n := 0
			return d.array(MaxAttributesPerSpan, &n, "span attributes", func() error {
				kv, err := d.keyValue()
				if err != nil {
					return err
				}
				sp.Attributes = append(sp.Attributes, kv)
				return nil
			})
		default:
			return d.skip()
		}
	})
	return sp, err
}

// keyValue walks one OTLP/JSON KeyValue, and its AnyValue, with the same
// repeated-key refusal as every other object. Only stringValue and intValue
// are kept (the allowlist never needs another type); any other AnyValue member
// (arrayValue, kvlistValue, bytesValue, …) is scanned past without being built.
func (d *walker) keyValue() (keyValue, error) {
	var kv keyValue
	err := d.object(func(key string) error {
		switch key {
		case "key":
			return d.value(&kv.Key)
		case "value":
			return d.object(func(key string) error {
				switch key {
				case "stringValue":
					var s string
					if err := d.value(&s); err != nil {
						return err
					}
					kv.Value.StringValue = &s
					return nil
				case "intValue":
					var n jsonInt64
					if err := d.value(&n); err != nil {
						return err
					}
					kv.Value.IntValue = &n
					return nil
				default:
					return d.skip()
				}
			})
		default:
			return d.skip()
		}
	})
	return kv, err
}
