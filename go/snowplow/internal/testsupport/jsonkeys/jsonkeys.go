// Package jsonkeys reads the keys of a JSON log line from its bytes, for tests
// only (#916).
//
// WHY NOT encoding/json. Unmarshal into a map keeps the last copy of a repeated
// key and says nothing, so a line that carries "traceId" twice decodes to one
// key. A test that asks "does this line carry the key once?" has to read the
// tokens.
package jsonkeys

import (
	"bytes"
	"encoding/json"
	"fmt"
)

// Line is what Read found on one JSON object.
type Line struct {
	// Top is the keys of the top-level object in order, every copy of a
	// repeated key included.
	Top []string
	// Repeated is the dotted path of each key that appears more than once in
	// the same object, at any depth, once per extra copy.
	Repeated []string
}

// Count is how many times key appears in the top-level object.
func (l Line) Count(key string) int {
	n := 0
	for _, k := range l.Top {
		if k == key {
			n++
		}
	}
	return n
}

// Read reads line, which must be exactly one JSON object.
func Read(line []byte) (Line, error) {
	dec := json.NewDecoder(bytes.NewReader(line))
	dec.UseNumber()
	tok, err := dec.Token()
	if err != nil {
		return Line{}, fmt.Errorf("jsonkeys: %w", err)
	}
	if d, ok := tok.(json.Delim); !ok || d != '{' {
		return Line{}, fmt.Errorf("jsonkeys: the line is not a JSON object")
	}
	var out Line
	if err := readObject(dec, "", &out, true); err != nil {
		return Line{}, err
	}
	if _, err := dec.Token(); err == nil {
		return Line{}, fmt.Errorf("jsonkeys: more than one JSON value on the line")
	}
	return out, nil
}

// readObject reads the members of an object whose '{' was just read.
func readObject(dec *json.Decoder, path string, out *Line, top bool) error {
	seen := map[string]bool{}
	for {
		tok, err := dec.Token()
		if err != nil {
			return fmt.Errorf("jsonkeys: %w", err)
		}
		if d, ok := tok.(json.Delim); ok && d == '}' {
			return nil
		}
		k, ok := tok.(string)
		if !ok {
			return fmt.Errorf("jsonkeys: want a key, got %v", tok)
		}
		p := k
		if path != "" {
			p = path + "." + k
		}
		if top {
			out.Top = append(out.Top, k)
		}
		if seen[k] {
			out.Repeated = append(out.Repeated, p)
		}
		seen[k] = true
		if err := readValue(dec, p, out); err != nil {
			return err
		}
	}
}

// readValue reads one value, descending into objects and arrays.
func readValue(dec *json.Decoder, path string, out *Line) error {
	tok, err := dec.Token()
	if err != nil {
		return fmt.Errorf("jsonkeys: %w", err)
	}
	d, ok := tok.(json.Delim)
	if !ok {
		return nil
	}
	switch d {
	case '{':
		return readObject(dec, path, out, false)
	case '[':
		for dec.More() {
			if err := readValue(dec, path+"[]", out); err != nil {
				return err
			}
		}
		_, err := dec.Token() // ']'
		return err
	}
	return fmt.Errorf("jsonkeys: unexpected %v", d)
}
