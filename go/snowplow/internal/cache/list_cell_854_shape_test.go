package cache

// list_cell_854_shape_test.go — #854: the anchor's pairing proof compares the
// fill's envelope in place against a shape DERIVED from the cell's own envelope
// writer (no envelope is rebuilt).
//
//   SH1  over a concatenating writer with a one-byte separator, holds(env) is
//        exactly Marshal(raws) == env: true for the writer's own output, false
//        for a changed byte, a missing item, an extra item, a moved separator.
//   SH2  a writer with a two-byte separator is refused (envLen counts one byte
//        per separator). Mutation → RED: drop the separator-length check.
//   SH3  a writer that does not concatenate its items verbatim, or that fails,
//        is refused.

import (
	"bytes"
	"errors"
	"fmt"
	"testing"
)

// sh854Writer joins raws between a fixed head and tail. Its keys are not the
// production envelope's: the shape is read off whatever the writer emits.
func sh854Writer(sep string, wrap func([]byte) []byte) func([][]byte) ([]byte, error) {
	return func(raws [][]byte) ([]byte, error) {
		var b bytes.Buffer
		b.WriteString(`{"av":"v1","xs":[`)
		for i, r := range raws {
			if i > 0 {
				b.WriteString(sep)
			}
			b.Write(wrap(r))
		}
		b.WriteString(`],"k":"ThingList"}`)
		return b.Bytes(), nil
	}
}

func sh854Same(r []byte) []byte { return r }

func sh854Keyed(raws [][]byte) []keyedRaw {
	out := make([]keyedRaw, len(raws))
	for i, r := range raws {
		out[i] = keyedRaw{key: listObjKey{name: fmt.Sprint(i)}, raw: r}
	}
	return out
}

func TestListCell854_SH1_HoldsIsExactlyTheWritersOutput(t *testing.T) {
	w := sh854Writer(",", sh854Same)
	sh, ok := deriveEnvelopeShape(w)
	if !ok {
		t.Fatal("SH1: a concatenating writer with a one-byte separator was refused")
	}
	for n := 0; n <= 5; n++ {
		raws := make([][]byte, n)
		for i := range raws {
			raws[i] = []byte(fmt.Sprintf(`{"metadata":{"name":"o%d"},"v":[%d,0]}`, i, i))
		}
		env, _ := w(raws)
		keyed := sh854Keyed(raws)
		if !sh.holds(env, keyed) {
			t.Fatalf("SH1 RED: n=%d: the writer's own envelope does not hold", n)
		}
		if n == 0 {
			continue
		}
		flipped := bytes.Clone(env)
		flipped[bytes.Index(env, raws[n-1])+2] ^= 1
		missing, _ := w(raws[:n-1])
		extra, _ := w(append(append([][]byte{}, raws...), []byte(`{}`)))
		cases := map[string][]byte{"changed byte": flipped, "missing item": missing, "extra item": extra}
		if n > 1 {
			moved := bytes.Clone(env)
			i := bytes.Index(moved, []byte("},{"))
			moved[i], moved[i+1] = moved[i+1], moved[i]
			cases["moved separator"] = moved
		}
		for name, bad := range cases {
			if sh.holds(bad, keyed) {
				t.Fatalf("SH1 RED: n=%d: %s holds", n, name)
			}
		}
	}
}

func TestListCell854_SH2_ATwoByteSeparatorIsRefused(t *testing.T) {
	if _, ok := deriveEnvelopeShape(sh854Writer(", ", sh854Same)); ok {
		t.Fatal("SH2 RED: a two-byte separator was accepted — envLen would undercount every multi-item envelope")
	}
}

func TestListCell854_SH3_ANonConcatenatingOrFailingWriterIsRefused(t *testing.T) {
	wrapped := sh854Writer(",", func(r []byte) []byte { return append(append([]byte("["), r...), ']') })
	if _, ok := deriveEnvelopeShape(wrapped); ok {
		t.Fatal("SH3 RED: a writer that wraps each item was accepted")
	}
	failing := func([][]byte) ([]byte, error) { return nil, errors.New("no") }
	if _, ok := deriveEnvelopeShape(failing); ok {
		t.Fatal("SH3 RED: a failing writer was accepted")
	}
}
