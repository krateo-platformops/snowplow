// Package workclassprobe reads the #871 work-class label back, for tests only.
//
// Two instruments, both reading what the Go runtime itself recorded — never
// the context the code under test was handed:
//
//   - GoroutineClass: the snowplow_work label on the CALLING goroutine, from a
//     goroutine profile (debug=1 prints each goroutine group's labels). It is
//     deterministic: a probe installed in a unit's body calls it while the
//     unit runs.
//   - CPUClasses: the snowplow_work label on the CPU-profile samples taken
//     while Spin burned CPU. That is the property the bench relies on: a CPU
//     sample on the unit's goroutine, or on one it started, names the class.
//
// The CPU-profile decoder below reads only the fields it needs from the
// profile.proto wire format, so this package adds no module dependency.
package workclassprobe

import (
	"bytes"
	"compress/gzip"
	"context"
	"errors"
	"fmt"
	"io"
	"regexp"
	"runtime/pprof"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/krateo-platformops/snowplow/internal/workclass"
)

// None is what GoroutineClass returns for a goroutine with no work label.
const None = "<none>"

var labelRE = regexp.MustCompile(`"` + workclass.Key + `":"([^"]*)"`)

// GoroutineClass is the snowplow_work label on the calling goroutine ("<none>"
// when it carries none).
//
//go:noinline
func GoroutineClass() string {
	var buf bytes.Buffer
	if err := pprof.Lookup("goroutine").WriteTo(&buf, 1); err != nil {
		return "error: " + err.Error()
	}
	// debug=1 groups goroutines by (stack, labels); a group is a blank-line
	// separated record whose "# labels:" line, when present, precedes the frames.
	for _, rec := range strings.Split(buf.String(), "\n\n") {
		if !strings.Contains(rec, "workclassprobe.GoroutineClass") {
			continue
		}
		if m := labelRE.FindStringSubmatch(rec); m != nil {
			return m[1]
		}
		return None
	}
	return "error: calling goroutine not found in the goroutine profile"
}

// Probe records, each time Observe runs, the class the calling goroutine
// carries and — when Child is set — the class a goroutine it starts carries.
// Install Observe in a unit's body through the package's own test seam.
type Probe struct {
	// Child: Observe also starts a goroutine and records its class (the
	// inheritance arm).
	Child bool
	// Spin: Observe burns this much CPU (and so does the child), for a CPU
	// profile running around it (CPUClasses).
	Spin time.Duration

	mu    sync.Mutex
	self  []string
	child []string
	ctx   []string
}

// Observe records the calling goroutine's class (and ctx's, when non-nil).
func (p *Probe) Observe(ctx context.Context) {
	self := GoroutineClass()
	var child string
	if p.Child {
		done := make(chan string)
		go func() {
			if p.Spin > 0 {
				Spin(p.Spin)
			}
			done <- GoroutineClass()
		}()
		child = <-done
	}
	if p.Spin > 0 {
		Spin(p.Spin)
	}
	c := None
	if ctx != nil {
		if s := workclass.Of(ctx).String(); s != "" {
			c = s
		}
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.self = append(p.self, self)
	p.ctx = append(p.ctx, c)
	if p.Child {
		p.child = append(p.child, child)
	}
}

// Self is every goroutine class Observe recorded.
func (p *Probe) Self() []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]string(nil), p.self...)
}

// Ctx is every context class Observe recorded.
func (p *Probe) Ctx() []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]string(nil), p.ctx...)
}

// Children is every child goroutine class Observe recorded.
func (p *Probe) Children() []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]string(nil), p.child...)
}

// RequireAll fails t unless got is non-empty and every element equals want.
func RequireAll(t testing.TB, what string, got []string, want string) {
	t.Helper()
	if len(got) == 0 {
		t.Fatalf("%s: the probe never ran (no observation) — the arm proves nothing", what)
	}
	for i, g := range got {
		if g != want {
			t.Fatalf("%s: observation %d carried snowplow_work=%q, want %q (all: %v)", what, i, g, want, got)
		}
	}
}

// sink defeats dead-code elimination of Spin's loop.
var sink uint64

// Spin burns CPU on the calling goroutine for about d. Its frame marks the
// samples CPUClasses counts.
//
//go:noinline
func Spin(d time.Duration) {
	end := time.Now().Add(d)
	x := sink
	for time.Now().Before(end) {
		for i := 0; i < 1000; i++ {
			x = x*6364136223846793005 + 1442695040888963407
		}
	}
	sink = x
}

// CPUClasses runs f under the CPU profiler and returns, per snowplow_work
// value ("<none>" for an unlabelled sample), the number of samples whose stack
// holds Spin.
func CPUClasses(t testing.TB, f func()) map[string]int {
	t.Helper()
	var buf bytes.Buffer
	if err := pprof.StartCPUProfile(&buf); err != nil {
		t.Fatalf("StartCPUProfile: %v (another CPU profile running?)", err)
	}
	f()
	pprof.StopCPUProfile()
	out, err := spinSampleClasses(buf.Bytes())
	if err != nil {
		t.Fatalf("decode CPU profile: %v", err)
	}
	return out
}

// --- a minimal profile.proto reader ------------------------------------------

type pbuf struct {
	b []byte
	i int
}

func (p *pbuf) varint() (uint64, error) {
	var x uint64
	var s uint
	for {
		if p.i >= len(p.b) {
			return 0, io.ErrUnexpectedEOF
		}
		c := p.b[p.i]
		p.i++
		x |= uint64(c&0x7f) << s
		if c < 0x80 {
			return x, nil
		}
		s += 7
		if s > 63 {
			return 0, errors.New("varint overflow")
		}
	}
}

// fields calls f(field, wiretype, varint value, bytes) for each field of msg.
func fields(msg []byte, f func(num int, wt int, v uint64, b []byte) error) error {
	p := &pbuf{b: msg}
	for p.i < len(p.b) {
		key, err := p.varint()
		if err != nil {
			return err
		}
		num, wt := int(key>>3), int(key&7)
		switch wt {
		case 0:
			v, err := p.varint()
			if err != nil {
				return err
			}
			if err := f(num, wt, v, nil); err != nil {
				return err
			}
		case 1:
			p.i += 8
		case 5:
			p.i += 4
		case 2:
			n, err := p.varint()
			if err != nil {
				return err
			}
			if p.i+int(n) > len(p.b) {
				return io.ErrUnexpectedEOF
			}
			if err := f(num, wt, 0, p.b[p.i:p.i+int(n)]); err != nil {
				return err
			}
			p.i += int(n)
		default:
			return fmt.Errorf("unsupported wire type %d", wt)
		}
	}
	return nil
}

// uints decodes a repeated uint64 field (packed or not).
func uints(wt int, v uint64, b []byte) ([]uint64, error) {
	if wt == 0 {
		return []uint64{v}, nil
	}
	var out []uint64
	p := &pbuf{b: b}
	for p.i < len(p.b) {
		x, err := p.varint()
		if err != nil {
			return nil, err
		}
		out = append(out, x)
	}
	return out, nil
}

func spinSampleClasses(gz []byte) (map[string]int, error) {
	zr, err := gzip.NewReader(bytes.NewReader(gz))
	if err != nil {
		return nil, err
	}
	raw, err := io.ReadAll(zr)
	if err != nil {
		return nil, err
	}
	type sample struct {
		locs   []uint64
		labels [][2]uint64 // key, str (string-table indexes)
	}
	var (
		samples []sample
		strs    []string
		locFns  = map[uint64][]uint64{} // location id → function ids
		fnName  = map[uint64]uint64{}   // function id → name index
	)
	err = fields(raw, func(num, wt int, v uint64, b []byte) error {
		switch num {
		case 2: // sample
			var s sample
			if err := fields(b, func(num, wt int, v uint64, b []byte) error {
				switch num {
				case 1:
					ids, err := uints(wt, v, b)
					if err != nil {
						return err
					}
					s.locs = append(s.locs, ids...)
				case 3:
					var k, sv uint64
					if err := fields(b, func(num, _ int, v uint64, _ []byte) error {
						switch num {
						case 1:
							k = v
						case 2:
							sv = v
						}
						return nil
					}); err != nil {
						return err
					}
					s.labels = append(s.labels, [2]uint64{k, sv})
				}
				return nil
			}); err != nil {
				return err
			}
			samples = append(samples, s)
		case 4: // location
			var id uint64
			var fns []uint64
			if err := fields(b, func(num, _ int, v uint64, b []byte) error {
				switch num {
				case 1:
					id = v
				case 4: // line
					return fields(b, func(num, _ int, v uint64, _ []byte) error {
						if num == 1 {
							fns = append(fns, v)
						}
						return nil
					})
				}
				return nil
			}); err != nil {
				return err
			}
			locFns[id] = fns
		case 5: // function
			var id, name uint64
			if err := fields(b, func(num, _ int, v uint64, _ []byte) error {
				switch num {
				case 1:
					id = v
				case 2:
					name = v
				}
				return nil
			}); err != nil {
				return err
			}
			fnName[id] = name
		case 6: // string_table
			strs = append(strs, string(b))
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	str := func(i uint64) string {
		if i < uint64(len(strs)) {
			return strs[i]
		}
		return ""
	}
	out := map[string]int{}
	for _, s := range samples {
		spin := false
		for _, l := range s.locs {
			for _, f := range locFns[l] {
				if strings.HasSuffix(str(fnName[f]), "workclassprobe.Spin") {
					spin = true
				}
			}
		}
		if !spin {
			continue
		}
		class := None
		for _, kv := range s.labels {
			if str(kv[0]) == workclass.Key {
				class = str(kv[1])
			}
		}
		out[class]++
	}
	return out, nil
}
