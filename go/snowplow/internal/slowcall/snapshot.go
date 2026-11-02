package slowcall

import (
	"bufio"
	"bytes"
	"context"
	"log/slog"
	"path"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/krateo-platformops/snowplow/internal/workclass"
)

// startWatchdog starts the monitor's watchdog and its CPU sampler once. Both
// run for the process lifetime (the middleware is built once, in main).
func (m *monitor) startWatchdog() {
	m.watchOnce.Do(func() {
		m.cpu.start()
		go m.watch()
	})
}

func (m *monitor) watch() {
	// The watchdog is process infrastructure, not the work of whichever
	// request built the middleware.
	workclass.ClearGoroutine()
	t := time.NewTicker(m.tick)
	defer t.Stop()
	for now := range t.C {
		m.check(now)
	}
}

// inflightSlow returns every request in flight longer than the bound at now,
// oldest first, and the number in flight.
func (m *monitor) inflightSlow(now time.Time) (slow []*Record, inflight int) {
	for i := range m.shards {
		s := &m.shards[i]
		s.mu.Lock()
		inflight += s.n
		for r := s.head; r != nil; r = r.next {
			if now.Sub(r.start) > m.bound {
				slow = append(slow, r)
			}
		}
		s.mu.Unlock()
	}
	sort.Slice(slow, func(i, j int) bool { return slow[i].start.Before(slow[j].start) })
	return slow, inflight
}

// check takes a goroutine snapshot when a CUSTOMER request has been in flight
// longer than the bound and no snapshot has covered it yet, at most one per
// snapEvery. EVERY such request is considered, however many are in flight: a
// snapshot covers all of them, and one first seen inside the rate window is
// counted as suppressed, once. A background request (a self-loopback of the
// refresher or prewarm) never triggers or uses the snapshot; it is still
// listed when one is taken. It returns whether it took a snapshot.
func (m *monitor) check(now time.Time) bool {
	slow, inflight := m.inflightSlow(now)
	m.watchMu.Lock()
	var customers []*Record
	fresh := false
	for _, r := range slow {
		if r.workClass().Background() {
			continue
		}
		customers = append(customers, r)
		if !r.snapped {
			fresh = true
		}
	}
	if !fresh {
		m.watchMu.Unlock()
		return false
	}
	if !m.lastSnap.IsZero() && now.Sub(m.lastSnap) < m.snapEvery {
		for _, r := range customers {
			if !r.snapped && !r.suppressed {
				r.suppressed = true
				m.stats.suppressed.Add(1)
			}
		}
		m.watchMu.Unlock()
		return false
	}
	m.lastSnap = now
	for _, r := range customers {
		r.snapped = true
	}
	m.watchMu.Unlock()

	attrs := m.snapshotAttrs(now, customers, slow, inflight)
	m.stats.noteSnapshot(attrMap(attrs, now))
	m.logger().LogAttrs(context.Background(), slog.LevelWarn, "slowcall.goroutines", attrs...)
	return true
}

// snapshotAttrs is one snapshot's log record: the in-flight slow requests
// (customers first, each with its class, age, open phases and open waits),
// the CPU starvation since the oldest customer one began, and the goroutine
// summary. FLAT, like the request record; the two text blocks are capped
// together at maxSnapshotBytes and the request list at maxSnapshotRequests.
func (m *monitor) snapshotAttrs(now time.Time, customers, slow []*Record, inflight int) []slog.Attr {
	listed := append([]*Record(nil), customers...)
	for _, r := range slow {
		if r.workClass().Background() {
			listed = append(listed, r)
		}
	}
	omitted := 0
	if len(listed) > maxSnapshotRequests {
		omitted = len(listed) - maxSnapshotRequests
		listed = listed[:maxSnapshotRequests]
	}
	var reqs strings.Builder
	for _, r := range listed {
		writeRequestLine(&reqs, r, now)
	}
	oldest := customers[0]

	t0 := time.Now()
	profile := m.capture()
	sum := summarizeGoroutines(profile, maxSnapshotBytes-reqs.Len())
	captureMS := time.Since(t0).Milliseconds()

	attrs := []slog.Attr{
		slog.String("subsystem", "slowcall"),
		slog.Int("inflight", inflight),
		slog.Int("slow_inflight", len(slow)),
		slog.Int("slow_customers", len(customers)),
		slog.Int("requests_omitted", omitted),
		slog.Int64("oldest_age_ms", now.Sub(oldest.start).Milliseconds()),
		slog.String("route", oldest.route),
		slog.Int64("yield_parked", yieldParked.Load()),
		slog.Int64("capture_ms", captureMS),
		slog.Int("goroutines", sum.total),
		slog.Int("request_goroutines", sum.requestGoroutines),
		slog.Int("background_goroutines", sum.backgroundGoroutines),
		slog.Int("other_goroutines", sum.otherGoroutines),
		slog.Int("stacks_kept", sum.kept),
		slog.Int("stacks_omitted", sum.omitted),
		slog.Bool("truncated", sum.truncated),
	}
	wait, dial, _, _ := oldest.reported(now)
	attrs = append(attrs, infoAttrs("wait_", wait, true)...)
	attrs = append(attrs, infoAttrs("dial_", dial, false)...)
	attrs = append(attrs, m.cpu.windowAttrs(oldest.start)...)
	return append(attrs,
		slog.String("requests", reqs.String()),
		slog.String("stacks", sum.text))
}

func writeRequestLine(b *strings.Builder, r *Record, now time.Time) {
	b.WriteString(r.route)
	b.WriteString(" class:")
	b.WriteString(r.workClass().String())
	b.WriteString(" age_ms:")
	b.WriteString(strconv.FormatInt(now.Sub(r.start).Milliseconds(), 10))
	for _, v := range r.phases() {
		if v.o > 0 {
			b.WriteString(" open:")
			b.WriteString(v.p.String())
			b.WriteString(":")
			b.WriteString(strconv.FormatInt(v.o, 10))
		}
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	write := func(tag string, o waitInfo) {
		b.WriteString(" ")
		b.WriteString(tag)
		b.WriteString("{")
		b.WriteString(o.phase.String())
		b.WriteString(" ")
		b.WriteString(o.kind)
		if o.key != "" {
			b.WriteString(" ")
			b.WriteString(waitKeyLabel(o.key))
		}
		if o.gvr != "" {
			b.WriteString(" ")
			b.WriteString(o.gvr)
		}
		if o.verb != "" {
			b.WriteString(" ")
			b.WriteString(o.verb)
		}
		if o.leaderWork != "" {
			b.WriteString(" leader:")
			b.WriteString(o.leaderWork)
		}
		if o.dial != nil {
			b.WriteString(" attempts:")
			b.WriteString(strconv.FormatInt(o.dial.attempts.Load(), 10))
			b.WriteString(" status:")
			b.WriteString(strconv.FormatInt(o.dial.lastStatus.Load(), 10))
		}
		b.WriteString(" age_ms:")
		b.WriteString(strconv.FormatInt(now.Sub(o.since).Milliseconds(), 10))
		b.WriteString("}")
	}
	for _, o := range r.open {
		if o.set {
			write("wait", o)
		}
	}
	if r.openLd.set {
		write("lead", r.openLd)
	}
	b.WriteString("\n")
}

// goroutineSummary is a goroutine profile reduced to the stacks that can name
// a shared wait.
type goroutineSummary struct {
	total                                                    int
	requestGoroutines, backgroundGoroutines, otherGoroutines int
	kept, omitted                                            int
	truncated                                                bool
	text                                                     string
}

type stackGroup struct {
	count  int
	labels string
	frames []string
	// Classified over EVERY frame, before the maxFramesPerStack cut: a
	// request's outermost frame is net/http's conn.serve.
	request, module bool
}

// The goroutines that can be part of a request's wait: the ones serving
// requests (their innermost frames are where each request blocks), and the
// module's own background goroutines (a fill leader, a refresher worker, a
// lock holder). The rest (client-go's informer machinery, the runtime, the
// exporters) is only counted.
const (
	requestFrame = "net/http.(*conn).serve"
	modulePrefix = "github.com/krateo-platformops/snowplow/"
)

var (
	labelAllowMu sync.RWMutex
	// labelAllow are the pprof label keys a summary keeps. Every other label
	// is dropped: a label is a free string some code chose, and the summary
	// must hold no identity whatever a future label carries.
	labelAllow = map[string]bool{workclass.Key: true}
)

// AllowStackLabels lets the goroutine summary keep the pprof labels with
// these keys. Register only identity-free labels (a closed enum, a GVR).
func AllowStackLabels(keys ...string) {
	labelAllowMu.Lock()
	defer labelAllowMu.Unlock()
	for _, k := range keys {
		labelAllow[k] = true
	}
}

func labelAllowed(k string) bool {
	labelAllowMu.RLock()
	defer labelAllowMu.RUnlock()
	return labelAllow[k]
}

// summarizeGoroutines reduces a debug=1 goroutine profile ("N @ pcs", an
// optional "# labels: {...}" line, then "#\tpc\tfunc+off\tfile:line" frames).
// It keeps, per stack, the count, the allowlisted labels and the function
// names with file BASENAMES and lines: never a pc, an argument or a label
// outside the allowlist. Request and background stacks are merged by
// (labels, frames), ranked by count and rendered until maxBytes.
func summarizeGoroutines(profile []byte, maxBytes int) goroutineSummary {
	var sum goroutineSummary
	var cur *stackGroup
	var groups []*stackGroup
	flush := func() {
		if cur != nil && len(cur.frames) > 0 {
			groups = append(groups, cur)
		}
		cur = nil
	}
	sc := bufio.NewScanner(bytes.NewReader(profile))
	sc.Buffer(make([]byte, 0, 64<<10), 1<<20)
	for sc.Scan() {
		line := sc.Text()
		switch {
		case strings.HasPrefix(line, "goroutine profile: total "):
			sum.total, _ = strconv.Atoi(strings.TrimPrefix(line, "goroutine profile: total "))
		case line == "":
			flush()
		case strings.HasPrefix(line, "# labels: "):
			if cur != nil {
				cur.labels = keptLabels(strings.TrimPrefix(line, "# labels: "))
			}
		case strings.HasPrefix(line, "#\t"):
			if cur == nil {
				continue
			}
			f, ok := frameOf(line)
			if !ok {
				continue
			}
			cur.request = cur.request || strings.HasPrefix(f, requestFrame+" ")
			cur.module = cur.module || strings.HasPrefix(f, modulePrefix)
			if len(cur.frames) < maxFramesPerStack {
				cur.frames = append(cur.frames, f)
			}
		default:
			if i := strings.Index(line, " @"); i > 0 {
				flush()
				n, err := strconv.Atoi(line[:i])
				if err == nil {
					cur = &stackGroup{count: n}
				}
			}
		}
	}
	flush()

	var req, bg []*stackGroup
	for _, g := range groups {
		switch {
		case g.request:
			req = append(req, g)
			sum.requestGoroutines += g.count
		case g.module:
			bg = append(bg, g)
			sum.backgroundGoroutines += g.count
		default:
			sum.otherGoroutines += g.count
		}
	}
	req, bg = merge(req), merge(bg)

	var b strings.Builder
	render := func(title string, gs []*stackGroup) {
		header := "== " + title + " ==\n"
		if b.Len()+len(header) > maxBytes {
			sum.truncated = true
			sum.omitted += len(gs)
			return
		}
		b.WriteString(header)
		for i, g := range gs {
			if i >= maxStacksPerClass {
				sum.omitted += len(gs) - i
				return
			}
			s := renderGroup(g)
			if b.Len()+len(s) > maxBytes {
				sum.truncated = true
				sum.omitted += len(gs) - i
				return
			}
			b.WriteString(s)
			sum.kept++
		}
	}
	render("request stacks", req)
	render("background stacks", bg)
	sum.text = b.String()
	return sum
}

// frameOf renders "#\t0xpc\tpkg.fn+0xoff\t/path/file.go:123" as
// "pkg.fn file.go:123".
func frameOf(line string) (string, bool) {
	parts := strings.Split(line, "\t")
	if len(parts) < 3 {
		return "", false
	}
	fn := parts[2]
	if i := strings.LastIndex(fn, "+0x"); i > 0 {
		fn = fn[:i]
	}
	if fn == "" {
		return "", false
	}
	if len(parts) >= 4 {
		return fn + " " + path.Base(strings.TrimSpace(parts[3])), true
	}
	return fn, true
}

// keptLabels parses a pprof label set (`{"k":"v", "k2":"v2"}`, each key and
// value Go-quoted) and renders only the allowlisted pairs, sorted.
func keptLabels(s string) string {
	s = strings.TrimSpace(s)
	s = strings.TrimPrefix(s, "{")
	s = strings.TrimSuffix(s, "}")
	var kept []string
	for s != "" {
		k, rest, ok := unquotePrefix(s)
		if !ok || !strings.HasPrefix(rest, ":") {
			break
		}
		v, rest2, ok := unquotePrefix(rest[1:])
		if !ok {
			break
		}
		if labelAllowed(k) {
			kept = append(kept, k+"="+v)
		}
		s = strings.TrimPrefix(strings.TrimPrefix(rest2, ","), " ")
	}
	sort.Strings(kept)
	return strings.Join(kept, ",")
}

func unquotePrefix(s string) (string, string, bool) {
	q, err := strconv.QuotedPrefix(s)
	if err != nil {
		return "", s, false
	}
	v, err := strconv.Unquote(q)
	if err != nil {
		return "", s, false
	}
	return v, s[len(q):], true
}

// merge folds groups with the same labels and frames (stacks the profile kept
// apart only by pc or by a dropped label) and ranks them by count.
func merge(gs []*stackGroup) []*stackGroup {
	idx := map[string]*stackGroup{}
	var out []*stackGroup
	for _, g := range gs {
		k := g.labels + "\x00" + strings.Join(g.frames, "\x00")
		if m, ok := idx[k]; ok {
			m.count += g.count
			continue
		}
		c := *g
		idx[k] = &c
		out = append(out, &c)
	}
	sort.SliceStable(out, func(i, j int) bool {
		if out[i].count != out[j].count {
			return out[i].count > out[j].count
		}
		return strings.Join(out[i].frames, "\x00") < strings.Join(out[j].frames, "\x00")
	})
	return out
}

func renderGroup(g *stackGroup) string {
	var b strings.Builder
	b.WriteString(strconv.Itoa(g.count))
	if g.labels != "" {
		b.WriteString(" {")
		b.WriteString(g.labels)
		b.WriteString("}")
	}
	b.WriteString("\n")
	for _, f := range g.frames {
		b.WriteString("  ")
		b.WriteString(f)
		b.WriteString("\n")
	}
	return b.String()
}
