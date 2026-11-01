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

// startWatchdog starts the monitor's watchdog once. It runs for the process
// lifetime (the middleware is built once, in main).
func (m *monitor) startWatchdog() {
	m.watchOnce.Do(func() {
		m.stop = make(chan struct{})
		go m.watch()
	})
}

func (m *monitor) watch() {
	// The watchdog is process infrastructure, not the work of whichever
	// request built the middleware.
	workclass.ClearGoroutine()
	t := time.NewTicker(m.tick)
	defer t.Stop()
	for {
		select {
		case <-m.stop:
			return
		case now := <-t.C:
			m.check(now)
		}
	}
}

// check takes a goroutine snapshot when a tracked request has been in flight
// longer than the bound and no snapshot has covered it yet, at most one per
// snapEvery. A slow request first seen inside the rate window is counted as
// suppressed, once. It returns whether it took a snapshot.
func (m *monitor) check(now time.Time) bool {
	m.mu.Lock()
	var slow []*Record
	fresh := false
	// The list is in arrival order, so the slow requests are a prefix of it.
	for r := m.head; r != nil && len(slow) < maxSnapshotRequests; r = r.next {
		if now.Sub(r.start) <= m.bound {
			break
		}
		slow = append(slow, r)
		if !r.snapped {
			fresh = true
		}
	}
	if !fresh {
		m.mu.Unlock()
		return false
	}
	inflight := m.n
	if !m.lastSnap.IsZero() && now.Sub(m.lastSnap) < m.snapEvery {
		for _, r := range slow {
			if !r.snapped && !r.suppressed {
				r.suppressed = true
				m.stats.suppressed.Add(1)
			}
		}
		m.mu.Unlock()
		return false
	}
	m.lastSnap = now
	for _, r := range slow {
		r.snapped = true
	}
	m.mu.Unlock()

	attrs := m.snapshotAttrs(now, slow, inflight)
	m.stats.noteSnapshot(attrMap(attrs, now))
	m.logger().LogAttrs(context.Background(), slog.LevelWarn, "slowcall.goroutines", attrs...)
	return true
}

// snapshotAttrs is one snapshot's log record: the in-flight slow requests
// (route, age, open phases, open waits) and the goroutine summary. FLAT, like
// the request record; the two text blocks are capped together at
// maxSnapshotBytes.
func (m *monitor) snapshotAttrs(now time.Time, slow []*Record, inflight int) []slog.Attr {
	var reqs strings.Builder
	oldest := slow[0]
	for _, r := range slow {
		reqs.WriteString(r.route)
		reqs.WriteString(" age_ms=")
		reqs.WriteString(strconv.FormatInt(now.Sub(r.start).Milliseconds(), 10))
		for _, v := range r.phases() {
			if v.o > 0 {
				reqs.WriteString(" open:")
				reqs.WriteString(v.p.String())
				reqs.WriteString("=")
				reqs.WriteString(strconv.FormatInt(v.o, 10))
			}
		}
		r.mu.Lock()
		for _, o := range r.open {
			if !o.set {
				continue
			}
			reqs.WriteString(" wait{")
			reqs.WriteString(o.phase.String())
			reqs.WriteString(" ")
			reqs.WriteString(o.kind)
			if o.key != "" {
				reqs.WriteString(" ")
				reqs.WriteString(waitKeyLabel(o.key))
			}
			if o.gvr != "" {
				reqs.WriteString(" ")
				reqs.WriteString(o.gvr)
			}
			reqs.WriteString(" age_ms=")
			reqs.WriteString(strconv.FormatInt(now.Sub(o.since).Milliseconds(), 10))
			reqs.WriteString("}")
		}
		r.mu.Unlock()
		reqs.WriteString("\n")
	}
	budget := maxSnapshotBytes - reqs.Len()
	sum := summarizeGoroutines(m.capture(), budget)

	attrs := []slog.Attr{
		slog.String("subsystem", "slowcall"),
		slog.Int("inflight", inflight),
		slog.Int("slow_inflight", len(slow)),
		slog.Int64("oldest_age_ms", now.Sub(oldest.start).Milliseconds()),
		slog.String("route", oldest.route),
		slog.Int64("yield_parked", yieldParked.Load()),
		slog.Int("goroutines", sum.total),
		slog.Int("request_goroutines", sum.requestGoroutines),
		slog.Int("background_goroutines", sum.backgroundGoroutines),
		slog.Int("other_goroutines", sum.otherGoroutines),
		slog.Int("stacks_kept", sum.kept),
		slog.Int("stacks_omitted", sum.omitted),
		slog.Bool("truncated", sum.truncated),
	}
	if w := oldest.reportedWait(now); w.set {
		attrs = append(attrs, waitAttrs(w)...)
	}
	return append(attrs,
		slog.String("requests", reqs.String()),
		slog.String("stacks", sum.text))
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
