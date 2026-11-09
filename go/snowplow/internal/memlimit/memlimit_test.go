//go:build unit

package memlimit

// memlimit_test.go — the five arms of the cgroup-derived soft memory limit:
// cgroup v2, cgroup v1, no limit, an explicit GOMEMLIMIT, and an unreadable
// cgroup file. Each drives apply with the REAL debug.SetMemoryLimit and then
// reads back three things a reader would: the runtime's effective limit
// (debug.SetMemoryLimit(-1)), the published snowplow_go_runtime expvar, and
// cache.AdmissionCeiling, which must follow the effective limit.
//
// Not parallel: the soft memory limit is process-global.

import (
	"context"
	"encoding/json"
	"expvar"
	"io/fs"
	"log/slog"
	"math"
	"runtime/debug"
	"strings"
	"sync"
	"testing"
	"testing/fstest"

	"github.com/krateo-platformops/snowplow/internal/cache"
)

const gib = int64(1) << 30

// v2 with a private cgroup namespace (the Kubernetes default on cgroup v2):
// the container's cgroup is the root of what it sees.
const v2Mountinfo = "24 30 0:22 / /proc rw,nosuid,nodev,noexec,relatime shared:5 - proc proc rw\n" +
	"30 25 0:26 / /sys/fs/cgroup ro,nosuid,nodev,noexec,relatime - cgroup2 cgroup2 rw,nsdelegate,memory_recursiveprot\n"

// v1 (hybrid host: a v1 memory hierarchy beside an empty unified one), no
// cgroup namespace: the mount exposes the container's own cgroup as its root.
const v1Mountinfo = "30 25 0:26 / /sys/fs/cgroup/unified rw,nosuid,nodev,noexec,relatime - cgroup2 cgroup2 rw\n" +
	"41 30 0:36 /kubepods/burstable/pod1/ctr1 /sys/fs/cgroup/memory ro,nosuid,nodev,noexec,relatime master:17 - cgroup cgroup rw,memory\n" +
	"42 30 0:37 /kubepods/burstable/pod1/ctr1 /sys/fs/cgroup/cpu,cpuacct ro,nosuid,nodev,noexec,relatime master:18 - cgroup cgroup rw,cpu,cpuacct\n"

const v1ProcCgroup = "12:memory:/kubepods/burstable/pod1/ctr1\n" +
	"11:cpu,cpuacct:/kubepods/burstable/pod1/ctr1\n" +
	"0::/kubepods/burstable/pod1/ctr1\n"

func v2FS(memoryMax string) fstest.MapFS {
	return fstest.MapFS{
		"proc/self/cgroup":         {Data: []byte("0::/\n")},
		"proc/self/mountinfo":      {Data: []byte(v2Mountinfo)},
		"sys/fs/cgroup/memory.max": {Data: []byte(memoryMax)},
	}
}

func v1FS(limitInBytes string) fstest.MapFS {
	return fstest.MapFS{
		"proc/self/cgroup":                            {Data: []byte(v1ProcCgroup)},
		"proc/self/mountinfo":                         {Data: []byte(v1Mountinfo)},
		"sys/fs/cgroup/memory/memory.limit_in_bytes":  {Data: []byte(limitInBytes)},
		"sys/fs/cgroup/unified/memory.max":            {Data: []byte("max\n")},
		"sys/fs/cgroup/cpu,cpuacct/cpu.cfs_quota_us":  {Data: []byte("400000\n")},
		"sys/fs/cgroup/cpu,cpuacct/cpu.cfs_period_us": {Data: []byte("100000\n")},
	}
}

func noEnv(string) string { return "" }

// withProcessLimit starts an arm from a known process limit and restores the
// test binary's own limit afterwards.
func withProcessLimit(t *testing.T, start int64) {
	t.Helper()
	orig := debug.SetMemoryLimit(start)
	prev := Current()
	t.Cleanup(func() {
		debug.SetMemoryLimit(orig)
		decisionMu.Lock()
		decision = prev
		decisionMu.Unlock()
	})
}

// recordingHandler keeps every record so an arm can assert level and message.
type recordingHandler struct {
	mu   sync.Mutex
	recs []slog.Record
}

func (h *recordingHandler) Enabled(context.Context, slog.Level) bool { return true }
func (h *recordingHandler) Handle(_ context.Context, r slog.Record) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.recs = append(h.recs, r.Clone())
	return nil
}
func (h *recordingHandler) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h *recordingHandler) WithGroup(string) slog.Handler      { return h }

func (h *recordingHandler) only(t *testing.T) slog.Record {
	t.Helper()
	h.mu.Lock()
	defer h.mu.Unlock()
	if len(h.recs) != 1 {
		t.Fatalf("want exactly one log record, got %d", len(h.recs))
	}
	return h.recs[0]
}

func published(t *testing.T) map[string]any {
	t.Helper()
	PublishExpvar()
	v := expvar.Get("snowplow_go_runtime")
	if v == nil {
		t.Fatal("snowplow_go_runtime is not published")
	}
	var m map[string]any
	if err := json.Unmarshal([]byte(v.String()), &m); err != nil {
		t.Fatalf("snowplow_go_runtime is not JSON: %v (%s)", err, v.String())
	}
	return m
}

// assertPublished checks the expvar against the runtime and the decision.
// JSON numbers decode as float64, which is exact for these values except
// MaxInt64 (2^63 rounds to itself), so compare as float64.
func assertPublished(t *testing.T, wantLimit int64, wantSource Source, wantCgroup int64) {
	t.Helper()
	m := published(t)
	if got := m["memory_limit_bytes"].(float64); got != float64(wantLimit) {
		t.Errorf("published memory_limit_bytes = %.0f, want %d", got, wantLimit)
	}
	if got := m["memory_limit_source"].(string); got != string(wantSource) {
		t.Errorf("published memory_limit_source = %q, want %q", got, wantSource)
	}
	if got := m["cgroup_memory_limit_bytes"].(float64); got != float64(wantCgroup) {
		t.Errorf("published cgroup_memory_limit_bytes = %.0f, want %d", got, wantCgroup)
	}
	gogc, _ := Effective()
	if got := m["gogc"].(float64); got != float64(gogc) {
		t.Errorf("published gogc = %.0f, want %d", got, gogc)
	}
}

// assertCeilingFollows: AdmissionCeiling (production samplers, no seams)
// must compute against the effective limit. Its reserve is limit/8 and its
// headroom limit − liveHeap, so ceiling = limit − limit/8 − liveHeap. The
// upper bound alone separates "follows the soft limit" from "follows the
// cgroup limit" (the latter is limit/7 higher, ≥ 1 GiB here); the lower
// bound allows the test binary's live heap, which is a few MiB.
func assertCeilingFollows(t *testing.T, limit int64) {
	t.Helper()
	ceiling, unlimited := cache.AdmissionCeiling()
	if limit == math.MaxInt64 {
		if !unlimited {
			t.Errorf("AdmissionCeiling() = (%d, limited), want unlimited with no soft limit", ceiling)
		}
		return
	}
	if unlimited {
		t.Fatalf("AdmissionCeiling() is unlimited, want it computed against the %d soft limit", limit)
	}
	top := limit - limit/8
	if ceiling > top || ceiling < top-256<<20 {
		t.Errorf("AdmissionCeiling() = %d, want in [%d, %d] (limit %d − limit/8 − live heap)", ceiling, top-256<<20, top, limit)
	}
}

func TestApply_CgroupV2Limit(t *testing.T) {
	withProcessLimit(t, math.MaxInt64)
	h := &recordingHandler{}

	d := apply(v2FS("8589934592\n"), noEnv, debug.SetMemoryLimit, slog.New(h))

	want := 7 * gib // 8 GiB − 8 GiB/8
	if d.Source != SourceCgroupV2 || d.CgroupLimitBytes != 8*gib || d.AppliedBytes != want {
		t.Fatalf("decision = %+v, want cgroup_v2, cgroup 8 GiB, applied 7 GiB", d)
	}
	if got := debug.SetMemoryLimit(-1); got != want {
		t.Fatalf("effective limit = %d, want %d", got, want)
	}
	if _, l := Effective(); l != want {
		t.Errorf("Effective() limit = %d, want %d", l, want)
	}
	if r := h.only(t); r.Level != slog.LevelInfo {
		t.Errorf("log level = %v, want INFO", r.Level)
	}
	assertPublished(t, want, SourceCgroupV2, 8*gib)
	assertCeilingFollows(t, want)
}

// The limit that applies is the smallest along the visible ancestry: here
// the container's own cgroup has none and its pod's has 6 GiB.
func TestApply_CgroupV2LimitFromAncestor(t *testing.T) {
	withProcessLimit(t, math.MaxInt64)
	fsys := fstest.MapFS{
		"proc/self/cgroup":                            {Data: []byte("0::/kubepods/pod1/ctr1\n")},
		"proc/self/mountinfo":                         {Data: []byte(v2Mountinfo)},
		"sys/fs/cgroup/kubepods/pod1/ctr1/memory.max": {Data: []byte("max\n")},
		"sys/fs/cgroup/kubepods/pod1/memory.max":      {Data: []byte("6442450944\n")},
		"sys/fs/cgroup/kubepods/memory.max":           {Data: []byte("17179869184\n")},
	}
	d := apply(fsys, noEnv, debug.SetMemoryLimit, slog.New(&recordingHandler{}))
	want := 6*gib - 6*gib/8
	if d.Source != SourceCgroupV2 || d.CgroupLimitBytes != 6*gib || debug.SetMemoryLimit(-1) != want {
		t.Fatalf("decision = %+v, effective %d, want cgroup 6 GiB → %d", d, debug.SetMemoryLimit(-1), want)
	}
	assertCeilingFollows(t, want)
}

func TestApply_CgroupV1Limit(t *testing.T) {
	withProcessLimit(t, math.MaxInt64)
	h := &recordingHandler{}

	d := apply(v1FS("4294967296\n"), noEnv, debug.SetMemoryLimit, slog.New(h))

	want := 4*gib - 4*gib/8
	if d.Source != SourceCgroupV1 || d.CgroupLimitBytes != 4*gib || d.AppliedBytes != want {
		t.Fatalf("decision = %+v, want cgroup_v1, cgroup 4 GiB, applied %d", d, want)
	}
	if got := debug.SetMemoryLimit(-1); got != want {
		t.Fatalf("effective limit = %d, want %d", got, want)
	}
	if r := h.only(t); r.Level != slog.LevelInfo {
		t.Errorf("log level = %v, want INFO", r.Level)
	}
	assertPublished(t, want, SourceCgroupV1, 4*gib)
	assertCeilingFollows(t, want)
}

func TestApply_NoLimit(t *testing.T) {
	cases := map[string]fs.FS{
		"cgroup v2 memory.max = max": v2FS("max\n"),
		// v1's "no limit": PAGE_COUNTER_MAX × 4 KiB pages.
		"cgroup v1 unlimited": v1FS("9223372036854771712\n"),
		"not in a cgroup":     fstest.MapFS{},
	}
	for name, fsys := range cases {
		t.Run(name, func(t *testing.T) {
			withProcessLimit(t, math.MaxInt64)
			h := &recordingHandler{}

			d := apply(fsys, noEnv, debug.SetMemoryLimit, slog.New(h))

			if d.Source != SourceNone || d.CgroupLimitBytes != 0 || d.AppliedBytes != 0 {
				t.Fatalf("decision = %+v, want none", d)
			}
			if got := debug.SetMemoryLimit(-1); got != math.MaxInt64 {
				t.Fatalf("effective limit = %d, want none (MaxInt64)", got)
			}
			r := h.only(t)
			if r.Level != slog.LevelInfo || !strings.Contains(r.Message, "no cgroup memory limit") {
				t.Errorf("log = %v %q, want INFO saying no cgroup memory limit applies", r.Level, r.Message)
			}
			assertPublished(t, math.MaxInt64, SourceNone, 0)
			assertCeilingFollows(t, math.MaxInt64)
		})
	}
}

// The runtime applied GOMEMLIMIT before main; apply must leave it, even with
// a cgroup limit readable that would have given a different value. The
// cgroup limit is still read and published beside it, so an operator value
// at or above the container limit (the old OOM incident) is visible: INFO
// when it is below, WARN when it is at or above, and the operator's value is
// never overridden either way.
func TestApply_ExplicitGOMEMLIMITIsHonoured(t *testing.T) {
	const atOrAbove = "at or above the container's cgroup memory limit"
	cases := []struct {
		name     string
		env      string
		operator int64 // what the runtime set from env before main
		fsys     fs.FS
		cgroup   int64
		level    slog.Level
		message  string
	}{
		{"below the container limit", "5GiB", 5 * gib, v2FS("8589934592\n"), 8 * gib, slog.LevelInfo, "GOMEMLIMIT is set"},
		{"equal to the container limit", "8GiB", 8 * gib, v2FS("8589934592\n"), 8 * gib, slog.LevelWarn, atOrAbove},
		{"above the container limit", "9GiB", 9 * gib, v2FS("8589934592\n"), 8 * gib, slog.LevelWarn, atOrAbove},
		{"off under a container limit", "off", math.MaxInt64, v2FS("8589934592\n"), 8 * gib, slog.LevelWarn, atOrAbove},
		{"above a v1 container limit", "5GiB", 5 * gib, v1FS("4294967296\n"), 4 * gib, slog.LevelWarn, atOrAbove},
		{"no container limit", "5GiB", 5 * gib, v2FS("max\n"), 0, slog.LevelInfo, "GOMEMLIMIT is set"},
		{"container limit unreadable", "5GiB", 5 * gib, v2FS("eight\n"), 0, slog.LevelWarn, "could not be read"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			withProcessLimit(t, tc.operator)
			h := &recordingHandler{}
			getenv := func(k string) string {
				if k == "GOMEMLIMIT" {
					return tc.env
				}
				return ""
			}
			set := func(n int64) int64 {
				if n >= 0 {
					t.Errorf("SetMemoryLimit(%d) called with GOMEMLIMIT set; want the operator's limit left alone", n)
				}
				return debug.SetMemoryLimit(n)
			}

			d := apply(tc.fsys, getenv, set, slog.New(h))

			if d.Source != SourceEnv || d.CgroupLimitBytes != tc.cgroup || d.AppliedBytes != 0 {
				t.Fatalf("decision = %+v, want env with cgroup %d", d, tc.cgroup)
			}
			if got := debug.SetMemoryLimit(-1); got != tc.operator {
				t.Fatalf("effective limit = %d, want the operator's %d", got, tc.operator)
			}
			r := h.only(t)
			if r.Level != tc.level || !strings.Contains(r.Message, tc.message) {
				t.Errorf("log = %v %q, want %v containing %q", r.Level, r.Message, tc.level, tc.message)
			}
			assertPublished(t, tc.operator, SourceEnv, tc.cgroup)
			assertCeilingFollows(t, tc.operator)
		})
	}
}

// The cgroup path must be reached through a mount whose root is a path
// prefix of it, never by cleaning ".." away (the Go runtime's own rule,
// internal/runtime/cgroup). Before the fix, (a) and (b) read ANOTHER
// cgroup's limit and (c) missed the right mount.
func TestApply_CgroupPathResolution(t *testing.T) {
	t.Run("(a) mount root /.. is not the process's cgroup", func(t *testing.T) {
		// The mount exposes the PARENT of the cgroup namespace's root: its
		// memory.max is the parent's 16 GiB, not this container's.
		withProcessLimit(t, math.MaxInt64)
		h := &recordingHandler{}
		fsys := fstest.MapFS{
			"proc/self/cgroup":         {Data: []byte("0::/\n")},
			"proc/self/mountinfo":      {Data: []byte("30 25 0:26 /.. /sys/fs/cgroup ro,nosuid - cgroup2 cgroup2 rw\n")},
			"sys/fs/cgroup/memory.max": {Data: []byte("17179869184\n")},
		}
		d := apply(fsys, noEnv, debug.SetMemoryLimit, slog.New(h))
		if got := debug.SetMemoryLimit(-1); d.Source != SourceUnreadable || got != math.MaxInt64 {
			t.Fatalf("decision = %+v, effective %d; want unreadable and no limit (the mount cannot reach this cgroup)", d, got)
		}
		if r := h.only(t); r.Level != slog.LevelWarn {
			t.Errorf("log level = %v, want WARN", r.Level)
		}
		assertPublished(t, math.MaxInt64, SourceUnreadable, 0)
	})

	t.Run("(b) a cgroup outside the namespace (/../../init) is not reachable", func(t *testing.T) {
		withProcessLimit(t, math.MaxInt64)
		h := &recordingHandler{}
		fsys := fstest.MapFS{
			"proc/self/cgroup":              {Data: []byte("0::/../../init\n")},
			"proc/self/mountinfo":           {Data: []byte(v2Mountinfo)},
			"sys/fs/cgroup/init/memory.max": {Data: []byte("2147483648\n")},
			"sys/fs/cgroup/memory.max":      {Data: []byte("8589934592\n")},
		}
		d := apply(fsys, noEnv, debug.SetMemoryLimit, slog.New(h))
		if got := debug.SetMemoryLimit(-1); d.Source != SourceUnreadable || got != math.MaxInt64 {
			t.Fatalf("decision = %+v, effective %d; want unreadable and no limit (no mount reaches /../../init)", d, got)
		}
		if r := h.only(t); r.Level != slog.LevelWarn {
			t.Errorf("log level = %v, want WARN", r.Level)
		}
	})

	t.Run("(b2) /../../init is reached through a later mount rooted at /../..", func(t *testing.T) {
		// The Go runtime's own example: the namespace-root mount (root "/")
		// cannot reach /../../init and is skipped, not an error, so a later
		// mount that exposes it is used.
		withProcessLimit(t, math.MaxInt64)
		fsys := fstest.MapFS{
			"proc/self/cgroup": {Data: []byte("0::/../../init\n")},
			"proc/self/mountinfo": {Data: []byte(
				"30 25 0:26 / /sys/fs/cgroup ro - cgroup2 cgroup2 rw\n" +
					"31 25 0:26 /../.. /host/cgroup ro - cgroup2 cgroup2 rw\n")},
			"sys/fs/cgroup/memory.max":      {Data: []byte("17179869184\n")},
			"host/cgroup/init/memory.max":   {Data: []byte("2147483648\n")},
			"sys/fs/cgroup/init/memory.max": {Data: []byte("1073741824\n")},
		}
		d := apply(fsys, noEnv, debug.SetMemoryLimit, slog.New(&recordingHandler{}))
		want := 2*gib - 2*gib/8
		if got := debug.SetMemoryLimit(-1); d.Source != SourceCgroupV2 || d.CgroupLimitBytes != 2*gib || got != want {
			t.Fatalf("decision = %+v, effective %d; want cgroup_v2 2 GiB → %d through the /../.. mount", d, got, want)
		}
	})

	t.Run("(c) an unrelated first cgroup2 mount is skipped for the one that reaches the cgroup", func(t *testing.T) {
		withProcessLimit(t, math.MaxInt64)
		fsys := fstest.MapFS{
			"proc/self/cgroup": {Data: []byte("0::/kubepods/pod1/ctr1\n")},
			"proc/self/mountinfo": {Data: []byte(
				"29 25 0:26 /system.slice /run/other ro - cgroup2 cgroup2 rw\n" +
					"30 25 0:26 /kubepods/pod1 /sys/fs/cgroup ro - cgroup2 cgroup2 rw\n")},
			"run/other/memory.max":          {Data: []byte("1073741824\n")},
			"sys/fs/cgroup/ctr1/memory.max": {Data: []byte("8589934592\n")},
			"sys/fs/cgroup/memory.max":      {Data: []byte("17179869184\n")},
		}
		d := apply(fsys, noEnv, debug.SetMemoryLimit, slog.New(&recordingHandler{}))
		if got := debug.SetMemoryLimit(-1); d.Source != SourceCgroupV2 || d.CgroupLimitBytes != 8*gib || got != 7*gib {
			t.Fatalf("decision = %+v, effective %d; want cgroup_v2 8 GiB → 7 GiB through the second mount", d, got)
		}
		assertCeilingFollows(t, 7*gib)
	})

	t.Run("a root sharing a prefix but not a path boundary does not match", func(t *testing.T) {
		// /kube is not an ancestor of /kubepods/pod1.
		withProcessLimit(t, math.MaxInt64)
		fsys := fstest.MapFS{
			"proc/self/cgroup":                   {Data: []byte("0::/kubepods/pod1\n")},
			"proc/self/mountinfo":                {Data: []byte("30 25 0:26 /kube /sys/fs/cgroup ro - cgroup2 cgroup2 rw\n")},
			"sys/fs/cgroup/memory.max":           {Data: []byte("8589934592\n")},
			"sys/fs/cgroup/pods/pod1/memory.max": {Data: []byte("4294967296\n")},
		}
		d := apply(fsys, noEnv, debug.SetMemoryLimit, slog.New(&recordingHandler{}))
		if got := debug.SetMemoryLimit(-1); d.Source != SourceUnreadable || got != math.MaxInt64 {
			t.Fatalf("decision = %+v, effective %d; want unreadable and no limit", d, got)
		}
	})
}

// deniedFS refuses one path with a permission error, as a cgroup file the
// container cannot read would.
type deniedFS struct {
	fs.FS
	denied string
}

func (d deniedFS) Open(name string) (fs.File, error) {
	if name == d.denied {
		return nil, &fs.PathError{Op: "open", Path: name, Err: fs.ErrPermission}
	}
	return d.FS.Open(name)
}

func TestApply_UnreadableCgroupFile(t *testing.T) {
	cases := map[string]fs.FS{
		"memory.max permission denied":   deniedFS{FS: v2FS("8589934592\n"), denied: "sys/fs/cgroup/memory.max"},
		"memory.max not a number":        v2FS("eight gigabytes\n"),
		"mountinfo permission denied":    deniedFS{FS: v2FS("8589934592\n"), denied: "proc/self/mountinfo"},
		"limit_in_bytes not a number":    v1FS("\n"),
		"no cgroup mount for the cgroup": fstest.MapFS{"proc/self/cgroup": {Data: []byte("0::/\n")}, "proc/self/mountinfo": {Data: []byte("24 30 0:22 / /proc rw - proc proc rw\n")}},
	}
	for name, fsys := range cases {
		t.Run(name, func(t *testing.T) {
			withProcessLimit(t, math.MaxInt64)
			h := &recordingHandler{}

			d := apply(fsys, noEnv, debug.SetMemoryLimit, slog.New(h))

			if d.Source != SourceUnreadable || d.CgroupLimitBytes != 0 || d.AppliedBytes != 0 {
				t.Fatalf("decision = %+v, want unreadable", d)
			}
			if got := debug.SetMemoryLimit(-1); got != math.MaxInt64 {
				t.Fatalf("effective limit = %d, want none (MaxInt64): an unreadable limit is never guessed", got)
			}
			if r := h.only(t); r.Level != slog.LevelWarn {
				t.Errorf("log level = %v, want WARN (a failure)", r.Level)
			}
			assertPublished(t, math.MaxInt64, SourceUnreadable, 0)
			assertCeilingFollows(t, math.MaxInt64)
		})
	}
}

// Effective reads GOGC without changing it, including GOGC=off.
func TestEffective_GOGC(t *testing.T) {
	orig := debug.SetGCPercent(73)
	t.Cleanup(func() { debug.SetGCPercent(orig) })
	if g, _ := Effective(); g != 73 {
		t.Fatalf("Effective() gogc = %d, want 73", g)
	}
	if got := published(t)["gogc"].(float64); got != 73 {
		t.Errorf("published gogc = %.0f, want 73", got)
	}
	debug.SetGCPercent(-1)
	if g, _ := Effective(); g != -1 {
		t.Errorf("Effective() gogc with GC off = %d, want -1", g)
	}
	debug.SetGCPercent(73)
	if g, _ := Effective(); g != 73 {
		t.Errorf("Effective() changed GOGC: %d after a read, want 73", g)
	}
}

func TestUnescapeMountinfo(t *testing.T) {
	if got := unescapeMountinfo(`/sys/fs/cgroup\040x`); got != "/sys/fs/cgroup x" {
		t.Errorf("unescape = %q", got)
	}
	if got := unescapeMountinfo(`/a\0`); got != `/a\0` {
		t.Errorf("a truncated escape must be kept as is, got %q", got)
	}
}
