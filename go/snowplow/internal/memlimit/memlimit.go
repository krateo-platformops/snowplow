// Package memlimit sets the Go runtime's soft memory limit from the
// container's cgroup memory limit, once, at startup.
//
// WHY. The chart used to hand-set GOMEMLIMIT=7GiB next to an 8Gi container
// limit, plus GOGC=50. Two numbers that had to move together and were set
// by hand. Paired run 23 (50K compositions × 1000 users, 4 cores) measured
// GC at ~40% of CPU with GOGC=50, so the chart now leaves GOGC at the runtime
// default (100), and the soft limit is derived here from the limit the
// container actually runs under, so it follows resources.limits.memory.
//
// THE SHARE. The soft limit is the cgroup limit less 1/limitHeadroomDivisor
// of it (7/8 of the container). Derivation, from run 23:
//
//	pod working set ≈ live heap × (1 + GOGC/100) + ~1 GiB
//
// The ~1 GiB term is memory the heap goal does not explain: goroutine
// stacks, runtime metadata, span fragmentation, pages the scavenger has not
// returned yet, and what the kernel charges the container outside the Go
// runtime. Part of it is inside what GOMEMLIMIT governs and part is not;
// counting ALL of it as outside the limit is the conservative reading. It
// was measured under an 8 GiB container, so the headroom it needs is
// 1 GiB / 8 GiB = 1/8 of the container, and the soft limit is the other 7/8.
// It is a share, not a byte count, so it scales with the container instead
// of going negative on a small one.
//
// On the chart's 8Gi container this gives 7 GiB, the value the chart hand-set
// and run 23 ran under with no restarts, so the limit itself does not move at
// the chart's size; only GOGC does. The predicted GOGC=100 peak working set
// (~6.5 GiB) sits under the 7 GiB soft limit, so the limit is not expected to
// bind (and drive GC back up) in steady state.
//
// PRECEDENCE.
//   - GOMEMLIMIT in the environment (any non-empty value, "off" included):
//     the operator chose. The runtime already applied it before main; it is
//     never overridden. The cgroup limit is still read and published beside
//     it: INFO when the operator's limit is below it, WARN when it is at or
//     above it (Linux then OOM-kills before the GC back-pressures: a past
//     incident) or the cgroup cannot be read to check.
//   - Otherwise, the cgroup limit (v2 memory.max, or v1
//     memory.limit_in_bytes, the smallest along the cgroup's visible
//     ancestry) × 7/8 is applied with debug.SetMemoryLimit. The cgroup
//     directory is found the way the Go runtime finds it: through the first
//     mount whose root is a path prefix of the cgroup path, never by
//     cleaning ".." away; no such mount is "unreadable".
//   - No cgroup limit: no soft limit is set (the runtime default, unlimited),
//     logged at INFO.
//   - The cgroup files cannot be read or parsed: no soft limit is set,
//     logged at WARN. That is the pre-existing behaviour of a pod without
//     GOMEMLIMIT, never a guess.
//
// Readers of the limit (cache.AdmissionCeiling, the nested-resolve bound)
// read the effective value with debug.SetMemoryLimit(-1), never the env, so
// they follow whatever this package decided.
package memlimit

import (
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"math"
	"os"
	"runtime/debug"
	"strconv"
	"strings"
	"sync"
)

// limitHeadroomDivisor: the soft limit leaves 1/limitHeadroomDivisor of the
// container's memory limit for memory outside the heap goal. See the package
// comment for the derivation from run 23 (~1 GiB of non-heap overhead in an
// 8 GiB container).
const limitHeadroomDivisor int64 = 8

// Source says where the effective soft memory limit came from.
type Source string

const (
	// SourceEnv: GOMEMLIMIT was set in the environment; honoured as is.
	SourceEnv Source = "env"
	// SourceCgroupV2: derived from a cgroup v2 memory.max.
	SourceCgroupV2 Source = "cgroup_v2"
	// SourceCgroupV1: derived from a cgroup v1 memory.limit_in_bytes.
	SourceCgroupV1 Source = "cgroup_v1"
	// SourceNone: no cgroup memory limit applies; no soft limit set.
	SourceNone Source = "none"
	// SourceUnreadable: the cgroup limit could not be read; no soft limit set.
	SourceUnreadable Source = "unreadable"
)

// Decision is what Apply decided. Published on /debug/vars and OTLP.
type Decision struct {
	Source Source
	// CgroupLimitBytes is the container's cgroup memory limit, 0 when none
	// was found or it could not be read. Read and kept when the env took
	// precedence too, so an operator limit can be compared with it.
	CgroupLimitBytes int64
	// AppliedBytes is the limit passed to debug.SetMemoryLimit, 0 when this
	// package set none.
	AppliedBytes int64
}

var (
	decisionMu sync.RWMutex
	decision   = Decision{Source: SourceNone}
)

// Current returns the last decision Apply made.
func Current() Decision {
	decisionMu.RLock()
	defer decisionMu.RUnlock()
	return decision
}

// Apply decides and applies the soft memory limit for this process from the
// real environment and the real /proc and /sys. Call once, early in main.
func Apply(log *slog.Logger) Decision {
	return apply(os.DirFS("/"), os.Getenv, debug.SetMemoryLimit, log)
}

// apply is Apply over injectable inputs. fsys is rooted at "/".
func apply(fsys fs.FS, getenv func(string) string, setLimit func(int64) int64, log *slog.Logger) Decision {
	d := decide(fsys, getenv, setLimit, log)
	decisionMu.Lock()
	decision = d
	decisionMu.Unlock()
	return d
}

func decide(fsys fs.FS, getenv func(string) string, setLimit func(int64) int64, log *slog.Logger) Decision {
	limit, src, err := cgroupMemoryLimit(fsys)

	// An empty GOMEMLIMIT is unset to the runtime too.
	if v := getenv("GOMEMLIMIT"); v != "" {
		return decideEnv(v, limit, src, err, setLimit(-1), log)
	}

	if err != nil {
		// log-ctx-waiver: boot: main applies the limit once at start, before a context exists
		log.Warn("memory limit: the container's cgroup memory limit could not be read; no soft memory limit is set",
			slog.String("subsystem", "runtime"),
			slog.Any("err", err))
		return Decision{Source: SourceUnreadable}
	}
	if limit == 0 {
		log.Info("memory limit: no cgroup memory limit applies to this process; no soft memory limit is set",
			slog.String("subsystem", "runtime"))
		return Decision{Source: SourceNone}
	}

	soft := limit - limit/limitHeadroomDivisor
	setLimit(soft)
	log.Info("memory limit: soft memory limit derived from the container's cgroup memory limit",
		slog.String("subsystem", "runtime"),
		slog.String("source", string(src)),
		slog.Int64("cgroup_memory_limit_bytes", limit),
		slog.Int64("memory_limit_bytes", soft),
		slog.String("share", fmt.Sprintf("%d/%d", limitHeadroomDivisor-1, limitHeadroomDivisor)))
	return Decision{Source: src, CgroupLimitBytes: limit, AppliedBytes: soft}
}

// decideEnv: the operator set GOMEMLIMIT and the runtime already applied it
// (effective). It is never overridden. The container's limit is still read
// and published beside it, so a value at or above the container limit, the
// configuration behind a past OOM-kill incident (Linux kills the pod before
// the GC back-pressures), is a WARN instead of going unseen. "off" is the
// largest limit there is, so under any container limit it warns too.
func decideEnv(v string, limit int64, src Source, err error, effective int64, log *slog.Logger) Decision {
	attrs := []any{
		slog.String("subsystem", "runtime"),
		slog.String("gomemlimit", v),
		slog.Int64("memory_limit_bytes", effective),
	}
	switch {
	case err != nil:
		// log-ctx-waiver: boot: main applies the limit once at start, before a context exists
		log.Warn("memory limit: GOMEMLIMIT is set in the environment and honoured, but the container's cgroup memory limit could not be read to check it against",
			append(attrs, slog.Any("err", err))...)
		return Decision{Source: SourceEnv}
	case limit > 0 && effective >= limit:
		// log-ctx-waiver: boot: main applies the limit once at start, before a context exists
		log.Warn("memory limit: GOMEMLIMIT is set in the environment at or above the container's cgroup memory limit; honoured, but Linux will OOM-kill the pod before the GC back-pressures",
			append(attrs, slog.String("cgroup", string(src)), slog.Int64("cgroup_memory_limit_bytes", limit))...)
	default:
		log.Info("memory limit: GOMEMLIMIT is set in the environment; honoured, not derived from the container",
			append(attrs, slog.Int64("cgroup_memory_limit_bytes", limit))...)
	}
	return Decision{Source: SourceEnv, CgroupLimitBytes: limit}
}

// cgroupMemoryLimit returns the memory limit that applies to this process's
// cgroup: the smallest limit from the process's cgroup directory up to the
// root of the cgroup mount it can see. 0 means no limit applies. A missing
// /proc/self/cgroup (not Linux) is no limit, not an error.
func cgroupMemoryLimit(fsys fs.FS) (int64, Source, error) {
	cg, err := fs.ReadFile(fsys, "proc/self/cgroup")
	if errors.Is(err, fs.ErrNotExist) {
		return 0, SourceNone, nil
	}
	if err != nil {
		return 0, SourceUnreadable, err
	}
	v1Path, v2Path, haveV1, haveV2 := parseProcCgroup(string(cg))
	if !haveV1 && !haveV2 {
		return 0, SourceUnreadable, errors.New("proc/self/cgroup names neither a cgroup v2 path nor a v1 memory controller")
	}

	mi, err := fs.ReadFile(fsys, "proc/self/mountinfo")
	if err != nil {
		return 0, SourceUnreadable, err
	}

	// The memory controller is attached to exactly one hierarchy. When a v1
	// hierarchy lists it (v1, or a hybrid host), that is where the limit is;
	// otherwise it is the unified v2 hierarchy.
	src, cgPath, file, parse := SourceCgroupV2, v2Path, "memory.max", parseV2Limit
	if haveV1 {
		src, cgPath, file, parse = SourceCgroupV1, v1Path, "memory.limit_in_bytes", parseV1Limit
	}
	top, dir, err := cgroupDir(string(mi), cgPath, src == SourceCgroupV1)
	if err != nil {
		return 0, SourceUnreadable, fmt.Errorf("%s: %w", src, err)
	}

	// Walk from the cgroup's directory up to the mount point, taking the
	// smallest limit. Directories are cut at their last '/', never cleaned:
	// cgroupDir guarantees no "." or ".." element.
	var limit int64
	for {
		f := joinFS(dir, file)
		b, err := fs.ReadFile(fsys, f)
		switch {
		case errors.Is(err, fs.ErrNotExist):
			// The root cgroup (and a v2 cgroup without the memory controller
			// enabled) has no limit file: no limit at this level.
		case err != nil:
			return 0, SourceUnreadable, err
		default:
			l, err := parse(strings.TrimSpace(string(b)))
			if err != nil {
				return 0, SourceUnreadable, fmt.Errorf("%s: %w", f, err)
			}
			if l > 0 && (limit == 0 || l < limit) {
				limit = l
			}
		}
		if dir == top {
			break
		}
		dir = dir[:max(strings.LastIndexByte(dir, '/'), 0)]
	}
	return limit, src, nil
}

// joinFS joins an fs.FS directory ("" is the root) and a file name.
func joinFS(dir, file string) string {
	if dir == "" {
		return file
	}
	return dir + "/" + file
}

// cgroupDir resolves the process's cgroup path to its directory under a
// cgroup mount, the way the Go runtime does (internal/runtime/cgroup,
// getCgroupMount): walk EVERY matching mount in mountinfo and take the first
// whose root is a path prefix of the cgroup path (at a path boundary); the
// directory is the mount point plus the rest of the path. Nothing is
// cleaned, so ".." can never be resolved away:
//   - a remainder starting with "/.." is a cgroup outside this cgroup
//     namespace, which this mount cannot reach: skipped;
//   - a mount root like "/.." is not a prefix of the cgroup path: skipped;
//   - any other "." or ".." element is refused rather than interpreted.
//
// No mount reaching the cgroup is an error: the limit cannot be read.
// Returned paths are fs.FS paths (no leading '/'): top is the mount point,
// dir the cgroup's directory under it.
//
// mountinfo line: id parent maj:min root mountpoint opts [optional...] - fstype source superopts
func cgroupDir(mountinfo, cgPath string, v1 bool) (top, dir string, err error) {
	if !strings.HasPrefix(cgPath, "/") {
		return "", "", fmt.Errorf("proc/self/cgroup path %q is not absolute", cgPath)
	}
	for _, line := range strings.Split(mountinfo, "\n") {
		fields := strings.Fields(line)
		sep := -1
		for i, f := range fields {
			if f == "-" {
				sep = i
				break
			}
		}
		if sep < 5 || len(fields) < sep+4 {
			continue
		}
		fstype, superopts := fields[sep+1], fields[sep+3]
		switch {
		case !v1 && fstype == "cgroup2":
		case v1 && fstype == "cgroup" && hasOpt(superopts, "memory"):
		default:
			continue
		}
		root := unescapeMountinfo(fields[3])
		if !strings.HasPrefix(root, "/") {
			return "", "", fmt.Errorf("proc/self/mountinfo: cgroup mount root %q is not absolute", root)
		}
		if !hasPathPrefix(cgPath, root) {
			continue
		}
		rel := cgPath[len(root):]
		if root == "/" {
			rel = cgPath
		}
		if hasPathPrefix(rel, "/..") {
			continue // outside this mount's reach (outside the cgroup namespace)
		}
		if hasDotElement(rel) {
			return "", "", fmt.Errorf("proc/self/cgroup path %q has a '.' or '..' element", cgPath)
		}
		mnt := unescapeMountinfo(fields[4])
		if !strings.HasPrefix(mnt, "/") || hasDotElement(mnt) {
			return "", "", fmt.Errorf("proc/self/mountinfo: cgroup mount point %q is not a clean absolute path", mnt)
		}
		top = strings.TrimSuffix(strings.TrimPrefix(mnt, "/"), "/")
		dir = strings.TrimPrefix(strings.TrimSuffix(top+rel, "/"), "/")
		return top, dir, nil
	}
	return "", "", errors.New("no cgroup mount in proc/self/mountinfo reaches this process's cgroup")
}

// hasPathPrefix reports whether p starts with prefix at a path boundary
// ("/" is a prefix of every absolute path). Byte comparison, no cleaning.
func hasPathPrefix(p, prefix string) bool {
	if prefix == "/" {
		return true
	}
	return strings.HasPrefix(p, prefix) && (len(p) == len(prefix) || p[len(prefix)] == '/')
}

// hasDotElement reports whether a slash-separated path has a "." or ".."
// element.
func hasDotElement(p string) bool {
	for _, e := range strings.Split(p, "/") {
		if e == "." || e == ".." {
			return true
		}
	}
	return false
}

// parseProcCgroup reads /proc/self/cgroup ("id:controllers:path" per line):
// the v2 unified entry is "0::<path>"; a v1 entry lists its controllers.
func parseProcCgroup(s string) (v1Path, v2Path string, haveV1, haveV2 bool) {
	for _, line := range strings.Split(s, "\n") {
		parts := strings.SplitN(line, ":", 3)
		if len(parts) != 3 {
			continue
		}
		if parts[0] == "0" && parts[1] == "" {
			v2Path, haveV2 = parts[2], true
			continue
		}
		for _, c := range strings.Split(parts[1], ",") {
			if c == "memory" {
				v1Path, haveV1 = parts[2], true
			}
		}
	}
	return
}

func hasOpt(opts, want string) bool {
	for _, o := range strings.Split(opts, ",") {
		if o == want {
			return true
		}
	}
	return false
}

// unescapeMountinfo decodes the kernel's octal escapes (\040 for a space,
// \011, \012, \134) in a mountinfo path field.
func unescapeMountinfo(s string) string {
	if !strings.Contains(s, `\`) {
		return s
	}
	var b strings.Builder
	for i := 0; i < len(s); i++ {
		if s[i] == '\\' && i+3 < len(s) {
			if n, err := strconv.ParseUint(s[i+1:i+4], 8, 8); err == nil {
				b.WriteByte(byte(n))
				i += 3
				continue
			}
		}
		b.WriteByte(s[i])
	}
	return b.String()
}

// parseV2Limit parses memory.max: "max" is no limit (0).
func parseV2Limit(s string) (int64, error) {
	if s == "max" {
		return 0, nil
	}
	return parseBytes(s)
}

// parseV1Limit parses memory.limit_in_bytes. v1 reports "no limit" as the
// largest page-aligned int64 (PAGE_COUNTER_MAX × page size), returned as 0.
func parseV1Limit(s string) (int64, error) {
	n, err := parseBytes(s)
	if err != nil {
		return 0, err
	}
	ps := int64(os.Getpagesize())
	if n >= math.MaxInt64/ps*ps {
		return 0, nil
	}
	return n, nil
}

func parseBytes(s string) (int64, error) {
	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("not a byte count: %w", err)
	}
	if n <= 0 {
		return 0, fmt.Errorf("not a positive byte count: %d", n)
	}
	return n, nil
}
