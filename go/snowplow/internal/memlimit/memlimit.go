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
//     left alone and logged at INFO.
//   - Otherwise, the cgroup limit (v2 memory.max, or v1
//     memory.limit_in_bytes, the smallest along the cgroup's visible
//     ancestry) × 7/8 is applied with debug.SetMemoryLimit.
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
	"path"
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
	// was found or it could not be read (and when the env took precedence).
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
	// An empty GOMEMLIMIT is unset to the runtime too.
	if v := getenv("GOMEMLIMIT"); v != "" {
		log.Info("memory limit: GOMEMLIMIT is set in the environment; honoured, not derived from the container",
			slog.String("subsystem", "runtime"),
			slog.String("gomemlimit", v),
			slog.Int64("memory_limit_bytes", setLimit(-1)))
		return Decision{Source: SourceEnv}
	}

	limit, src, err := cgroupMemoryLimit(fsys)
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
	mountPoint, mountRoot, ok := findCgroupMount(string(mi), src == SourceCgroupV1)
	if !ok {
		return 0, SourceUnreadable, fmt.Errorf("no %s mount in proc/self/mountinfo", src)
	}
	rel, ok := relUnder(mountRoot, cgPath)
	if !ok {
		return 0, SourceUnreadable, fmt.Errorf("the %s cgroup path is not under the mount's root", src)
	}

	top := strings.TrimPrefix(path.Clean(mountPoint), "/")
	dir := path.Join(top, rel)
	var limit int64
	for {
		b, err := fs.ReadFile(fsys, path.Join(dir, file))
		switch {
		case errors.Is(err, fs.ErrNotExist):
			// The root cgroup (and a v2 cgroup without the memory controller
			// enabled) has no limit file: no limit at this level.
		case err != nil:
			return 0, SourceUnreadable, err
		default:
			l, err := parse(strings.TrimSpace(string(b)))
			if err != nil {
				return 0, SourceUnreadable, fmt.Errorf("%s: %w", path.Join(dir, file), err)
			}
			if l > 0 && (limit == 0 || l < limit) {
				limit = l
			}
		}
		if dir == top || dir == "." || dir == "" {
			break
		}
		dir = path.Dir(dir)
	}
	return limit, src, nil
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

// findCgroupMount finds the cgroup2 mount (v1=false) or the v1 cgroup mount
// carrying the memory controller (v1=true) in /proc/self/mountinfo and returns
// its mount point and the cgroup path it exposes (its root).
//
// mountinfo line: id parent maj:min root mountpoint opts [optional...] - fstype source superopts
func findCgroupMount(s string, v1 bool) (mountPoint, root string, ok bool) {
	for _, line := range strings.Split(s, "\n") {
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
		return unescapeMountinfo(fields[4]), unescapeMountinfo(fields[3]), true
	}
	return "", "", false
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

// relUnder returns p relative to root ("." when equal), both cgroup paths.
func relUnder(root, p string) (string, bool) {
	root, p = path.Clean("/"+root), path.Clean("/"+p)
	if root == "/" {
		return strings.TrimPrefix(p, "/"), true
	}
	if p == root {
		return ".", true
	}
	if strings.HasPrefix(p, root+"/") {
		return strings.TrimPrefix(p, root+"/"), true
	}
	return "", false
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
