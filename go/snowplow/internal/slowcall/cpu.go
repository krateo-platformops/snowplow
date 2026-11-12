package slowcall

// cpu.go — CPU starvation over a slow request's lifetime (#1054 F4).
//
// In run 20 snowplow sat at its 4-CPU limit the whole time. A request that
// waited 30 s because the CFS quota throttled the container is a different
// finding from one that waited on a lock, and only the cgroup knows: its
// cpu.stat counts the periods in which the container was throttled
// (nr_throttled) and for how long (throttled_usec). The process's own CPU time
// (getrusage) over the same window says how much of the limit snowplow used.
//
// Reading cpu.stat at every request's start would cost each request a file
// read, so a sampler reads it (and getrusage) every sampleEvery into a ring.
// A slow request's window starts at the last sample at or before its start
// (so it covers up to sampleEvery more than the request) and ends with a
// fresh read at its emit: the fast path pays nothing. cpu_window_ms says how
// long the window really was; a window older than the ring is marked
// cpu_window_partial.

import (
	"bufio"
	"bytes"
	"errors"
	"io/fs"
	"log/slog"
	"math"
	"os"
	"strconv"
	"sync"
	"time"
)

// sampleEvery is the sampler's period: a fifteenth of the bound, so a slow
// request's window is never more than ~7 % longer than the request.
const sampleEvery = Bound / 15

// cpuRingSize samples cover sampleEvery × cpuRingSize (204.8 s): longer than
// any request but the server write deadline allows to matter.
const cpuRingSize = 1024

type cpuSample struct {
	at      time.Time
	procNS  int64 // process user+system CPU
	cgOK    bool
	usageNS int64 // cgroup CPU used; -1 when cpu.stat carries none (v1: it is in cpuacct)
	periods int64 // cgroup CFS periods elapsed
	thrPer  int64 // periods in which the cgroup was throttled
	thrNS   int64 // time the cgroup was throttled
	procOK  bool
}

// cgroupDir is the cgroup filesystem's mount point.
const cgroupDir = "/sys/fs/cgroup"

// cgroupStatPaths are cpu.stat's places under cgroupDir: cgroup v2 (one
// unified file), then v1's cpu controller.
var cgroupStatPaths = []string{"cpu.stat", "cpu,cpuacct/cpu.stat", "cpu/cpu.stat"}

// cgroupRoot is cgroupDir opened as an os.Root, so a cgroupStatPaths entry is
// read beneath it and cannot reach any other file, whatever the entry holds.
var cgroupRoot = &rootCache{dir: cgroupDir, open: os.OpenRoot}

// rootCache opens dir as an os.Root once it can and keeps it for the process:
// one descriptor, shared by the sampler's goroutine and the requests' emit
// reads (os.Root's methods are safe for concurrent use). A directory that does
// not exist (no cgroup filesystem: not Linux, the browser build) is settled
// too, as nil; a sample then carries no cgroup fields, as it does when no
// cpu.stat is found. Any other failure (a transient one such as EMFILE) is not
// kept: that sample carries no cgroup fields and the next one opens again.
type rootCache struct {
	dir  string
	open func(string) (*os.Root, error)

	mu      sync.Mutex
	settled bool
	root    *os.Root
}

func (c *rootCache) get() *os.Root {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.settled {
		return c.root
	}
	r, err := c.open(c.dir)
	switch {
	case err == nil:
		c.root, c.settled = r, true
	case errors.Is(err, fs.ErrNotExist):
		c.settled = true
	}
	return c.root
}

type cpuSampler struct {
	read func() cpuSample

	mu   sync.Mutex
	ring [cpuRingSize]cpuSample
	n    int // samples taken
	once sync.Once
}

func newCPUSampler() *cpuSampler {
	return &cpuSampler{read: readCPU}
}

func (c *cpuSampler) start() {
	c.once.Do(func() {
		go func() {
			t := time.NewTicker(sampleEvery)
			defer t.Stop()
			c.sample()
			for range t.C {
				c.sample()
			}
		}()
	})
}

func (c *cpuSampler) sample() {
	s := c.read()
	c.mu.Lock()
	c.ring[c.n%cpuRingSize] = s
	c.n++
	c.mu.Unlock()
}

// before returns the newest sample at or before t, or the oldest held one
// (partial=true) when t is older than the ring; ok=false with no sample.
func (c *cpuSampler) before(t time.Time) (s cpuSample, partial, ok bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.n == 0 {
		return cpuSample{}, false, false
	}
	lo := 0
	if c.n > cpuRingSize {
		lo = c.n - cpuRingSize
	}
	for i := c.n - 1; i >= lo; i-- {
		if e := c.ring[i%cpuRingSize]; !e.at.After(t) {
			return e, false, true
		}
	}
	return c.ring[lo%cpuRingSize], true, true
}

// windowAttrs is the CPU starvation between the sample before start and a
// fresh read now, as flat attributes (none when no sample exists).
func (c *cpuSampler) windowAttrs(start time.Time) []slog.Attr {
	from, partial, ok := c.before(start)
	if !ok {
		return nil
	}
	to := c.read()
	win := to.at.Sub(from.at)
	out := []slog.Attr{slog.Int64("cpu_window_ms", win.Milliseconds())}
	if partial {
		out = append(out, slog.Bool("cpu_window_partial", true))
	}
	if from.procOK && to.procOK {
		cpu := to.procNS - from.procNS
		out = append(out, slog.Int64("process_cpu_ms", cpu/int64(time.Millisecond)))
		if win > 0 {
			out = append(out, slog.Float64("process_cores", math.Round(float64(cpu)/float64(win)*100)/100))
		}
	}
	if from.cgOK && to.cgOK {
		// Usage only when both reads carried it: a v1 cpu.stat has none, and
		// the delta of two missing values is UNMEASURED, never 0.
		if from.usageNS >= 0 && to.usageNS >= 0 {
			out = append(out, slog.Int64("cgroup_cpu_ms", (to.usageNS-from.usageNS)/int64(time.Millisecond)))
		}
		out = append(out,
			slog.Int64("cfs_periods", to.periods-from.periods),
			slog.Int64("cfs_throttled_periods", to.thrPer-from.thrPer),
			slog.Int64("cfs_throttled_ms", (to.thrNS-from.thrNS)/int64(time.Millisecond)))
	}
	return out
}

func readCPU() cpuSample { return readCPUFrom(cgroupRoot.get()) }

// readCPUFrom is one sample, its cpu.stat read beneath cg (none when cg is nil).
func readCPUFrom(cg *os.Root) cpuSample {
	t0 := time.Now() // the sample's own instant
	s := cpuSample{at: t0}
	s.procNS, s.procOK = processCPU()
	if cg == nil {
		return s
	}
	for _, p := range cgroupStatPaths {
		if b, err := cg.ReadFile(p); err == nil {
			s.usageNS, s.periods, s.thrPer, s.thrNS, s.cgOK = parseCPUStat(b)
			if s.cgOK {
				break
			}
		}
	}
	return s
}

// parseCPUStat reads a cgroup v2 cpu.stat (usage_usec, nr_periods,
// nr_throttled, throttled_usec) or a v1 one (nr_periods, nr_throttled,
// throttled_time in ns; usage lives in cpuacct, not here).
//
// usageNS is -1 when the file carries no usage_usec.
func parseCPUStat(b []byte) (usageNS, periods, thrPer, thrNS int64, ok bool) {
	usageNS = -1
	sc := bufio.NewScanner(bytes.NewReader(b))
	for sc.Scan() {
		f := bytes.Fields(sc.Bytes())
		if len(f) != 2 {
			continue
		}
		v, err := strconv.ParseInt(string(f[1]), 10, 64)
		if err != nil {
			continue
		}
		switch string(f[0]) {
		case "usage_usec":
			usageNS = v * 1000
		case "nr_periods":
			periods, ok = v, true
		case "nr_throttled":
			thrPer, ok = v, true
		case "throttled_usec":
			thrNS = v * 1000
		case "throttled_time":
			thrNS = v
		}
	}
	return usageNS, periods, thrPer, thrNS, ok
}
