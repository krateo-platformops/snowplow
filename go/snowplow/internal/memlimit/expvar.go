package memlimit

// expvar.go — the effective GC settings as an observable, so a bench run and
// 057 can read what the process actually runs with instead of inferring it
// from the chart (the live value, read from the image's runtime, never from
// notes). Values are read from runtime/metrics at each read, so they reflect
// a later debug.SetGCPercent / SetMemoryLimit too.

import (
	"expvar"
	"math"
	"runtime/metrics"
	"sync"
)

const (
	gogcMetric     = "/gc/gogc:percent"
	memLimitMetric = "/gc/gomemlimit:bytes"
)

// Effective returns the runtime's current GOGC (-1 when GC is off) and soft
// memory limit in bytes (math.MaxInt64 when there is none). Read-only: unlike
// debug.SetGCPercent, reading it changes nothing.
func Effective() (gogc, memoryLimit int64) {
	s := []metrics.Sample{{Name: gogcMetric}, {Name: memLimitMetric}}
	metrics.Read(s)
	if s[0].Value.Kind() == metrics.KindUint64 {
		// The runtime stores GOGC as an int32 and exports it widened to a
		// uint64, so GOGC=off (-1) arrives as 2^64-1; the int32 view is the
		// configured value.
		gogc = int64(int32(s[0].Value.Uint64()))
	}
	memoryLimit = math.MaxInt64
	if s[1].Value.Kind() == metrics.KindUint64 {
		if v := s[1].Value.Uint64(); v < math.MaxInt64 {
			memoryLimit = int64(v)
		}
	}
	return gogc, memoryLimit
}

var expvarOnce sync.Once

// PublishExpvar publishes snowplow_go_runtime on /debug/vars:
//
//	gogc                       effective GOGC (-1 = off)
//	memory_limit_bytes         effective soft memory limit (9223372036854775807 = none)
//	memory_limit_source        env | cgroup_v2 | cgroup_v1 | none | unreadable
//	cgroup_memory_limit_bytes  the container's cgroup limit the limit was derived from (0 = not derived)
//
// Registered unconditionally: GC settings are not a cache concept. Safe to
// call more than once.
func PublishExpvar() {
	expvarOnce.Do(func() {
		expvar.Publish("snowplow_go_runtime", expvar.Func(func() any {
			gogc, limit := Effective()
			d := Current()
			return map[string]any{
				"gogc":                      gogc,
				"memory_limit_bytes":        limit,
				"memory_limit_source":       string(d.Source),
				"cgroup_memory_limit_bytes": d.CgroupLimitBytes,
			}
		}))
	})
}
