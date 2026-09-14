// retired_flags.go — #57 retired-flag startup audit.
//
// Task #57 folded formerly-standalone env flags into the single
// CACHE_ENABLED master gate (project_single_cache_flag_direction):
//
//   - PREWARM_ENABLED      — startup prewarm is now implicit-on-cache
//                            (PrewarmEnabled() == !Disabled()).
//   - RESOLVER_USE_INFORMER — the resolver/objects informer-serve pivot is
//                            now implicit-on-cache (resolverUseInformer() /
//                            useInformer() == !cache.Disabled()).
//   - RESOLVED_CACHE_APISTAGE_ENABLED — the api-stage L1 key-swap is now
//                            on iff ResolvedCacheEnabled()
//                            (ApistageL1Enabled() == ResolvedCacheEnabled()),
//                            same class as the two above.
//
// The helpers no longer READ these env vars, so a stale value in the
// environment is functionally ignored. We do NOT hard-error on a retired
// key: the standard `helm upgrade` recovery pattern legitimately carries
// the deployed release's values (which still set these keys) into the next
// upgrade through no operator fault — a hard error would brick that path
// (feedback_helm_no_reuse_values_on_chart_default_change). Instead we
// IGNORE + audit-log once per flag per process.
//
// Severity split (PM gate #57 C2):
//   - value "false"            -> slog.Warn. This is a SILENT behavior
//                                 change: the operator deliberately asked
//                                 for prewarm/pivot OFF, and now gets it ON
//                                 (it is implicit-on-cache). A behavior
//                                 change the operator did not consent to
//                                 must be loud (feedback_no_park_broken_behind_flag
//                                 spirit). The line names the flag, says it
//                                 is ignored, states the new effective
//                                 behavior, and gives the remediation.
//   - any other value ("true",
//     "1", ...)                -> slog.Info. A true no-op: the feature is
//                                 on anyway, so a single informational line
//                                 is sufficient.
//
// Absent keys emit nothing (os.LookupEnv distinguishes absent from set-empty).

package cache

import (
	"log/slog"
	"os"
	"strings"
	"sync"
)

// retiredFlag describes one folded env key and the implicit behavior that
// replaced it. behavior is interpolated into both the WARN and INFO lines.
type retiredFlag struct {
	name     string
	behavior string
	// warnWhen, when non-nil, replaces the default "value == false" Warn
	// trigger: it returns true for the values that mean the operator relied
	// on behaviour that no longer exists (a silent change worth a WARN).
	warnWhen func(val string) bool
}

// retiredFlags is the closed list of env keys folded into CACHE_ENABLED by
// #57. Adding a future fold appends here; the audit + its test enumerate
// this slice, so a new entry is covered automatically.
//
// These string literals are the ONLY surviving source-level occurrences of
// the folded env names outside test fixtures (falsifier (d)): no production
// code path reads them via os.Getenv any more.
var retiredFlags = []retiredFlag{
	{
		name:     "PREWARM_ENABLED",
		behavior: "startup prewarm is now implicit-on-cache; set CACHE_ENABLED=false to disable",
	},
	{
		name:     "RESOLVER_USE_INFORMER",
		behavior: "the informer-serve pivot is now implicit-on-cache; set CACHE_ENABLED=false to disable",
	},
	{
		name:     "RESOLVED_CACHE_APISTAGE_ENABLED",
		behavior: "the api-stage L1 is now on iff the resolved cache is on; set RESOLVED_CACHE_ENABLED=false (or CACHE_ENABLED=false) to disable",
	},
	// FOLDED 2026-07-03 (docs/prewarm-engine-implicit-on-cache-2026-07-03.md):
	// the prewarm FAMILY is now implicit-on-cache. These four on/off gates each
	// stopped reading their own env and return cache.PrewarmEnabled(). The
	// severity split makes any of them =false a loud Warn (the installer-test
	// footgun: an operator asked a stage OFF and now gets it ON). The seed
	// sizing knobs SEED_FOOTPRINT_BUDGET_BYTES / SEED_EST_UNIT_BYTES_FALLBACK
	// are NOT here — they were inert default-off CAPACITY knobs replaced by the
	// adaptive bound (no silent behavior change to warn about; §3.5).
	{
		name:     "PREWARM_CONTENT_ENABLED",
		behavior: "the Phase-1 content pass is now implicit-on-cache; set CACHE_ENABLED=false to disable",
	},
	{
		name:     "PREWARM_PIP_ENABLED",
		behavior: "the per-identity prewarm seed is now implicit-on-cache; set CACHE_ENABLED=false to disable",
	},
	{
		name:     "PREWARM_ENGINE_ENABLED",
		behavior: "the prewarm engine is now implicit-on-cache; set CACHE_ENABLED=false to disable",
	},
	{
		name:     "PROACTIVE_RA_SEED_ENABLED",
		behavior: "the proactive RESTAction seed is now implicit-on-cache; set CACHE_ENABLED=false to disable",
	},
	// RETIRED 1.12.6 C9 (design-1.12.6-event-hardening §10): the D bounded-
	// partial Put layer (partial_result_ttl.go) is gone. The flag was default
	// "0" (off) and never enabled on a deployment; a NON-zero value is the
	// silent change — the operator asked for a bounded-stale partial cache and
	// now gets a bare decline — so that is the Warn trigger, not "false".
	{
		name:     "PARTIAL_RESULT_TTL_SECONDS",
		behavior: "the bounded partial-result cache layer was removed; a stage-error resolve is served and never cached (no replacement knob)",
		warnWhen: func(val string) bool {
			v := strings.TrimSpace(val)
			return v != "" && v != "0"
		},
	},
}

// retiredFlagAuditedOnce guards warn-once-per-flag-per-process. Keyed by
// flag name; the stored *sync.Once fires the log line at most once even
// under concurrent AuditRetiredFlags calls.
var (
	retiredFlagAuditMu     sync.Mutex
	retiredFlagAuditedOnce = map[string]*sync.Once{}
)

// AuditRetiredFlags emits a one-time-per-flag audit line for each retired
// env flag (see retiredFlags) still present in the environment, at the
// severity dictated by the #57 C2 split (value "false" => Warn, else =>
// Info). Absent flags emit nothing. Safe for concurrent callers and
// idempotent across repeated calls (warn-once per flag per process).
//
// Invoked from main.go near the cache-mode banner. log must be non-nil
// (main.go passes the configured handler); a nil log is a no-op so a
// mis-wired caller cannot panic at boot.
func AuditRetiredFlags(log *slog.Logger) {
	if log == nil {
		return
	}
	for _, rf := range retiredFlags {
		val, present := os.LookupEnv(rf.name)
		if !present {
			continue
		}
		once := retiredFlagOnce(rf.name)
		once.Do(func() {
			warn := strings.EqualFold(strings.TrimSpace(val), "false")
			if rf.warnWhen != nil {
				warn = rf.warnWhen(val)
			}
			if warn {
				// Silent behavior change — the operator asked for OFF and
				// now gets ON. Loud by design.
				log.Warn("config.retired_flag_ignored",
					slog.String("subsystem", "cache"),
					slog.String("flag", rf.name),
					slog.String("value", val),
					slog.String("status", "ignored"),
					slog.String("effective_behavior", rf.behavior),
					slog.String("remediation", "remove "+rf.name+" from values/env"),
				)
				return
			}
			// Harmless no-op — the feature is on anyway.
			log.Info("config.retired_flag_ignored",
				slog.String("subsystem", "cache"),
				slog.String("flag", rf.name),
				slog.String("value", val),
				slog.String("status", "ignored"),
				slog.String("effective_behavior", rf.behavior),
			)
		})
	}
}

// retiredFlagOnce returns the per-flag sync.Once, lazily creating it under
// the mutex so AuditRetiredFlags is concurrency-safe.
func retiredFlagOnce(name string) *sync.Once {
	retiredFlagAuditMu.Lock()
	defer retiredFlagAuditMu.Unlock()
	once, ok := retiredFlagAuditedOnce[name]
	if !ok {
		once = &sync.Once{}
		retiredFlagAuditedOnce[name] = once
	}
	return once
}

// ResetRetiredFlagAuditForTest clears the warn-once state so a test can
// assert the audit fires afresh. TEST-ONLY — the production lifecycle
// audits once per process.
func ResetRetiredFlagAuditForTest() {
	retiredFlagAuditMu.Lock()
	defer retiredFlagAuditMu.Unlock()
	retiredFlagAuditedOnce = map[string]*sync.Once{}
}

// RetiredFlagNamesForTest returns the audited env-key names so a test can
// drive the audit without hardcoding the list.
func RetiredFlagNamesForTest() []string {
	out := make([]string, 0, len(retiredFlags))
	for _, rf := range retiredFlags {
		out = append(out, rf.name)
	}
	return out
}
