// shadow_parity_env.go — #367: boot-time env backing for the dark shadow-parity
// toggle (shadow_hook.go). Lets a cold boot start with the dark measurement ON so
// the boot-walk populate arm (populate_seed_installs_total + the four classify
// buckets) is reachable on a real cluster — the boot seed walk runs during readyz,
// before any POST /debug/shadow-parity could flip the toggle, and the keepwarm
// re-seed fallback is a load-anti-correlated subset (the TTL/4 age-skip), so it
// cannot stand in for the boot walk that certifies the populator keying (#254).

package rbac

import (
	"log/slog"
	"os"
	"strconv"
	"sync/atomic"
)

// ShadowParityEnvVar is the boot-time env var that seeds shadowParityEnabled at
// startup. DEFAULT-OFF: unset, empty, "false" or any unparseable value leaves the
// dark measurement off; only an explicit truthy value enables it.
const ShadowParityEnvVar = "SHADOW_PARITY_ENABLED"

// resolveShadowParityEnabled encodes the DEFAULT-OFF env contract: true IFF raw
// parses (strconv.ParseBool) to true; every other input — unset/""/"false"/
// unparseable — is false. Mirrors the POST /debug/shadow-parity handler's
// strconv.ParseBool so the env and the runtime override agree on what "true"
// means. Pure; unit-tested. (Contrast resolvePrewarmRegisterDefault in main.go,
// which is default-ON; this toggle is default-OFF — absence must never enable a
// measurement.)
func resolveShadowParityEnabled(raw string) bool {
	v, err := strconv.ParseBool(raw)
	return err == nil && v
}

// InitShadowParityFromEnv initialises shadowParityEnabled from ShadowParityEnvVar
// ONCE at process startup. main() MUST call it BEFORE the boot seed walk so the
// boot-walk populate arm is reachable (the walk runs during readyz). The runtime
// POST /debug/shadow-parity override remains available afterwards and wins by
// running later. Default-off: an unset env leaves the toggle off, so a pod without
// the env is byte-identical to the pre-#367 default.
//
// DARK (unchanged by this env backing): the toggle still gates ONLY the dark
// shadow-parity measurement — no verdict, no served byte, no cache key. The one
// property #367 deliberately gives up is the old "structurally cannot persist
// across a restart" guarantee (see the shadow_hook.go header): with the env SET a
// fresh pod boots with the dark measurement ON, so the MEASUREMENT window wants
// the env set while a LATENCY-ACCEPTANCE window wants it unset (chart default-off;
// operator opt-in).
// Toggle-source labels for the /debug/vars detectability scalar (#367): what most
// recently DETERMINED the toggle's current state.
const (
	shadowParitySourceDefault     = "default"      // never set, or env unset/off
	shadowParitySourceEnv         = "env"          // SHADOW_PARITY_ENABLED at boot
	shadowParitySourceRuntimePost = "runtime-post" // POST /debug/shadow-parity
)

// shadowParitySource records what most recently set shadowParityEnabled, for the
// /debug/vars detectability scalar. nil ⇒ default.
var shadowParitySource atomic.Pointer[string]

func setShadowParitySource(s string) { shadowParitySource.Store(&s) }

// ShadowParitySource reports the source of the toggle's CURRENT state —
// "default" | "env" | "runtime-post" — for the /debug/vars scalar (#367), so a
// latency-acceptance window can assert the toggle is off (enabled==0) instead of
// trusting the convention.
func ShadowParitySource() string {
	if p := shadowParitySource.Load(); p != nil {
		return *p
	}
	return shadowParitySourceDefault
}

func InitShadowParityFromEnv() {
	raw, present := os.LookupEnv(ShadowParityEnvVar)
	shadowParityEnabled.Store(resolveShadowParityEnabled(raw))
	// Source fidelity (#367, arch/pm nit): the env DETERMINED the state whenever it
	// is PRESENT and non-empty — even when it sets OFF ("false") or is unparseable —
	// so report source="env" then; "default" only when truly unset/empty. The
	// assert-off contract (enabled==0) is unaffected; this just distinguishes "env
	// explicitly off" from "nothing set it".
	if present && raw != "" {
		setShadowParitySource(shadowParitySourceEnv)
	} else {
		setShadowParitySource(shadowParitySourceDefault)
	}
	// Fail-safe-OFF with a one-shot WARN on a NON-EMPTY unparseable value ("ture",
	// "2", "yes!"): a typo'd opt-in must not SILENTLY stay off (#367 — pm cond. 5 /
	// TL). Empty/unset is the silent default. raw is a boolean flag (no per-identity
	// data), so logging the raw value is safe. Init runs once at boot ⇒ one WARN.
	if _, err := strconv.ParseBool(raw); raw != "" && err != nil {
		slog.Default().Warn("SHADOW_PARITY_ENABLED unparseable; shadow-parity stays OFF",
			slog.String("value", raw))
	}
}
