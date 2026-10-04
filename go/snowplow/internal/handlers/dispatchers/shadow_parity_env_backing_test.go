// shadow_parity_env_backing_test.go — #367 acceptance arms for the boot-time env
// backing of the dark shadow-parity toggle (rbac.InitShadowParityFromEnv /
// rbac.ShadowParityEnvVar). These live in package dispatchers (NOT package rbac,
// whose TestMain os.Exit(0)s without RBAC_TEST_ALLOW_DESTRUCTIVE — rbac unit arms
// are CI-skipped; see sa_producer_census_268_267_test.go) and reuse the #275 2E
// boot-seed harness (drive2ERestaction/seed2ECohortCtx/reset2E) to exercise the
// REAL boot seed walk (seedModeBoot). The toggle is enabled ONLY via the env here,
// so these prove the env reaches the boot-walk populate arm — the whole point of
// #367 (the boot walk runs during readyz, before any POST can flip the toggle).
//
// RED-first: each arm fails if the env backing is absent/broken — Arm 1 installs
// nothing (toggle stays off), Arm 2 installs when it must not (default-on bug),
// Arm 3's runtime override regresses, Arm 4 couples the key to the toggle.

package dispatchers

import (
	"bytes"
	"encoding/json"
	"expvar"
	"log/slog"
	"strings"
	"testing"

	"github.com/krateo-platformops/snowplow/internal/cache"
	"github.com/krateo-platformops/snowplow/internal/rbac"
)

// Arm R — the DEFAULT-OFF env contract, exercised through the real boot init
// (rbac.InitShadowParityFromEnv; the pure resolver is unexported). Only an
// explicit truthy env enables; unset/empty/false/garbage stay OFF.
func TestF367_ArmR_InitFromEnvContract(t *testing.T) {
	t.Cleanup(func() { rbac.SetShadowParityEnabled(false) })
	cases := []struct {
		raw  string
		want bool
	}{
		{"true", true}, {"1", true}, {"t", true},
		{"", false}, {"false", false}, {"0", false}, {"garbage", false},
	}
	for _, c := range cases {
		// Start from the OPPOSITE state so Init must actually DRIVE the toggle to
		// the env value (not just leave a prior state).
		rbac.SetShadowParityEnabled(!c.want)
		t.Setenv(rbac.ShadowParityEnvVar, c.raw)
		rbac.InitShadowParityFromEnv()
		if got := rbac.ShadowParityEnabled(); got != c.want {
			t.Errorf("InitShadowParityFromEnv with %s=%q → %v, want %v", rbac.ShadowParityEnvVar, c.raw, got, c.want)
		}
	}

	// Unparseable NON-EMPTY values fail-safe OFF AND emit one WARN naming the value
	// (so a typo'd opt-in is not silent, #367 cond. 5); empty/unset stays SILENT.
	// Capture slog to assert the WARN behaviour.
	var buf bytes.Buffer
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelWarn})))
	t.Cleanup(func() { slog.SetDefault(prev) })

	for _, raw := range []string{"ture", "2", "yes!"} {
		buf.Reset()
		rbac.SetShadowParityEnabled(true) // prove it is driven OFF
		t.Setenv(rbac.ShadowParityEnvVar, raw)
		rbac.InitShadowParityFromEnv()
		if rbac.ShadowParityEnabled() {
			t.Fatalf("ArmR: unparseable %q must fail-safe OFF", raw)
		}
		if rbac.ShadowParitySource() != "env" {
			t.Fatalf("ArmR: present-but-unparseable %q must report source=env (the env determined the off state); got %q", raw, rbac.ShadowParitySource())
		}
		if !strings.Contains(buf.String(), "unparseable") || !strings.Contains(buf.String(), raw) {
			t.Fatalf("ArmR: unparseable %q must emit a WARN naming the value; got %q", raw, buf.String())
		}
	}

	// Source fidelity (#367 arch/pm nit): a PRESENT env determines the state even
	// when it sets OFF — source=env (distinguishes "env set it off" from "nothing
	// set it"). An explicit, parseable false must NOT emit the unparseable WARN.
	buf.Reset()
	rbac.SetShadowParityEnabled(true)
	t.Setenv(rbac.ShadowParityEnvVar, "false")
	rbac.InitShadowParityFromEnv()
	if rbac.ShadowParityEnabled() || rbac.ShadowParitySource() != "env" {
		t.Fatalf("ArmR: env=false must be enabled=false/source=env; got %v/%q", rbac.ShadowParityEnabled(), rbac.ShadowParitySource())
	}
	if strings.Contains(buf.String(), "unparseable") {
		t.Fatalf("ArmR: a parseable env=false must NOT emit the unparseable WARN; got %q", buf.String())
	}

	buf.Reset()
	t.Setenv(rbac.ShadowParityEnvVar, "")
	rbac.InitShadowParityFromEnv()
	if strings.Contains(buf.String(), "unparseable") {
		t.Fatalf("ArmR: empty/unset must stay SILENT (no unparseable WARN); got %q", buf.String())
	}
	if rbac.ShadowParitySource() != "default" {
		t.Fatalf("ArmR: empty/unset must report source=default; got %q", rbac.ShadowParitySource())
	}
}

// Arm 1 — with the env SET, a cold boot populates populate_seed_installs_total>0
// from the REAL boot walk (seedModeBoot), and b1+b2+b3+b4 == installs. Enabled
// ONLY via the env (never SetShadowParityEnabled), so this is the #367 surface:
// the env reaches the boot-walk populate arm. RED if the env backing does not
// enable the toggle (installs stays 0).
func TestF367_Arm1_EnvEnablesBootWalkPopulate(t *testing.T) {
	a1BuildTwoTenantWatcher(t)
	rbac.ResetRequesterProfileMemoForTest()
	t.Cleanup(func() { rbac.SetShadowParityEnabled(false) })

	rbac.SetShadowParityEnabled(false) // not riding a prior on-state
	t.Setenv(rbac.ShadowParityEnvVar, "true")
	rbac.InitShadowParityFromEnv()
	if !rbac.ShadowParityEnabled() {
		t.Fatalf("Arm1 precondition: %s=true did not enable the toggle via InitShadowParityFromEnv — the env backing is not wired", rbac.ShadowParityEnvVar)
	}

	reset2E()
	// One real boot-seed drive into each of the four classify buckets.
	drive2ERestaction(t, seed2ECohortCtx(a1Alice), "f367-allow", true, []string{a1TenantA})           // b1
	drive2ERestaction(t, seed2ECohortCtx("carol"), "f367-deny", true, []string{a1TenantA, a1TenantB}) // b2
	drive2ERestaction(t, seed2ECohortCtx(a1Alice), "f367-nocheck", true, nil)                         // b3
	drive2ERestaction(t, seed2ECohortCtx(a1Alice), "f367-passthrough", false, nil)                    // b4

	installs := shadowPopulateSeedInstallsTotal.Load()
	b1 := shadowPopulateAllowBearingTotal.Load()
	b2 := shadowPopulateRBACDeniedTotal.Load()
	b3 := shadowPopulateRBACGatedNoCheckTotal.Load()
	b4 := shadowPopulatePassthroughTotal.Load()

	if installs == 0 {
		t.Fatalf("Arm1 RED: the env-enabled boot walk installed nothing — populate_seed_installs_total==0 (the env backing did not reach the boot walk)")
	}
	if installs != 4 {
		t.Fatalf("Arm1: four real boot-seed drives must install four times; installs=%d", installs)
	}
	if b1+b2+b3+b4 != installs {
		t.Fatalf("Arm1 SUM-INTEGRITY: b1+b2+b3+b4=%d != installs=%d (a silently-missed resolve)", b1+b2+b3+b4, installs)
	}
	if b1 != 1 || b2 != 1 || b3 != 1 || b4 != 1 {
		t.Fatalf("Arm1: each drive must land in exactly its bucket: b1=%d b2=%d b3=%d b4=%d, want 1/1/1/1", b1, b2, b3, b4)
	}
}

// Arm 2 — with the env UNSET, the boot walk is fully dark (installs==0) —
// byte-identical to the pre-#367 default. Also proves Init drives the toggle DOWN
// to the env value (starts ON). RED if an unset env leaves the toggle on
// (default-on bug) — then the boot walk would install.
func TestF367_Arm2_EnvUnsetBootWalkDark(t *testing.T) {
	a1BuildTwoTenantWatcher(t)
	rbac.ResetRequesterProfileMemoForTest()
	t.Cleanup(func() { rbac.SetShadowParityEnabled(false) })

	rbac.SetShadowParityEnabled(true)     // prove Init drives it DOWN
	t.Setenv(rbac.ShadowParityEnvVar, "") // unset/empty → OFF
	rbac.InitShadowParityFromEnv()
	if rbac.ShadowParityEnabled() {
		t.Fatalf("Arm2: an unset env must leave the toggle OFF after InitShadowParityFromEnv (default-off; a pod without the env matches today)")
	}

	reset2E()
	drive2ERestaction(t, seed2ECohortCtx(a1Alice), "f367-unset", true, []string{a1TenantA})
	if in := shadowPopulateSeedInstallsTotal.Load(); in != 0 {
		t.Fatalf("Arm2: env unset ⇒ the boot walk must be fully dark; populate_seed_installs_total=%d, want 0", in)
	}
}

// Arm 3 — the runtime POST override (rbac.SetShadowParityEnabled, what
// POST /debug/shadow-parity calls) still works in BOTH directions after the boot
// env init and takes precedence by running later.
func TestF367_Arm3_RuntimePostOverridesEnvInit(t *testing.T) {
	t.Cleanup(func() { rbac.SetShadowParityEnabled(false) })
	t.Setenv(rbac.ShadowParityEnvVar, "true")
	rbac.InitShadowParityFromEnv()
	if !rbac.ShadowParityEnabled() {
		t.Fatalf("Arm3 precondition: env init did not enable the toggle")
	}
	rbac.SetShadowParityEnabled(false) // POST enabled=false
	if rbac.ShadowParityEnabled() {
		t.Fatalf("Arm3: runtime POST false did not override the env-on toggle")
	}
	rbac.SetShadowParityEnabled(true) // POST enabled=true
	if !rbac.ShadowParityEnabled() {
		t.Fatalf("Arm3: runtime POST true did not re-enable the toggle")
	}
}

// Arm 4 — DARK invariant: the env backing changes NO cache key. ComputeKey for a
// representative input is byte-identical with the toggle off vs env-on, so
// enabling the dark measurement never shifts the key space. (resolvedKeyVersion is
// golden-locked separately in package cache; this diff touches no key-path code.)
func TestF367_Arm4_DarkComputeKeyUnchangedByEnv(t *testing.T) {
	t.Cleanup(func() { rbac.SetShadowParityEnabled(false) })
	in := cache.RAFullListKeyInputsForTest("templates.krateo.io", "v1", "restactions", "ns-x", "ra-x", "bind-uid-x", nil)

	rbac.SetShadowParityEnabled(false)
	rbac.InitShadowParityFromEnv() // env unset in this sub-scope → off
	off := cache.ComputeKey(in)

	t.Setenv(rbac.ShadowParityEnvVar, "true")
	rbac.InitShadowParityFromEnv()
	if !rbac.ShadowParityEnabled() {
		t.Fatalf("Arm4 precondition: env did not enable the toggle")
	}
	on := cache.ComputeKey(in)

	if off != on {
		t.Fatalf("Arm4 DARK: ComputeKey changed with the shadow-parity env on (%q) vs off (%q) — the env backing must not touch the key space", on, off)
	}
}

// Arm 5 — DETECTABILITY (#367 TL ruling): the effective toggle state + its source
// are published on /debug/vars (snowplow_v7_shadow_parity{enabled,source}) so a
// latency-acceptance window can ASSERT enabled==0 instead of trusting the default.
// env set → 1/"env"; env unset → 0/"default" (the healthy state); runtime POST →
// 1/"runtime-post". Reads the REAL expvar var, not just the accessors.
func TestF367_Arm5_DebugVarsStateAndSource(t *testing.T) {
	t.Cleanup(func() { rbac.SetShadowParityEnabled(false) })
	registerShadowParityToggleState() // idempotent (sync.Once); the UNGATED publisher
	v := expvar.Get("snowplow_v7_shadow_parity_toggle")
	if v == nil {
		t.Fatal("Arm5: snowplow_v7_shadow_parity_toggle is not published on /debug/vars (must be UNGATED — readable regardless of CACHE_ENABLED)")
	}
	read := func() (float64, string) {
		t.Helper()
		var m map[string]any
		if err := json.Unmarshal([]byte(v.String()), &m); err != nil {
			t.Fatalf("Arm5: expvar JSON unmarshal: %v", err)
		}
		en, _ := m["enabled"].(float64) // JSON numbers decode to float64
		src, _ := m["source"].(string)
		return en, src
	}

	t.Setenv(rbac.ShadowParityEnvVar, "true")
	rbac.InitShadowParityFromEnv()
	if en, src := read(); en != 1 || src != "env" {
		t.Fatalf("Arm5 env-on: /debug/vars enabled=%v source=%q, want 1/\"env\"", en, src)
	}

	t.Setenv(rbac.ShadowParityEnvVar, "")
	rbac.InitShadowParityFromEnv()
	if en, src := read(); en != 0 || src != "default" {
		t.Fatalf("Arm5 env-unset: /debug/vars enabled=%v source=%q, want 0/\"default\" (the healthy latency-window state)", en, src)
	}

	rbac.SetShadowParityEnabled(true) // runtime POST
	if en, src := read(); en != 1 || src != "runtime-post" {
		t.Fatalf("Arm5 runtime POST: /debug/vars enabled=%v source=%q, want 1/\"runtime-post\"", en, src)
	}
}
