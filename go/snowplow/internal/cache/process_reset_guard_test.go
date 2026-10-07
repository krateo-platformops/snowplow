// process_reset_guard_test.go — #471: the structural guard that keeps
// ResetCacheProcessStateForTest complete.
//
// THE DEFECT THIS EXISTS FOR. #471's -count non-idempotence was not a missing
// reset — resetSliceabilityMemoForTest had existed all along, called at 18
// sites inside this package. It was a reset the package that NEEDED it could
// not reach, because it is unexported, and nothing anywhere said that the set
// of hooks a cross-package harness must call was incomplete. The two 30s-TTL
// discovery memos are the same story with the opposite spelling: exported, and
// widgets/apiref had 0 call sites for either. ~64 independent opt-in hooks with
// no single entry point means correctness depends on each harness author
// guessing the right subset. A one-off fix leaves that intact.
//
// THE RULE, checked over this package's NON-TEST sources (every build tag —
// files are parsed directly, so a constraint cannot hide one):
//
//	every declaration whose name matches ^[Rr]eset[A-Za-z0-9_]*ForTest$ is
//	either REACHABLE from ResetCacheProcessStateForTest through intra-package
//	calls, or carries an entry in resetExemptions below with a reason code and
//	a note.
//
// So a new process-global's reset cannot silently fail to be composed: the
// author must either wire it in or write down why not. The list is also checked
// in the other direction — a stale exemption (a name no longer declared) and a
// redundant one (a name that IS composed) both fail, so it cannot rot into a
// list nobody reads.
//
// Methods are in scope too, keyed "(recv).Name". There is exactly one today —
// (*crdDiscovery).ResetCRDDiscoveryFingerprintsForTest — and this guard is how
// it was found: a `grep '^func .*ForTest'` sweep misses it because of the
// receiver. A method reset is unreachable from the entry point by construction
// (a package function cannot call it without a receiver), so it can only ever
// be exempted; see its entry for why that is the right answer here.
//
// WHAT THIS GUARD CANNOT SEE, stated so nobody reads more into a green run:
//
//   - A process-global with NO reset function at all. sensitiveSkippedPut
//     (sensitive_touched_sink.go:87) is today's example and it has no reset.
//     It is benign in the fail-direction — carry-over on a monotonic counter
//     can only make an assertion MORE likely to pass — and four of its five
//     readers take a delta. The fifth, sensitive398_test.go:113, reads the
//     ABSOLUTE value, which is why it is NOT given a reset and composed here:
//     that would make that arm order-dependent on the entry point's callers.
//   - Time-gated globals. The 60s sliceability rate floor and the 30s discovery
//     memo TTLs are production semantics; the entry point resets them between
//     tests but cannot stop one long test from crossing a floor.
//   - Anything outside this package. go test runs one process per package, so a
//     green guard says nothing about a whole-module ./... run.

package cache

import (
	"go/ast"
	"go/parser"
	"go/token"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"
)

// resetEntryPoint is the root of the reachability walk.
const resetEntryPoint = "ResetCacheProcessStateForTest"

// resetNamePattern is the shape of a reset hook.
var resetNamePattern = regexp.MustCompile(`^[Rr]eset[A-Za-z0-9_]*ForTest$`)

// resetExemptReason is the closed set of reasons a reset may stay out of the
// entry point. A free-text reason would become "because I said so"; a code
// forces the author to say which of four arguments they are making, and each
// code's doc comment is the argument.
type resetExemptReason string

const (
	// exemptWrapper — an exported/unexported shim whose whole body is a call
	// to another reset in this inventory. The inner entry's decision governs;
	// composing both would do the work twice.
	exemptWrapper resetExemptReason = "wrapper"

	// exemptCounter — monotonic counters, log-once ledgers and published
	// gauges. Carry-over on a monotonic counter can only make an assertion
	// EASIER to pass, so it is not a source of the #471 red-on-replay class;
	// and every reader either takes a delta or resets at its own arm's top, so
	// composing it would zero a count a caller is mid-measurement of.
	exemptCounter resetExemptReason = "counter"

	// exemptSeam — a test-installed hook, fake or function seam. The
	// INSTALLING arm owns the restore, through t.Cleanup or a returned
	// closure. Composing it would move that ownership to a shared entry point
	// and silently un-install a fixture its installer still expects.
	exemptSeam resetExemptReason = "seam"

	// exemptUnmeasured — a GENUINE carry-over candidate that #471 did not
	// measure. Composing it is a real blast-radius change across the 230
	// existing reset call sites and the subsystem's own harnesses, and this
	// issue's evidence does not cover it. Recorded here rather than left
	// unmentioned so the next -count defect in one of these subsystems has a
	// named first suspect.
	exemptUnmeasured resetExemptReason = "unmeasured"

	// exemptBreaks — composing it would REMOVE state that nothing in the
	// process re-populates, degrading every later test in the binary.
	exemptBreaks resetExemptReason = "breaks"
)

var resetExemptReasons = map[resetExemptReason]bool{
	exemptWrapper: true, exemptCounter: true, exemptSeam: true,
	exemptUnmeasured: true, exemptBreaks: true,
}

type resetExemption struct {
	reason resetExemptReason
	note   string
}

// resetExemptions — the reset hooks deliberately NOT composed into
// ResetCacheProcessStateForTest. Adding a hook here is a decision; the note
// must say what the decision was, not restate the code.
var resetExemptions = map[string]resetExemption{
	// --- wrappers: the inner reset's decision governs -----------------------
	"ResetResolvedCacheForTest":   {exemptWrapper, "cross-package shim over resetResolvedCacheForTest, which IS composed"},
	"ResetRefresherForTest":       {exemptWrapper, "cross-package shim over resetRefresherForTest, which IS composed"},
	"ResetRefreshTerminalForTest": {exemptWrapper, "shim over resetRefreshTerminalForTest, composed via resetRefresherForTest"},
	"ResetRefreshBroadcasterForTest": {exemptWrapper,
		"shim over resetRefreshBroadcasterForTest, itself exempt as unmeasured"},

	// --- counters, log-once ledgers, published gauges ----------------------
	"ResetBindingNoopCountersForTest":             {exemptCounter, "binding noop/update totals"},
	"ResetClusterListCellCountersForTest":         {exemptCounter, "cluster_list warm/cold-fallback totals"},
	"ResetDiscoveryCountersForTest":               {exemptCounter, "per-group fetched/spawned tallies"},
	"ResetFallthroughCountersForTest":             {exemptCounter, "fallthrough + diagnostic tallies and the WARN sampler"},
	"ResetInformerWatchStatsForTest":              {exemptCounter, "watch-error/confirm tallies, plus the stats override its installer restores"},
	"ResetLazyRegisterSkipCountersForTest":        {exemptCounter, "lazy-register skip total"},
	"ResetRBACReestablishmentForTest":             {exemptCounter, "per-GVR re-establishment tallies"},
	"ResetRBACWatchErrorForTest":                  {exemptCounter, "per-GVR RBAC watch-error tallies"},
	"ResetReflectorPathStatsForTest":              {exemptCounter, "reflector path tallies, plus the override its installer restores"},
	"ResetRetiredFlagAuditForTest":                {exemptCounter, "log-ONCE ledger; resetting re-fires a retired-flag WARN already emitted"},
	"ResetRoleNoopCountersForTest":                {exemptCounter, "role noop/update totals"},
	"ResetSeedUnitFootprintViolationsForTest":     {exemptCounter, "assertion-violation ledger"},
	"ResetSeriesTruncatedForTest":                 {exemptCounter, "per-instrument OTLP truncation tallies"},
	"ResetServeRequiresServableViolationsForTest": {exemptCounter, "assertion-violation ledger"},
	"ResetStoreVerificationStatsForTest":          {exemptCounter, "store verify/repair tallies"},
	"ResetSubGenBumpsBySourceForTest":             {exemptCounter, "sub-gen bump tallies by event source"},
	"ResetUAFPutDeclineCountersForTest":           {exemptCounter, "UAF Put-decline and RA-full-list bypass totals"},
	"ResetUnguardedPutTotalForTest":               {exemptCounter, "unguarded-Put total on the deps singleton"},
	"resetPrewarmCompleteObservedForTest":         {exemptCounter, "phase1Done nanos — an observed-timestamp gauge"},
	"resetStripLoggingForTest":                    {exemptCounter, "log-ONCE dedup sets; resetting re-fires strip logs already emitted"},

	// --- test-installed seams: the installer owns the restore --------------
	"ResetAdmissionRuntimeSeamsForTest": {exemptSeam, "restores the two prod memory-limit fns; installed per arm"},
	"ResetCRDSchemaInvalidatorForTest":  {exemptSeam, "sibling-package trampoline, installed by the owning harness"},
	"ResetSADiscoveryInvalidatorForTest": {exemptSeam,
		"sibling-package trampoline, installed by the owning harness"},
	"ResetGoneForgetHooksForTest":     {exemptSeam, "hook chain; dispatchers installs and restores its own"},
	"ResetGVRDiscoveredHooksForTest":  {exemptSeam, "hook chain; the prewarm engine installs and restores its own"},
	"ResetRBACShiftHooksForTest":      {exemptSeam, "hook chain; the reseed engine installs and restores its own"},
	"ResetHandlerExtensionsForTest":   {exemptSeam, "handler-extension registry, installed per arm"},
	"resetCachedDiscoveryForTest":     {exemptSeam, "clears the installed fake discovery client"},
	"ResetProcessSARestConfigForTest": {exemptSeam, "clears the installed fake SA rest.Config"},

	// --- composing would break state nothing re-populates -------------------
	"ResetRouteScopeRegistryForTest": {exemptBreaks,
		"route scopes are registered once at route-registration; a mid-process clear is unrecoverable " +
			"and would leave the fallthrough middleware unable to label any route for the rest of the binary"},

	// --- genuine carry-over candidates, outside #471's measured scope -------
	"ResetBindingsByGVRIndexForTest":      {exemptUnmeasured, "live binding index; harnesses that build it also reset it"},
	"ResetControllerHealthForTest":        {exemptUnmeasured, "controller-health singleton + its own publish/rebuild atomics"},
	"resetCRDDiscoveryForTest":            {exemptUnmeasured, "stops a worker goroutine; only the harnesses that start it reset it"},
	"resetDepsReconcileForTest":           {exemptUnmeasured, "joins a reconcile goroutine; same"},
	"ResetDiscoverySingleflightForTest":   {exemptUnmeasured, "per-group singleflight mutex map"},
	"ResetLearnedIdentitiesForTest":       {exemptUnmeasured, "learned-identity store + its admission gauges"},
	"resetMetadataOnlyAnnotationsForTest": {exemptUnmeasured, "per-GVR metadata-only annotation set"},
	"ResetNavigationDiscoveredGroupsForTest": {exemptUnmeasured,
		"nav-discovered group set; re-populated by discovery, but composing it is unmeasured here"},
	"ResetPhase1DoneForTest":             {exemptUnmeasured, "prewarm-complete latch; readiness arms own it"},
	"ResetPluralsStoreForTest":           {exemptUnmeasured, "discovery-derived plural/kind memos"},
	"resetRefreshBroadcasterForTest":     {exemptUnmeasured, "closes live SSE subscribers; only the arms that open them reset it"},
	"ResetSecretsInformerForTest":        {exemptUnmeasured, "secrets informer wiring flags + its assertion ledger"},
	"ResetSecretsSnapshotForTest":        {exemptUnmeasured, "published secrets snapshot + publish seq"},
	"resetStoreRepairForTest":            {exemptUnmeasured, "store-repair queue and in-flight flag"},
	"resetStoreVerificationStateForTest": {exemptUnmeasured, "per-GVR verification state"},

	// --- the one METHOD reset in the package -------------------------------
	// Found by this guard, not by hand: a `grep '^func .*ForTest'` sweep misses
	// it because of the receiver, which is precisely the class of blind spot
	// the guard is for.
	"(*crdDiscovery).ResetCRDDiscoveryFingerprintsForTest": {exemptUnmeasured,
		"per-INSTANCE state on the crdDiscovery singleton that resetCRDDiscoveryForTest (also exempt) " +
			"replaces wholesale — the fingerprint maps go with the instance, so a process-level call " +
			"would be redundant even if a receiver were reachable from a package function"},
}

// resetMustCompose names the hooks #471 specifically turns on. Checking them by
// name as well as by reachability means a refactor that quietly drops one from
// the chain fails with a message that says which, instead of only failing the
// generic reachability assertion.
var resetMustCompose = []string{
	"resetResolvedCacheForTest",
	"resetSliceabilityMemoForTest",
	"resetSliceabilityReverifyWorkerForTest",
	"resetRefresherForTest",
	"resetDepsForTest",
	"ResetServedGroupsMemoForTest",
	"ResetResourceVerbsMemoForTest",
	"ResetRBACSubGenForTest",
	"ResetPendingSubGenBumpsForTest",
	"ResetRBACGenForTest",
}

// resetDeclName is the inventory key for a func decl: bare name for a package
// function, "(recv).Name" for a method.
func resetDeclName(fd *ast.FuncDecl) string {
	if fd.Recv == nil || len(fd.Recv.List) == 0 {
		return fd.Name.Name
	}
	var recv string
	switch t := fd.Recv.List[0].Type.(type) {
	case *ast.StarExpr:
		if id, ok := t.X.(*ast.Ident); ok {
			recv = "*" + id.Name
		}
	case *ast.Ident:
		recv = t.Name
	}
	return "(" + recv + ")." + fd.Name.Name
}

// calledIdents returns the plain-identifier callees in body. Intra-package
// calls are exactly the ones spelled without a package qualifier, which is
// what the reachability walk needs; a cache.X call from inside package cache
// does not exist.
func calledIdents(body *ast.BlockStmt) []string {
	var out []string
	ast.Inspect(body, func(n ast.Node) bool {
		call, ok := n.(*ast.CallExpr)
		if !ok {
			return true
		}
		if id, ok := call.Fun.(*ast.Ident); ok {
			out = append(out, id.Name)
		}
		return true
	})
	return out
}

func TestIssue471_EveryResetHookIsComposedOrExempted(t *testing.T) {
	files, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatal(err)
	}
	fset := token.NewFileSet()

	// callees[name] = intra-package functions called by name.
	callees := map[string][]string{}
	// resets[name] = "file:line" of the declaration.
	resets := map[string]string{}
	parsed := 0

	for _, fn := range files {
		if strings.HasSuffix(fn, "_test.go") {
			continue
		}
		f, err := parser.ParseFile(fset, fn, nil, 0)
		if err != nil {
			t.Fatalf("parse %s: %v", fn, err)
		}
		parsed++
		for _, d := range f.Decls {
			fd, ok := d.(*ast.FuncDecl)
			if !ok || fd.Body == nil {
				continue
			}
			name := resetDeclName(fd)
			callees[name] = append(callees[name], calledIdents(fd.Body)...)
			if resetNamePattern.MatchString(fd.Name.Name) {
				resets[name] = fset.Position(fd.Pos()).String()
			}
		}
	}

	// Positive controls on the instrument itself: an empty or near-empty
	// inventory is the scanner failing, not the package being clean.
	if parsed < 50 {
		t.Fatalf("scanner: parsed only %d non-test files — expected the whole package", parsed)
	}
	if _, ok := resets[resetEntryPoint]; !ok {
		t.Fatalf("scanner: %s not found among %d reset decls — the entry point moved or the scan is broken",
			resetEntryPoint, len(resets))
	}
	if len(resets) < 40 {
		t.Fatalf("scanner: found only %d reset hooks — the package has ~64; the scan is broken", len(resets))
	}

	// Reachability from the entry point, over intra-package calls.
	reachable := map[string]bool{resetEntryPoint: true}
	queue := []string{resetEntryPoint}
	for len(queue) > 0 {
		cur := queue[0]
		queue = queue[1:]
		for _, callee := range callees[cur] {
			if !reachable[callee] {
				reachable[callee] = true
				queue = append(queue, callee)
			}
		}
	}

	// The named set #471 turns on, checked by name for a pointed failure.
	for _, name := range resetMustCompose {
		if _, declared := resets[name]; !declared {
			t.Errorf("%s is named in resetMustCompose but is not declared in this package — "+
				"rename the entry in the list or restore the hook", name)
			continue
		}
		if !reachable[name] {
			t.Errorf("%s is no longer reachable from %s. #471 turned this hook ON deliberately; "+
				"dropping it from the chain re-opens the -count defect for its subsystem.",
				name, resetEntryPoint)
		}
	}

	// Direction 1 — every declared reset is composed or exempted.
	var uncovered []string
	for name, pos := range resets {
		if reachable[name] {
			continue
		}
		ex, exempted := resetExemptions[name]
		if !exempted {
			uncovered = append(uncovered, name+" ("+pos+")")
			continue
		}
		if !resetExemptReasons[ex.reason] {
			t.Errorf("%s: exemption reason %q is not one of the closed set", name, ex.reason)
		}
		if strings.TrimSpace(ex.note) == "" {
			t.Errorf("%s: exemption carries no note — say what the decision was", name)
		}
	}
	sort.Strings(uncovered)
	for _, name := range uncovered {
		t.Errorf("%s is neither reachable from %s nor in resetExemptions.\n"+
			"\tA process-global's reset must be composed into the ONE entry point, or the "+
			"reason it is not must be written down. This is #471: the hook that caused it "+
			"had existed all along and nothing said the composed set was incomplete.",
			name, resetEntryPoint)
	}

	// Direction 2 — the list cannot rot. A stale entry (no such decl) or a
	// redundant one (the hook IS composed) both fail.
	var stale, redundant []string
	for name := range resetExemptions {
		if _, declared := resets[name]; !declared {
			stale = append(stale, name)
			continue
		}
		if reachable[name] {
			redundant = append(redundant, name)
		}
	}
	sort.Strings(stale)
	sort.Strings(redundant)
	for _, name := range stale {
		t.Errorf("resetExemptions has a STALE entry %q — no such reset is declared in this package. "+
			"Delete the entry (or fix the spelling); a list with dead rows stops being read.", name)
	}
	for _, name := range redundant {
		t.Errorf("resetExemptions has a REDUNDANT entry %q — that hook IS reachable from %s. "+
			"Delete the exemption so the list keeps meaning \"deliberately not composed\".",
			name, resetEntryPoint)
	}

	composed := 0
	for name := range resets {
		if reachable[name] {
			composed++
		}
	}
	t.Logf("#471 reset inventory: %d hooks over %d non-test files — %d composed, %d exempted, %d uncovered",
		len(resets), parsed, composed, len(resetExemptions), len(uncovered))
}
