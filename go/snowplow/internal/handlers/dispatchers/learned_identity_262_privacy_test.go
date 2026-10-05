package dispatchers

// learned_identity_262_privacy_test.go — #262 F6: privacy.
//
// A learned class is a real tenant username (the certificate CN) and its full
// group set; its `<user>-clientconfig` Secret name embeds the username. The
// hard rule: none of them reaches a log line or /debug. The arm drives every
// learned path with logging at DEBUG into a captured sink — Secret ingest
// (informer), the boot seed pass, a keepwarm pass, a re-login through the
// class-seed scope (engine worker), the #258 rotation reseed and a refresher
// re-resolve of a learned cell — with the resolve path logging its requester
// through the ctx logger the way its own sites do (EvaluateRBAC, the refilter,
// the informer serve), and then scans the logs AND every snowplow_* expvar.
//
// NON-VACUITY: the class must actually be seeded (its keys warm), and the
// class's sha256 label must appear in the captured logs (the redaction ran on
// records that carried the tokens).

import (
	"bytes"
	"context"
	"encoding/json"
	"expvar"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	xcontext "github.com/krateo-platformops/plumbing/context"
	"github.com/krateo-platformops/plumbing/kubeutil"
	"github.com/krateo-platformops/snowplow/internal/cache"
	"github.com/krateo-platformops/snowplow/internal/redact"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

type l262LockedBuf struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *l262LockedBuf) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *l262LockedBuf) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

func TestF6_262_Privacy_NoClassTokenInLogsOrDebug(t *testing.T) {
	const cn = "carla.zq7@tenant.example"
	const privGroup = "grp-zq7-private"
	const privGroup2 = "grp-zq7-second"
	secretName := kubeutil.MakeDNS1123Compatible(cn) + "-clientconfig"

	sink := &l262LockedBuf{}
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(sink, &slog.HandlerOptions{Level: slog.LevelDebug})))
	t.Cleanup(func() { slog.SetDefault(prev) })

	cache.RegisterLearnedClassesExpvar()
	registerS258WideningHook(t)
	s258ResetSink()
	// #453 — a BASE (non-learned) group cohort is an identity too: its group
	// string may not reach a log line in clear either.
	const baseGroup = "grp-zq7-base-cohort"
	env := l262Setup(t, l262Opts{
		extraUsers:   []string{cn},
		groupCohorts: []string{baseGroup},
		secrets: []*corev1.Secret{
			l262ClientconfigSecret(t, secretName, cn, []string{l262Group, privGroup}, time.Now().Add(-time.Hour), "1"),
		},
	})
	l262Stubs(t, l262StubOpts{probe: true})
	l262WaitLearned(t, 1)

	// Boot + keepwarm passes.
	l262Boot(t, env, seedModeBoot)
	carla := l262CustomerCtx(cn, []string{l262Group, privGroup})
	if w, n := l262Warm(t, env, carla); w != n {
		t.Fatalf("NON-VACUITY: the learned class must be seeded (%d/%d warm)", w, n)
	}
	time.Sleep(10 * time.Millisecond)
	l262Boot(t, env, seedModeKeepwarm)

	// A re-login with another private group, through the engine worker.
	e := newTestEngine()
	e.scopeHandler = makeBootScopeHandler(env.deps)
	registerEngineLearnedClassHook(e)
	l262StartWorker(t, e)
	upd := l262ClientconfigSecret(t, secretName, cn, []string{l262Group, privGroup, privGroup2}, time.Now(), "2")
	if _, err := env.kube.CoreV1().Secrets(l262AuthnNS).Update(context.Background(), upd, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	relogin := l262CustomerCtx(cn, []string{l262Group, privGroup, privGroup2})
	deadline := time.Now().Add(10 * time.Second)
	for {
		if w, n := l262Warm(t, env, relogin); w == n {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("NON-VACUITY: the re-login class-seed never warmed the new class")
		}
		time.Sleep(5 * time.Millisecond)
	}

	// The #258 rotation reseed naming the user.
	cache.NotifyRBACShiftForTest([]cache.RotatedSubject{{Kind: "User", Name: cn}})
	rotated, ok := s258RotatedFor(cn)
	if !ok {
		t.Fatal("precondition: the rotated set was not captured")
	}
	if err := rePrewarmRBACShift(context.Background(), env.deps, rotated); err != nil {
		t.Fatal(err)
	}

	// A refresher re-resolve of a learned cell.
	keys, handle := l262Keys(t, env, relogin)
	entry, ok := handle.GetNoTouch(keys[0])
	if !ok || entry.Inputs == nil {
		t.Fatal("precondition: the learned widget cell is resident")
	}
	origOnce := resolveOnceFn
	t.Cleanup(func() { resolveOnceFn = origOnce })
	resolveOnceFn = func(ctx context.Context, _ cache.ResolvedKeyInputs) ([]byte, error) {
		if ui, err := xcontext.UserInfo(ctx); err == nil {
			xcontext.Logger(ctx).Warn("l262.refresh_path.requester", "user", ui.Username, "groups", ui.Groups)
		}
		return []byte(`{"refreshed":true}`), nil
	}
	rctx := xcontext.BuildContext(context.Background(), xcontext.WithLogger(slog.Default()))
	if err := resolveAndPopulateL1(rctx, *entry.Inputs, nil, nil); err != nil {
		t.Fatalf("resolveAndPopulateL1: %v", err)
	}

	logs := sink.String()
	var dbg strings.Builder
	expvar.Do(func(kv expvar.KeyValue) {
		if strings.HasPrefix(kv.Key, "snowplow_") {
			dbg.WriteString(kv.Key + "=" + kv.Value.String() + "\n")
		}
	})
	debugVars := dbg.String()

	// THE SCAN: every leaking record is reported (by its msg), then the arm fails.
	leaks := map[string]int{}
	for _, line := range strings.Split(logs, "\n") {
		for _, tok := range []string{cn, secretName, privGroup, privGroup2} {
			if strings.Contains(line, tok) {
				var rec struct {
					Msg string `json:"msg"`
				}
				_ = json.Unmarshal([]byte(line), &rec)
				leaks[rec.Msg+" ⇐ "+tok]++
			}
		}
	}
	for _, tok := range []string{cn, secretName, privGroup, privGroup2} {
		if strings.Contains(debugVars, tok) {
			leaks["/debug/vars ⇐ "+tok]++
		}
	}
	// #453 — base identities. The arm's own l262.* probe records stand in for
	// resolve-path sites and log their requester raw on purpose (the #262
	// redactor's input). The production sites they model are redacted at
	// source, and internal/redact's structural guard covers those, so the
	// probes are excluded here. The group is matched as a whole string value or
	// a pre-#453 "group:<g>" label, not as a substring: the harness binding
	// UID "uid-l262-cohort-<g>" embeds the group name and is not an identity.
	for _, line := range strings.Split(logs, "\n") {
		var rec struct {
			Msg string `json:"msg"`
		}
		_ = json.Unmarshal([]byte(line), &rec)
		if strings.HasPrefix(rec.Msg, "l262.") {
			continue
		}
		for _, tok := range []string{`"` + baseGroup + `"`, "group:" + baseGroup} {
			if strings.Contains(line, tok) {
				leaks["#453 "+rec.Msg+" ⇐ "+tok]++
			}
		}
	}
	for _, tok := range []string{`"` + baseGroup + `"`, "group:" + baseGroup} {
		if strings.Contains(debugVars, tok) {
			leaks["#453 /debug/vars ⇐ "+tok]++
		}
	}
	if len(leaks) > 0 {
		for k, n := range leaks {
			t.Errorf("F6 RED: learned-class token in %s (%d record(s))", k, n)
		}
		t.FailNow()
	}

	label := cache.LearnedClassLabel(cn, []string{l262Group, privGroup})
	if !strings.Contains(logs, label) {
		t.Fatalf("NON-VACUITY: the class label %s never appeared in the captured logs — the learned seed's "+
			"records did not reach the sink, so a clean scan proves nothing", label)
	}
	if !strings.Contains(logs, redact.Group(baseGroup)) {
		t.Fatalf("NON-VACUITY (#453): the base group cohort's label %s never appeared — its seed records did not "+
			"reach the sink, so a clean base scan proves nothing", redact.Group(baseGroup))
	}
	if !strings.Contains(logs, "l262.refresh_path.requester") || !strings.Contains(logs, "l262.resolve_path.requester") {
		t.Fatal("NON-VACUITY: the resolve-path and refresh-path probe records were not captured")
	}
	if !strings.Contains(debugVars, "snowplow_learned_classes_registered") {
		t.Fatal("NON-VACUITY: the learned-class expvars are not published")
	}
}
