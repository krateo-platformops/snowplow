"""Stage runners + STAGE_REGISTRY (Block 4).

Phase 6 (browser scaling) is the ONLY scored phase post-restructure.
Phase 7 (user-scaling) and Phase 8 (per-mutation) are retained as separate
subcommands.

Each stage is a callable that reads `state.json`, runs its work, writes a
stage-proof JSON, and updates `state.json`. `--from-stage S5` resumes from
disk; the proof-validation re-runner per R4.1 sanity-checks prior proofs
against the live cluster (<=10s budget).

`stage_runner` wraps `browser.browser_measure_stage` in
`try/except ConvergenceTimeout`: on catch we build a StageProof with
`passed=False, convergence_timeout=true`, call save_state(...) to persist
BEFORE re-raise, then re-raise. `cli.main()` catches and exits code 4.

Post-mutation pause approach (per pre-commit STOP): NOT rewritten in
worktree source. The 7 call sites are inlined INTO this module's stage
functions via `_post_mutation_pause()` (whose body is literally
`if cache_mode == 'ON': time.sleep(5)`). Worktree source remains
untouched at Block 4 — Block 5 deletes the whole file. This matches
the plan §B.4 alternative path and means G11 diff against worktree is
EMPTY.

Per docs/bench-restructure-path-b-plan-2026-06-02.md §A.8 + §E.
"""

from __future__ import annotations

import datetime
import json
import os
import statistics
import subprocess
import threading
import time
from dataclasses import dataclass, field, asdict
from pathlib import Path
from typing import Any, Callable

from bench import cluster, lifecycle, browser, ledger
from bench.browser import ConvergenceTimeout


__all__ = [
    "STAGE_REGISTRY",
    "StageContext",
    "StageProof",
    "IncompatibleStateSchema",
    "load_state",
    "save_state",
    "run_phase6",
    "run_phase7_user_scaling",
    "run_phase8_per_mutation",
    "SCHEMA_VERSION",
    "SCHEMA_MAJOR",
]


# ─── Schema constants ───────────────────────────────────────────────────────


SCHEMA_VERSION = "1.1.0"
SCHEMA_MAJOR = 1
#
# 1.1.0 (Task #250 Block 2 — 2026-06-05): additive S8 / S9 RBAC-mutation
# stages + N-aware EXPECTED_CALLS overlay. Old S8 (delete 1 ns) and S9
# (build canonical ledger row) RENAMED to S10 / S11; new S8 / S9 add
# RoleBinding-add / RoleBinding-remove cells. All new fields on
# stage_proofs[*].proof are READ-BY-KEY (no positional / structural
# changes), so a 1.0.0 state.json resumes cleanly into the 1.1.0
# harness (SCHEMA_MAJOR unchanged). The load_state guard at
# `major > SCHEMA_MAJOR` continues to allow 1.0.0 → 1.1.0 forward.


# S8 Role rules — re-gated through Task #262 (architect doc
# `docs/task-262-s8-cj-tablist-trace-2026-06-09.md`). v5 / 2026-06-09:
# adds `templates.krateo.io: restactions` to the prior v4 rules.
#
# Why this matters (TRACED in §3.3 of the architect doc): cj's S8
# Panel-widget render fans out to `panel.spec.apiRef` →
# `templates.krateo.io: restactions: <…-composition-values>`. Pre-v5,
# the bench Role granted compositions + the four widget kinds but NOT
# restactions. Snowplow's `objects.Get(panel.spec.apiRef)` ran under
# cj's bearer and the apiserver returned a real 403, which then
# round-tripped through `apiref.Resolve` and surfaced on the SPA wire
# as `cj_widget_error_count = 15` (5 cards × 1 panel × 3 navs) —
# matching the proofs/S8.json defect signature exactly.
#
# The v5 rule grants cj the minimum apiRef-resolution permission a
# real customer portal Role would carry. The tablists grant from v4
# stays in place (cj still needs it for click-nav even though task #262
# proved it was NOT the cause of the render-time 15-error symptom).
#
# Production-realistic shape: every rule in this tuple is what a
# narrow-RBAC end user (devs group) would get from the portal's
# composition-reader ClusterRole template, modulo namespace scope.
# Format: (api_groups, resources, verbs) — exactly what
# cluster.k8s_create_namespaced_role consumes.
S8_ROLE_RULES = [
    (["composition.krateo.io"], ["*"], ["get", "list"]),
    # The four widget kinds the per-card Panel fans out to.
    # tablists is the 4th widget per card (click-nav target); see
    # `Panel.spec.resourcesRefs[3]` live verification 2026-06-08.
    (["widgets.templates.krateo.io"],
     ["panels", "markdowns", "buttons", "tablists"],
     ["get", "list"]),
    # Task #262 / 2026-06-09: panel.spec.apiRef → restactions.
    # Without this, widgets.Resolve fails on objects.Get(apiRef) →
    # apiserver 403 → apiref boundary returns wrapped StatusError →
    # SPA renders .ant-result-error per card. 5 cards × 3 navs =
    # 15 errors at S8 (proofs/S8.json signature).
    (["templates.krateo.io"], ["restactions"], ["get", "list"]),
]


class IncompatibleStateSchema(Exception):
    """Raised when state.json carries a future-major schema version."""


class PhantomCompPanelAbort(RuntimeError):
    """Raised at S3 start when the cluster carries composition-panel CRs
    with NO live bench namespace — i.e. residual phantom panels from a
    prior incomplete clean (Task #275 root cause).

    This is a HARD, LOUD abort, NOT a soft warn: a RuntimeError raised
    inside the S3 `_work` propagates through `_run_stage` (which persists a
    failed StageProof + state.json) and out through `run_phase6` →
    `cmd_phase6` returns exit code 1. The run dies with a red banner so the
    operator cleans the residue before any S1/S2 measurement can be
    silently corrupted by phantom Compositions cards.
    """


# ─── Module-level scale/smoke env (mirrors worktree 62-64) ──────────────────


def _env_scale() -> int:
    return int(os.environ.get("SCALE", "5000"))


def _env_smoke() -> bool:
    return os.environ.get("SMOKE", "0") == "1"


# ─── Dataclasses ────────────────────────────────────────────────────────────


@dataclass
class StageContext:
    """Per-stage execution context.

    tokens / user_pages are mutable; stage runners may re-login and
    push the refreshed bearers back into user_pages so subsequent
    stages see the new tokens. cache_mode is "ON" or "OFF" (the
    outer loop alternates).

    `video` controls per-cell .webm recording:
      "none"           — no recording (CI / smoke).
      "representative" — n=1 per (stage, user, cache_mode, page) cell.
      "all"            — every navigation recorded (diagnostic only).
    Threaded from CLI --video → run_phase6 → StageContext → _setup_users
    where it is honoured via browser.make_browser_context(record_video_dir=...).
    Per plan §F.6 + R3.1; default "representative" matches the §I R3.1
    soft-cap target of ≤320 MB.
    """
    tag: str
    scale: int
    tokens: dict[str, str] = field(default_factory=dict)
    user_pages: dict[str, dict] = field(default_factory=dict)
    run_dir: Path = field(default_factory=lambda: Path("."))
    state_path: Path = field(default_factory=lambda: Path("state.json"))
    cache_mode: str = "OFF"
    smoke: bool = False
    all_results: list[dict] = field(default_factory=list)
    admin_token: str | None = None
    video: str = "representative"


@dataclass
class StageProof:
    """Persisted proof of a single stage.

    `what_breaks_if_skipped` is REQUIRED (asserted by save_state). Empty
    rejected per Risk Register §I Block 4 (R4.1).

    `convergence_timeout` is True when the stage raised ConvergenceTimeout;
    the ledger verdict logic flips the row to INVALID in that case.
    """
    stage_id: str
    started_at: str
    ended_at: str
    passed: bool
    proof: dict
    artifacts: list[str]
    what_breaks_if_skipped: str
    convergence_timeout: bool = False
    convergence_ms: int | None = None
    navs: list[dict] = field(default_factory=list)

    def to_dict(self) -> dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, d: dict) -> "StageProof":
        # Tolerate extra keys (forward-compat for additive fields).
        known = {f.name for f in cls.__dataclass_fields__.values()}
        return cls(**{k: v for k, v in d.items() if k in known})


# ─── state.json round-trip ──────────────────────────────────────────────────


def _now_iso() -> str:
    return datetime.datetime.now(datetime.timezone.utc).isoformat()


def load_state(run_dir: Path) -> dict:
    """Read $run_dir/state.json; raise IncompatibleStateSchema on major bump.

    Returns {} when the file is missing (fresh run). On schema mismatch,
    raises IncompatibleStateSchema with diagnostic text.
    """
    p = Path(run_dir) / "state.json"
    if not p.exists():
        return {}
    raw = json.loads(p.read_text())
    ver = (raw.get("schema_version") or "").strip()
    if not ver:
        raise IncompatibleStateSchema(
            f"state.json missing schema_version (run_dir={run_dir})")
    try:
        major = int(ver.split(".")[0])
    except (ValueError, IndexError):
        raise IncompatibleStateSchema(
            f"state.json carries unparseable schema_version={ver!r}")
    if major > SCHEMA_MAJOR:
        raise IncompatibleStateSchema(
            f"state.json schema {ver} exceeds harness SCHEMA_MAJOR="
            f"{SCHEMA_MAJOR}; bump requires a PM gate")
    return raw


def save_state(run_dir: Path, state: dict) -> None:
    """Persist state.json under $run_dir.

    Asserts each stage_proofs entry carries a non-empty
    `what_breaks_if_skipped` (per R4.1). Writes the schema_version + the
    last_updated_at timestamp.
    """
    proofs = state.get("stage_proofs") or {}
    for sid, p in proofs.items():
        wbis = p.get("what_breaks_if_skipped") if isinstance(p, dict) else None
        assert wbis, (
            f"stage_proofs.{sid}.what_breaks_if_skipped is empty/missing — "
            f"per Risk Register R4.1 every stage MUST document its "
            f"downstream effect"
        )

    state.setdefault("schema_version", SCHEMA_VERSION)
    state["last_updated_at"] = _now_iso()
    run_dir = Path(run_dir)
    run_dir.mkdir(parents=True, exist_ok=True)
    (run_dir / "state.json").write_text(json.dumps(state, indent=2,
                                                   default=str))


def _write_stage_proof(run_dir: Path, proof: StageProof) -> Path:
    proofs_dir = Path(run_dir) / "proofs"
    proofs_dir.mkdir(parents=True, exist_ok=True)
    out = proofs_dir / f"{proof.stage_id}.json"
    out.write_text(json.dumps(proof.to_dict(), indent=2, default=str))
    return out


def _stage_label(stage_num) -> str:
    return f"S{stage_num}"


def _post_mutation_pause(cache_mode: str) -> None:
    """Post-cluster-mutation pause; matches the worktree's local helper
    body byte-for-byte (worktree source 6678-6682, deleted).

    Body is literally `if cache_mode == "ON": time.sleep(5)`. The VERIFY
    poll handles real convergence; this exists only to let informer
    events fire before the next browser navigation begins.
    """
    if cache_mode == "ON":
        time.sleep(5)


# ─── Per-stage pod log streamer (Task #251 — 2026-06-09) ────────────────────


class _PerStageLogStreamer:
    """Stream `kubectl logs -f deployment/snowplow --since-time=<stage_start>`
    into `pod_logs/S<N>.txt` for the duration of one stage.

    Why this exists
    ---------------
    Phase 6 measurement stages produced log evidence only via the
    aggregate `pod_logs/full_run.txt` capture at run finalise. When a
    pod restart, stage failure, or long run pushed finalise outside
    the measurement window (kubectl tail bounded by ring-buffer +
    restart-boundary), the per-stage diagnostic evidence was lost —
    the agent a16e4da1a29434f24 TRACE on run-20260609-004834-cache-on
    found S8 window 23:30:39 -> 23:35:48 UTC but the aggregate log
    covered 23:59:11 -> 00:04:09 UTC across a pod restart, so the cj
    `allCompositions` UAF events were unfalsifiable.

    Design contract
    ---------------
    - Stream opens BEFORE the stage's measurement loop fires
    - Stream closes (close + flush + fsync) BEFORE _run_stage persists
      the proof + state.json, so the file is always present when the
      ledger records `__passed__`
    - `--since-time=<stage_started_utc>` ensures the apiserver buffer
      covers the full measurement window even if the streaming
      subprocess starts a few hundred ms late
    - Pod-restart tolerance: when the kubectl subprocess exits, the
      supervisor thread writes a `--- STREAM RECONNECT @ <utc> ---`
      marker and re-spawns with the SAME `--since-time`, appending to
      the file. Caller's `stop()` clears `_running`, the supervisor
      observes it on the next iteration, and exits without respawning
    - Uniform across S0-S11 (no per-stage special-cases — fired from
      _run_stage)

    Opt-out
    -------
    `BENCH_NO_PER_STAGE_LOGS=1` disables the streamer (fall back to
    aggregate full_run.txt only). Matches the existing
    `BENCH_ALLOW_NON_GKE` env-flag pattern; no CLI plumbing needed.
    """

    # Per-iteration wait between subprocess restart attempts. The pod
    # may take several seconds to come up after a restart; a too-tight
    # loop spams kubectl. 1.5s is the same back-off cadence used by
    # _grep_pod_logs_for_traces (R7 retry logic).
    _RESPAWN_BACKOFF_S = 1.5

    # Cap on the stream-supervisor thread's join() during stop(). The
    # subprocess.terminate() path takes ~100ms; allowing 5s leaves
    # ample room for the supervisor loop iteration to break out.
    _STOP_JOIN_TIMEOUT_S = 5.0

    def __init__(self,
                 stage_id: str,
                 stage_started_utc: str,
                 out_path: Path,
                 *,
                 deployment: str = "deployment/snowplow",
                 namespace: str = "krateo-system",
                 container: str = "snowplow"):
        self._stage_id = stage_id
        self._since_time = stage_started_utc
        self._out_path = Path(out_path)
        self._deployment = deployment
        self._namespace = namespace
        self._container = container
        self._proc: subprocess.Popen | None = None
        self._running = threading.Event()
        self._thread: threading.Thread | None = None
        self._fh = None  # file handle held by the supervisor

    @classmethod
    def disabled(cls) -> bool:
        v = os.environ.get("BENCH_NO_PER_STAGE_LOGS", "0").strip().lower()
        return v in ("1", "true", "yes")

    def start(self) -> None:
        if self.disabled():
            return
        self._out_path.parent.mkdir(parents=True, exist_ok=True)
        # Append-mode so reconnect-after-restart preserves earlier output.
        self._fh = open(self._out_path, "ab", buffering=0)
        self._running.set()
        self._thread = threading.Thread(
            target=self._supervise, daemon=True,
            name=f"per-stage-logs-{self._stage_id}")
        self._thread.start()

    def stop(self) -> None:
        if self.disabled():
            return
        self._running.clear()
        # Terminate the in-flight kubectl subprocess so the supervisor
        # loop iteration unblocks immediately.
        proc = self._proc
        if proc is not None:
            try:
                proc.terminate()
            except Exception:
                pass
            try:
                proc.wait(timeout=2.0)
            except subprocess.TimeoutExpired:
                try:
                    proc.kill()
                except Exception:
                    pass
                try:
                    proc.wait(timeout=1.0)
                except Exception:
                    pass
            except Exception:
                pass
        if self._thread is not None:
            self._thread.join(timeout=self._STOP_JOIN_TIMEOUT_S)
        # Flush + fsync the file handle so the per-stage log is on disk
        # BEFORE _run_stage persists the proof + state.json.
        fh = self._fh
        if fh is not None:
            try:
                fh.flush()
                os.fsync(fh.fileno())
            except Exception:
                pass
            try:
                fh.close()
            except Exception:
                pass
            self._fh = None

    def _supervise(self) -> None:
        """Spawn -> stream -> on-exit decide-respawn loop.

        Each iteration:
          1. Spawn `kubectl logs -f --since-time=<self._since_time>`
          2. Pipe its stdout straight into the open file handle
          3. On subprocess exit:
             - If self._running is clear: stop() initiated; return
             - Else: write reconnect marker; sleep backoff; respawn
        """
        while self._running.is_set():
            try:
                proc = subprocess.Popen(
                    ["kubectl", *cluster.kubectl_context_args(),
                     "logs",
                     "-n", self._namespace,
                     self._deployment,
                     "-c", self._container,
                     "-f",
                     f"--since-time={self._since_time}"],
                    stdout=subprocess.PIPE,
                    stderr=subprocess.DEVNULL,
                    bufsize=0,
                )
            except Exception as e:
                # Subprocess spawn failed (kubectl missing, etc.).
                # Log the error inline and back off. Don't crash the bench.
                try:
                    self._fh.write(
                        f"--- STREAM SPAWN FAILED: "
                        f"{type(e).__name__}: {e} ---\n".encode())
                except Exception:
                    pass
                time.sleep(self._RESPAWN_BACKOFF_S)
                continue
            self._proc = proc
            # Pump stdout into the file. Read in chunks so a very chatty
            # pod doesn't starve the supervisor loop on per-line work.
            try:
                while self._running.is_set():
                    chunk = proc.stdout.read(4096)
                    if not chunk:
                        break  # subprocess exited or pipe closed
                    try:
                        self._fh.write(chunk)
                    except Exception:
                        # File handle gone; abort supervision.
                        self._running.clear()
                        break
            except Exception:
                pass
            finally:
                # Drain remaining buffered output before we decide
                # whether to respawn.
                try:
                    proc.stdout.close()
                except Exception:
                    pass
                try:
                    proc.wait(timeout=2.0)
                except Exception:
                    try:
                        proc.kill()
                    except Exception:
                        pass
                    try:
                        proc.wait(timeout=1.0)
                    except Exception:
                        pass
            # Respawn-decision branch.
            if not self._running.is_set():
                return
            # Subprocess exited but we're still in-stage — likely a pod
            # restart. Emit a marker and respawn with the SAME
            # --since-time so the new pod's startup logs land in this
            # same file with full coverage from stage_started_utc.
            try:
                self._fh.write(
                    f"--- STREAM RECONNECT @ {_now_iso()} "
                    f"(pod restart / stream EOF) ---\n".encode())
                self._fh.flush()
            except Exception:
                pass
            time.sleep(self._RESPAWN_BACKOFF_S)


# ─── Stage runner wrapper (catches ConvergenceTimeout per R3.2) ─────────────


def _snowplow_deploy_fingerprint() -> dict | None:
    """Best-effort {revision, restarted_at} of the live snowplow Deployment.

    Task #320 (from the #319 trace): a snowplow re-roll inside a measurement
    window invalidates the sample — and once cost a full mis-attribution
    investigation. `deployment.kubernetes.io/revision` moves on every
    rollout; `kubectl.kubernetes.io/restartedAt` fingerprints
    `kubectl rollout restart` actors specifically. Returns None when the
    deployment is unreadable (never raises — this is telemetry).
    """
    rc, out, _ = cluster.kubectl(
        "get", "deploy", "snowplow", "-n", "krateo-system", "-o", "json")
    if rc != 0 or not out.strip():
        return None
    try:
        d = json.loads(out)
    except ValueError:
        return None
    return {
        "revision": (d.get("metadata", {}).get("annotations", {})
                     .get("deployment.kubernetes.io/revision")),
        "restarted_at": (d.get("spec", {}).get("template", {})
                         .get("metadata", {}).get("annotations", {})
                         .get("kubectl.kubernetes.io/restartedAt")),
    }


def _annotate_deploy_revision(stage_id: str, proof_dict: dict,
                              before: dict | None) -> None:
    """Stamp the stage proof with the deploy-revision window (#320).

    REPORT-ONLY — never gates the stage (cache-toggle stages restart
    snowplow intentionally; the annotation makes both the intentional and
    the unexpected re-rolls visible in the proof instead of silent).
    """
    after = _snowplow_deploy_fingerprint()
    proof_dict["deploy_revision_before"] = (before or {}).get("revision")
    proof_dict["deploy_revision_after"] = (after or {}).get("revision")
    rerolled = (before is not None and after is not None
                and before.get("revision") != after.get("revision"))
    proof_dict["pod_rerolled_mid_stage"] = rerolled
    if rerolled:
        lifecycle.log(
            f"  WARNING {stage_id}: snowplow Deployment rolled MID-STAGE "
            f"(revision {before.get('revision')} -> {after.get('revision')}, "
            f"restartedAt={after.get('restarted_at')}). If this stage did "
            f"not intentionally restart snowplow, treat its measurements "
            f"as suspect (#320).")


def _run_stage(stage_id: str,
               ctx: StageContext,
               work: Callable[[StageContext], dict],
               what_breaks_if_skipped: str,
               *,
               artifacts: list[str] | None = None) -> StageProof:
    """Execute `work(ctx)` and persist the resulting StageProof.

    On ConvergenceTimeout: build a proof with passed=False +
    convergence_timeout=True, persist state.json BEFORE re-raise.
    The CLI catches the re-raised exception and exits 4.

    On any other exception: build a proof with passed=False and the
    error in `proof.error`, persist, then re-raise — the CLI's main
    exits 1.

    Task #251 (2026-06-09): opens a `_PerStageLogStreamer` for the
    stage's measurement window BEFORE `work(ctx)` fires; closes and
    flushes it BEFORE the proof is persisted. The streamer is uniform
    across S0-S11 (no special-cases per stage). Opt-out via the
    `BENCH_NO_PER_STAGE_LOGS=1` env flag.
    """
    started = _now_iso()
    # Open per-stage log stream BEFORE work runs so the kubectl --since-time
    # buffer covers the full measurement window even if the subprocess takes
    # a few hundred ms to attach.
    stage_log_path = Path(ctx.run_dir) / "pod_logs" / f"{stage_id}.txt"
    streamer = _PerStageLogStreamer(
        stage_id=stage_id,
        stage_started_utc=started,
        out_path=stage_log_path,
    )
    streamer.start()
    deploy_fp_before = _snowplow_deploy_fingerprint()
    # Task #217 (report-only): snapshot the per-(handlerKind, gvr) L1
    # hit/miss counters at window-open so the success-path proof can carry
    # the per-stage L1 lookup delta. None-safe — a cache-OFF / unreachable
    # pod publishes no such expvar, so the snapshot (and hence the proof
    # field) is None and NEVER crashes the stage. Reuses the established
    # #334-A expvar read path (no new I/O mechanism).
    l1_lookups_before = browser.read_snowplow_expvar_map(
        "snowplow_dispatch_l1_lookups")
    try:
        try:
            try:
                proof_dict = work(ctx) or {}
            finally:
                # Architect Option A: finalize any subject-user videos that
                # were deferred PAST the stage's CONTENT asserts (which run
                # inside `work`). Runs on BOTH success and failure, and BEFORE
                # the proof's video-artifact attach below, so the deferred
                # `.webm` is in __stage_videos__ when it is attached.
                _drain_pending_video_finalize(ctx)
            # Task #217 (report-only): snapshot L1 counters again at
            # window-close (success path) and diff into the stage proof.
            # compute_l1_lookup_delta is None-safe: if either snapshot is
            # None the field records null. NEVER feeds compute_verdict.
            l1_lookups_after = browser.read_snowplow_expvar_map(
                "snowplow_dispatch_l1_lookups")
            proof_dict["l1_lookup_delta"] = browser.compute_l1_lookup_delta(
                l1_lookups_before, l1_lookups_after)
            _annotate_deploy_revision(stage_id, proof_dict, deploy_fp_before)
            passed = bool(proof_dict.pop("__passed__", True))
            proof = StageProof(
                stage_id=stage_id,
                started_at=started,
                ended_at=_now_iso(),
                passed=passed,
                proof=proof_dict,
                artifacts=list(artifacts or []),
                what_breaks_if_skipped=what_breaks_if_skipped,
            )
        except ConvergenceTimeout as ct:
            proof = StageProof(
                stage_id=stage_id,
                started_at=started,
                ended_at=_now_iso(),
                passed=False,
                proof={
                    "error": "ConvergenceTimeout",
                    "stage": ct.stage, "user": ct.user,
                    "api": ct.api, "ui": ct.ui, "cluster": ct.cluster,
                    "timeout_secs": ct.timeout_secs,
                },
                artifacts=list(artifacts or []),
                what_breaks_if_skipped=what_breaks_if_skipped,
                convergence_timeout=True,
            )
            # Close the streamer BEFORE we persist the proof so the
            # per-stage log file is on disk + the artifact path attached.
            streamer.stop()
            _attach_per_stage_log_artifact(proof, ctx.run_dir, stage_log_path)
            _attach_stage_video_artifacts(proof, ctx)
            _write_stage_proof(ctx.run_dir, proof)
            _record_proof_to_state(ctx, proof)
            raise  # re-raise AFTER state persisted
        except Exception as e:
            proof = StageProof(
                stage_id=stage_id,
                started_at=started,
                ended_at=_now_iso(),
                passed=False,
                proof={"error": type(e).__name__, "msg": str(e)[:500]},
                artifacts=list(artifacts or []),
                what_breaks_if_skipped=what_breaks_if_skipped,
            )
            streamer.stop()
            _attach_per_stage_log_artifact(proof, ctx.run_dir, stage_log_path)
            _attach_stage_video_artifacts(proof, ctx)
            _write_stage_proof(ctx.run_dir, proof)
            _record_proof_to_state(ctx, proof)
            raise

        # Success path: stop streamer BEFORE persisting proof.
        streamer.stop()
        _attach_per_stage_log_artifact(proof, ctx.run_dir, stage_log_path)
        _attach_stage_video_artifacts(proof, ctx)
        _write_stage_proof(ctx.run_dir, proof)
        _record_proof_to_state(ctx, proof)
        return proof
    finally:
        # Defensive: ensure the streamer is never left running even if
        # _write_stage_proof or _record_proof_to_state raise (which they
        # don't today, but better safe than orphan).
        if streamer._running.is_set():
            streamer.stop()


def _attach_per_stage_log_artifact(proof: StageProof,
                                   run_dir: Path,
                                   stage_log_path: Path) -> None:
    """Record the per-stage log file path under proof.artifacts.

    Stored as a path relative to run_dir for portability (matches the
    convention in _attach_video_artifacts_to_last_measurement_proof).
    No-op if the streamer was disabled or the file is empty/missing.
    """
    try:
        if not stage_log_path.exists() or stage_log_path.stat().st_size == 0:
            return
        rel = stage_log_path.absolute().relative_to(Path(run_dir).absolute())
        rel_str = str(rel)
        if rel_str not in proof.artifacts:
            proof.artifacts.append(rel_str)
    except Exception:
        # Artifact attach is best-effort; never let it block proof
        # persistence.
        pass


def _attach_stage_video_artifacts(proof: StageProof,
                                  ctx: StageContext) -> None:
    """Attach THIS stage's per-stage videos to its own proof.

    `_measure_all_users` finalized `S{n}_{user}_{slug}.webm`/.gif for this
    stage and stashed their absolute paths under
    `ctx.user_pages["__stage_videos__"][proof.stage_id]`. We record them on
    the stage proof relative to run_dir (mirrors
    `_attach_per_stage_log_artifact`). Called on BOTH the success and
    failure/ConvergenceTimeout paths so a failed stage still carries the
    partial-but-real video it produced. Best-effort; never raises.
    """
    try:
        stage_videos = (ctx.user_pages.get("__stage_videos__") or {})
        paths = stage_videos.get(proof.stage_id) or []
        if not paths:
            return
        run_dir_abs = Path(ctx.run_dir).absolute()
        for p in paths:
            try:
                rel = str(Path(p).absolute().relative_to(run_dir_abs))
            except ValueError:
                rel = str(p)
            if rel not in proof.artifacts:
                proof.artifacts.append(rel)
    except Exception:
        # Artifact attach is best-effort; never block proof persistence.
        pass


def _record_proof_to_state(ctx: StageContext, proof: StageProof) -> None:
    state = load_state(ctx.run_dir) or {}
    state.setdefault("tag", ctx.tag)
    state.setdefault("scale", ctx.scale)
    state.setdefault("started_at", _now_iso())
    state.setdefault("stages_completed", [])
    state.setdefault("stage_proofs", {})
    state["cache_mode"] = ctx.cache_mode
    state["run_dir"] = str(Path(ctx.run_dir).absolute())
    state["stage_proofs"][proof.stage_id] = proof.to_dict()
    if proof.passed and proof.stage_id not in state["stages_completed"]:
        state["stages_completed"].append(proof.stage_id)
    state["current_stage"] = proof.stage_id
    save_state(ctx.run_dir, state)


# ─── Shared helpers ─────────────────────────────────────────────────────────


def _video_page_slug(page_name: str) -> str:
    """Map a BROWSER_SCALING_PAGES name to a filename slug.

    "Dashboard" -> "dashboard", "Compositions" -> "compositions". Used to
    name the per-page .webm `{stage}_{user}_{slug}.webm` (Task #267 FIX 2),
    replacing the old hardcoded `_cold_dashboard` suffix.
    """
    return (page_name or "page").strip().lower().replace(" ", "-") or "page"


def _setup_users(ctx: StageContext) -> None:
    """Populate ctx.user_pages per user_subject for the cache_mode loop.

    Two shapes, by recording mode
    -----------------------------
    NOT recording (`ctx.video == "none"`): open ONE persistent browser
    context + page per user (one login, reused across all stages) — the
    cheap legacy shape. `page_slots` is None in `_measure_all_users`
    so `browser_measure_stage` uses the single page exactly as before (zero
    behavioural change).

    Recording (`ctx.video in ("representative", "all")`): do NOT open a
    long-lived recording context here. Playwright records exactly ONE .webm
    per BrowserContext over that context's WHOLE lifetime, so a context that
    spanned the entire run would yield ONE giant .webm with every stage's
    navs concatenated — NOT what Diego needs. Instead we store the user's
    credentials + token only; `_measure_all_users` opens FRESH per-page
    recording contexts AT EACH STAGE, films that stage, and closes them so
    every stage yields its own `S{n}_{user}_{slug}.webm` (per-STAGE
    segmentation — Task #267 correction, Diego 2026-06-10). This holds for
    BOTH `representative` and `all` (they differ only in intended capture
    frequency, not in which pages/stages are filmed).
    """
    from playwright.sync_api import sync_playwright  # local import

    creds = browser._ensure_users()  # type: ignore[attr-defined]
    user_subjects = list(ctx.user_pages.get("__subjects__",
                                            ["admin", "cyberjoker"]))
    ctx.user_pages.pop("__subjects__", None)

    pw = sync_playwright().start()
    pw_browser = pw.chromium.launch(headless=True)
    ctx.user_pages["__pw__"] = pw  # type: ignore[assignment]
    ctx.user_pages["__browser__"] = pw_browser  # type: ignore[assignment]

    record_video = (ctx.video or "none").lower() in ("representative", "all")
    if record_video:
        (Path(ctx.run_dir) / "videos").mkdir(parents=True, exist_ok=True)
        # Per-stage video paths produced by _measure_all_users, keyed by
        # stage_id ("S6" → [rel paths]); consumed by _run_stage to attach
        # each stage's videos to THAT stage's proof.
        ctx.user_pages.setdefault("__stage_videos__", {})  # type: ignore[arg-type]
        # Task #310a — per-RUN storage-state login memo: created ONCE here (the
        # recording driver) and threaded explicitly into _open_stage_recording_
        # pages so each user's throwaway login fires at most once per run rather
        # than once per STAGE. Per-RUN scope (lives only as long as this ctx →
        # no cross-run token reuse, constraint (a)); NOT a module global
        # (constraint (b)). {user: {"storage_state": <ss>|None,
        # "captured_at": <monotonic>}}.
        ctx.user_pages.setdefault("__storage_state_memo__", {})  # type: ignore[arg-type]

    for user_subject in user_subjects:
        pwd = creds.get(user_subject)
        if not pwd:
            continue

        if record_video:
            # Credentials-only: per-stage recording contexts are created by
            # _measure_all_users so each stage yields its own .webm files.
            ctx.user_pages[user_subject] = {
                "ctx": None, "page": None,
                "pwd": pwd,
                "token": ctx.tokens.get(user_subject),
                "record_video": True,
            }
        else:
            # No recording: single persistent context + page (cheaper).
            u_ctx = browser.make_browser_context(
                pw_browser, record_video_dir=None)
            u_page = u_ctx.new_page()
            if not browser.browser_login(u_page, user_subject, pwd):
                try:
                    u_ctx.close()
                except Exception:
                    pass
                continue
            ctx.user_pages[user_subject] = {
                "ctx": u_ctx, "page": u_page,
                "token": ctx.tokens.get(user_subject),
                "record_video": False,
            }


def _finalize_one_video(raw_webm: Path | None, videos_dir: Path,
                        final_name: str,
                        produced_artifacts: list[str]) -> None:
    """Rename a finalized .webm to its canonical name + render the .gif.

    Shared by every video teardown path so the rename→gif pipeline is
    identical. `final_name` is the canonical `{stage}_{user}_{slug}.webm`
    basename.

    NO size-based drop (Task #267 correction, Diego 2026-06-10): the .webm
    is ALWAYS retained (appended to artifacts) regardless of size, and
    record_video_to_gif no longer deletes the gif for size either — every
    stage video AND its gif are kept. Best-effort; logs and continues on
    failure.
    """
    if raw_webm is None:
        return
    try:
        final_webm = videos_dir / final_name
        # Playwright may write under the context's own video dir even though
        # we passed `videos_dir`; consult the path it actually returned.
        if raw_webm.exists() and raw_webm != final_webm:
            final_webm.parent.mkdir(parents=True, exist_ok=True)
            raw_webm.replace(final_webm)
        elif raw_webm.exists():
            final_webm = raw_webm
        else:
            return
        if final_webm.exists():
            # .webm retained unconditionally (never dropped for size).
            produced_artifacts.append(str(final_webm))
            gif_path = final_webm.with_suffix(".gif")
            # Gif is a convenience preview; max_seconds bounds gif DURATION
            # (quality), not a drop. The gif is kept regardless of size.
            if browser.record_video_to_gif(final_webm, gif_path):
                produced_artifacts.append(str(gif_path))
    except Exception as e:
        print(f"  WARN: post-process video {final_name}: "
              f"{type(e).__name__}: {e}", flush=True)


def _capture_video_path(page) -> Path | None:
    """Return the Playwright-assigned .webm path for a page, or None.

    MUST be called BEFORE the owning context closes (page.video.path()
    requires the context alive). Never raises.
    """
    try:
        vid = getattr(page, "video", None)
        if vid is not None:
            raw = vid.path()
            if raw:
                return Path(raw)
    except Exception:
        pass
    return None


# Task #310a — per-run storage-state login memo.
#
# AUTHN issues an HS256 JWT with a fixed 24h TTL (empirically: exp-iat == 86400s,
# decoded from a live admin /basic/login token 2026-06-12). A full bench run is
# ~2.5h — well inside the TTL with a ~10× margin — so a single per-RUN capture
# per user cannot expire mid-run under any realistic run budget. The age guard
# below is therefore DEFENSE-IN-DEPTH, not a correctness requirement: its
# threshold is set to a conservative fraction of the 24h TTL so that even a
# pathologically long run (or a future AUTHN TTL reduction) re-captures a fresh
# storage_state rather than letting recording pages silently 401. A memo entry
# older than this is treated as a miss and re-captured (constraint (c)).
_STORAGE_STATE_MAX_AGE_S = 4 * 3600  # 4h: ~1.6× the longest expected run, 1/6 TTL


def _acquire_storage_state(pw_browser, user: str, pwd: str,
                           storage_memo: dict | None):
    """Return an authenticated Playwright storage_state for `user`, or None.

    Task #310a — per-RUN login memo. Without a memo this performs the legacy
    per-call behaviour: one throwaway NON-recording login → `storage_state()`
    capture → close (so the existing per-stage fallback path stays reachable,
    constraint (d)). With a memo it logs in AT MOST ONCE per user per run:

      • Memo HIT (entry present, has a storage_state, and not aged past
        `_STORAGE_STATE_MAX_AGE_S`): return the cached storage_state WITHOUT a
        login — this is what collapses the ~24 redundant per-stage logins to one
        per user.
      • Memo MISS / aged-out entry / prior FAILED capture: perform a fresh
        throwaway login + capture (constraint (c) fresh-capture fallback).

    ONLY a SUCCESSFUL capture is written back into the memo. A FAILED capture
    leaves the memo untouched, so the next stage retries with a fresh login
    rather than serving a cached failure — matching the pre-fix behaviour where
    a transient AUTHN blip on one stage did not suppress recording on the next
    (constraint (c): a failed login must fall back to a fresh capture, never be
    cached).

    `storage_memo` is the per-RUN dict threaded from the recording driver
    (`ctx.user_pages["__storage_state_memo__"]`); it is mutated in place. It is
    NEVER a module global — passing None yields the un-memoised per-call path.
    Never raises (a recording-capture failure must not abort the stage).
    """
    if storage_memo is not None:
        entry = storage_memo.get(user)
        if entry is not None and entry.get("storage_state") is not None:
            age = time.monotonic() - entry.get("captured_at", 0.0)
            if age <= _STORAGE_STATE_MAX_AGE_S:
                return entry["storage_state"]
            # Aged out → fall through to a fresh capture below.

    storage_state = None
    login_ctx = None
    try:
        login_ctx = browser.make_browser_context(pw_browser)
        login_page = login_ctx.new_page()
        if browser.browser_login(login_page, user, pwd):
            storage_state = login_ctx.storage_state()
        else:
            print(f"  WARN: storage-state login failed {user}; "
                  f"recording pages skipped", flush=True)
    except Exception as e:
        print(f"  WARN: storage-state capture {user}: "
              f"{type(e).__name__}: {e}", flush=True)
    finally:
        if login_ctx is not None:
            try:
                login_ctx.close()
            except Exception:
                pass

    # Cache ONLY a successful capture (a failed login is NOT memoised so the
    # next stage retries fresh — constraint (c)).
    if storage_memo is not None and storage_state is not None:
        storage_memo[user] = {"storage_state": storage_state,
                              "captured_at": time.monotonic()}
    return storage_state


def _open_stage_recording_pages(pw_browser, videos_dir: Path,
                                user: str, pwd: str,
                                storage_memo: dict | None = None,
                                ) -> dict[str, dict]:
    """Open recording BrowserContext(s) per scaling page (storage-state reuse).

    Used per STAGE so each stage gets its own short-lived recording contexts
    → one .webm per (stage, user, page). Returns {page_name: {ctx, page,
    slug, make}} LAZY slots (page/ctx None until the `make` factory runs);
    if the throwaway login fails the dict is empty. Never raises (best-effort
    — a recording failure must not abort the stage measurement).

    Task #307 (Diego 2026-06-10; ArchLazyDash / Option A 2026-06-11) — kill the
    login/dashboard head AND the in-frame idle:

      1. STORAGE-STATE REUSE. Log in ONCE on a throwaway NON-recording context,
         capture `context.storage_state()`, and create every per-page recording
         context with `storage_state=<captured>`. Each recording context is then
         already authenticated, so its FIRST `goto` (in browser_measure_navigation)
         lands directly on its target page — no `/login` drive and no dashboard
         landing is filmed. (Previously each recording context drove the login
         form, so its `.webm` began on `/login` → dashboard.)

         Task #310a — the storage_state capture is hoisted into a per-RUN memo
         (`storage_memo`, threaded from the recording driver). With the memo the
         throwaway login fires AT MOST ONCE per user per run instead of once per
         STAGE per user (~24 redundant logins/run before). On a memo miss /
         aged-out entry / failed cached login it falls back to a fresh capture;
         `storage_memo=None` reproduces the legacy per-stage login. See
         `_acquire_storage_state` for the hit/miss/age-guard contract.

      2. LAZY CONTEXTS — EVERY page (incl. Dashboard). No recording context is
         created here. Each slot carries a zero-arg `make` factory (closing over
         the captured storage_state + videos_dir) plus `page=None`, `ctx=None`.
         `browser.browser_measure_stage` invokes that factory at THAT page's own
         loop iteration, as the first statement of the iteration body, so the
         context — and thus its video clock — does not exist until ITS nav
         begins. For the Dashboard (the FIRST BROWSER_SCALING_PAGES entry) this
         means its `.webm` starts at the /dashboard nav rather than filming the
         in-frame `count_compositions()` header probe + cold-nav poll idle; the
         Compositions context still materialises strictly AFTER the Dashboard
         VERIFY/convergence poll completes. The factory writes the materialised
         ctx/page back into the same slot dict, so the deferred finalize
         (`_finalize_stage_videos`) and the subject content-gate
         (`u_state["page"]`) read the live filmed page unchanged.
    """
    pages: dict[str, dict] = {}

    # (1) storage_state capture (NOT recorded). Task #310a: routed through the
    # per-RUN login memo so each user logs in AT MOST ONCE per run; on a memo
    # miss / aged-out entry / cached-login failure this falls back to a fresh
    # throwaway login + capture (constraint (c)). `storage_memo=None` reproduces
    # the legacy per-call login exactly (constraint (d) — fallback path stays
    # reachable).
    storage_state = _acquire_storage_state(pw_browser, user, pwd, storage_memo)

    if storage_state is None:
        # No auth → no recording pages (the stage still measures on the
        # persistent non-recording page); never raise.
        return pages

    for page_name, _page_path in browser.BROWSER_SCALING_PAGES:
        slug = _video_page_slug(page_name)

        # (2) LAZY (ALL pages, incl. Dashboard): defer context creation to that
        # page's loop iteration in browser_measure_stage. The factory closes
        # over the captured storage_state + videos_dir, materialises ctx/page,
        # and writes them back into THIS slot — so the recording context (and
        # its video clock) does not exist until the nav begins.
        slot: dict = {"ctx": None, "page": None, "slug": slug}

        def _make(_slot=slot):
            if _slot.get("page") is not None:
                return _slot["page"]
            c = browser.make_browser_context(
                pw_browser, record_video_dir=videos_dir,
                storage_state=storage_state)
            pg = c.new_page()
            _slot["ctx"] = c
            _slot["page"] = pg
            return pg

        slot["make"] = _make
        pages[page_name] = slot
    return pages


def _finalize_stage_videos(pages: dict[str, dict], videos_dir: Path,
                           stage_num, user: str) -> list[str]:
    """Close per-page recording contexts and finalize one .webm/.gif each.

    Names each file `S{stage_num}_{user}_{slug}.webm` (per-STAGE segmentation)
    — the stage number is the live `stage_num` passed into _measure_all_users,
    NOT `_first_stage_label` (which only knew the first stage of the window).
    Captures every video path while the contexts are still alive, THEN closes
    + finalizes so no path read races a closed context. Returns the produced
    absolute paths (.webm + .gif). Never raises.
    """
    captured: list[tuple[Path | None, str, object]] = []
    for page_name, pp in pages.items():
        raw = _capture_video_path(pp.get("page"))
        slug = pp.get("slug") or _video_page_slug(page_name)
        captured.append(
            (raw, f"S{stage_num}_{user}_{slug}.webm", pp.get("ctx")))
    produced: list[str] = []
    for raw, final_name, p_ctx in captured:
        try:
            if p_ctx is not None:
                p_ctx.close()
        except Exception:
            pass
        _finalize_one_video(raw, videos_dir, final_name, produced)
    return produced


def _drain_pending_video_finalize(ctx: StageContext) -> None:
    """Finalize any subject-user videos deferred past the content asserts.

    Architect Option A: `_measure_all_users` keeps the SUBJECT user's
    Compositions recording context OPEN (with its live page exposed as
    `u_state["page"]`) so the stage's S8/S9 CONTENT asserts inspect the page
    that actually filmed the nav. This drains those deferred finalizes — it
    is called by `_run_stage` AFTER `_work` returns (and in `_work`'s failure
    path) so the context closes + the `.webm` is written and stashed into
    `__stage_videos__[stage_id]`, AND the now-dead page is cleared back to
    None on the user_state so a later stage never inspects a closed page.
    Best-effort; never raises.
    """
    videos_dir = Path(ctx.run_dir) / "videos"
    pending = ctx.user_pages.pop("__pending_video_finalize__", None)
    if not pending:
        return
    try:
        sv = ctx.user_pages.setdefault("__stage_videos__", {})  # type: ignore[union-attr]
        for item in pending:
            produced = _finalize_stage_videos(
                item["pages"], videos_dir,
                item["stage_num"], item["user"])
            if produced:
                sv.setdefault(item["stage_id"], []).extend(produced)
            # The deferred page's context is now closed; clear the live page
            # reference so a subsequent stage's content assert can't read it.
            u_state = ctx.user_pages.get(item["user"])
            if isinstance(u_state, dict) and u_state.get("page") is not None:
                u_state["page"] = None
    except Exception as e:
        print(f"  WARN: drain pending video finalize: "
              f"{type(e).__name__}: {e}", flush=True)


def _teardown_users(ctx: StageContext) -> None:
    """Close persistent browser contexts + the Playwright browser/driver.

    Per-STAGE videos are finalized inside `_measure_all_users` at each stage
    (one `S{n}_{user}_{slug}.webm`/.gif per stage), so by the time this runs
    there are no per-page recording contexts left to drain — each stage
    already closed its own. This only:
      - closes the persistent single context for each NON-recording user
        (the recording path stores `ctx=None`, nothing to close);
      - tears down the shared pw_browser + sync_playwright driver.

    The flat `__video_artifacts__` list is still published (aggregated from
    every stage's `__stage_videos__`) for any consumer that wants the whole
    run's video set; the authoritative per-stage attachment happens on each
    stage proof via `_attach_stage_video_artifacts`.
    """
    # Defensive: drain any deferred subject-video finalize that a stage left
    # pending (normally _run_stage drains per stage, but never close the
    # pw_browser with a recording context still open).
    _drain_pending_video_finalize(ctx)

    for u_name, v in list(ctx.user_pages.items()):
        if u_name.startswith("__"):
            continue
        # Non-recording users hold a persistent context; recording users
        # hold ctx=None (per-stage contexts already closed in measurement).
        u_ctx = v.get("ctx")
        if u_ctx is not None:
            try:
                u_ctx.close()
            except Exception:
                pass

    pw_browser = ctx.user_pages.pop("__browser__", None)
    pw = ctx.user_pages.pop("__pw__", None)
    if pw_browser is not None:
        try:
            pw_browser.close()
        except Exception:
            pass
    if pw is not None:
        try:
            pw.stop()
        except Exception:
            pass

    # Aggregate every stage's produced video paths into the flat list (kept
    # for backward-compat consumers; per-stage proofs already carry theirs).
    stage_videos = ctx.user_pages.get("__stage_videos__") or {}
    flat: list[str] = []
    for paths in stage_videos.values():
        flat.extend(paths)
    ctx.user_pages["__video_artifacts__"] = flat  # type: ignore[assignment]


def _first_stage_label(ctx: StageContext) -> str:
    """Return a stage label suitable for naming the .webm.

    The bench runs one BrowserContext per user across the full window,
    so the .webm captures the entire window. We label it with the FIRST
    stage in the completed-or-running set, matching the operator's
    intuition (`--to-stage S1` → `S1_admin_cold_dashboard.webm`).
    """
    state = load_state(ctx.run_dir) or {}
    completed = state.get("stages_completed") or []
    current = state.get("current_stage")
    if completed:
        # Prefer the lowest-indexed completed stage that is NOT S0 or S11
        # (those are meta — preflight + report — and never produce nav
        # samples). Pre-1.1.0 the report stage was S9; renamed to S11
        # by Task #250 Block 2.
        for sid in completed:
            if sid not in ("S0", "S11"):
                return sid
        # Fallback to the first completed entry (S0 / S11 case).
        return completed[0]
    if current:
        return current
    return "S0"


def _collect_conv_discrimination(results: list[dict]) -> dict:
    """Task #334a (report-only): surface the convergence-cell discrimination
    tuples from a stage's measurement entries into the stage proof, so
    post-run analysis reads them directly (without walking
    all_results[*].pages[*].navigations).

    browser.browser_measure_stage attaches `conv_discrimination` to the
    Dashboard nav dict that also carries `convergence_ms` (the VERIFY-window
    convergence). This collects ALL such tuples per user (admin + the
    RBAC-scoped subject), keyed by user. Returns an empty dict when no tuple
    is present (e.g. a non-Dashboard-only stage, or cache-off pods where the
    capture recorded all-None fields — those tuples are still surfaced so the
    None-ness is itself the cache-off signal).

    Purely additive: this does NOT feed convergence_p99_for_stage /
    compute_verdict (those read nav["convergence_ms"], untouched here).
    """
    by_user: dict[str, list[dict]] = {}
    for entry in results or []:
        if not isinstance(entry, dict):
            continue
        user = entry.get("user") or "<unknown>"
        for _page_name, pg in (entry.get("pages") or {}).items():
            for nav in (pg.get("navigations") or []):
                if not isinstance(nav, dict):
                    continue
                tup = nav.get("conv_discrimination")
                if isinstance(tup, dict):
                    by_user.setdefault(user, []).append(tup)
    return by_user


# Task #334 (the original): S6 convergence-mass first-poll settle.
#
# The S6 convergence-mass metric (convergence_mass_s6_p99 — REPORT-ONLY; only
# conv_s8_p99 gates, see ledger.compute_verdict / convergence_p99_for_stage)
# is a SINGLE first-poll wall-clock anchored at the VERIFY-window open. It is
# sensitive to install-completion timing: when verify_start fires while the
# composition count is still climbing, the convergence chases a moving target
# (the #335 trace recorded S6 71.9s vs 24.8s SAME-CODE — cluster-state, not a
# regression). This adds a SETTLE DISCIPLINE recorded as a SEPARATE report-only
# proof field `conv_settle_s6`; it NEVER mutates nav["convergence_ms"] (the
# field convergence_p99_for_stage reads), so the S6/S7/S8 verdict inputs are
# untouched. It records BOTH disciplines the task allows (discard-first AND
# median-of-N) so the scoring choice stays open for the gating half (#334b,
# Diego-gated).
_S6_SETTLE_SAMPLES = 5
_S6_SETTLE_INTERVAL_S = 3.0


def _s6_settle_convergence(ctx: StageContext,
                           *,
                           samples: int = _S6_SETTLE_SAMPLES,
                           interval_s: float = _S6_SETTLE_INTERVAL_S) -> dict:
    """Re-sample the admin convergence-equality predicate N times AFTER S6's
    primary measurement, to settle the install-timing-sensitive first-poll
    wall-clock. REPORT-ONLY.

    Each sample records the admin piechart api_count vs cluster truth
    (cluster.count_compositions) and whether they are equal — the same
    predicate browser_measure_stage's admin VERIFY poll uses
    (verify_against_cluster path). The settle signal is whether the cluster
    count has STOPPED changing (install complete) and the spread of the
    per-sample elapsed-to-equal.

    Returns a dict (always JSON-serialisable, fields None-safe):
      samples:               [{i, t_ms, api_count, cluster_count, equal}, ...]
      median_ms:             median t_ms across EQUAL samples (the median-of-N
                             discipline), or None if no sample reached equality.
      discard_first_ms:      first EQUAL sample's t_ms AFTER discarding sample 0
                             (the discard-first-sample discipline), or None.
      cluster_count_stable:  True iff cluster_count was identical across all
                             samples (install settled — variance source gone).
      equal_count:           how many samples saw api==cluster.
      sample_count:          how many samples were taken.

    No admin token (cache-off pre-login / harness edge) → a None-filled dict
    with an explicit reason; never raises into the stage.
    """
    token = ctx.admin_token or (ctx.tokens or {}).get("admin")
    base = {
        "samples": [],
        "median_ms": None,
        "discard_first_ms": None,
        "cluster_count_stable": None,
        "equal_count": 0,
        "sample_count": 0,
    }
    if not token:
        base["reason"] = "no_admin_token"
        return base

    t0 = time.time()
    rows: list[dict] = []
    for i in range(max(samples, 0)):
        try:
            api_count = browser.verify_composition_count_api(token)
        except Exception:
            api_count = -1
        try:
            cluster_count = cluster.count_compositions()
        except Exception:
            cluster_count = -1
        equal = (api_count >= 0 and cluster_count >= 0
                 and api_count == cluster_count)
        rows.append({
            "i": i,
            "t_ms": int((time.time() - t0) * 1000),
            "api_count": api_count,
            "cluster_count": cluster_count,
            "equal": equal,
        })
        if i < samples - 1:
            time.sleep(interval_s)

    equal_t = [r["t_ms"] for r in rows if r["equal"]]
    # discard-first-sample: drop sample 0, take the first remaining EQUAL.
    discard_first = next(
        (r["t_ms"] for r in rows[1:] if r["equal"]), None)
    cluster_counts = [r["cluster_count"] for r in rows
                      if r["cluster_count"] >= 0]
    stable = (len(cluster_counts) == len(rows) and len(set(cluster_counts)) == 1
              if rows else None)

    return {
        "samples": rows,
        "median_ms": int(statistics.median(equal_t)) if equal_t else None,
        "discard_first_ms": discard_first,
        "cluster_count_stable": stable,
        "equal_count": len(equal_t),
        "sample_count": len(rows),
    }


def _measure_all_users(ctx: StageContext, stage_num, stage_desc,
                       deleted_ns=None) -> list[dict]:
    """Run browser_measure_stage on every (user, page); return entries.

    Each entry tagged with `user=<u_name>`. ConvergenceTimeout propagates.

    Per-STAGE video (Task #267 correction, Diego 2026-06-10): for recording
    users this opens FRESH per-page recording contexts for THIS stage, films
    the stage on them, and finalizes one `S{stage_num}_{user}_{slug}.webm`/.gif
    per page — so EACH stage produces its own separate watchable videos
    (S0/S1/S6/S8/…), not one whole-run file. Videos are finalized + stashed
    (under `ctx.user_pages["__stage_videos__"][f"S{stage_num}"]`) even if the
    measurement raises, so a failed stage still carries its partial-but-real
    video; `_run_stage` attaches them to that stage's proof. Non-recording
    users measure on the single persistent page (zero behavioural change).

    Architect Option A (content-gate correctness): the SUBJECT user's (the
    RBAC-scoped non-admin) Compositions recording context is NOT finalized
    here — it is kept OPEN and its live Page written into `u_state["page"]`,
    because the stage's S8/S9 CONTENT asserts run in `_work` AFTER this
    returns and inspect that exact page. `_run_stage` drains the deferred
    finalize via `_drain_pending_video_finalize` once `_work` completes/fails.
    Without this, recording mode left page=None → S8 false-FAILed (#149 gate)
    and S9 false-PASSed (blind to a revocation defect).

    `deleted_ns` (Task #296): the bench-deleted namespace for a bulk-delete
    stage (S10). Threaded into widget-terminal-state validation so a
    controller-churn ghost-panel 404 OUTSIDE the deleted ns demotes to a
    recorded WARN, while a ghost IN the deleted ns stays a HARD fail. None
    for non-delete stages (no behavioural change).
    """
    out = []
    pw_browser = ctx.user_pages.get("__browser__")
    videos_dir = Path(ctx.run_dir) / "videos"
    stage_id = f"S{stage_num}"
    stage_video_paths: list[str] = []

    try:
        for u_name, u_state in list(ctx.user_pages.items()):
            if u_name.startswith("__"):
                continue

            recording = (bool(u_state.get("record_video"))
                         and pw_browser is not None)

            # Per-STAGE recording (Task #267 correction): hand
            # browser_measure_stage one lazy `make` factory per page so each
            # stage yields its own S{n}_{user}_{slug}.webm on a context created
            # at that page's own nav. The single persistent page is used only
            # for the non-recording path (and as a pure fallback).
            stage_pages: dict[str, dict] = {}
            page_slots = None
            measure_page = u_state.get("page")
            if recording:
                # Task #310a — thread the per-RUN login memo (created in
                # _setup_users) so this user's throwaway login is reused across
                # all stages in the run instead of re-driven per stage.
                stage_pages = _open_stage_recording_pages(
                    pw_browser, videos_dir, u_name, u_state.get("pwd"),
                    storage_memo=ctx.user_pages.get("__storage_state_memo__"))
                if stage_pages:
                    # Task #307 / ArchLazyDash: EVERY slot (incl. Dashboard) is
                    # lazy — page=None + a `make` factory. #310b: hand
                    # browser_measure_stage the slot map DIRECTLY (one structure
                    # carrying both the routing sentinel `slot["page"]` and the
                    # shadow factory `slot["make"]`) instead of two derived
                    # parallel dicts. The factory it invokes at each page's own
                    # loop iteration writes the materialised page back into the
                    # SAME slot; `measure_page` stays the persistent
                    # non-recording page (a pure fallback on the recording path).
                    page_slots = stage_pages

            # Architect Option A (content-gate correctness): the S8/S9
            # CONTENT asserts run in the stage's `_work` AFTER this function
            # returns and inspect the SUBJECT user's Compositions Page via
            # `u_state["page"]`. If we close that recording context here (as
            # the other pages do) and leave page=None, those asserts read a
            # dead/None page → S8 false-FAILs (#149 gate broken) and S9
            # false-PASSes (blind to a revocation defect). So for the subject
            # user's Compositions page we DEFER the finalize until after
            # `_work` (drained by _run_stage) and expose the LIVE filmed page
            # as `u_state["page"]` for the assert window. The page that filmed
            # the nav is then exactly the page the content gate inspects.
            #
            # Subject = the RBAC-scoped non-admin user (verify_against_cluster
            # is False for it). Admin's content gate compares cached names vs
            # cluster-truth, never the page DOM, so admin needs no deferral.
            is_subject = (u_name != "admin")
            deferred_pages: dict[str, dict] = {}
            if recording and is_subject and "Compositions" in stage_pages:
                deferred_pages["Compositions"] = stage_pages["Compositions"]

            try:
                r = browser.browser_measure_stage(
                    measure_page, stage_num, stage_desc, ctx.cache_mode,
                    token=u_state["token"], user=u_name,
                    verify_against_cluster=(u_name == "admin"),
                    deleted_ns=deleted_ns,
                    page_slots=page_slots)
                if r:
                    r["user"] = u_name
                    out.append(r)
                    ctx.all_results.append(r)
            finally:
                # Finalize NON-deferred pages now (in ALL cases — even on a
                # ConvergenceTimeout the partial recording is real work).
                immediate = {pn: pp for pn, pp in stage_pages.items()
                             if pn not in deferred_pages}
                if immediate:
                    stage_video_paths.extend(
                        _finalize_stage_videos(
                            immediate, videos_dir, stage_num, u_name))
                if deferred_pages:
                    # Expose the LIVE filmed Compositions page for the
                    # content-gate window, and register the finalize to run
                    # AFTER `_work` (drained by _run_stage). The context stays
                    # OPEN so the asserts inspect the post-pick DOM.
                    u_state["page"] = deferred_pages["Compositions"]["page"]
                    pending = ctx.user_pages.setdefault(
                        "__pending_video_finalize__", [])  # type: ignore[union-attr]
                    pending.append({
                        "pages": deferred_pages,
                        "stage_num": stage_num,
                        "user": u_name,
                        "stage_id": stage_id,
                    })
    finally:
        # Stash NON-deferred videos under __stage_videos__[stage_id] even when
        # a user's measurement raised (e.g. ConvergenceTimeout propagating out
        # of the loop) so _run_stage can attach the partial video to the
        # failing stage's proof before the exception unwinds. Deferred subject
        # videos are stashed when _drain_pending_video_finalize runs them.
        if stage_video_paths:
            sv = ctx.user_pages.setdefault("__stage_videos__", {})  # type: ignore[union-attr]
            sv.setdefault(stage_id, []).extend(stage_video_paths)

    return out


def _snapshot_l1_ready_ts(ctx: StageContext) -> int:
    if ctx.cache_mode != "ON":
        return 0
    try:
        return browser._read_l1_ready_ts()  # type: ignore[attr-defined]
    except Exception:
        return 0


# ── Task #347: phase6 L1-readiness boundary + expvar trajectory snapshots ────


def _wait_for_l1_warmup_boundary(ctx: StageContext) -> bool | None:
    """Route the phase6 run-start L1-readiness wait through the #221
    prefer-expvar / fallback-to-slog-scan boundary (`wait_for_l1_warmup`).

    Before #347 the phase6 path only ever called the side-effect-free
    `_snapshot_l1_ready_ts` (the /metrics/cache l1_hits PROXY) — so the #221
    "using expvar snowplow_prewarm_complete" / "falling back to slog-scan"
    boundary log NEVER fired on a phase6 run. Calling `wait_for_l1_warmup`
    here is the smaller, lower-risk wiring (vs porting its BLOCKING
    prefer-expvar poll loop into `_read_l1_ready_ts`, which is a non-blocking
    snapshot read consumed by 8+ call sites with a different contract — that
    would be the wrong shape).

    Gated on cache_mode == "ON": the prewarm/L1-warmup boundary is only
    meaningful when the cache is on (mirrors `_snapshot_l1_ready_ts`'s own
    `cache_mode != "ON"` guard; `wait_for_l1_warmup` itself also falls back
    cleanly under CACHE_ENABLED=false via the absent-expvar path).

    Returns:
        True/False — boundary detected / timed out (wait_for_l1_warmup's bool).
        None       — skipped (cache off) or the call raised (None-safe:
                     NEVER crashes the run; the run proceeds regardless).
    """
    if ctx.cache_mode != "ON":
        return None
    try:
        return browser.wait_for_l1_warmup()
    except Exception as e:
        print(f"  WARN: L1 warmup boundary wait failed: "
              f"{type(e).__name__}: {e}", flush=True)
        return None


# Map-valued expvars snapshotted via read_snowplow_expvar_map; int-valued via
# read_snowplow_expvar_int. Both seams return None when the key is absent /
# the pod is cache-off / unreachable, so every field is independently
# None-safe (a cache-off run yields an all-null snapshot, never a crash).
_EXPVAR_TRAJECTORY_MAP_KEYS = (
    "snowplow_dispatch_l1_lookups",   # per-(handlerKind,gvr) L1 hit/miss
    "snowplow_prewarm_complete",      # {done, elapsed_ms} prewarm boundary
)
_EXPVAR_TRAJECTORY_INT_KEYS = (
    "snowplow_rbac_publish_seq",
    "snowplow_refresher_completed_total",
)


def _write_expvar_trajectory_snapshot(run_dir: Path, label: str) -> Path | None:
    """Write a report-only expvar trajectory snapshot `expvars-{label}.json`
    into the run dir (Task #347).

    Bench-native replacement for the old tester-wrapper scripts that curled
    /debug/vars at run-start / post-storm / final. Reuses the established
    `read_snowplow_expvar_map` (+ `read_snowplow_expvar_int`) transport seams
    — single GET /debug/vars, gzip-decompress, json.loads.

    REPORT-ONLY: never feeds compute_verdict or the ledger row; purely a
    diagnostic artifact. None-safe end-to-end — an absent key records null,
    and any I/O / write failure is swallowed (returns None) so a snapshot can
    NEVER fail a stage or the run.

    Returns the written Path on success, None if nothing could be written.
    """
    try:
        snapshot: dict[str, Any] = {
            "label": label,
            "captured_at": _now_iso(),
            "maps": {},
            "ints": {},
        }
        for key in _EXPVAR_TRAJECTORY_MAP_KEYS:
            try:
                snapshot["maps"][key] = browser.read_snowplow_expvar_map(key)
            except Exception:
                snapshot["maps"][key] = None
        for key in _EXPVAR_TRAJECTORY_INT_KEYS:
            try:
                snapshot["ints"][key] = browser.read_snowplow_expvar_int(key)
            except Exception:
                snapshot["ints"][key] = None
        out = Path(run_dir) / f"expvars-{label}.json"
        out.write_text(json.dumps(snapshot, indent=2, default=str))
        return out
    except Exception as e:
        print(f"  WARN: expvar trajectory snapshot '{label}' failed: "
              f"{type(e).__name__}: {e}", flush=True)
        return None


# ─── S0: preflight ──────────────────────────────────────────────────────────


def stage_s0_preflight(ctx: StageContext) -> StageProof:
    """Run the same 7 preflight gates as `bench check`.

    Side-effect-free; returns the proof dict.
    """
    def _work(c: StageContext) -> dict:
        # Lightweight inline preflight — cmd_check has rich logging but
        # we don't want to print the section banner twice. Trust the
        # caller to have run `bench check` separately for ANSI output.
        rc, ctx_out, _ = cluster.kubectl("config", "current-context")
        ctx_str = (ctx_out or "").strip()
        return {
            "kubectl_context": ctx_str,
            "tag": c.tag,
            "scale": c.scale,
            "cache_mode": c.cache_mode,
            "smoke": c.smoke,
        }

    return _run_stage(
        "S0", ctx, _work,
        what_breaks_if_skipped=(
            "without preflight, the run may target the wrong cluster, "
            "an unexpected image tag, or a stale overlay — all later "
            "measurements would be silently invalid."
        ),
    )


# ─── S1: zero state ─────────────────────────────────────────────────────────


def stage_s1_zero_state(ctx: StageContext) -> StageProof:
    def _work(c: StageContext) -> dict:
        comps = cluster.count_compositions()
        nss = cluster.count_bench_ns()
        results = _measure_all_users(c, 1, "Zero state")
        return {
            "compositions_before": comps,
            "bench_namespaces_before": nss,
            "measurement_count": len(results),
            "navs": [
                {"user": r.get("user"), "pages": list(r.get("pages") or {})}
                for r in results
            ],
            "__passed__": True,
        }

    return _run_stage(
        "S1", ctx, _work,
        what_breaks_if_skipped=(
            "without the zero-state baseline, S2-S8 deltas cannot be "
            "attributed to specific mutations; the cold/warm ratio "
            "loses its denominator."
        ),
    )


# ─── S2: 1 ns + compdef ─────────────────────────────────────────────────────


def stage_s2_one_ns_compdef(ctx: StageContext) -> StageProof:
    def _work(c: StageContext) -> dict:
        _ = _snapshot_l1_ready_ts(c)
        lifecycle.create_bench_namespaces(1, 1)
        lifecycle.wait_for_bench_namespaces(1)
        cd_ready = cluster.deploy_compositiondefinition("bench-ns-01")
        time.sleep(15)
        _post_mutation_pause(c.cache_mode)
        results = _measure_all_users(c, 2, "1 ns + compdef")
        return {
            "ns_count": cluster.count_bench_ns(),
            "compdef_ready": cd_ready,
            "measurement_count": len(results),
            "__passed__": cd_ready,
        }

    return _run_stage(
        "S2", ctx, _work,
        what_breaks_if_skipped=(
            "without the compdef-present proof, S4-S6 cannot assume the "
            "Composition CRD exists; deploy_compositions would fail to "
            "apply at the wire level."
        ),
    )


# ─── S3: 20 namespaces ──────────────────────────────────────────────────────


def _assert_no_phantom_comp_panels() -> dict:
    """S3-start phantom-panel sentinel (Task #275 prevention gap #276-B.1).

    The #275 root cause was residual `Terminating` composition-panel CRs
    from a prior incomplete clean: `count_bench_ns()` excludes Terminating
    namespaces so the bench believed the cluster was clean, while the
    apiserver still held composition-panel CRs that the admin /compositions
    datagrid rendered as phantom cards (some failing widgetData resolution).

    At S3 start — the first stage that ramps namespaces, run AFTER S1/S2
    have already taken zero-state baselines — verify cluster-wide that
    there are NO composition-panel CRs while NO live (Active) bench
    namespace exists. That combination can ONLY be prior-run residue
    (a legitimate run materializes panels only after S4 deploys
    compositions into live namespaces). If found, HALT the run loudly.

    Queries the SAME direct-apiserver mechanism the trace named
    (`count_compositions_with_panels_ready(target_ns=None)`,
    cluster.py) — NOT through snowplow, so it reflects true apiserver
    state including Terminating-phase residue.

    Returns a small diagnostic dict (folded into the S3 proof on the
    happy path). Raises PhantomCompPanelAbort when residue is found.

    None-safety: `count_compositions_with_panels_ready` returns None on a
    transport / client failure. None means "could not determine" — we do
    NOT abort on None (no false-positive on a flaky k8s client); we record
    it and proceed (S3's own measurements still run).
    """
    live_ns = cluster.count_bench_ns()
    comp_panels = cluster.count_compositions_with_panels_ready(target_ns=None)
    diag = {
        "live_bench_ns": live_ns,
        "comp_panels_cluster_wide": comp_panels,
    }
    if comp_panels is None:
        lifecycle.log(
            "  S3 phantom-panel sentinel: comp-panel count unavailable "
            "(transport failure) — proceeding without the check")
        return diag
    if comp_panels > 0 and live_ns == 0:
        # Surface the masking diagnostic the trace named so the operator
        # sees WHY: Terminating bench-ns the usable-ns counter hides.
        terminating = cluster.count_bench_ns_terminating()
        diag["bench_ns_terminating"] = terminating
        msg = (
            f"S3 ABORT — {comp_panels} composition-panel CR(s) present with "
            f"0 live bench namespaces ({terminating} Terminating). These are "
            f"phantom panels from a prior incomplete clean (Task #275): the "
            f"admin /compositions datagrid would render them as cards and "
            f"silently corrupt the S1/S2 zero-state baselines. Run "
            f"`python -m bench check` / clean the cluster before re-running."
        )
        lifecycle.log(msg)
        raise PhantomCompPanelAbort(msg)
    return diag


def stage_s3_20_ns(ctx: StageContext) -> StageProof:
    def _work(c: StageContext) -> dict:
        # Task #275 / #276-B.1: HARD abort if the cluster carries phantom
        # composition-panel CRs with no live bench namespace (prior-run
        # Terminating residue). Runs FIRST, before any new namespace is
        # created, so the check sees the true prior-run baseline.
        sentinel = _assert_no_phantom_comp_panels()
        _ = _snapshot_l1_ready_ts(c)
        lifecycle.create_bench_namespaces(2, 20)
        lifecycle.wait_for_bench_namespaces(20)
        time.sleep(10)
        _post_mutation_pause(c.cache_mode)
        results = _measure_all_users(c, 3, "20 bench ns")
        return {
            "ns_count": cluster.count_bench_ns(),
            "measurement_count": len(results),
            "phantom_panel_sentinel": sentinel,
            "__passed__": True,
        }

    return _run_stage(
        "S3", ctx, _work,
        what_breaks_if_skipped=(
            "without 20-ns ramp, S4 composition deploy may reuse stale "
            "ns state from a prior run and skew the per-ns RBAC view."
        ),
    )


# ─── S4: 20 compositions ────────────────────────────────────────────────────


def stage_s4_20_compositions(ctx: StageContext) -> StageProof:
    def _work(c: StageContext) -> dict:
        _ = _snapshot_l1_ready_ts(c)
        lifecycle.wait_for_crd()
        # 20 compositions across ns 1-20 (1 each). deploy_compositions_parallel
        # handles arbitrary scale and replaces the worktree's sequential
        # deploy_compositions(1, 20, 1) call at source 6708.
        lifecycle.deploy_compositions_parallel(1, 20, 1)
        browser.wait_for_compositions(20)
        _post_mutation_pause(c.cache_mode)
        results = _measure_all_users(c, 4, "20 compositions")
        return {
            "compositions_deployed": cluster.count_compositions(),
            "measurement_count": len(results),
            "__passed__": True,
        }

    return _run_stage(
        "S4", ctx, _work,
        what_breaks_if_skipped=(
            "without 20-composition baseline, the small-N cell anchors "
            "(cold_ms at 20) cannot be compared against SCALE-N S6 "
            "values; per-stage convergence_mass loses its low-end point."
        ),
    )


# ─── S5: SCALE ns ──────────────────────────────────────────────────────────


def stage_s5_scale_ns(ctx: StageContext) -> StageProof:
    def _work(c: StageContext) -> dict:
        if c.scale >= 10000:
            s5_ns_end = 50
        else:
            s5_ns_end = c.scale // 10 if c.scale >= 10 else 1
        _ = _snapshot_l1_ready_ts(c)
        lifecycle.create_bench_namespaces(21, s5_ns_end)
        lifecycle.wait_for_bench_namespaces(s5_ns_end, timeout=600)
        _post_mutation_pause(c.cache_mode)
        results = _measure_all_users(c, 5, f"{s5_ns_end} bench ns")
        return {
            "ns_target": s5_ns_end,
            "ns_actual": cluster.count_bench_ns(),
            "measurement_count": len(results),
            "__passed__": True,
        }

    return _run_stage(
        "S5", ctx, _work,
        what_breaks_if_skipped=(
            "without the scaled namespace count, S6 composition deploy "
            "cannot spread across enough namespaces to exercise the "
            "informer's pagination."
        ),
    )


# ─── S6: SCALE compositions ────────────────────────────────────────────────


def stage_s6_scale_compositions(ctx: StageContext) -> StageProof:
    def _work(c: StageContext) -> dict:
        if c.scale >= 10000:
            s5_ns_end = 50
            s6_comps_per_ns = c.scale // s5_ns_end
        else:
            s5_ns_end = c.scale // 10 if c.scale >= 10 else 1
            s6_comps_per_ns = 10
        s6_timeout = 1200 if c.scale <= 5000 else 3600
        s6_quiesce = 60 if c.scale <= 5000 else 120

        # Background piechart progression polling — see worktree 6731-6738.
        pie_stop = threading.Event()
        pie_thread = threading.Thread(
            target=browser._poll_piechart_progression,  # type: ignore[attr-defined]
            args=(c.admin_token, pie_stop),
            daemon=True,
        )
        pie_thread.start()

        try:
            lifecycle.deploy_compositions_parallel(1, s5_ns_end, s6_comps_per_ns)
            browser.wait_for_compositions(c.scale, timeout=s6_timeout)
            time.sleep(s6_quiesce)
            _post_mutation_pause(c.cache_mode)
        finally:
            pie_stop.set()
            pie_thread.join(timeout=10)

        # Task #216 (Diego ratified): the blocking 600s
        # wait_for_restaction_steady_state pre-wait is DELETED — it added up
        # to 600s of RESTAction-count-stability polling to the SCORED S6 poll,
        # and its return was discarded. The install-settling signal it was a
        # blocking proxy for is captured REPORT-ONLY by `conv_settle_s6` below
        # (cluster_count_stable). To preserve the RESTAction-reconcile flavour
        # as telemetry, take a single non-blocking RESTAction-count snapshot at
        # measure-start (no loop, no timeout) — pure telemetry.
        restaction_count_at_measure = lifecycle.restaction_count_snapshot()
        results = _measure_all_users(c, 6, f"{c.scale} compositions")
        # Task #334 (original): settle the install-timing-sensitive S6
        # convergence-mass first-poll. REPORT-ONLY — records a separate
        # `conv_settle_s6` proof field; does NOT touch nav["convergence_ms"]
        # (the field convergence_p99_for_stage reads), so the verdict inputs
        # are unchanged. S6 conv is report-only (only conv_s8_p99 gates).
        conv_settle_s6 = _s6_settle_convergence(c)
        return {
            "ns_count": s5_ns_end,
            "comps_per_ns": s6_comps_per_ns,
            "compositions_actual": cluster.count_compositions(),
            "measurement_count": len(results),
            # Task #334a (report-only): per-user convergence discrimination
            # tuples for this stage; does NOT feed compute_verdict.
            "conv_discrimination": _collect_conv_discrimination(results),
            # Task #334 (original, report-only): S6 first-poll settle metric.
            "conv_settle_s6": conv_settle_s6,
            # Task #216 (report-only): one-shot RESTAction count at S6
            # measure-start, replacing the deleted blocking steady-state wait
            # (the RESTAction-reconcile signal demoted to telemetry, not lost).
            # None on snapshot failure.
            "restaction_count_at_measure": restaction_count_at_measure,
            "__passed__": True,
        }

    return _run_stage(
        "S6", ctx, _work,
        what_breaks_if_skipped=(
            "S6 is the SCALE-anchor cell — mix_weighted.warm_p50_ms "
            "without it cannot reflect customer experience at 50K."
        ),
    )


# ─── S7: delete 1 composition ──────────────────────────────────────────────


def stage_s7_delete_one_comp(ctx: StageContext) -> StageProof:
    def _work(c: StageContext) -> dict:
        _ = _snapshot_l1_ready_ts(c)
        lifecycle.delete_one_composition("bench-ns-01", "bench-app-01-01")
        lifecycle.wait_for_composition_gone("bench-ns-01", "bench-app-01-01")
        _post_mutation_pause(c.cache_mode)
        results = _measure_all_users(c, 7, "Deleted 1 comp")
        return {
            "ns": "bench-ns-01",
            "name": "bench-app-01-01",
            "measurement_count": len(results),
            # Task #334a (report-only): per-user convergence discrimination
            # tuples for this stage; does NOT feed compute_verdict.
            "conv_discrimination": _collect_conv_discrimination(results),
            "__passed__": True,
        }

    return _run_stage(
        "S7", ctx, _work,
        what_breaks_if_skipped=(
            "without per-mutation delete cell, "
            "convergence_mass_s7_p99 is null — cache invalidation "
            "regression cannot be detected."
        ),
    )


# ─── Task #250 Block 2 — RBAC mutation stages S8 (RB-add) + S9 (RB-remove) ──
#
# Stage runners are pure helpers — they read RBAC layout from ctx-derived
# parameters. Production-realistic shape (Diego choice 2 ratified
# 2026-06-05): the RB-add targets `bench-ns-01`, a pre-populated namespace
# carrying ~SCALE/50 compositions at S6 entry. This exercises the
# subject-index propagation path (Probe A on /debug/vars +
# Probe B on /call view convergence).
#
# Parametric per feedback_no_special_cases: subject_user, target_ns,
# role/RB names, subject group are all named local variables — no
# cj-hardcoded literal participates in cluster mutations. The single
# point of "which group is subject_user in" is the `_user_group` lookup
# table below (data, not policy).


def _user_group(subject_user: str) -> str:
    """Return the primary RBAC Group for a bench user.

    Parametric data table — NOT a special case. Mirrors the portal-chart
    user provisioning fact: today the portal chart provisions
    `cyberjoker` into the `devs` group. If portal adds a third user this
    table grows in ONE place. PM Q2 ratified 2026-06-05.

    Cross-reference: `bench/storm.py:577-590` references `cyberjoker`
    + `devs` in the CRB-burst harness (defect reproducer, NOT a Phase
    6 stage). When this table is extended, that site MUST be updated
    in lockstep — see the doc comment at storm.py:577-590.

    Args:
        subject_user: bench user identity ("cyberjoker", "admin", ...).

    Returns:
        Group name string ("devs" for cyberjoker), empty string when
        the user has no Group-based RBAC entry (admin is provisioned
        via a ClusterRoleBinding; group lookup is unused for admin).
    """
    return {
        "cyberjoker": "devs",
        # admin is provisioned via a CRB — Group lookup not used.
    }.get(subject_user, "")


def _pick_visible_composition_names(target_ns: str, k: int = 5) -> list[str]:
    r"""Return the newest-K composition names that should render on the
    datagrid's FIRST page (per_page=5) for a viewer with full ns access.

    Any-of-newest-K gate (Task #280, docs/task-280-s8-card-absent-trace-
    2026-06-09.md §6 Option A1). The single-newest panel can sit inside
    snowplow's serve-stale window (dirty-mark-not-evict + customer-priority
    refresher are ratified, eventually-consistent design): a panel that was
    ADDed seconds before cj's nav may not yet be re-resolved into the
    served RAFullList cell, so the exact-newest assert is structurally
    racy while panels materialise (~1/75s) faster than the refresher
    drains under a 50K install storm. Asserting ANY-of-K tolerates the
    freshness window while still catching the #149/#186 classes
    (RBAC-narrowing breakage = ZERO of K present, or per-card widget 403s).

    K invariant: callers MUST pass k >= the datagrid per_page (=5) so the
    newest-K list always covers the full first page; a k below per_page
    could pass on a stale older card while the actual first-page newest
    card is absent. The S8/S9 pick site uses k=8 (Task #282 Option C),
    derived from the measured worst-case serve-stale depth (4 panels at
    p50 drain + ~1-2 panels p99 variance => margin >=3). The signature
    default stays generic (k=5 = per_page floor) for the single-name
    wrapper and any future caller.

    The live `compositions-panels` RESTAction sorts PANELS by
    `creationTimestamp DESC` (newest-first) — NOT composition
    creationTimestamp. With workers=32 parallel deploy + the controller's
    own panel-creation queue, panel order diverges from composition order
    (empirically: newest panel = bench-app-01-40 while newest composition
    = bench-app-01-999 in run-20260609-200710). Query panels with the
    portal-page label and extract the composition-name label. Label key
    needs the dot escaped in custom-columns:
    `.metadata.labels.krateo\.io/composition-name`.

    Args:
        target_ns: namespace to scope the lookup to.
        k:         how many newest names to return (datagrid first page).

    Returns:
        List of composition names, NEWEST-LAST (matching the existing
        ascending kubectl sort-by output), at most `k` entries with the
        `<none>` label rows skipped. Falls back to a single-element list
        of a synthesised newest-name (`bench-app-01-39`) on kubectl
        failure / empty output so callers always have a deterministic
        non-empty input.
    """
    rc, out, _ = cluster.kubectl(
        "get", "panels.widgets.templates.krateo.io", "-n", target_ns,
        "-l", "krateo.io/portal-page=compositions",
        "--no-headers",
        "--sort-by=.metadata.creationTimestamp",
        "-o", r"custom-columns=NAME:.metadata.labels.krateo\.io/composition-name")
    if rc == 0 and out.strip():
        lines = [n.strip() for n in out.splitlines()
                 if n.strip() and n.strip() != "<none>"]
        if lines:
            # kubectl sort-by produces ascending order; the LAST k lines are
            # the newest panels — the first page of the customer datagrid.
            # Preserve newest-LAST ordering (slice tail) for parity with the
            # single-name helper which returns lines[-1].
            return lines[-k:] if k > 0 else lines
    # Fallback when kubectl fails: a name from the architect-observed
    # newest-panel range (bench-app-01-{36..40} in runs 175816 + 200710).
    return ["bench-app-01-39"]


def _pick_visible_composition_name(target_ns: str) -> str:
    """Return THE single newest composition name (datagrid row 1).

    Thin wrapper over `_pick_visible_composition_names` for callers that
    need a single deterministic name (S9's absence check reuses S8's
    recorded name). Returns the newest (last) of the newest-K list.

    Args:
        target_ns: namespace to scope the lookup to.

    Returns:
        Composition name string; falls back to `bench-app-01-39` on
        kubectl failure so callers always have a deterministic input.
    """
    names = _pick_visible_composition_names(target_ns, k=1)
    return names[-1] if names else "bench-app-01-39"


def _user_card_text_present(ctx: StageContext,
                            subject_user: str,
                            card_name: str) -> bool:
    """Return True iff `subject_user`'s live Compositions page contains
    a DOM element whose text matches `card_name`.

    CONTENT-not-status check per feedback_validate_content_not_just_status
    + Task #250 R4 mitigation: even when call_count matches the formula,
    a per-card widget rendering empty placeholders (or 403'ing silently)
    will NOT show the composition name in the DOM. The card-text-present
    check eliminates that failure mode.

    Args:
        ctx:          StageContext carrying ctx.user_pages.
        subject_user: bench user whose Page to inspect.
        card_name:    composition name to search for in the rendered DOM.

    Returns:
        True iff `page.locator(text=card_name).count() >= 1`.
        False on missing card_name, missing user_state, missing page, or
        any locator exception.
    """
    if not card_name:
        return False
    u_state = ctx.user_pages.get(subject_user)
    if not u_state:
        return False
    page = u_state.get("page")
    if page is None:
        return False
    try:
        return page.locator(f"text={card_name}").count() >= 1
    except Exception:
        return False


def _reload_user_compositions_page(ctx: StageContext,
                                   subject_user: str) -> None:
    """Reload `subject_user`'s Compositions page and let it settle.

    Task #280 / Option A reload-after-pick: the S8/S9 picks query the
    LIVE cluster AFTER the navs already rendered the DOM, so the assert
    would otherwise compare a `T_pick`-newest name against a stale
    `T_nav`-rendered DOM. Reloading here makes the inspected DOM reflect
    the post-pick serve state.

    Uses the existing browser-layer reload+settle idiom (see
    browser.py:1678-1680 "reload page for fresh data"): a networkidle
    goto with a 30s budget followed by a short settle wait. No new wait
    mechanism is introduced. All-best-effort: any exception is swallowed
    so a reload hiccup degrades to inspecting the pre-reload DOM rather
    than crashing the stage.
    """
    u_state = ctx.user_pages.get(subject_user)
    if not u_state:
        return
    page = u_state.get("page")
    if page is None:
        return
    try:
        page.goto(f"{browser.FRONTEND}/compositions",
                  wait_until="networkidle", timeout=30000)
        page.wait_for_timeout(2000)
    except Exception:
        pass


def _user_card_any_present(ctx: StageContext,
                           subject_user: str,
                           card_names: list[str]) -> tuple[bool, str | None]:
    """Return (any_present, matched_name) over a list of candidate names.

    Any-of-newest-K gate (Task #280). Iterates the candidate names and
    returns True with the first matched name as soon as one is present in
    the user's rendered DOM. Returns (False, None) when none match.

    Delegates each per-name check to `_user_card_text_present` so the
    missing-user / missing-page / locator-exception fallbacks stay
    single-sourced.
    """
    for name in card_names or []:
        if _user_card_text_present(ctx, subject_user, name):
            return (True, name)
    return (False, None)


def _count_widget_errors(results: list, *, user: str, page: str) -> int:
    """Sum `validation.errored_count` across nav results for (user, page).

    Task #250 R4 mitigation — load-bearing on S8 `__passed__`. The
    `errored_count` value is the `.ant-result-error` DOM count captured
    by `_validate_widget_terminal_state` (browser.py:809). A non-zero
    value at S8 means per-card widget RESTActions are 403'ing while the
    composition card is otherwise rendered — exactly the #186-class
    failure mode that would silently pass a call-count-only gate.

    Args:
        results: list of nav dicts emitted by `_measure_all_users`.
        user:    subject to filter on.
        page:    page_name to filter on (matches `pages` keys, e.g.
                 "Compositions" / "Dashboard").

    Returns:
        Sum of errored_count across matching navs. 0 when no match.
    """
    out = 0
    for r in results or []:
        if r.get("user") != user:
            continue
        # The result shape is per-stage (each entry from
        # browser_measure_stage carries pages_data keyed by page_name).
        pages_data = r.get("pages") or {}
        page_entry = pages_data.get(page)
        if not page_entry:
            continue
        for nav in page_entry.get("navigations") or []:
            v = nav.get("validation") or {}
            try:
                out += int(v.get("errored_count") or 0)
            except (TypeError, ValueError):
                pass
    return out


def _wait_rbac_propagation_to_snowplow(ctx: StageContext,
                                       subject_user: str,
                                       target_ns: str,
                                       expected_visible: int,
                                       timeout_secs: int = 180
                                       ) -> tuple[bool, int, dict]:
    """Wait for BOTH probes to agree before declaring RBAC propagation.

    Probe A (mechanism-independent): snowplow's
    `snowplow_rbac_publish_seq` expvar must INCREMENT vs the
    pre-mutation snapshot. The counter is sourced from cache.RBACGen()
    (snowplow rbac_snapshot.go:251), bumped exactly once per successful
    rebuildRBACSnapshot publish. Cannot be tricked by a stale `/call`
    cell — only a real informer event + snapshot rebuild moves it.

    Probe B (UX-side): `count_user_compositions_in_ns(user, token, ns)`
    must equal `expected_visible` within the same budget. Confirms 'by
    the time the browser nav fires, the view has caught up'.

    BOTH probes must pass for `propagation_ok=True`. Mismatch surfaces
    the failure mode in the diag dict — Probe A pass + Probe B fail
    means snapshot rebuilt but evaluator returned the wrong view
    (#149-class regression); Probe A fail means no informer event
    observed OR rebuild stalled.

    Timeout (re-gate v4 / 2026-06-08): bumped 30s → 180s. Tester
    evidence showed the RB-add cold path needs > 30s for the
    /compositions view to reach the user; the prior 30s budget
    surfaced a Probe B miss (uvc=0 at +30s) while the actual browser
    nav observed convergence at ~260s (api=999, ui=999, converged in
    23.5s). The mechanism IS propagating — the 30s budget was too
    tight. 180s leaves 6x headroom over the empirical 30s mid-band
    and covers cache-OFF S9 (~30s) plus jitter.

    Returns:
        (ok, elapsed_ms, diag) — diag captures pre/post seq + final n.
    """
    token = ctx.tokens.get(subject_user)
    if not token:
        return (False, 0, {"error": "no_token",
                           "subject_user": subject_user})

    seq_before = browser.read_snowplow_expvar_int(
        "snowplow_rbac_publish_seq")
    if seq_before is None:
        # Expvar not wired or unreachable — FAIL CLOSED. Block 2a
        # expvar shim MUST publish the key for the gate to operate.
        return (False, 0, {
            "error": "expvar_unreadable",
            "key": "snowplow_rbac_publish_seq",
        })

    deadline = time.time() + timeout_secs
    started = time.time()
    seq_now = seq_before
    n = -1
    probe_a_pass = False
    probe_b_pass = False
    while time.time() < deadline:
        seq_now = browser.read_snowplow_expvar_int(
            "snowplow_rbac_publish_seq")
        n = browser.count_user_compositions_in_ns(
            subject_user, token, target_ns)
        probe_a_pass = (seq_now is not None and seq_now > seq_before)
        probe_b_pass = (n == expected_visible)
        if probe_a_pass and probe_b_pass:
            return (True, int((time.time() - started) * 1000), {
                "rbac_publish_seq_before": seq_before,
                "rbac_publish_seq_after": seq_now,
                "user_visible_count": n,
                "expected_visible": expected_visible,
                "probe_a_pass": True,
                "probe_b_pass": True,
            })
        time.sleep(1)
    return (False, int((time.time() - started) * 1000), {
        "rbac_publish_seq_before": seq_before,
        "rbac_publish_seq_after": seq_now,
        "user_visible_count": n,
        "expected_visible": expected_visible,
        "probe_a_pass": probe_a_pass,
        "probe_b_pass": probe_b_pass,
    })


def stage_s8_add_rb_to_populated_ns(ctx: StageContext) -> StageProof:
    """RB-add into a pre-populated ns; cj transitions 0 → N visible cards.

    Tests snowplow's subject-index propagation: a RoleBinding ADD in
    `bench-ns-01` must (a) bump rbac_publish_seq, (b) cause cj's /call
    view to include the bench-ns-01 compositions, (c) trigger
    per-card widget RESTAction fan-out on the rendered datagrid.

    Per Task #250 design §3.1. Parametric — no cj-hardcoded path
    participates in mutation.
    """
    def _work(c: StageContext) -> dict:
        # Diego choice 2 (production-realistic): cj is the only non-admin
        # bench user today; subject_user parameter would expand to a
        # stage-arg if a third subject is added.
        subject_user = "cyberjoker"
        subject_group = _user_group(subject_user)
        target_ns = "bench-ns-01"

        # Pre-mutation snapshot for proof bookkeeping.
        comps_in_ns = cluster.count_compositions_in_ns(target_ns)
        _ = _snapshot_l1_ready_ts(c)

        # Pre-check (re-gate v2 defence-in-depth): the bench's
        # kubeconfig identity MUST be able to create RoleBindings in
        # target_ns. A 403 here surfaces as `rbac_precheck_denied`
        # which is far more diagnosable than a generic create failure.
        precheck_ok, precheck_diag = cluster.k8s_can_i_create_rolebinding(
            target_ns)
        if not precheck_ok:
            return {
                "subject_user": subject_user,
                "subject_group": subject_group,
                "target_ns": target_ns,
                "error": precheck_diag,
                "precheck_allowed": False,
                "__passed__": False,
            }

        # 1. Create the Role using the module-level S8_ROLE_RULES
        # constant (see top-of-module definition). v5 / 2026-06-09
        # adds the `templates.krateo.io: restactions` rule per
        # architect trace task-262 (closes Task #272). The full
        # provenance / rationale block lives next to the constant
        # so it stays close to the rule data the bench actually
        # writes to the apiserver.
        #
        # Re-gate history (kept here for log-search across stages):
        #   - v3 / 2026-06-05 (Diego, #186 Option (a)) — composition
        #     + 3 widget kinds.
        #   - v4 / 2026-06-08 — adds tablists per task-215 design.
        #   - v5 / 2026-06-09 — adds restactions per task-262 trace
        #     (the actual S8 cj_widget_error_count=15 cause).
        # GVR group strings empirically verified against the live
        # cluster via `kubectl api-resources --api-group=…`.
        role_name = f"bench-{subject_user}-{target_ns}-comp-reader"
        rb_name = f"bench-{subject_user}-{target_ns}-comp-reader-binding"
        role_ok, role_diag = cluster.k8s_create_namespaced_role(
            target_ns, role_name,
            rules=S8_ROLE_RULES,
        )
        rb_ok, rb_diag = cluster.k8s_create_namespaced_role_binding(
            target_ns, rb_name,
            role_ref=("Role", role_name),
            subjects=[{"kind": "Group", "name": subject_group}],
        )
        if not (role_ok and rb_ok):
            # Surface the FULL diagnostic from the cluster.py helper
            # so SDK / RBAC drift is debuggable from the proof body
            # alone (re-gate v2: previously this said only
            # `error: rbac_create_failed` and hid the AttributeError
            # on `V1Subject` for the entire S8 wall-clock).
            return {
                "subject_user": subject_user,
                "subject_group": subject_group,
                "target_ns": target_ns,
                "role_name": role_name,
                "rb_name": rb_name,
                "role_ok": role_ok,
                "rb_ok": rb_ok,
                "role_diag": role_diag,
                "rb_diag": rb_diag,
                "precheck_allowed": True,
                "error": (role_diag if not role_ok else rb_diag),
                "__passed__": False,
            }

        # 2. Two-probe inner gate (mechanism + UX). cj's visible count
        # should rise from 0 (pre-mutation) to comps_in_ns.
        # Timeout 180s (re-gate v4 / 2026-06-08): tester evidence
        # showed the RB-add cold path needs > 30s for Probe B to
        # observe the cj view; prior 30s budget was too tight while
        # the real browser nav saw convergence ~260s later. 180s
        # leaves 6x headroom over the empirical mid-band.
        prop_ok, prop_ms, prop_diag = _wait_rbac_propagation_to_snowplow(
            c, subject_user, target_ns,
            expected_visible=comps_in_ns,
            timeout_secs=180,
        )

        # 3. Browser measurement (cj's /compositions cell should now
        # fan out to per-card widgets — call_count gate verifies this
        # via the N-aware EXPECTED_CALLS formula).
        _post_mutation_pause(c.cache_mode)
        results = _measure_all_users(c, 8, "RB-add to pre-populated ns")

        # 4. CONTENT-not-status assertion (per R4 mitigation).
        # Any-of-newest-K gate (Task #280 / Option A1): pick the newest
        # K panel composition-names and pass S8 if ANY render. The single-
        # newest assert was structurally racy against snowplow's ratified
        # serve-stale window (dirty-mark-not-evict + customer-priority
        # refresher); any-of-K tolerates the freshness window while still
        # failing if ZERO cards render — which is the #149 (RBAC view stays
        # empty) / #186 (per-card widget 403) breakage the gate exists to
        # catch.
        #
        # K=8 derivation (Task #282 Option C, Diego-ratified — see
        # docs/task-282-serve-stale-depth-trace-2026-06-10.md): measured
        # worst-case serve-stale depth = 4 panels at p50 drain; p99
        # per-resolve 52.8s adds ~1-2 panels of variance. K=8 gives margin
        # >=3 at the measured worst case while the gate floor (zero-of-K ->
        # FAIL) is unchanged. K MUST stay >= datagrid per_page=5 so the
        # K-list always covers the full first page (a K below per_page could
        # pass while the actual first-page newest card is absent).
        expected_card_names = _pick_visible_composition_names(target_ns, k=8)
        # Reload-after-pick: the pick queried the live cluster AFTER the
        # navs rendered the DOM. Reload so the assert inspects post-pick
        # serve state, not the stale T_nav DOM.
        _reload_user_compositions_page(c, subject_user)
        cj_card_present, matched_card_name = _user_card_any_present(
            c, subject_user, expected_card_names)
        cj_widget_errors = _count_widget_errors(
            results, user=subject_user, page="Compositions")

        return {
            "subject_user": subject_user,
            "subject_group": subject_group,
            "target_ns": target_ns,
            "role_name": role_name,
            "rb_name": rb_name,
            "comps_in_ns": comps_in_ns,
            "propagation_ok": prop_ok,
            "propagation_ms": prop_ms,
            "propagation_diag": prop_diag,
            "expected_card_names": expected_card_names,
            "matched_card_name": matched_card_name,
            "content_card_present": cj_card_present,
            "cj_widget_error_count": cj_widget_errors,
            "measurement_count": len(results),
            # Task #334a (report-only): per-user convergence discrimination
            # tuples for this stage; does NOT feed compute_verdict.
            "conv_discrimination": _collect_conv_discrimination(results),
            "__passed__": (
                prop_ok
                and cj_card_present
                and cj_widget_errors == 0
            ),
        }

    return _run_stage(
        "S8", ctx, _work,
        what_breaks_if_skipped=(
            "without RB-add into pre-populated ns, the subject-index "
            "propagation regression of Ship 0.30.235 (#149) cannot be "
            "detected — cj's narrowed view stays empty even when an "
            "admin grants ns access mid-session."
        ),
    )


def stage_s9_remove_rb_from_populated_ns(ctx: StageContext) -> StageProof:
    """RB-remove from same ns; cj transitions N visible → 0 cards.

    Tests snowplow's RBAC-snapshot revocation path. The bidirectional
    pair (S8 add + S9 remove) is the falsifier that the #149 defect
    would have triggered — symmetric add/remove cycles each catch a
    different end of the regression.

    Reads target ns + role/RB names from S8's proof on disk (parametric
    flow — no string literal pass-through).
    """
    def _work(c: StageContext) -> dict:
        s8_proof = (load_state(c.run_dir) or {}).get(
            "stage_proofs", {}).get("S8", {})
        s8_body = (s8_proof.get("proof") or {})
        subject_user = s8_body.get("subject_user")
        target_ns = s8_body.get("target_ns")
        role_name = s8_body.get("role_name")
        rb_name = s8_body.get("rb_name")
        if not (subject_user and target_ns and role_name and rb_name):
            return {
                "error": "s8_proof_missing",
                "subject_user": subject_user,
                "target_ns": target_ns,
                "role_name": role_name,
                "rb_name": rb_name,
                "__passed__": False,
            }

        _ = _snapshot_l1_ready_ts(c)

        # 1. Delete the RB. Role is left in place — symmetric add/remove
        # is "subject access", not "permission inventory"; Role removal
        # is a separate stage we explicitly do not model here. Cleanup
        # of the Role happens after the propagation gate so a partial
        # failure path doesn't leak.
        rb_gone = cluster.k8s_delete_rolebinding(target_ns, rb_name)
        if not rb_gone:
            return {
                "subject_user": subject_user,
                "target_ns": target_ns,
                "role_name": role_name,
                "rb_name": rb_name,
                "error": "rb_delete_failed",
                "__passed__": False,
            }

        # 2. Wait for revocation propagation. expected_visible=0 means
        # the user's narrowed view drops back to empty. Timeout 180s
        # for symmetry with S8 (re-gate v4); revocation is typically
        # faster (cache-ON ~6.5s, cache-OFF ~30s) but the symmetric
        # budget absorbs jitter without over-provisioning the bench
        # wall-clock.
        prop_ok, prop_ms, prop_diag = _wait_rbac_propagation_to_snowplow(
            c, subject_user, target_ns,
            expected_visible=0,
            timeout_secs=180,
        )

        # 3. Cleanup the Role (test hygiene — not a gate assertion).
        cluster.k8s_delete_role(target_ns, role_name)

        # 4. Browser measurement (cj's /compositions should drop back
        # to BASE structural ceiling).
        _post_mutation_pause(c.cache_mode)
        results = _measure_all_users(c, 9, "RB-remove from same ns")

        # 5. CONTENT-absent assertion (revocation symmetric to S8).
        # S9's contract DIFFERS from S8 (Task #280): revocation removes
        # ALL access to target_ns, so the WHOLE grid disappears for cj.
        # Reuse the SAME newest-K names S8 recorded (`expected_card_names`)
        # and assert NONE of them is present — i.e. ALL K must be absent.
        # This is strictly stronger than the prior single-name check, not
        # weaker: with any-of-K add semantics, the symmetric remove gate
        # must verify the absence of every formerly-visible card, so a
        # revocation that only drops SOME rows still fails S9.
        s8_cards = s8_body.get("expected_card_names")
        if not s8_cards:
            # Backward-compat with pre-#280 S8 proofs that recorded a
            # single `expected_card_name`.
            single = (s8_body.get("expected_card_name") or "").strip()
            s8_cards = [single] if single else []
        s8_cards = [n for n in s8_cards if n]
        # Reload-after so the absence assert inspects post-revocation serve
        # state rather than the stale pre-revocation nav DOM.
        _reload_user_compositions_page(c, subject_user)
        still_present, present_card = _user_card_any_present(
            c, subject_user, s8_cards)
        # Absence holds iff NONE of the K names is present. When S8 had no
        # recorded cards (degenerate), absence is trivially True.
        cj_card_absent = (not s8_cards) or (not still_present)

        return {
            "subject_user": subject_user,
            "target_ns": target_ns,
            "role_name": role_name,
            "rb_name": rb_name,
            "propagation_ok": prop_ok,
            "propagation_ms": prop_ms,
            "propagation_diag": prop_diag,
            "expected_card_names": s8_cards,
            "still_present_card_name": present_card,
            "content_card_absent": cj_card_absent,
            "measurement_count": len(results),
            "__passed__": (prop_ok and cj_card_absent),
        }

    return _run_stage(
        "S9", ctx, _work,
        what_breaks_if_skipped=(
            "without RB-remove, snowplow's RBAC-snapshot revocation "
            "path is uncovered — a defect that leaves cj's narrowed "
            "view populated AFTER the RB is gone would ship undetected."
        ),
    )


# ─── S8b / S8c: CR-definition-modification reconcile latency (Task #314) ────
#
# Measure wall-clock from a `kubectl patch` of a BENCH-OWNED Widget /
# RESTAction CR's definition to the moment a customer `/call` serves output
# reflecting the modified definition. Two SEPARATE metrics on two SEPARATE
# invalidation paths (per Task #314 design
# docs/task-314-widget-ra-mod-reconcile-bench-design-2026-06-11.md):
#   - S8b widget: dirty-mark via recordWidgetDeps self-dep on the widgets GVR.
#   - S8c RA:     dirty-mark via the restactions.go:252 self-dep on the
#                 restactions GVR.
# Report-only (the #121 pattern): metrics land in proofs + the canonical
# ledger row; they do NOT feed compute_verdict this ship.
#
# ECHO-FIELD CHOICES (read from the resolver code, NOT assumed — the design's
# flagged INFERRED-pending-read item; file:line cited in the dev report):
#   - Widget: `spec.widgetData.<scalar>` round-trips VERBATIM into
#     status.widgetData when the widget carries NO widgetDataTemplate
#     (internal/resolvers/widgets/resolve.go:182-238 returns
#     src = GetWidgetData(spec.widgetData) and only overwrites keys named by
#     a widgetDataTemplate eval; widgets.go:60-66 reads spec.widgetData). The
#     served body is the whole resolved Widget marshaled
#     (widgets.go dispatcher → encodeResolvedJSON). A Button with no apiRef /
#     widgetDataTemplate is NOT RBAC-sensitive (widgets.go dispatcher:134 →
#     isRBACSensitiveApiRefWidget), so it lands in the per-cohort `widgets`
#     cell — exactly the cell a customer /call hits.
#   - RESTAction: the top-level `spec.filter` jq is evaluated over the
#     resolved api dict and its string output IS the served Status.Raw
#     (internal/resolvers/restactions/restactions.go:57-79). With NO spec.api
#     (api.Resolve returns {} for empty Items — resolve.go:149-151) a constant
#     filter `'{ probe: "v1" }'` round-trips to body `{"probe":"v1"}`.
#
# Probe A SUBSTITUTION (deliberate deviation from the design, stated in the
# dev report): the design cited `dirtyMark_total` from the ORPHAN file
# internal/cache/apistage_bypass_counter.go (slated for exclusion/deletion in
# #315). We instead read the WIRED signal `snowplow_refresher_completed_total`
# (internal/cache/refresher_metrics.go:70, an expvar.Publish) — its delta
# across the mutate→converge window proves the refresher ran. The design
# already lists this expvar + the pod-log lines (deps.go:549 cache_event.consumed
# / resolve_populate.go:291 re-resolved+stored) as corroboration.

# Bench namespace the fixtures live in (per design §3.1 — already populated +
# cj-granted by the time S8 has run; S8b/S8c run after S9).
_S8BC_FIXTURE_NS = "bench-ns-01"

# Widget fixture GVR (the portal-shipped widgets group; a plain `Button` has a
# free `spec.widgetData.label` scalar and no apiRef → per-cohort `widgets`
# cell). resource plural `buttons`.
_WIDGET_FIXTURE_GVR = {
    "group": "widgets.templates.krateo.io",
    "version": "v1beta1",
    "plural": "buttons",
}
_WIDGET_PROBE_NAME = "bench-widget-mod-probe"
_WIDGET_CONTROL_NAME = "bench-widget-mod-control"

# RESTAction fixture GVR (templates.krateo.io/v1, resource `restactions`).
_RA_FIXTURE_GVR = {
    "group": "templates.krateo.io",
    "version": "v1",
    "plural": "restactions",
}
_RA_PROBE_NAME = "bench-ra-mod-probe"
_RA_CONTROL_NAME = "bench-ra-mod-control"

# Convergence poll envelope (design §1.4 — principled, 6× the 10s AC-98.12
# CRUD-to-completed SLA the refresher was tuned against).
_S8BC_POLL_BUDGET_S = 60
_S8BC_POLL_INTERVAL_S = 1.0


def _widget_fixture_yaml(label_probe: str, label_control: str) -> str:
    """Bench Widget fixtures: probe (mutated) + control (NEVER mutated).

    A `Button` with a plain `spec.widgetData.label` scalar and NO apiRef /
    widgetDataTemplate — so it routes to the per-cohort `widgets` L1 cell and
    `status.widgetData.label` echoes the spec scalar verbatim (resolve.go:
    182-238). The probe's label is the convergence marker; the control's
    label is the negative-control invariant.
    """
    g = _WIDGET_FIXTURE_GVR
    return f"""\
---
apiVersion: {g['group']}/{g['version']}
kind: Button
metadata:
  name: {_WIDGET_PROBE_NAME}
  namespace: {_S8BC_FIXTURE_NS}
spec:
  widgetData:
    actions: {{}}
    clickActionId: nop
    type: text
    label: "{label_probe}"
---
apiVersion: {g['group']}/{g['version']}
kind: Button
metadata:
  name: {_WIDGET_CONTROL_NAME}
  namespace: {_S8BC_FIXTURE_NS}
spec:
  widgetData:
    actions: {{}}
    clickActionId: nop
    type: text
    label: "{label_control}"
"""


def _ra_fixture_yaml(probe_value: str, control_value: str) -> str:
    """Bench RESTAction fixtures: probe (mutated) + control (NEVER mutated).

    No `spec.api` stage — the top-level `spec.filter` jq emits a constant
    object `{{ probe: "<value>" }}`. api.Resolve returns {{}} for empty Items
    (resolve.go:149-151); restactions.Resolve evaluates spec.filter over it
    and the string output IS the served Status.Raw (restactions.go:57-79). So
    the served body is `{{"probe":"<value>"}}`. The probe's value is the
    convergence marker; the control's value is the negative-control invariant.
    """
    g = _RA_FIXTURE_GVR
    return f"""\
---
apiVersion: {g['group']}/{g['version']}
kind: RESTAction
metadata:
  name: {_RA_PROBE_NAME}
  namespace: {_S8BC_FIXTURE_NS}
spec:
  filter: '{{ probe: "{probe_value}" }}'
---
apiVersion: {g['group']}/{g['version']}
kind: RESTAction
metadata:
  name: {_RA_CONTROL_NAME}
  namespace: {_S8BC_FIXTURE_NS}
spec:
  filter: '{{ probe: "{control_value}" }}'
"""


def _call_url_for(gvr: dict, name: str, ns: str) -> str:
    """Build the snowplow /call query path for a CR (design §2 / phases.py:
    2557-2559 prior art). apiVersion is `<group>/<version>`, url-encoded.
    """
    api_version = f"{gvr['group']}/{gvr['version']}"
    api_version_enc = api_version.replace("/", "%2F")
    return (f"/call?apiVersion={api_version_enc}"
            f"&resource={gvr['plural']}&name={name}&namespace={ns}")


def _s8bc_call_body(gvr: dict, name: str, ns: str, token: str) -> str | None:
    """GET a CR's /call body once and return the DECODED text (gzip-aware via
    browser._decompress), or None on any transport failure.

    Reuses _phase8_poll_via_snowplow's URL/header shape but decodes the body
    properly (the phase8 helper grepped raw bytes; decoding first is strictly
    more correct and matches browser.read_snowplow_expvar_int's pattern).
    """
    import urllib.request
    snowplow = os.environ.get("SNOWPLOW_URL", browser.SNOWPLOW)
    url = snowplow.rstrip("/") + _call_url_for(gvr, name, ns)
    try:
        req = urllib.request.Request(
            url,
            headers={"Authorization": "Bearer " + token,
                     "Accept-Encoding": "gzip"})
        with urllib.request.urlopen(req, timeout=10) as r:
            body = r.read()
            body = browser._decompress(body, headers=dict(r.headers))
            return body.decode("utf-8", errors="replace")
    except Exception:
        return None


def _mutate_poll_reconcile(gvr: dict, probe_name: str, ns: str, token: str,
                           marker: str) -> tuple[int, int]:
    """Poll the probe CR's /call body until `marker` appears; budget 60s @ 1s.

    CONTENT predicate (never status-200, per feedback_validate_content_not_
    just_status): the decoded /call body must CONTAIN `marker` (the new echo
    value). The caller has already issued the kubectl patch at t0.

    Returns (conv_ms, poll_samples):
      conv_ms       — ms from this function's entry to the first marker
                      sighting; -1 on timeout.
      poll_samples  — number of poll iterations attempted (proof bookkeeping).

    The clock anchors at this function's OWN entry, so callers MUST invoke it
    immediately after the `kubectl patch` returns. The patch RTT is therefore
    EXCLUDED from the measurement (the clock starts after the apiserver has
    committed the patch) — the correct anchor for "snowplow reconcile time":
    it isolates informer→dirty-mark→refresher→serve from the bench client's
    own network hop.
    """
    start = time.time()
    deadline = start + _S8BC_POLL_BUDGET_S
    samples = 0
    while time.time() < deadline:
        samples += 1
        body = _s8bc_call_body(gvr, probe_name, ns, token)
        if body is not None and marker in body:
            return int((time.time() - start) * 1000), samples
        time.sleep(_S8BC_POLL_INTERVAL_S)
    return -1, samples


def _run_cr_mod_reconcile_stage(stage_id: str,
                                ctx: StageContext,
                                *,
                                gvr: dict,
                                probe_name: str,
                                control_name: str,
                                fixture_yaml: str,
                                patch_to_v2,
                                patch_to_v1,
                                metric_key: str,
                                kind_label: str,
                                what_breaks_if_skipped: str,
                                gating: bool = True) -> StageProof:
    """Shared S8b/S8c body. Symmetric across widget / RESTAction — only the
    fixture YAML, the GVR, the patch builders and the metric key differ.

    With gating=False the stage is INFORMATIONAL: the measurement and the
    would-be verdict are still recorded on the proof (`would_pass`), but the
    stage never fails the run. Harness exceptions still FAIL via _run_stage —
    a crashed harness is not a measurement.

    Flow (design §4.2):
      0. precheck token present.
      1. apply fixtures (probe="v1", control="ctrl"); idempotent.
      2. PRIME: GET both /call once (admin); assert probe body carries "v1"
         and control body carries "ctrl" — populates the per-cohort cell +
         records the self-dep edge. (Cold-miss resolve, not the measurement.)
      3. snapshot Probe-A expvar refresher_completed_total.
      4. MUTATE: patch probe echo-field -> "v2"; t0 = now.
      5. POLL probe /call body for "v2"; budget 60s. record conv_ms.
         Probe A: assert refresher_completed_total INCREMENTED vs snapshot.
      6. NEGATIVE CONTROL: GET control /call; assert body STILL "ctrl".
      7. REVERT probe -> "v1" (best-effort) + TEARDOWN both fixtures.
         REVERT/TEARDOWN run in a `finally` so they fire on BOTH the success
         and the failure paths.
    """
    V1, V2, CTRL = "v1", "v2", "ctrl"

    def _measure(c: StageContext) -> dict:
        ns = _S8BC_FIXTURE_NS
        token = c.tokens.get("admin")

        proof: dict = {
            "kind": kind_label,
            "fixture_ns": ns,
            "fixture_gvr": dict(gvr),
            "probe_name": probe_name,
            "control_name": control_name,
            "marker_observed": V2,
            "poll_budget_s": _S8BC_POLL_BUDGET_S,
            "poll_interval_s": _S8BC_POLL_INTERVAL_S,
        }

        # 0. precheck — without an admin token we cannot /call-poll.
        if not token:
            proof.update({
                "error": "no_admin_token",
                metric_key: -1,
                "__passed__": False,
            })
            return proof

        teardown_done = {"v": False}

        def _teardown() -> dict:
            """Revert probe to v1 (best-effort) then delete every fixture.
            Idempotent + safe to call twice (guarded). Returns a small diag.
            """
            if teardown_done["v"]:
                return {}
            teardown_done["v"] = True
            revert_ok, revert_diag = patch_to_v1(probe_name)
            d1 = cluster.k8s_delete_custom(
                gvr["group"], gvr["version"], gvr["plural"], ns, probe_name)
            d2 = cluster.k8s_delete_custom(
                gvr["group"], gvr["version"], gvr["plural"], ns, control_name)
            return {
                "revert_ok": bool(revert_ok),
                "revert_diag": revert_diag,
                "probe_deleted": bool(d1),
                "control_deleted": bool(d2),
            }

        try:
            # 1. create fixtures (idempotent server-side apply).
            apply_ok, apply_diag = cluster.k8s_apply_yaml(fixture_yaml)
            proof["apply_ok"] = apply_ok
            if not apply_ok:
                proof.update({
                    "apply_diag": apply_diag,
                    "error": "fixture_apply_failed",
                    metric_key: -1,
                    "__passed__": False,
                })
                return proof

            # 2. PRIME both cells (cold-miss resolve + self-dep record).
            prime_probe = _s8bc_call_body(gvr, probe_name, ns, token)
            prime_control = _s8bc_call_body(gvr, control_name, ns, token)
            prime_probe_ok = prime_probe is not None and V1 in prime_probe
            prime_control_ok = prime_control is not None and CTRL in prime_control
            proof["prime_probe_ok"] = prime_probe_ok
            proof["prime_control_ok"] = prime_control_ok
            if not (prime_probe_ok and prime_control_ok):
                proof.update({
                    "error": "prime_failed",
                    "prime_probe_snippet": (prime_probe or "")[:200],
                    "prime_control_snippet": (prime_control or "")[:200],
                    metric_key: -1,
                    "__passed__": False,
                })
                return proof

            # 3. Probe-A snapshot (WIRED refresher_completed_total expvar).
            refresher_before = browser.read_snowplow_expvar_int(
                "snowplow_refresher_completed_total")

            # 4. MUTATE probe -> v2. t0 anchors the reconcile wall-clock.
            patch_ok, patch_diag = patch_to_v2(probe_name)
            proof["mutate_ok"] = patch_ok
            if not patch_ok:
                proof.update({
                    "mutate_diag": patch_diag,
                    "error": "mutate_failed",
                    metric_key: -1,
                    "__passed__": False,
                })
                return proof

            # 5. POLL probe /call body for "v2".
            conv_ms, poll_samples = _mutate_poll_reconcile(
                gvr, probe_name, ns, token, V2)
            proof[metric_key] = conv_ms
            proof["poll_samples"] = poll_samples

            # Probe A: refresher_completed_total must have incremented.
            refresher_after = browser.read_snowplow_expvar_int(
                "snowplow_refresher_completed_total")
            refresher_delta = None
            if refresher_before is not None and refresher_after is not None:
                refresher_delta = refresher_after - refresher_before
            proof["refresher_completed_before"] = refresher_before
            proof["refresher_completed_after"] = refresher_after
            proof["refresher_completed_delta"] = refresher_delta

            # 6. NEGATIVE CONTROL: control body MUST still carry "ctrl" and
            #    MUST NOT carry "v2" at the moment the probe converged.
            control_body = _s8bc_call_body(gvr, control_name, ns, token)
            control_unchanged = (
                control_body is not None
                and CTRL in control_body
                and V2 not in control_body
            )
            proof["control_unchanged"] = control_unchanged
            proof["control_snippet"] = (control_body or "")[:200]

            converged = (conv_ms is not None and conv_ms >= 0)
            # Probe-A is corroboration; treat an unreadable expvar (None
            # delta) as non-blocking but require a positive delta when it IS
            # readable (a converge with zero refresher delta means the body
            # changed for the wrong reason — design falsifier #2).
            refresher_ok = (refresher_delta is None or refresher_delta > 0)
            proof["refresher_probe_ok"] = refresher_ok

            proof["__passed__"] = bool(
                converged and control_unchanged and refresher_ok)
            return proof
        finally:
            # 7. REVERT + TEARDOWN on BOTH success and failure paths.
            proof["teardown"] = _teardown()

    def _work(c: StageContext) -> dict:
        proof = _measure(c)
        if not gating:
            # Diego 2026-06-11 ("we should not care about restaction
            # reconcile"): record the measurement and the would-be verdict,
            # never fail the run on it.
            proof["informational"] = True
            proof["would_pass"] = bool(proof.get("__passed__", False))
            proof["__passed__"] = True
        return proof

    return _run_stage(stage_id, ctx, _work,
                      what_breaks_if_skipped=what_breaks_if_skipped)


def stage_s8b_widget_mod_reconcile(ctx: StageContext) -> StageProof:
    """S8b — Widget CR definition-modification reconcile latency (#314).

    Patches a bench-owned Button's `spec.widgetData.label` "v1"->"v2" and
    measures wall-clock to the moment a customer /call serves the new label.
    Exercises the recordWidgetDeps self-dep -> dirty-mark -> refresher
    re-resolve path on the widgets GVR. Report-only (`conv_widget_mod_ms`).
    """
    g = _WIDGET_FIXTURE_GVR

    def _patch_label(name: str, value: str):
        return cluster.k8s_patch_cr_spec(
            g["group"], g["version"], g["plural"], _S8BC_FIXTURE_NS, name,
            {"widgetData": {"label": value}})

    return _run_cr_mod_reconcile_stage(
        "S8b", ctx,
        gvr=g,
        probe_name=_WIDGET_PROBE_NAME,
        control_name=_WIDGET_CONTROL_NAME,
        fixture_yaml=_widget_fixture_yaml("v1", "ctrl"),
        patch_to_v2=lambda name: _patch_label(name, "v2"),
        patch_to_v1=lambda name: _patch_label(name, "v1"),
        metric_key="conv_widget_mod_ms",
        kind_label="widget",
        what_breaks_if_skipped=(
            "without the widget-CR-modification reconcile stage, a "
            "regression that stalls the recordWidgetDeps self-dep -> "
            "dirty-mark -> refresher re-resolve path (an edited widget "
            "definition never reaching a customer /call) would ship "
            "undetected."
        ),
    )


def stage_s8c_ra_mod_reconcile(ctx: StageContext) -> StageProof:
    """S8c — RESTAction CR definition-modification reconcile latency (#314).

    Patches a bench-owned RESTAction's top-level `spec.filter` jq literal
    '{ probe: "v1" }' -> '{ probe: "v2" }' and measures wall-clock to the
    moment a customer /call serves `"probe":"v2"`. Exercises the
    restactions.go:252 self-dep -> dirty-mark -> refresher re-resolve path on
    the restactions GVR. Report-only (`conv_ra_mod_ms`).

    INFORMATIONAL (gating=False, Diego 2026-06-11): records the reconcile
    time but never fails the run — S8b (widget) is the gating reconcile
    stage; RESTAction definition-edits are a rare admin authoring event.
    """
    g = _RA_FIXTURE_GVR

    def _patch_filter(name: str, value: str):
        return cluster.k8s_patch_cr_spec(
            g["group"], g["version"], g["plural"], _S8BC_FIXTURE_NS, name,
            {"filter": f'{{ probe: "{value}" }}'})

    return _run_cr_mod_reconcile_stage(
        "S8c", ctx,
        gvr=g,
        probe_name=_RA_PROBE_NAME,
        control_name=_RA_CONTROL_NAME,
        fixture_yaml=_ra_fixture_yaml("v1", "ctrl"),
        patch_to_v2=lambda name: _patch_filter(name, "v2"),
        patch_to_v1=lambda name: _patch_filter(name, "v1"),
        metric_key="conv_ra_mod_ms",
        kind_label="restaction",
        gating=False,
        what_breaks_if_skipped=(
            "without the RESTAction-CR-modification reconcile stage, a "
            "regression that stalls the restactions self-dep -> dirty-mark "
            "-> refresher re-resolve path (an edited RESTAction filter/output "
            "never reaching a customer /call) would ship undetected."
        ),
    )


# ─── S10: delete 1 namespace (was S8 pre-1.1.0) ────────────────────────────


def stage_s10_delete_one_ns(ctx: StageContext) -> StageProof:
    def _work(c: StageContext) -> dict:
        if c.scale >= 10000:
            s5_ns_end = 50
        else:
            s5_ns_end = c.scale // 10 if c.scale >= 10 else 1
        s10_ns = f"bench-ns-{s5_ns_end:02d}"
        _ = _snapshot_l1_ready_ts(c)
        lifecycle.delete_one_bench_namespace(s10_ns)
        lifecycle.wait_for_namespace_gone(s10_ns)
        _post_mutation_pause(c.cache_mode)
        # Task #296: thread the deleted ns so widget-terminal-state
        # validation can demote controller-churn ghost-panel 404s OUTSIDE
        # s10_ns to a recorded WARN (a ghost IN s10_ns stays a HARD fail).
        results = _measure_all_users(c, 10, "Deleted 1 ns",
                                     deleted_ns=s10_ns)
        # Surface the demotion in the proof (telemetry, like #289a did for
        # skeletons): count demoted navs + collect their churn-error detail.
        churn_demoted_navs = 0
        churn_errors: list[dict] = []
        for r in results:
            for _pg in (r.get("pages") or {}).values():
                for nav in (_pg.get("navigations") or []):
                    v = nav.get("validation") or {}
                    if v.get("s10_churn_demoted"):
                        churn_demoted_navs += 1
                        if v.get("s10_churn_errors"):
                            churn_errors.append({
                                "label": nav.get("label"),
                                **v["s10_churn_errors"],
                            })
        return {
            "ns": s10_ns,
            "measurement_count": len(results),
            "s10_churn_demoted_navs": churn_demoted_navs,
            "s10_churn_errors": churn_errors,
            "__passed__": True,
        }

    return _run_stage(
        "S10", ctx, _work,
        what_breaks_if_skipped=(
            "without ns-cascade delete cell, convergence_mass_s10_p99 "
            "is null — bulk delete invalidation regression cannot be "
            "detected."
        ),
    )


# ─── S11: build canonical ledger row + write bundle (was S9 pre-1.1.0) ─────


def stage_s11_report(ctx: StageContext) -> StageProof:
    def _work(c: StageContext) -> dict:
        per_stage_proofs = {}
        state = load_state(c.run_dir) or {}
        for sid, p in (state.get("stage_proofs") or {}).items():
            per_stage_proofs[sid] = p
        row = ledger.write_run_bundle(
            c.run_dir, c.all_results,
            per_stage_proofs=per_stage_proofs,
            tag=c.tag, scale=c.scale,
        )
        return {
            "verdict": row.get("verdict"),
            "ledger_row_keys": sorted(list(row.keys())),
            "__passed__": row.get("verdict") in ("PASS", "WEAK_PASS",
                                                 "FLOOR"),
        }

    return _run_stage(
        "S11", ctx, _work,
        what_breaks_if_skipped=(
            "S11 emits the canonical ledger row + summary.json bundle — "
            "without it the PM has no scored artifact to compare against "
            "the north-star ledger."
        ),
    )


# ─── STAGE_REGISTRY ─────────────────────────────────────────────────────────


STAGE_REGISTRY: dict[str, Callable[[StageContext], StageProof]] = {
    "S0": stage_s0_preflight,
    "S1": stage_s1_zero_state,
    "S2": stage_s2_one_ns_compdef,
    "S3": stage_s3_20_ns,
    "S4": stage_s4_20_compositions,
    "S5": stage_s5_scale_ns,
    "S6": stage_s6_scale_compositions,
    "S7": stage_s7_delete_one_comp,
    # Task #250 Block 2: new RBAC-mutation stages inserted after S7.
    "S8": stage_s8_add_rb_to_populated_ns,
    "S9": stage_s9_remove_rb_from_populated_ns,
    # Task #314 Block: CR-definition-modification reconcile-latency stages,
    # inserted after S9 / before S10 so they measure invalidation under the
    # fully-populated 50K dep graph (lettered sub-stages avoid renumbering
    # S10/S11 again — the 1.1.0 churn already renamed S8/S9→S10/S11 once).
    "S8b": stage_s8b_widget_mod_reconcile,
    "S8c": stage_s8c_ra_mod_reconcile,
    # Renamed from pre-1.1.0 layout (was S8 / S9).
    "S10": stage_s10_delete_one_ns,
    "S11": stage_s11_report,
}

STAGE_ORDER = ["S0", "S1", "S2", "S3", "S4", "S5", "S6", "S7",
               "S8", "S9", "S8b", "S8c", "S10", "S11"]


# ─── --from-stage / --to-stage semantics (per §E.3) ─────────────────────────


def _stages_in_window(from_stage: str | None,
                      to_stage: str | None) -> list[str]:
    start = STAGE_ORDER.index(from_stage) if from_stage else 0
    end = STAGE_ORDER.index(to_stage) + 1 if to_stage else len(STAGE_ORDER)
    if end <= start:
        return []
    return STAGE_ORDER[start:end]


def _validate_resume(state: dict, from_stage: str) -> tuple[bool, str]:
    """Per R4.1: ensure prior proofs in state.json precede from_stage in order.

    Returns (ok, diag). When state.json is empty (fresh resume) we accept.
    """
    completed = state.get("stages_completed") or []
    if not completed:
        return True, "no prior stages — fresh run"
    from_idx = STAGE_ORDER.index(from_stage)
    for sid in completed:
        try:
            idx = STAGE_ORDER.index(sid)
        except ValueError:
            return False, f"state.json carries unknown stage {sid!r}"
        if idx >= from_idx:
            return False, (
                f"state.json contains stages beyond requested "
                f"--from-stage={from_stage}: completed includes {sid}. "
                f"Pass a different --run-dir or remove the file."
            )
    return True, "resume window OK"


def _proof_validation_re_runner(state: dict,
                                from_stage: str,
                                *, budget_secs: float = 10.0) -> dict:
    """Per R4.1: re-verify prior stage proofs against the live cluster.

    Bounded to `budget_secs`. Stages whose proof cannot be validated in
    budget opt-out with `proof_validation: skipped`. Returns a dict
    {stage_id: "pass" | "fail:<reason>" | "skipped"}.
    """
    out: dict[str, str] = {}
    deadline = time.time() + budget_secs
    proofs = state.get("stage_proofs") or {}
    from_idx = STAGE_ORDER.index(from_stage)
    for sid in STAGE_ORDER[:from_idx]:
        if time.time() >= deadline:
            out[sid] = "skipped:budget_exhausted"
            continue
        p = proofs.get(sid)
        if not p:
            out[sid] = "skipped:no_proof"
            continue
        proof_body = p.get("proof") or {}
        if sid == "S2":
            # Compdef present?
            rc, _, _ = cluster.kubectl(
                "get", "compositiondefinitions.core.krateo.io",
                "-A", "--no-headers", "--ignore-not-found",
                timeout_secs=5)
            if rc == 0:
                out[sid] = "pass"
            else:
                out[sid] = "fail:compdef_missing"
        elif sid == "S3":
            expected = proof_body.get("ns_count", 20)
            actual = cluster.count_bench_ns()
            out[sid] = ("pass" if actual >= expected
                        else f"fail:ns_count_{actual}_lt_{expected}")
        elif sid in ("S4", "S5", "S6"):
            expected = proof_body.get("compositions_deployed") or \
                       proof_body.get("compositions_actual") or 0
            actual = cluster.count_compositions()
            if expected > 0 and actual < expected * 0.9:
                out[sid] = f"fail:comp_count_{actual}_lt_{int(expected * 0.9)}"
            else:
                out[sid] = "pass"
        elif sid == "S8":
            # S8 created a RoleBinding; verify it still exists at
            # apiserver. The Role + RB names are stamped on the proof
            # so the re-runner is parametric (no string literal).
            rb_name = proof_body.get("rb_name")
            target_ns = proof_body.get("target_ns")
            if rb_name and target_ns:
                rb = cluster.k8s_read_namespaced_role_binding(
                    target_ns, rb_name)
                out[sid] = "pass" if rb is not None else "fail:rb_missing"
            else:
                out[sid] = "skipped:no_rb_metadata"
        elif sid == "S9":
            # S9 deleted the RB; verify it is gone.
            rb_name = proof_body.get("rb_name")
            target_ns = proof_body.get("target_ns")
            if rb_name and target_ns:
                rb = cluster.k8s_read_namespaced_role_binding(
                    target_ns, rb_name)
                out[sid] = ("pass" if rb is None
                            else "fail:rb_still_present")
            else:
                out[sid] = "skipped:no_rb_metadata"
        elif sid in ("S8b", "S8c"):
            # Task #314: S8b/S8c tear down their bench fixtures (probe +
            # control CR) at stage end. Re-verify the probe fixture is gone
            # at the apiserver — parametric off the proof's stamped GVR +
            # name (no string literal). Mirrors the S9 "verify RB gone"
            # branch. When the GVR/name metadata is absent (older proof) we
            # fall back to the safe `skipped:no_live_check` default per the
            # design's §4.4 safe-default guidance.
            gvr = proof_body.get("fixture_gvr") or {}
            grp = gvr.get("group")
            ver = gvr.get("version")
            plural = gvr.get("plural")
            ns = proof_body.get("fixture_ns")
            probe_name = proof_body.get("probe_name")
            if grp and ver and plural and ns and probe_name:
                obj = cluster.k8s_get_custom(grp, ver, plural, ns, probe_name)
                out[sid] = ("pass" if obj is None
                            else "fail:fixture_still_present")
            else:
                out[sid] = "skipped:no_live_check"
        else:
            # S0/S1/S7/S10: no live-cluster reverify (S0 is meta,
            # S1 zero-state, S7/S10 already mutated — can't undo).
            out[sid] = "skipped:no_live_check"
    return out


# ─── Phase 6 orchestrator ──────────────────────────────────────────────────


def run_phase6(tag: str,
               scale: int,
               *,
               from_stage: str | None = None,
               to_stage: str | None = None,
               cache_mode: str = "OFF",
               video: str = "representative",
               run_dir: Path | None = None,
               smoke: bool | None = None) -> dict:
    """Run Phase 6 stages within [from_stage, to_stage] inclusive.

    On `--from-stage`, calls load_state + _validate_resume + the proof-
    validation re-runner (R4.1). On ConvergenceTimeout from any stage,
    state.json is already persisted by the inner _run_stage handler;
    we re-raise so the CLI can exit 4.

    Returns the final state dict (after the window completes).
    """
    if scale is None:
        scale = _env_scale()
    if smoke is None:
        smoke = _env_smoke()

    # Default run_dir under /tmp/snowplow-runs/{tag}/run-{ts}.
    if run_dir is None:
        ts = time.strftime("%Y%m%d-%H%M%S")
        run_dir = Path("/tmp/snowplow-runs") / tag / f"run-{ts}"
    run_dir = Path(run_dir)
    run_dir.mkdir(parents=True, exist_ok=True)

    state = load_state(run_dir) or {}
    if from_stage:
        ok, diag = _validate_resume(state, from_stage)
        if not ok:
            raise RuntimeError(f"resume validation failed: {diag}")
        if state.get("stage_proofs"):
            verdicts = _proof_validation_re_runner(state, from_stage)
            state["proof_validation"] = verdicts
            failures = [k for k, v in verdicts.items()
                        if v.startswith("fail:")]
            if failures:
                raise RuntimeError(
                    f"stale state.json — proofs failed re-validation: "
                    f"{failures}. Re-run from earlier stage."
                )

    state.setdefault("schema_version", SCHEMA_VERSION)
    state.setdefault("tag", tag)
    state.setdefault("scale", scale)
    state.setdefault("started_at", _now_iso())
    state.setdefault("stages_completed", [])
    state.setdefault("stage_proofs", {})
    state.setdefault("users", ["admin", "cyberjoker"])
    state["cache_mode"] = cache_mode
    state["run_dir"] = str(run_dir.absolute())
    save_state(run_dir, state)

    window = _stages_in_window(from_stage, to_stage)
    ctx = StageContext(
        tag=tag, scale=scale,
        tokens={}, user_pages={"__subjects__": ["admin", "cyberjoker"]},
        run_dir=run_dir,
        state_path=run_dir / "state.json",
        cache_mode=cache_mode,
        smoke=bool(smoke),
        all_results=[],
        video=(video or "representative"),
    )

    # Lifecycle: log in (login_all writes to ctx.tokens) only when window
    # contains a real measurement stage (S1+). Pure S0 / S11 skip login.
    needs_login = any(s != "S0" and s != "S11" for s in window)
    if needs_login:
        try:
            ctx.tokens = browser.login_all()
            ctx.admin_token = ctx.tokens.get("admin")
        except Exception:
            ctx.tokens = {}
            ctx.admin_token = None

    # Task #250 Block 2: new S8/S9 stages run measurements via
    # _measure_all_users, so they share the browser scaffolding. S10
    # (was S8 — delete 1 ns) keeps the existing measurement footprint.
    #
    # Task #314: S8b/S8c poll snowplow's /call over urllib (NOT Chrome) and
    # do not use the per-user Playwright pages. They are listed here to keep
    # the scaffolding/teardown symmetry with the S8/S9 prior art the design
    # cites; the poll TOKEN they actually need comes from the `needs_login`
    # path above (browser.login_all → ctx.tokens), which already covers any
    # non-S0/S11 stage. So a standalone `--from-stage S8b --to-stage S8c`
    # window still acquires the admin poll token even though _setup_users
    # is the heavier (Playwright) half of the scaffolding.
    needs_browser = any(s in ("S1", "S2", "S3", "S4", "S5", "S6", "S7",
                              "S8", "S9", "S8b", "S8c", "S10")
                        for s in window)
    if needs_browser and browser.FRONTEND is not None:
        try:
            _setup_users(ctx)
        except Exception as e:
            # Surface but continue with degraded measurement set.
            print(f"  WARN: _setup_users failed: {type(e).__name__}: {e}",
                  flush=True)

    # Task #347: route the phase6 run-start L1-readiness wait through the
    # #221 prefer-expvar / fallback-to-slog-scan boundary so the "using
    # expvar" / "falling back" log fires on a phase6 run (None-safe, never
    # blocks a cache-OFF run). Capture the run-start expvar trajectory
    # snapshot right after (report-only).
    _wait_for_l1_warmup_boundary(ctx)
    _write_expvar_trajectory_snapshot(run_dir, "run-start")

    try:
        for stage_id in window:
            fn = STAGE_REGISTRY[stage_id]
            fn(ctx)  # _run_stage handles persist + raise on ConvergenceTimeout
            # Task #347: post-S6-storm trajectory snapshot — S6 is the 50K
            # composition storm; capture the expvar state immediately after
            # it completes (report-only, None-safe).
            if stage_id == "S6":
                _write_expvar_trajectory_snapshot(run_dir, "post-s6-storm")
    finally:
        if needs_browser:
            # Per-stage videos are attached to each stage's proof inside
            # _run_stage (via _attach_stage_video_artifacts) at the moment
            # the stage completes/fails — no run-end re-attribution needed.
            _teardown_users(ctx)
        # Task #347: final trajectory snapshot — runs on BOTH the happy path
        # and after a ConvergenceTimeout re-raise (finally), so the run dir
        # always carries an end-of-run expvar snapshot. Report-only, None-safe.
        _write_expvar_trajectory_snapshot(run_dir, "final")

    return load_state(run_dir) or {}


def _attach_video_artifacts_to_last_measurement_proof(ctx: StageContext) -> None:
    """DEPRECATED no-op (Task #267 correction — Diego 2026-06-10).

    Superseded by PER-STAGE attachment: `_measure_all_users` produces one
    `S{n}_{user}_{slug}.webm`/.gif per stage and `_run_stage` attaches each
    stage's videos to THAT stage's proof via `_attach_stage_video_artifacts`,
    at the moment the stage completes or fails. The old behaviour
    re-attributed the single whole-run .webm to the earliest stage
    (`_first_stage_label`); now that videos are segmented per stage, doing
    that here would DOUBLE-attach every stage's video to the first stage.

    Retained as an importable no-op so external callers/tests resolve; it
    intentionally does nothing.
    """
    return None


# ─── Phase 7 / Phase 8 thin wrappers ────────────────────────────────────────


def run_phase7_user_scaling(tag: str | None = None,
                            run_dir: Path | None = None) -> None:
    """Thin wrapper around bench.storm.run_user_scaling.

    Per plan §A.8: Phase 7 retained as a separate subcommand; the worktree
    function `run_phase_user_scaling` (source 7697-7951) has been moved
    to bench.storm.run_user_scaling (Block 2). This wrapper exists so
    operators can run it from `python -m bench phase7`.
    """
    from bench import storm
    try:
        tokens = browser.login_all()
    except Exception:
        tokens = {}
    storm.run_user_scaling(tokens)


def run_phase8_per_mutation(tag: str | None = None,
                            run_dir: Path | None = None) -> None:
    """Phase 8 entrypoint — mutate+poll per-event convergence.

    The worktree function `run_phase_per_mutation` (source 7954-8162) was
    held back from earlier blocks because it is independent of Phase 6's
    state machine. We pull it into bench.phases as a private helper so the
    CLI's `phase8` subcommand has a clean import surface.
    """
    try:
        tokens = browser.login_all()
    except Exception:
        tokens = {}
    if "admin" not in tokens or "cyberjoker" not in tokens:
        print("Phase 8: missing admin or cyberjoker token; aborting",
              flush=True)
        return

    targets = _phase8_pick_targets(int(os.environ.get("PHASE8_TARGETS", "60")))
    if not targets:
        print("Phase 8: no compositions present; cluster not at steady state",
              flush=True)
        return

    duration_min = float(os.environ.get("PHASE8_DURATION_MIN", "5"))
    timeout_s = int(os.environ.get("PHASE8_TIMEOUT_S", "30"))
    poll_interval = float(os.environ.get("PHASE8_POLL_INTERVAL", "0.25"))

    recent_touched = set()
    per_event: list[dict] = []
    cycle_interval = max(0.5, duration_min * 60.0 / max(len(targets), 1))
    for i, (ns, name) in enumerate(targets):
        cls = "HOT" if (ns, name) in recent_touched else "WARM"
        marker = f"phase8-{int(time.time() * 1000)}-{i}"
        t0 = _phase8_mutate_target(ns, name, marker)
        if t0 < 0:
            continue
        deadline = t0 + timeout_s
        for user_label, token in tokens.items():
            lat = _phase8_poll_via_snowplow(
                ns, name, marker, token, t0, deadline, poll_interval)
            if lat >= 0:
                per_event.append({
                    "class": cls, "user": user_label, "latency_ms": lat,
                    "ns": ns, "name": name})
        time.sleep(max(0.0, cycle_interval - (time.time() - t0)))

    admin_samples = [e["latency_ms"] for e in per_event if e["user"] == "admin"]
    cyber_samples = [e["latency_ms"] for e in per_event
                     if e["user"] == "cyberjoker"]
    admin_p99 = ledger.pct(admin_samples, 99) if admin_samples else 0
    cyber_p99 = ledger.pct(cyber_samples, 99) if cyber_samples else 0
    p99_mix = int(round(0.95 * cyber_p99 + 0.05 * admin_p99))

    out = {
        "samples_total": len(per_event),
        "samples_admin": len(admin_samples),
        "samples_cyberjoker": len(cyber_samples),
        "p99_admin": admin_p99,
        "p99_cyberjoker": cyber_p99,
        "p99_mix": p99_mix,
        "hot_p99":  ledger.pct(
            [e["latency_ms"] for e in per_event if e["class"] == "HOT"],
            99) or 0,
        "warm_p99": ledger.pct(
            [e["latency_ms"] for e in per_event if e["class"] == "WARM"],
            99) or 0,
        "cold_p99": ledger.pct(
            [e["latency_ms"] for e in per_event if e["class"] == "COLD"],
            99) or 0,
        "events": per_event,
    }
    # Block 4: writes under run_dir/phase8/ if supplied; legacy path else.
    if run_dir is not None:
        target_dir = Path(run_dir) / "phase8"
        target_dir.mkdir(parents=True, exist_ok=True)
        target_file = target_dir / "per_mutation_results.json"
    else:
        target_file = Path("/tmp/snowplow_per_mutation_results.json")
    target_file.write_text(json.dumps(out, indent=2, default=str))
    print(f"Phase 8 results saved to {target_file}", flush=True)


def _phase8_pick_targets(n: int) -> list[tuple[str, str]]:
    rc, out, _ = cluster.kubectl(
        "get",
        f"{cluster.COMP_RES}.{cluster.COMP_GVR}",
        "-A", "--no-headers",
        "-o", "custom-columns=NS:.metadata.namespace,NAME:.metadata.name")
    if rc != 0 or not out.strip():
        return []
    rows = []
    for line in out.splitlines():
        parts = line.split(None, 1)
        if len(parts) >= 2:
            rows.append((parts[0], parts[1]))
    import random
    random.seed(0)
    random.shuffle(rows)
    return rows[:n]


def _phase8_mutate_target(ns: str, name: str, marker: str) -> float:
    patch = json.dumps({"metadata": {"annotations": {
        "snowplow-bench/phase8-marker": marker}}})
    t0 = time.time()
    rc, _, _ = cluster.kubectl(
        "patch",
        f"{cluster.COMP_RES}.{cluster.COMP_GVR}", name,
        "-n", ns, "--type=merge", "-p", patch)
    if rc != 0:
        return -1
    return t0


def _phase8_poll_via_snowplow(ns: str, name: str, marker: str,
                              token: str, start_time: float,
                              deadline: float, poll_interval: float) -> int:
    import urllib.request
    snowplow = os.environ.get("SNOWPLOW_URL", "http://34.135.50.203:8081")
    path = ("/call?apiVersion=templates.krateo.io%2Fv1"
            "&resource=restactions&name=compositions-list"
            "&namespace=krateo-system")
    while time.time() < deadline:
        try:
            req = urllib.request.Request(
                snowplow + path,
                headers={"Authorization": "Bearer " + token,
                         "Accept-Encoding": "gzip"})
            with urllib.request.urlopen(req, timeout=10) as r:
                body = r.read()
                if marker in body.decode("utf-8", errors="replace"):
                    return int((time.time() - start_time) * 1000)
        except Exception:
            pass
        time.sleep(poll_interval)
    return -1
