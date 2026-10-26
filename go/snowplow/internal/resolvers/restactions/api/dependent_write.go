// dependent_write.go — #904: a write stage is not sent when the stage it
// depends on did not succeed.
//
// THE DEFECT. #313 C-A (0.30.257) made a per-item error non-truncating: the
// error is recorded under the stage's errorKey and every later stage still
// runs. That is right for read pages (progressive rendering: show what can be
// shown), and wrong for a write lifecycle: read → detach volumes → delete sent
// the DELETE after the detach failed, and /call answered 200.
//
// THE RULE (owner ruling 2026-10-26, team-lead outcome semantics on the issue).
// A write stage (any verb but GET/HEAD/OPTIONS/TRACE) whose dependsOn stage
// FAILED or was SKIPPED is not sent, unless that write stage itself sets
// continueOnError: true ("send me even if what I depend on did not succeed").
//   - The outcome is read from the resolve's own per-stage outcomes
//     (stage_outcomes.go), so a dependency that yielded zero iterator items
//     SUCCEEDED and its dependent write IS sent (the common "no data volumes"
//     delete); one failing item out of N is a failure and it is NOT sent.
//   - A skipped stage blocks its own dependents, so a chain stops at the first
//     failure however long it is.
//   - Reads are untouched: a read stage over a failed stage still runs.
//   - The skipped stage is never dispatched: no endpoint resolution, no
//     iterator, no egress. It writes nothing to the dict, and the response
//     header lists it as Skipped with the stage it depended on.
//
// No new sink bump is needed for the Put gates: a skip exists only downstream
// of a failure, which already bumped the stage-error sink.
package api

import (
	"log/slog"

	"github.com/krateo-platformops/plumbing/ptr"
	templates "github.com/krateo-platformops/snowplow/apis/templates/v1"
)

// dependentWriteBlocked reports whether apiCall is a write stage that must not
// be sent because its dependsOn stage did not succeed, and names that stage.
func (r *resolveRun) dependentWriteBlocked(apiCall *templates.API) (string, bool) {
	if apiCall == nil || apiCall.DependsOn == nil || !isWriteVerb(apiCall.Verb) ||
		ptr.Deref(apiCall.ContinueOnError, false) {
		return "", false
	}
	dep := apiCall.DependsOn.Name
	return dep, r.outcomes.blocksDependentWrite(dep)
}

// skipDependentWrite records the skip of stage id (it depended on dep).
func (r *resolveRun) skipDependentWrite(id, dep string) {
	r.outcomes.skip(id, dep)
	r.log.Info("write stage not sent: the stage it depends on did not succeed",
		slog.String("name", id),
		slog.String("depends_on", dep),
		slog.String("restaction_namespace", r.opts.RESTActionNamespace),
		slog.String("restaction_name", r.opts.RESTActionName))
}
