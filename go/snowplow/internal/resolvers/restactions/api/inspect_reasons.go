package api

import (
	"errors"
	"fmt"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
)

// The reason tokens of an Unresolved stage (#1619). They are a closed, stable
// set a client may switch on: a token is never reworded, never parsed from the
// message, and a new cause gets a new token. Message carries the prose, which
// may change.
//
// Each token is one cause of "this stage cannot be enumerated without
// dispatching". They are documented for clients in
// docs/restaction-rbac-endpoint-design.md (section 4) and on the GET /rbac
// swagger comment (internal/handlers/rbac.go); keep the three in step.
const (
	// ReasonStageDataInResourcesFrom: a userAccessFilter's resourcesFrom reads
	// the output of an upstream stage, which does not exist before dispatch.
	ReasonStageDataInResourcesFrom = "stageDataInResourcesFrom"
	// ReasonResourceNotServed: the apiserver serves no such resource (or group,
	// or version); the stage would 404 at /call.
	ReasonResourceNotServed = "resourceNotServed"
	// ReasonDiscoveryFailed: the apiserver's discovery could not be read, so the
	// resource could not be validated. Retryable; says nothing about the stage.
	ReasonDiscoveryFailed = "discoveryFailed"
	// ReasonNoRequestPath: the stage plans no request at all (an iterator that
	// yields nothing, with nothing refused).
	ReasonNoRequestPath = "noRequestPath"
	// ReasonUnrenderedTemplate: a literal ${...} survived rendering (#293), so
	// the coordinate is not determinable.
	ReasonUnrenderedTemplate = "unrenderedTemplate"
	// ReasonPathNotAsServed: the rendered path is one the apiserver would read as
	// a different path (#1137): a path with no leading "/", an empty segment a
	// dial would clean away, or more than one ${...} embedded in one path (which
	// does not render as a path).
	ReasonPathNotAsServed = "pathNotAsServed"
	// ReasonPathValueRefused: a value spliced into the path could not be one
	// segment or one query value, the path has a '.'/'..' segment, or an
	// apiserver path carries a percent-escape (#1123).
	ReasonPathValueRefused = "pathValueRefused"
	// ReasonJQPathError, ReasonJQPayloadError, ReasonJQHeaderError: the stage's
	// path, payload or header jq expression errored when rendered.
	ReasonJQPathError    = "jqPathError"
	ReasonJQPayloadError = "jqPayloadError"
	ReasonJQHeaderError  = "jqHeaderError"
	// ReasonTemplatedResource: the resource segment of the path is an expression,
	// so what is read is undetermined (#504 ruling 3).
	ReasonTemplatedResource = "templatedResource"
	// ReasonNoStaticResource: the path template has no static (group, version,
	// resource) to enumerate from.
	ReasonNoStaticResource = "noStaticResource"
	// ReasonNotEnumerablePath: the rendered path is not an in-cluster apiserver
	// resource path (a residual template, upstream-dependent, or non-kube).
	ReasonNotEnumerablePath = "notEnumerablePath"
	// ReasonUnknownDependency: the stage's dependsOn names a stage the
	// RESTAction does not declare. The RESTAction is invalid; the other stages
	// are still enumerated.
	ReasonUnknownDependency = "unknownDependency"
	// ReasonCyclicDependency: the stage is on a dependsOn cycle, or depends on a
	// stage that is.
	ReasonCyclicDependency = "cyclicDependency"
)

// UnresolvedReasons is every token, in the order above: the closed set.
var UnresolvedReasons = []string{
	ReasonStageDataInResourcesFrom,
	ReasonResourceNotServed,
	ReasonDiscoveryFailed,
	ReasonNoRequestPath,
	ReasonUnrenderedTemplate,
	ReasonPathNotAsServed,
	ReasonPathValueRefused,
	ReasonJQPathError,
	ReasonJQPayloadError,
	ReasonJQHeaderError,
	ReasonTemplatedResource,
	ReasonNoStaticResource,
	ReasonNotEnumerablePath,
	ReasonUnknownDependency,
	ReasonCyclicDependency,
}

// unresolvedError is an enumeration failure that carries its token. It is what
// the inspect helpers return, so the token is chosen where the cause is known and
// never recovered from the message.
type unresolvedError struct {
	reason string
	msg    string
}

func (e *unresolvedError) Error() string { return e.msg }

func unresolvedf(reason, format string, args ...any) error {
	return &unresolvedError{reason: reason, msg: fmt.Sprintf(format, args...)}
}

// unresolvedOf is the Unresolved entry for a stage whose enumeration failed with
// err. An error that carries no token is a stage that cannot be enumerated for a
// reason no helper named: ReasonNotEnumerablePath, the general one.
func unresolvedOf(stage string, err error) Unresolved {
	var ue *unresolvedError
	if errors.As(err, &ue) {
		return Unresolved{Stage: stage, Reason: ue.reason, Message: err.Error()}
	}
	return Unresolved{Stage: stage, Reason: ReasonNotEnumerablePath, Message: err.Error()}
}

// discoveryErr types a failure of the discovery read: the resource is not served
// (the apiserver answered NotFound for its group/version) or discovery itself
// failed.
func discoveryErr(err error) error {
	if apierrors.IsNotFound(err) {
		return &unresolvedError{reason: ReasonResourceNotServed, msg: err.Error()}
	}
	return &unresolvedError{reason: ReasonDiscoveryFailed, msg: err.Error()}
}

// skipToken is the token of a path the plan refused to dial (a skipReason class
// other than the collapsed-segment one, which is enumerated from the skeleton).
func skipToken(class string) string {
	switch class {
	case reasonUnrenderedTemplate:
		return ReasonUnrenderedTemplate
	case reasonPathNotAsServed:
		return ReasonPathNotAsServed
	case reasonPathValueRefused:
		return ReasonPathValueRefused
	case reasonJQPathError:
		return ReasonJQPathError
	case reasonJQPayloadError:
		return ReasonJQPayloadError
	case reasonJQHeaderError:
		return ReasonJQHeaderError
	}
	return ReasonNotEnumerablePath
}
