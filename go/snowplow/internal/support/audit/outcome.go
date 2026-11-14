package audit

import (
	"fmt"
	"net/http"

	krateoaudit "github.com/krateo-platformops/audit"
)

// outcome.go — the closed set of a record's body for an action that dialled an
// authority: the /call write record (handlers/call.go) and the RESTAction stage
// record (resolvers/restactions/api/stage_audit.go). Both build it from what
// was observed at the dial (credhttp.Answer), never from the envelope the
// caller receives, whose code snowplow may have made up itself (#1298 review).
//
// The trail is kept 365 days, so a record body never carries upstream text: an
// apiserver Status message names the user ("User \"…\" cannot …") and a
// non-Status error body can echo anything the upstream sent, a token included
// (#1298). Every entry says either what the authority answered (its status
// code, which is also the record's http.response.status_code) or what snowplow
// itself did; an entry about snowplow's own doing never reads as an answer.

// What snowplow did when nothing answered. A record with one of these bodies
// carries no http.response.status_code: no authority answered.
const (
	// MessageNoResponse: the dial got no response (connection refused, a
	// transport error, a request snowplow could not build).
	MessageNoResponse = "no response from the endpoint"
	// MessageDestinationRefused: snowplow's destination policy (#1131) refused
	// the address before a byte was sent.
	MessageDestinationRefused = "not sent: snowplow's destination policy refused the address"
	// MessageBudgetNotSent: the request's time budget (#1115) ran out before
	// it was sent, or while connecting: nothing was sent.
	MessageBudgetNotSent = "not sent: snowplow's time budget for the request ran out first"
	// MessageBudgetNoAnswer: snowplow stopped waiting for the answer when the
	// budget ran out; the request had not been fully written.
	MessageBudgetNoAnswer = "snowplow stopped waiting: the request's time budget ran out before an answer"
	// MessageBudgetMayBeApplied: snowplow stopped waiting for the answer of a
	// write whose request was written: its outcome is unknown (never re-sent).
	MessageBudgetMayBeApplied = "snowplow stopped waiting: the request's time budget ran out after the write was sent; it may have been applied"
)

// Notes on an accepted (2xx) write whose body snowplow did not use. The record
// is a success with the real 2xx code: the authority accepted the write, so the
// object changed, whatever snowplow then did with the answer's body.
const (
	// NoteBodyUnread: reading or decoding the 2xx body failed.
	NoteBodyUnread = "accepted; the response body could not be read"
	// NoteBodyUnconvertible: a stage's 2xx body is neither JSON nor YAML.
	NoteBodyUnconvertible = "accepted; the response body is not JSON or YAML"
	// NoteBodyNotJSON: a /call's 2xx body was refused unread: its content type
	// is not JSON (the caller receives 406).
	NoteBodyNotJSON = "accepted; snowplow refused the response body unread: its content type is not JSON"
)

// FailureMessage is the body of a failure record whose authority answered code
// (0 = nothing answered):
//
//   - 0: MessageNoResponse;
//   - a 3xx: snowplow refuses every redirect on a credentialed dial (#723/#729,
//     #1106), so the entry says so: "snowplow refused to follow a <code>
//     <text> redirect";
//   - a code with a standard status text: "upstream answered <code> <text>";
//   - any other code: "upstream answered <code>".
func FailureMessage(code int) string {
	switch {
	case code == 0:
		return MessageNoResponse
	case code >= 300 && code < 400:
		return fmt.Sprintf("snowplow refused to follow a %s redirect", codeText(code))
	}
	return "upstream answered " + codeText(code)
}

// Why snowplow itself refused (krateo.audit.denial_reason, on a denied record
// only; audit v0.12.0, audit#22 (a)): the request writer's closed set, each one
// of snowplow's own refusal causes. A denied record carries no status code.
const (
	DenialDestinationRefused       = krateoaudit.DenialReasonDestinationRefused
	DenialPerUserCredentialRef     = krateoaudit.DenialReasonPerUserCredentialRef
	DenialEndpointRefMalformed     = krateoaudit.DenialReasonEndpointRefMalformed
	DenialInternalIdentityContext  = krateoaudit.DenialReasonInternalIdentityContext
	DenialWriteRefused             = krateoaudit.DenialReasonWriteRefused
	DenialDryRunNotExecuted        = krateoaudit.DenialReasonDryRunNotExecuted
	DenialDependencyFailed         = krateoaudit.DenialReasonDependencyFailed
	DenialUAFOnWriteVerb           = krateoaudit.DenialReasonUafOnWriteVerb
	DenialInvalidPath              = krateoaudit.DenialReasonInvalidPath
	DenialTemplateError            = krateoaudit.DenialReasonTemplateError
	DenialSessionCredentialMissing = krateoaudit.DenialReasonSessionCredentialMissing
	DenialStepUpRequired           = krateoaudit.DenialReasonStepUpRequired
	DenialInvalidRequest           = krateoaudit.DenialReasonInvalidRequest
	// DenialRBACNotAllowed: snowplow's own RBAC evaluation refused the
	// verified caller before its handler ran (audit v0.13.0).
	DenialRBACNotAllowed = krateoaudit.DenialReasonRbacNotAllowed
)

// MessageRefused is the body of a denied record: snowplow refused before
// sending anything. The reason is krateo.audit.denial_reason, never the body.
const MessageRefused = "not sent: snowplow refused the request"

// MessageEndpointUnreadable is the body of the failure record of a write stage
// whose endpoint could not be read: nothing was sent, so the record carries no
// status code (the read's answer is the apiserver's to snowplow, not the
// write's).
const MessageEndpointUnreadable = "the stage's endpoint could not be read; nothing was sent"

// codeText is "<code> <status text>", or the bare code when it has no
// standard text.
func codeText(code int) string {
	if t := http.StatusText(code); t != "" {
		return fmt.Sprintf("%d %s", code, t)
	}
	return fmt.Sprintf("%d", code)
}
