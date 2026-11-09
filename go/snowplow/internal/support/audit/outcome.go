package audit

import (
	"fmt"
	"net/http"
)

// FailureMessage is the body of a record whose action failed: the reason class
// of what the authority answered, from a closed set. The trail is kept 365
// days, so a record body never carries upstream text: an apiserver Status
// message names the user ("User \"…\" cannot …") and a non-Status error body
// can echo anything the upstream sent, a token included (#1298). The status
// code itself is the record's http.response.status_code.
//
// THE one wording for every emit site that records an upstream answer: the
// /call write record (handlers/call.go) and the RESTAction stage record
// (resolvers/restactions/api/stage_audit.go).
//
//   - code 0 (nothing was received): "no response from the endpoint";
//   - a code with a standard status text: "upstream answered <code> <text>";
//   - any other code: "upstream answered <code>".
func FailureMessage(code int) string {
	if code == 0 {
		return "no response from the endpoint"
	}
	if t := http.StatusText(code); t != "" {
		return fmt.Sprintf("upstream answered %d %s", code, t)
	}
	return fmt.Sprintf("upstream answered %d", code)
}
