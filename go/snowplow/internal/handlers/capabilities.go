package handlers

import (
	"encoding/json"
	"net/http"
	"sort"
)

// Capability tokens advertised on GET /capabilities (#443). A client gates a
// behaviour on the PRESENCE of its token, never on a version number; a 404
// from /capabilities means none of them.
const (
	// CapCallDryRun: POST|PUT|PATCH /call/dry-run sends the apiserver
	// dryRun=All and echoes X-Snowplow-Dry-Run.
	CapCallDryRun = "call.dryRun"
	// CapCallFieldValidation: fieldValidation=Ignore|Strict is forwarded on
	// /call and /call/dry-run write verbs and echoed in
	// X-Snowplow-Field-Validation.
	CapCallFieldValidation = "call.fieldValidation"
	// CapCallRaw: raw=true on GET /call and POST /call/read returns the stored
	// restactions / widgets object, read as the caller, and echoes
	// X-Snowplow-Raw.
	CapCallRaw = "call.raw"
)

// capabilities is the static advertised set. A token is added here only in the
// change that ships its behaviour (call.read.inline lands with #443 part 2,
// call.warnings with the plumbing warning passthrough).
var capabilities = []string{
	CapCallDryRun,
	CapCallFieldValidation,
	CapCallRaw,
}

// Capabilities returns a sorted copy of the advertised capability tokens.
func Capabilities() []string {
	out := append([]string(nil), capabilities...)
	sort.Strings(out)
	return out
}

// CapabilitiesHandler serves GET /capabilities: a static, unauthenticated
// list of tokens. It carries no identity and makes no apiserver call.
func CapabilitiesHandler() http.Handler {
	body, err := json.Marshal(map[string][]string{"capabilities": Capabilities()})
	if err != nil {
		// A []string always marshals; this is unreachable.
		panic(err)
	}
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(body)
	})
}
