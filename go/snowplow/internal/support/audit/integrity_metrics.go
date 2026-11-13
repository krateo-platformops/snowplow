// integrity_metrics.go — the audit trail's two design-error counters (#1379
// review), on /debug/vars in either cache mode: zero is the only correct value
// of each, and each has a test that drives it off zero.
//
//   - snowplow_audit_unrecorded_exit_total{answered,panic}: an audited
//     request whose handler answered (a 2xx, 3xx or 5xx exit) or panicked
//     without recording its outcome; OpenRecord recorded it for it, as a
//     failure with its own fixed body.
//   - snowplow_audit_dial_method_mismatch_total: a record whose
//     http.request.method is not the method of its krateo.audit.verb, i.e. the
//     method of the request snowplow served instead of the one it sent (F1).
//     The record is still emitted, as given; the counter makes the defect
//     visible, and a test binary's guard (SetDialMethodGuardForTest) fails the
//     arm that emitted it.
package audit

import (
	"expvar"
	"net/http"
	"slices"
	"sync/atomic"
)

// DesignErrorVar is the /debug/vars name of the count of audited requests
// whose handler answered without recording its outcome, by kind.
const DesignErrorVar = "snowplow_audit_unrecorded_exit_total"

// DialMethodMismatchVar is the /debug/vars name of the count of records whose
// method is not their verb's.
const DialMethodMismatchVar = "snowplow_audit_dial_method_mismatch_total"

var (
	designErrors        = new(expvar.Map)
	dialMethodMismatch  = new(expvar.Int)
	dialMethodGuardFunc atomic.Pointer[func(method, verb string)]
)

func init() {
	designErrors.Add("answered", 0)
	designErrors.Add("panic", 0)
	expvar.Publish("snowplow_audit_unrecorded_exit_total", designErrors)
	expvar.Publish("snowplow_audit_dial_method_mismatch_total", dialMethodMismatch)
}

// DesignErrorCount is the count of DesignErrorVar's kind ("answered", "panic").
func DesignErrorCount(kind string) int64 {
	if v, ok := designErrors.Get(kind).(*expvar.Int); ok {
		return v.Value()
	}
	return 0
}

// DialMethodMismatchCount is DialMethodMismatchVar's value.
func DialMethodMismatchCount() int64 { return dialMethodMismatch.Value() }

// SetDialMethodGuardForTest installs f, called with the method and verb of
// every record whose method is not its verb's. A test binary installs one
// that fails the arm emitting the record, so every arm that emits a record
// runs the falsifier. nil removes it.
func SetDialMethodGuardForTest(f func(method, verb string)) {
	if f == nil {
		dialMethodGuardFunc.Store(nil)
		return
	}
	dialMethodGuardFunc.Store(&f)
}

// methodsSemconvNames are the methods http.request.method records as given;
// any other is _OTHER and is not judged.
var methodsSemconvNames = []string{
	http.MethodGet, http.MethodPost, http.MethodPut, http.MethodPatch,
	http.MethodDelete, http.MethodHead, http.MethodOptions,
}

// checkDialMethod counts, and reports to the test guard, a record whose method
// is not the method of its verb. A record with no verb, or a method semconv
// does not name, is not judged.
func checkDialMethod(ev Event) {
	if ev.Verb == "" || !slices.Contains(methodsSemconvNames, ev.HTTPMethod) {
		return
	}
	if MethodOfVerb(ev.Verb) == ev.HTTPMethod {
		return
	}
	dialMethodMismatch.Add(1)
	if g := dialMethodGuardFunc.Load(); g != nil {
		(*g)(ev.HTTPMethod, ev.Verb)
	}
}
