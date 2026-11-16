package credhttp

// connections_guarded_1131_test.go — #1131 on #1189's counter: a counted client
// that names no transport is both guarded and counted. Its dial carries the
// destination policy, and the connection it opens is still counted under its
// egress. The refusal arm uses 0.0.0.0, which the policy refuses in the control
// hook, so not even a mutant of the fallback sends a packet off this machine.

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func x1131Opened(egress string) uint64 {
	var n uint64
	for _, c := range EgressConnectionsSnapshot() {
		if c.Egress == egress {
			n += c.Opened
		}
	}
	return n
}

func TestIssue1131_ACountedClientWithNoTransportIsGuardedAndCounted(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Connection", "close") // a fresh connection for each case
	}))
	t.Cleanup(srv.Close)

	for name, tc := range map[string]struct {
		egress string
		build  func() *http.Client
	}{
		// The authn exchange and the RUM forwarder: Client's fallback, counted.
		"authn": {EgressAuthn, func() *http.Client { return CountConnectionsOn(EgressAuthn, Client(&http.Client{})) }},
		// countConnectionsTransport's own fallback, the line #1189 added.
		"counter_fallback": {EgressRUMForward, func() *http.Client { return CountConnectionsOn(EgressRUMForward, &http.Client{}) }},
	} {
		t.Run(name, func(t *testing.T) {
			cli := tc.build()

			before := x1131Opened(tc.egress)
			resp, err := cli.Get(srv.URL + "/x")
			if err != nil {
				t.Fatal(err)
			}
			_ = resp.Body.Close()
			if got := x1131Opened(tc.egress) - before; got != 1 {
				t.Fatalf("RED: the guarded dial opened %d counted connection(s) under %s, want 1", got, tc.egress)
			}

			before = x1131Opened(tc.egress)
			resp, err = cli.Get("http://0.0.0.0/x1131-path")
			if err == nil {
				_ = resp.Body.Close()
				t.Fatalf("RED: the unspecified address answered %d", resp.StatusCode)
			}
			x1131RequireRefused(t, err)
			if got := x1131Opened(tc.egress) - before; got != 0 {
				t.Fatalf("a refused dial counted %d connection(s)", got)
			}
		})
	}
}
