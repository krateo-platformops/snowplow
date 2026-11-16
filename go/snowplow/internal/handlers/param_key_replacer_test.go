// param_key_replacer_test.go — run 23 CPU profile: normalizeParamKey built a
// strings.Replacer on every call (1.65% of all CPU, 4.2% of allocated bytes,
// on the customer path through hasCallOnlyParam → nearMissParam). It now uses
// one package-level Replacer. These pin that the folding is unchanged and
// measure the before/after.
//
//	go test ./internal/handlers/ -run '^$' -bench ParamKey -benchmem -count=6
package handlers

import (
	"net/url"
	"strings"
	"testing"
)

// normalizeParamKeyBefore is normalizeParamKey as it was before run 23's fix:
// a fresh Replacer per call. The reference the parity test compares against.
func normalizeParamKeyBefore(k string) string {
	return strings.ToLower(strings.NewReplacer("_", "", "-", "").Replace(k))
}

// paramKeyCorpus is every query key the portal sends on /call (the dispatched
// and the call-only ones), the #443 near-miss spellings, and keys that stress
// the folding: empty, separators only, repeated and adjacent separators,
// non-ASCII case folding, and bytes that are not UTF-8.
var paramKeyCorpus = []string{
	"apiVersion", "resource", "name", "namespace", "perPage", "page", "cursor",
	"extras", "raw", "dryRun", "fieldValidation", "subresource", "kind",
	"dryrun", "DryRun", "DRYRUN", "dry_run", "dry-run", "Dry_Run",
	"FieldValidation", "fieldvalidation", "field_validation", "field-validation",
	"", "_", "-", "__--__", "a_-_b", "-dryRun-", "dry__run", "dry--run",
	"ÄÖÜ_straße", "İstanbul-key", "ΣΊΣΥΦΟΣ", "\xff\xfe_dry-run", "日本-語_キー",
	"x" + strings.Repeat("_y-", 200),
}

// TestParamKeyNormalize_IdenticalToPerCallReplacer — the shared Replacer folds
// every corpus key exactly as the per-call one did, and nearMissParam (the only
// reader) answers the same.
func TestParamKeyNormalize_IdenticalToPerCallReplacer(t *testing.T) {
	for _, k := range paramKeyCorpus {
		if got, want := normalizeParamKey(k), normalizeParamKeyBefore(k); got != want {
			t.Errorf("normalizeParamKey(%q) = %q, want %q", k, got, want)
		}
		before := ""
		n := normalizeParamKeyBefore(k)
		for _, p := range strictParams {
			if k != p && n == normalizeParamKeyBefore(p) {
				before = p
				break
			}
		}
		if got := nearMissParam(k); got != before {
			t.Errorf("nearMissParam(%q) = %q, want %q", k, got, before)
		}
	}
}

// TestParamKeyNormalize_ConcurrentUse — the one Replacer is shared by every
// request goroutine; under -race this proves the sharing is safe.
func TestParamKeyNormalize_ConcurrentUse(t *testing.T) {
	done := make(chan struct{})
	for g := 0; g < 8; g++ {
		go func() {
			defer func() { done <- struct{}{} }()
			for i := 0; i < 200; i++ {
				for _, k := range paramKeyCorpus {
					if normalizeParamKey(k) != normalizeParamKeyBefore(k) {
						t.Errorf("concurrent normalizeParamKey(%q) differs", k)
						return
					}
				}
			}
		}()
	}
	for g := 0; g < 8; g++ {
		<-done
	}
}

// paramKeyBenchQuery is a widget/RESTAction dispatch's query, the request
// shape every customer call carries through hasCallOnlyParam.
var paramKeyBenchQuery = url.Values{
	"apiVersion": {"templates.krateo.io/v1"}, "resource": {"restactions"},
	"name": {"compositions-list"}, "namespace": {"krateo-system"},
	"perPage": {"20"}, "page": {"1"}, "extras": {`{"status":"failed"}`},
}

var paramKeySink bool

// BenchmarkParamKey_HasCallOnlyParam — path=before rebuilds the Replacer per
// key (the pre-fix normalizeParamKey); path=after is the shipped code.
func BenchmarkParamKey_HasCallOnlyParam(b *testing.B) {
	nearMissBefore := func(key string) string {
		n := normalizeParamKeyBefore(key)
		for _, p := range strictParams {
			if key != p && n == normalizeParamKeyBefore(p) {
				return p
			}
		}
		return ""
	}
	hasBefore := func(q url.Values) bool {
		for _, p := range callOnlyParams {
			if _, ok := q[p]; ok {
				return true
			}
		}
		for k := range q {
			if nearMissBefore(k) != "" {
				return true
			}
		}
		return false
	}
	b.Run("path=before", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			paramKeySink = hasBefore(paramKeyBenchQuery)
		}
	})
	b.Run("path=after", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			paramKeySink = hasCallOnlyParam(paramKeyBenchQuery)
		}
	})
}
