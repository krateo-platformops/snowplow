package api

// own_credential_1382_bench_test.go — #1382 requirement 5: the cost of the
// extra lookup on a miss. A username whose exact and legacy names differ
// (john.doe) has two names to try; one whose names agree (alice) has one.
//
//   - Informer: each name is a map lookup in the Secrets snapshot.
//   - APIServer: each name is one GET (credhttp.GetSecret against a loopback
//     httptest apiserver: real client build, HTTP round trip and decode, no
//     network latency, so the GET's cost here is a floor).
//
// Run: go test -tags=unit -run '^$' -bench Issue1382 -benchmem ./internal/resolvers/restactions/api/

import (
	"context"
	"testing"

	corev1 "k8s.io/api/core/v1"

	"github.com/krateo-platformops/snowplow/internal/credsecret"
)

func benchInformer(b *testing.B, user string, secs []*corev1.Secret) {
	b.Setenv("CACHE_ENABLED", "true")
	oc1382Informer(b, secs)
	names := credsecret.SessionNames(user)
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _, _ = FromInformerOwnSecret(ctx, oc1382NS, user, names)
	}
}

func benchAPIServer(b *testing.B, user string, secs []*corev1.Secret) {
	rc, _ := oc1382APIServer(b, nil, secs...)
	names := credsecret.SessionNames(user)
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = FromAPIServerOwnSecret(ctx, rc, oc1382NS, user, names)
	}
}

// Informer, nothing for the user: one map lookup vs two.
func BenchmarkIssue1382_InformerMiss_OneName(b *testing.B) {
	benchInformer(b, "alice", []*corev1.Secret{oc1382Secret(b, "other-clientconfig", "t", "other", "")})
}

func BenchmarkIssue1382_InformerMiss_ExactAndLegacy(b *testing.B) {
	benchInformer(b, oc1382X, []*corev1.Secret{oc1382Secret(b, "other-clientconfig", "t", "other", "")})
}

// Informer, the user's legacy Secret only (the migration's steady state for a
// dotted username): the exact lookup misses, the legacy one serves.
func BenchmarkIssue1382_InformerHit_OneName(b *testing.B) {
	benchInformer(b, "alice", []*corev1.Secret{oc1382Secret(b, "alice-clientconfig", "t", "alice", "")})
}

func BenchmarkIssue1382_InformerHit_LegacyAfterExactMiss(b *testing.B) {
	benchInformer(b, oc1382X, []*corev1.Secret{oc1382Secret(b, "johndoe-clientconfig", "t", oc1382X, "")})
}

// Informer, a legacy Secret proven by its certificate (no annotation): the
// x509 parse OwnsSecret adds.
func BenchmarkIssue1382_InformerHit_LegacyCertOnly(b *testing.B) {
	benchInformer(b, oc1382X, []*corev1.Secret{oc1382Secret(b, "johndoe-clientconfig", "t", "", oc1382X)})
}

// APIServer, nothing for the user: one 404 vs two.
func BenchmarkIssue1382_APIServerMiss_OneName(b *testing.B) {
	benchAPIServer(b, "alice", nil)
}

func BenchmarkIssue1382_APIServerMiss_ExactAndLegacy(b *testing.B) {
	benchAPIServer(b, oc1382X, nil)
}

// APIServer, the user's Secret found: on the first GET vs after the exact
// name's 404.
func BenchmarkIssue1382_APIServerHit_OneName(b *testing.B) {
	benchAPIServer(b, "alice", []*corev1.Secret{oc1382Secret(b, "alice-clientconfig", "t", "alice", "")})
}

func BenchmarkIssue1382_APIServerHit_LegacyAfterExactMiss(b *testing.B) {
	benchAPIServer(b, oc1382X, []*corev1.Secret{oc1382Secret(b, "johndoe-clientconfig", "t", oc1382X, "")})
}
