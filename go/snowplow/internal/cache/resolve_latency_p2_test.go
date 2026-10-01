package cache

// resolve_latency_p2_test.go — #386 M1, PM gate condition 3: the P² p95
// estimator's ACCURACY is itself falsified, not just its sampling site. A p95
// that feeds the #384 build/no-build decision must be demonstrably within
// tolerance of the true p95 on a known distribution — otherwise a wrong estimate
// could flip the decision.

import (
	"math"
	"math/rand"
	"sort"
	"testing"
)

// truePercentile is the exact p-quantile of xs (nearest-rank on a sorted copy).
func truePercentile(xs []float64, p float64) float64 {
	s := append([]float64(nil), xs...)
	sort.Float64s(s)
	if len(s) == 0 {
		return 0
	}
	rank := int(math.Ceil(p*float64(len(s)))) - 1
	if rank < 0 {
		rank = 0
	}
	if rank >= len(s) {
		rank = len(s) - 1
	}
	return s[rank]
}

func TestIssue386_P2Quantile_AccuracyAgainstKnownDistribution(t *testing.T) {
	// Deterministic: a fixed seed so the falsifier is reproducible.
	rng := rand.New(rand.NewSource(0x386))

	cases := []struct {
		name string
		draw func() float64
		// tol is the allowed RELATIVE error vs the true p95. P² converges to
		// within a couple percent on smooth, heavy-tailed distributions; the
		// refresher's resolve latency is exactly that shape (a mass of fast
		// resolves with a decode-bound tail).
		tol float64
	}{
		{
			name: "exponential-ms (resolve-latency-like heavy tail)",
			// mean ~8ms, tail to tens of ms — the <=31ms p95 shape the issue cites.
			draw: func() float64 { return rng.ExpFloat64() * 8.0 },
			tol:  0.05,
		},
		{
			name: "uniform-0-100ms",
			draw: func() float64 { return rng.Float64() * 100.0 },
			tol:  0.03,
		},
		{
			name: "lognormal-ms",
			draw: func() float64 { return math.Exp(rng.NormFloat64()*0.6) * 6.0 },
			tol:  0.08,
		},
	}

	const N = 100000
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			est := newP2Quantile(0.95)
			xs := make([]float64, 0, N)
			for i := 0; i < N; i++ {
				x := tc.draw()
				xs = append(xs, x)
				est.Observe(x)
			}
			got := est.Value()
			want := truePercentile(xs, 0.95)
			relErr := math.Abs(got-want) / want
			if relErr > tc.tol {
				t.Fatalf("#386 M1: P2 p95 = %.4f, true p95 = %.4f, rel err %.4f > tol %.4f "+
					"— the estimator feeding the #384 decision is inaccurate on %s",
					got, want, relErr, tc.tol, tc.name)
			}
			t.Logf("#386 M1 %s: P2 p95 = %.4f, true p95 = %.4f, rel err %.4f (tol %.4f)",
				tc.name, got, want, relErr, tc.tol)
		})
	}
}

// TestIssue386_P2Quantile_SmallSample guards the <5-observation best-effort path
// (Value returns the max seen) so a just-booted refresher never divides by zero
// or returns a garbage marker before the estimator is seeded.
func TestIssue386_P2Quantile_SmallSample(t *testing.T) {
	est := newP2Quantile(0.95)
	if v := est.Value(); v != 0 {
		t.Fatalf("#386 M1: empty estimator Value = %v, want 0", v)
	}
	for _, x := range []float64{3, 1, 4, 1} { // 4 observations (<5)
		est.Observe(x)
	}
	if v := est.Value(); v != 4 {
		t.Fatalf("#386 M1: <5-sample Value = %v, want max seen (4)", v)
	}
}
