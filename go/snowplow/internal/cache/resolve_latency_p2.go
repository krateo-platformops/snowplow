package cache

// resolve_latency_p2.go — #386 M1: a dependency-light streaming p95 estimator
// for the refresher's REAL resolve latency (the registered re-resolve+Put
// handler `fn()` at refresher.go:1021), sampled ONLY on the ok=true path AFTER
// yieldToCustomer (refresher.go:756) returns — so it excludes the dequeue, the
// customer-priority park, the skipped_no_entry / skipped_no_handler skips, and
// the rate-floor defer. It yields the true resolve-work p95, not the
// idle-inclusive dequeue-to-completion average.
//
// It is the single-quantile P² algorithm (Jain & Chlamtac, 1985): O(1) time and
// O(1) space per observation, no stored samples and no bucket configuration —
// the scalars-only alternative to a Prometheus histogram. M1 is a DIAGNOSTIC
// sizing input for the #384 build/no-build decision (and the #365 model), not a
// detector; there is no expvar->OTLP bridge, so it stays an expvar scalar. Its
// accuracy is falsified against a known distribution in resolve_latency_p2_test.go
// (PM gate condition: a p95 that feeds a build decision must itself be falsified,
// not just its sampling site).

import "sort"

// p2Quantile is a single-quantile P² estimator. NOT safe for concurrent use —
// the caller serializes Observe/Value under its own lock (the refresher wraps it
// in resolveLatency, below).
type p2Quantile struct {
	p     float64    // target quantile, e.g. 0.95
	q     [5]float64 // marker heights (the running estimates)
	n     [5]int     // marker positions (1-based, actual)
	np    [5]float64 // marker positions (desired)
	dn    [5]float64 // desired-position increments per observation
	count int        // observations seen
	buf   []float64  // first five observations, sorted when the fifth arrives
}

func newP2Quantile(p float64) *p2Quantile {
	return &p2Quantile{p: p, buf: make([]float64, 0, 5)}
}

// Observe folds one sample into the estimator in O(1).
func (e *p2Quantile) Observe(x float64) {
	if e.count < 5 {
		e.buf = append(e.buf, x)
		e.count++
		if e.count == 5 {
			sort.Float64s(e.buf)
			for i := 0; i < 5; i++ {
				e.q[i] = e.buf[i]
				e.n[i] = i + 1
			}
			p := e.p
			e.np = [5]float64{1, 1 + 2*p, 1 + 4*p, 3 + 2*p, 5}
			e.dn = [5]float64{0, p / 2, p, (1 + p) / 2, 1}
		}
		return
	}
	e.count++

	// Find the cell k the new sample falls into; clamp the extreme markers.
	var k int
	switch {
	case x < e.q[0]:
		e.q[0] = x
		k = 0
	case x < e.q[1]:
		k = 0
	case x < e.q[2]:
		k = 1
	case x < e.q[3]:
		k = 2
	case x <= e.q[4]:
		k = 3
	default:
		e.q[4] = x
		k = 3
	}

	for i := k + 1; i < 5; i++ {
		e.n[i]++
	}
	for i := 0; i < 5; i++ {
		e.np[i] += e.dn[i]
	}

	// Adjust the three interior markers toward their desired positions.
	for i := 1; i < 4; i++ {
		d := e.np[i] - float64(e.n[i])
		if (d >= 1 && e.n[i+1]-e.n[i] > 1) || (d <= -1 && e.n[i-1]-e.n[i] < -1) {
			sign := 1
			if d < 0 {
				sign = -1
			}
			qp := e.parabolic(i, float64(sign))
			if e.q[i-1] < qp && qp < e.q[i+1] {
				e.q[i] = qp
			} else {
				e.q[i] = e.linear(i, sign)
			}
			e.n[i] += sign
		}
	}
}

// parabolic is the P² piecewise-parabolic prediction for marker i moving by d.
func (e *p2Quantile) parabolic(i int, d float64) float64 {
	ni := float64(e.n[i])
	niPrev := float64(e.n[i-1])
	niNext := float64(e.n[i+1])
	return e.q[i] + d/(niNext-niPrev)*
		((ni-niPrev+d)*(e.q[i+1]-e.q[i])/(niNext-ni)+
			(niNext-ni-d)*(e.q[i]-e.q[i-1])/(ni-niPrev))
}

// linear is the P² fallback when the parabolic prediction leaves [q[i-1],q[i+1]].
func (e *p2Quantile) linear(i, d int) float64 {
	return e.q[i] + float64(d)*(e.q[i+d]-e.q[i])/float64(e.n[i+d]-e.n[i])
}

// Value returns the current p-quantile estimate. With fewer than five
// observations it returns the largest seen (best effort for a tiny sample).
func (e *p2Quantile) Value() float64 {
	if e.count == 0 {
		return 0
	}
	if e.count < 5 {
		m := e.buf[0]
		for _, v := range e.buf[1:] {
			if v > m {
				m = v
			}
		}
		return m
	}
	return e.q[2]
}
