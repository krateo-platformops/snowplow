package jq

import "testing"

// The quantity module. What is pinned here is the arithmetic a wrong answer
// would hide: a nine-order-of-magnitude case error, a fabricated zero, and a
// verdict that grades the wrong end of the scale as bad.

func TestQuantityCPUSuffixes(t *testing.T) {
	// The forms metrics.k8s.io and the apiserver actually emit, in base units
	// (cores). The nanocore case is the one that arrives from a live sample.
	cases := []struct {
		in   string
		want float64
	}{
		{"1", 1},
		{"0.5", 0.5},
		{"100m", 0.1},
		{"1500m", 1.5},
		{"4817987n", 0.004817987},
		{"250u", 0.00025},
		{"1e3", 1000},
	}
	for _, c := range cases {
		got := eval(t, `include "quantity"; quantity`, c.in)
		if f, ok := got.(float64); !ok || !nearly(f, c.want) {
			t.Errorf("quantity(%q) = %v, want %v", c.in, got, c.want)
		}
	}
}

func TestQuantityMemorySuffixes(t *testing.T) {
	cases := []struct {
		in   string
		want float64
	}{
		{"71192Ki", 72900608},
		{"128Mi", 134217728},
		{"1Gi", 1073741824},
		{"1000000", 1000000},
		{"1k", 1000},
		{"1M", 1000000},
	}
	for _, c := range cases {
		got := eval(t, `include "quantity"; bytes`, c.in)
		if f, ok := got.(float64); !ok || !nearly(f, c.want) {
			t.Errorf("bytes(%q) = %v, want %v", c.in, got, c.want)
		}
	}
}

func TestQuantityCaseIsLoadBearing(t *testing.T) {
	// "100m" is a tenth of a core. "100M" is a hundred million. A parser that
	// lower-cases its suffix reports the second as the first and the number
	// still looks plausible on a dashboard.
	milli := eval(t, `include "quantity"; quantity`, "100m")
	mega := eval(t, `include "quantity"; quantity`, "100M")
	if !nearly(milli.(float64), 0.1) {
		t.Fatalf(`quantity("100m") = %v, want 0.1`, milli)
	}
	if !nearly(mega.(float64), 1e8) {
		t.Fatalf(`quantity("100M") = %v, want 1e8`, mega)
	}
}

func TestQuantityUnparseableIsNullNotZero(t *testing.T) {
	// A fabricated zero is the dangerous answer: it flows into a utilisation
	// ratio as "uses nothing" and labels the row Overprovisioned.
	for _, in := range []any{"", "   ", "abc", "100Qi", "12mm", nil, true, []any{}} {
		if got := eval(t, `include "quantity"; quantity`, in); got != nil {
			t.Errorf("quantity(%#v) = %v, want null", in, got)
		}
	}
}

func TestQuantityIsIdempotent(t *testing.T) {
	// An already-normalised value passes through, so a filter may parse twice
	// without silently rescaling.
	got := eval(t, `include "quantity"; quantity | quantity`, "100m")
	if f, ok := got.(float64); !ok || !nearly(f, 0.1) {
		t.Errorf("quantity|quantity = %v, want 0.1", got)
	}
}

func TestSumQuantities(t *testing.T) {
	// A pod is several containers; the request you compare against is the sum.
	got := eval(t, `include "quantity"; sum_quantities`, []any{"100m", "250m", "1"})
	if f, ok := got.(float64); !ok || !nearly(f, 1.35) {
		t.Errorf("sum_quantities = %v, want 1.35", got)
	}
	// Unparseable entries are skipped rather than poisoning the total...
	got = eval(t, `include "quantity"; sum_quantities`, []any{"100m", "junk"})
	if f, ok := got.(float64); !ok || !nearly(f, 0.1) {
		t.Errorf("sum_quantities with junk = %v, want 0.1", got)
	}
	// ...but nothing usable answers null, not 0.
	if got := eval(t, `include "quantity"; sum_quantities`, []any{"junk"}); got != nil {
		t.Errorf("sum_quantities all-junk = %v, want null", got)
	}
	if got := eval(t, `include "quantity"; sum_quantities`, []any{}); got != nil {
		t.Errorf("sum_quantities empty = %v, want null", got)
	}
}

func TestUtilizationPct(t *testing.T) {
	// The live shape: a nanocore sample against a milli request.
	got := eval(t, `include "quantity"; utilization_pct("4817987n"; "100m")`, nil)
	if f, ok := got.(float64); !ok || !nearly(f, 4.8) {
		t.Errorf("utilization_pct = %v, want 4.8", got)
	}
	// An unknown capacity must not fake a percentage, and a zero request must
	// not divide.
	for _, q := range []string{
		`include "quantity"; utilization_pct("100m"; null)`,
		`include "quantity"; utilization_pct(null; "100m")`,
		`include "quantity"; utilization_pct("100m"; "0")`,
	} {
		if got := eval(t, q, nil); got != nil {
			t.Errorf("%s = %v, want null", q, got)
		}
	}
}

func TestProvisioningVerdict(t *testing.T) {
	cases := []struct {
		pct  string
		want string
	}{
		{"4.8", "Overprovisioned"},
		{"25", "Overprovisioned"},
		{"50", "Right-sized"},
		{"79.9", "Right-sized"},
		{"80", "Underprovisioned"},
		{"140", "Underprovisioned"},
		{"null", "Unknown"},
	}
	for _, c := range cases {
		got := eval(t, `include "quantity"; provisioning_verdict(`+c.pct+`)`, nil)
		if got != c.want {
			t.Errorf("provisioning_verdict(%s) = %v, want %q", c.pct, got, c.want)
		}
	}
}

func TestProvisioningSeverityRoundTrips(t *testing.T) {
	// Underprovisioned outranks Overprovisioned: one throttles a workload now,
	// the other spends money. Unknown must not sort as though it were fine.
	want := map[string]float64{
		"Underprovisioned": 3, "Overprovisioned": 2, "Unknown": 1, "Right-sized": 0,
	}
	for verdict, rank := range want {
		got := eval(t, `include "quantity"; provisioning_severity`, verdict)
		if f, ok := got.(float64); !ok || f != rank {
			t.Errorf("provisioning_severity(%q) = %v, want %v", verdict, got, rank)
		}
	}
	// An unrecognised verdict degrades to Unknown rather than to Right-sized.
	if got := eval(t, `include "quantity"; provisioning_severity`, "nonsense"); got != float64(1) {
		t.Errorf("provisioning_severity(nonsense) = %v, want 1", got)
	}
}

func nearly(a, b float64) bool {
	d := a - b
	if d < 0 {
		d = -d
	}
	scale := 1.0
	if b < 0 {
		scale = -b
	} else if b > 0 {
		scale = b
	}
	return d <= 1e-9*scale || d <= 1e-9
}
