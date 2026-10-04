package metrics

// metrics_448_security_otlp_test.go — #448: the security/verification counters
// (#424 drift declines + binding-set memo, #262 learned classes) leave the
// process on OTLP, so a post-roll check reads ClickStack instead of the
// JWT-gated /debug/vars.
//
// The value arm is folded into TestC7_OTLP_EveryDerivedStatLeavesTheProcess
// (c7Seed448 / c7Assert448): every new series is driven to a DISTINCT value
// through the production recorders, one real export is made through the
// OTLP/HTTP exporter, and each series must arrive with its value. The arms in
// this file pin what the value arm cannot:
//   - CFG-1: under cache-off none of the #448 series exist (absent, not zero);
//   - F8 attribute hygiene: every attribute key/value is from a closed set;
//   - the closed sets equal what the producing source actually writes;
//   - the clientconfig alarm (from_secrets==0 while clientconfig_secrets>0) is
//     readable as two plain gauges in one scrape.

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/base64"
	"encoding/pem"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"math/big"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/krateo-platformops/snowplow/internal/cache"
	"github.com/krateo-platformops/snowplow/internal/handlers/dispatchers"
	"github.com/krateo-platformops/snowplow/internal/rbac"

	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// security448Scalars are the #448 instruments with no attributes.
var security448Scalars = []string{
	"snowplow_binding_set_memo_hits",
	"snowplow_binding_set_memo_misses",
	"snowplow_binding_set_memo_refused",
	"snowplow_binding_set_memo_entries",
	"snowplow_learned_classes_registered",
	"snowplow_learned_classes_seeded",
	"snowplow_learned_classes_unseeded_capacity",
	"snowplow_learned_classes_nav_only",
	"snowplow_learned_classes_from_secrets",
	"snowplow_learned_classes_secrets_unparseable",
	"snowplow_learned_clientconfig_secrets",
}

// security448Labelled are the #448 instruments keyed by closed attribute sets.
var security448Labelled = []string{
	"snowplow_l1_identity_class_drift_declined_total",
	"snowplow_l1_representative_repick_total",
	"snowplow_learned_classes_capacity",
	"snowplow_learned_classes_capacity_bound",
}

// c7Want448 is the seeded expectation: series id (name or name{k=v,...}) -> value.
type c7Want448 map[string]int64

func seriesID(name string, attrs map[string]string) string {
	if len(attrs) == 0 {
		return name
	}
	var kv []string
	for k, v := range attrs {
		kv = append(kv, k+"="+v)
	}
	sort.Strings(kv)
	return name + "{" + strings.Join(kv, ",") + "}"
}

func c448Cert(t *testing.T, cn string, orgs []string) []byte {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	nb := time.Now().Add(-10 * time.Minute).Truncate(time.Second)
	tmpl := &x509.Certificate{SerialNumber: big.NewInt(nb.UnixNano()), Subject: pkix.Name{CommonName: cn, Organization: orgs},
		NotBefore: nb, NotAfter: nb.Add(2 * time.Hour)}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
}

// c7Seed448 drives every #448 series to a distinct value through the
// production recorders and returns the expectation. Cleanups restore state.
func c7Seed448(t *testing.T) c7Want448 {
	t.Helper()
	want := c7Want448{}

	// --- #424 drift declines: every {site, reason} pair a distinct count. The
	// counters are process-global and other arms may have ticked them, so the
	// expectation is baseline + delta read through the same closed product.
	base := map[string]int64{}
	for _, c := range dispatchers.IdentityClassDriftDeclinedCells() {
		base[c.Site+"/"+c.Reason] = c.Count
	}
	n := 0
	for _, site := range dispatchers.IdentityClassDriftSites {
		for _, reason := range dispatchers.IdentityClassDriftReasons {
			n++
			for i := 0; i < n; i++ {
				dispatchers.NoteIdentityClassDriftForTest(site, reason)
			}
			want[seriesID("snowplow_l1_identity_class_drift_declined_total",
				map[string]string{"site": site, "reason": reason})] = base[site+"/"+reason] + int64(n)
		}
	}

	// --- #444 representative re-pick outcomes: baseline + a distinct delta.
	repBase := map[string]int64{}
	for _, c := range dispatchers.RepresentativeRepickCells() {
		repBase[c.Outcome] = c.Count
	}
	for i, outcome := range dispatchers.RepresentativeRepickOutcomes {
		for j := 0; j <= 20+i; j++ {
			dispatchers.NoteRepresentativeRepickForTest(outcome)
		}
		want[seriesID("snowplow_l1_representative_repick_total", map[string]string{"outcome": outcome})] =
			repBase[outcome] + int64(21+i)
	}

	// --- #424 binding-set memo, driven through the real memo against one
	// snapshot: cap+2 distinct subjects (cap entries, 2 refused, cap+2 misses),
	// then 3 repeats of a cached subject (3 hits).
	rbac.ResetSubjectBindingSetMemoForTest()
	t.Cleanup(rbac.ResetSubjectBindingSetMemoForTest)
	snap := &cache.RBACSnapshot{}
	const memoCap = 4096
	for i := 0; i < memoCap+2; i++ {
		rbac.SubjectBindingSetDigestForSnapshotForTest(snap, fmt.Sprintf("m448-u%d", i), nil)
	}
	for i := 0; i < 3; i++ {
		rbac.SubjectBindingSetDigestForSnapshotForTest(snap, "m448-u0", nil)
	}
	want["snowplow_binding_set_memo_hits"] = 3
	want["snowplow_binding_set_memo_misses"] = memoCap + 2
	want["snowplow_binding_set_memo_refused"] = 2
	want["snowplow_binding_set_memo_entries"] = memoCap

	// --- #262 learned classes: one cert-backed clientconfig Secret (a class
	// from a secret), two certless ones (unparseable), three live-only classes.
	cache.ResetLearnedIdentitiesForTest()
	t.Cleanup(cache.ResetLearnedIdentitiesForTest)
	cache.SyncLearnedFromSecretsForTest([]*corev1.Secret{
		{ObjectMeta: metav1.ObjectMeta{Name: "m448-a-clientconfig", ResourceVersion: "1"},
			Data: map[string][]byte{"client-certificate-data": []byte(base64.StdEncoding.EncodeToString(c448Cert(t, "m448-a", []string{"devs"})))}},
		{ObjectMeta: metav1.ObjectMeta{Name: "m448-b-clientconfig", ResourceVersion: "1"},
			Data: map[string][]byte{"bearer": []byte("opaque")}},
		{ObjectMeta: metav1.ObjectMeta{Name: "m448-c-clientconfig", ResourceVersion: "1"},
			Data: map[string][]byte{"bearer": []byte("opaque")}},
	})
	exp := time.Now().Add(time.Hour)
	for _, u := range []string{"m448-x", "m448-y", "m448-z"} {
		cache.ObserveLiveIdentity(u, []string{"devs"}, exp)
	}
	capacity := map[string]any{"bound": "memory"}
	for i, stat := range learnedCapacityStats {
		switch stat {
		case "t_widget_measured":
			capacity[stat] = true
			want[seriesID("snowplow_learned_classes_capacity", map[string]string{"stat": stat})] = 1
		case "t_ra_measured":
			capacity[stat] = false
			want[seriesID("snowplow_learned_classes_capacity", map[string]string{"stat": stat})] = 0
		default:
			v := int64(9000 + i)
			if i%2 == 0 {
				capacity[stat] = v // int64 (durations, headrooms)
			} else {
				capacity[stat] = int(v) // int (unit and class counts)
			}
			want[seriesID("snowplow_learned_classes_capacity", map[string]string{"stat": stat})] = v
		}
	}
	cache.SetLearnedAdmissionStats(5, 6, capacity)
	cache.SetLearnedNavOnly(7)
	for _, b := range learnedBounds {
		v := int64(0)
		if b == "memory" {
			v = 1
		}
		want[seriesID("snowplow_learned_classes_capacity_bound", map[string]string{"bound": b})] = v
	}
	want["snowplow_learned_classes_registered"] = 4
	want["snowplow_learned_classes_from_secrets"] = 1
	want["snowplow_learned_classes_secrets_unparseable"] = 2
	want["snowplow_learned_clientconfig_secrets"] = 3
	want["snowplow_learned_classes_seeded"] = 5
	want["snowplow_learned_classes_unseeded_capacity"] = 6
	want["snowplow_learned_classes_nav_only"] = 7
	return want
}

// c7Wire448 indexes every exported #448 data point by series id (latest wins).
func c7Wire448(exports []capturedExport) map[string]int64 {
	names := map[string]bool{}
	for _, n := range append(append([]string{}, security448Scalars...), security448Labelled...) {
		names[n] = true
	}
	out := map[string]int64{}
	for _, e := range exports {
		for _, rm := range e.req.GetResourceMetrics() {
			for _, sm := range rm.GetScopeMetrics() {
				for _, mt := range sm.GetMetrics() {
					if !names[mt.GetName()] {
						continue
					}
					var dps []*metricspb.NumberDataPoint
					switch d := mt.GetData().(type) {
					case *metricspb.Metric_Sum:
						dps = d.Sum.GetDataPoints()
					case *metricspb.Metric_Gauge:
						dps = d.Gauge.GetDataPoints()
					}
					for _, dp := range dps {
						attrs := map[string]string{}
						for _, kv := range dp.GetAttributes() {
							attrs[kv.GetKey()] = kv.GetValue().GetStringValue()
						}
						out[seriesID(mt.GetName(), attrs)] = dp.GetAsInt()
					}
				}
			}
		}
	}
	return out
}

// c7Assert448 requires every seeded #448 series on the wire with its value,
// and no #448 series the seed did not predict (a stray attribute value).
func c7Assert448(t *testing.T, exports []capturedExport, want c7Want448) {
	t.Helper()
	got := c7Wire448(exports)
	for id, w := range want {
		g, ok := got[id]
		if !ok {
			t.Errorf("#448: %s never left the process", id)
			continue
		}
		if g != w {
			t.Errorf("#448: %s = %d on the wire, want %d (the mirror observed the wrong counter)", id, g, w)
		}
	}
	for id := range got {
		if _, ok := want[id]; !ok {
			t.Errorf("#448: unexpected series %s (attribute set not closed)", id)
		}
	}
	if len(want) < len(security448Scalars)+12+3+len(learnedCapacityStats)+len(learnedBounds) {
		t.Fatalf("#448 non-exercise guard: only %d series seeded", len(want))
	}
}

// TestIssue448_CacheOff_SecuritySeriesAbsent — CFG-1: the #448 instruments are
// cache surfaces; under cache-off they must not be registered at all.
func TestIssue448_CacheOff_SecuritySeriesAbsent(t *testing.T) {
	for _, v := range []string{"false", "", "0"} {
		t.Run("CACHE_ENABLED="+v, func(t *testing.T) {
			t.Setenv("CACHE_ENABLED", v)
			all := flatten(collectViaRealCallback(t, "deadbeef"))
			for _, name := range append(append([]string{}, security448Scalars...), security448Labelled...) {
				if pts := pointsFor(all, name); len(pts) != 0 {
					t.Errorf("CFG-1: %s exported %d points under cache-off; the series must be absent", name, len(pts))
				}
			}
			if len(all) == 0 {
				t.Fatalf("non-exercise guard: nothing collected at all under cache-off")
			}
		})
	}
	t.Run("CACHE_ENABLED=true", func(t *testing.T) {
		t.Setenv("CACHE_ENABLED", "true")
		all := flatten(collectViaRealCallback(t, "deadbeef"))
		for _, name := range append(append([]string{}, security448Scalars...), "snowplow_l1_identity_class_drift_declined_total") {
			if len(pointsFor(all, name)) == 0 {
				t.Errorf("cache-on control: %s not registered", name)
			}
		}
	})
}

// TestIssue448_AttributeHygiene_ClosedSetsNoIdentity — F8: every attribute on a
// #448 series is a code-defined closed set, and no username, group or Secret
// name driven into the recorders reaches an attribute value.
func TestIssue448_AttributeHygiene_ClosedSetsNoIdentity(t *testing.T) {
	t.Setenv("CACHE_ENABLED", "true")
	c7Seed448(t)
	allowed := map[string]map[string]bool{
		"site":    setOf(dispatchers.IdentityClassDriftSites),
		"reason":  setOf(dispatchers.IdentityClassDriftReasons),
		"outcome": setOf(dispatchers.RepresentativeRepickOutcomes),
		"stat":    setOf(learnedCapacityStats),
		"bound":   setOf(learnedBounds),
	}
	all := flatten(collectViaRealCallback(t, "deadbeef"))
	seen := 0
	for _, p := range all {
		if !strings.HasPrefix(p.metric, "snowplow_l1_identity_class_drift") &&
			!strings.HasPrefix(p.metric, "snowplow_l1_representative_repick") &&
			!strings.HasPrefix(p.metric, "snowplow_binding_set_memo") &&
			!strings.HasPrefix(p.metric, "snowplow_learned_") {
			continue
		}
		seen++
		for k, v := range p.attrs {
			if !allowed[k][v] {
				t.Errorf("F8: %s carries attribute %s=%q outside the closed sets", p.metric, k, v)
			}
			if strings.Contains(v, "m448") || strings.Contains(v, "devs") || strings.Contains(v, "clientconfig") {
				t.Errorf("F8: %s attribute %s=%q carries identity or Secret material", p.metric, k, v)
			}
		}
	}
	if seen < len(security448Scalars) {
		t.Fatalf("non-exercise guard: saw %d #448 points", seen)
	}
}

func setOf(xs []string) map[string]bool {
	m := map[string]bool{}
	for _, x := range xs {
		m[x] = true
	}
	return m
}

// TestIssue448_ClientconfigAlarmIsReadable — the #262 format-drift alarm
// (from_secrets == 0 while clientconfig_secrets > 0) is expressible on the
// OTLP surface: a certless clientconfig Secret puts exactly that pair on the
// wire, and a cert-backed one clears it.
func TestIssue448_ClientconfigAlarmIsReadable(t *testing.T) {
	t.Setenv("CACHE_ENABLED", "true")
	cache.ResetLearnedIdentitiesForTest()
	t.Cleanup(cache.ResetLearnedIdentitiesForTest)
	read := func() (from, total int64) {
		all := flatten(collectViaRealCallback(t, "deadbeef"))
		f := pointsFor(all, "snowplow_learned_classes_from_secrets")
		c := pointsFor(all, "snowplow_learned_clientconfig_secrets")
		if len(f) != 1 || len(c) != 1 {
			t.Fatalf("alarm pair not on OTLP: from_secrets=%d points clientconfig_secrets=%d points", len(f), len(c))
		}
		return f[0].value, c[0].value
	}
	certless := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "m448-d-clientconfig", ResourceVersion: "1"},
		Data: map[string][]byte{"bearer": []byte("opaque")}}
	cache.SyncLearnedFromSecretsForTest([]*corev1.Secret{certless})
	if from, total := read(); !(from == 0 && total > 0) {
		t.Fatalf("certless clientconfig must trip the alarm on OTLP: from_secrets=%d clientconfig_secrets=%d", from, total)
	}
	cache.SyncLearnedFromSecretsForTest([]*corev1.Secret{certless, {
		ObjectMeta: metav1.ObjectMeta{Name: "m448-e-clientconfig", ResourceVersion: "1"},
		Data:       map[string][]byte{"client-certificate-data": []byte(base64.StdEncoding.EncodeToString(c448Cert(t, "m448-e", nil)))},
	}})
	if from, total := read(); from != 1 || total != 2 {
		t.Fatalf("cert-backed clientconfig must clear the alarm: from_secrets=%d clientconfig_secrets=%d, want 1 and 2", from, total)
	}
}

// TestIssue448_LearnedCapacityClosedSetsMatchTheProducer — the closed
// learnedCapacityStats / learnedBounds sets equal the keys and bound values
// the dispatchers bound decision actually writes, so a new input cannot be
// dropped from OTLP silently.
func TestIssue448_LearnedCapacityClosedSetsMatchTheProducer(t *testing.T) {
	fset := token.NewFileSet()
	path := filepath.Join("..", "handlers", "dispatchers", "learned_identity_seed.go")
	f, err := parser.ParseFile(fset, path, nil, 0)
	if err != nil {
		t.Fatalf("parse %s: %v", path, err)
	}
	keys := map[string]bool{}
	bounds := map[string]bool{}
	strLit := func(e ast.Expr) (string, bool) {
		lit, ok := e.(*ast.BasicLit)
		if !ok || lit.Kind != token.STRING {
			return "", false
		}
		v, err := strconv.Unquote(lit.Value)
		return v, err == nil
	}
	ast.Inspect(f, func(n ast.Node) bool {
		switch x := n.(type) {
		case *ast.CallExpr:
			sel, ok := x.Fun.(*ast.SelectorExpr)
			if !ok || sel.Sel.Name != "SetLearnedAdmissionStats" || len(x.Args) != 3 {
				return true
			}
			cl, ok := x.Args[2].(*ast.CompositeLit)
			if !ok {
				t.Errorf("SetLearnedAdmissionStats capacity must be a map literal")
				return true
			}
			for _, el := range cl.Elts {
				if kv, ok := el.(*ast.KeyValueExpr); ok {
					if k, ok := strLit(kv.Key); ok {
						keys[k] = true
					}
				}
			}
		case *ast.AssignStmt:
			// d.bound = "x"  /  n, d.bound = memFit, "x"
			for i, lhs := range x.Lhs {
				if sel, ok := lhs.(*ast.SelectorExpr); ok && sel.Sel.Name == "bound" && i < len(x.Rhs) {
					if v, ok := strLit(x.Rhs[i]); ok {
						bounds[v] = true
					}
				}
			}
		case *ast.KeyValueExpr:
			if id, ok := x.Key.(*ast.Ident); ok && id.Name == "bound" {
				if v, ok := strLit(x.Value); ok {
					bounds[v] = true
				}
			}
		}
		return true
	})
	wantKeys := setOf(append(append([]string{}, learnedCapacityStats...), "bound"))
	if fmt.Sprint(sortedKeys(keys)) != fmt.Sprint(sortedKeys(wantKeys)) {
		t.Errorf("#448: capacity keys written by the producer %v != closed set %v", sortedKeys(keys), sortedKeys(wantKeys))
	}
	if fmt.Sprint(sortedKeys(bounds)) != fmt.Sprint(sortedKeys(setOf(learnedBounds))) {
		t.Errorf("#448: bound values written by the producer %v != closed set %v", sortedKeys(bounds), learnedBounds)
	}
}

func sortedKeys(m map[string]bool) []string {
	var out []string
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}
