// secrets_transform_falsifier_510_test.go — #510 arms.
//
//	TestS510_InformerStoreHoldsNoUnreadSecretData — the production
//	    StartSecretsInformer over a fake clientset: a sentinel value parked in
//	    data keys nobody reads is ABSENT from the object taken out of the
//	    informer store (and out of the published snapshot), while every field
//	    the three traced consumers read is PRESENT and byte-identical.
//	TestS510_TransformRunsOnUpdates — the same, re-asserted after a real
//	    UPDATE delivered through the reflector's WATCH (the transform runs on
//	    every object, not just the initial LIST).
//	TestS510_TransformIsTotalAndDefensive — a non-Secret, a typed nil, a
//	    DeletedFinalStateUnknown tombstone (around a Secret and around a
//	    non-Secret) and double application: total, never an error, idempotent.
//
// The first two arms drive the REAL boundary — informers.WithTransform →
// reflector → FIFO → indexer — and never write into the indexer by hand, which
// would install the end state instead of detecting it.
package cache

import (
	"context"
	"encoding/base64"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
	clientcache "k8s.io/client-go/tools/cache"
)

const (
	s510NS       = "krateo-system"
	s510Name     = "alice-clientconfig"
	s510Sentinel = "S510-SENTINEL-MUST-NOT-BE-RESIDENT"
	s510CN       = "alice@example.com"
)

// s510UnreadDataKeys are plausible Secret data keys that NO consumer of the
// AUTHN_NAMESPACE store reads (see secrets_transform.go for the traced consumer
// set). Each one is seeded with a sentinel-bearing value.
var s510UnreadDataKeys = []string{"kubeconfig", ".dockerconfigjson", "ca.crt", "refresh-token"}

// s510ReadDataKeys are the data keys the consumers DO read, with the exact
// values the arm asserts survive. "client-certificate-data" is filled in by
// s510Secret with real x509 material (the #262 S1 consumer parses it).
var s510ReadDataKeys = map[string]string{
	"client-key-data":            "dW51c2VkLWtleQ==",
	"certificate-authority-data": "dW51c2VkLWNh",
	"server-url":                 "https://alice.example/k8s",
	"proxy-url":                  "http://proxy.example:3128",
	"token":                      "alice-bearer-token",
	"username":                   "alice",
	"password":                   "alice-password",
	"debug":                      "true",
	"insecure":                   "false",
	"aws-access-key":             "AKIA-alice",
	"aws-secret-key":             "alice-aws-secret",
	"aws-region":                 "eu-west-4",
	"aws-service":                "eks",
}

// s510Secret builds a `<user>-clientconfig` Secret shaped like authn's
// (base64(PEM) x509 under client-certificate-data) that additionally carries
// `sentinel` in every unread data key, a managedFields entry, and the
// kubectl last-applied annotation (which holds a verbatim copy of `data`).
func s510Secret(t *testing.T, rv, serverURL, sentinel string) *corev1.Secret {
	t.Helper()
	data := map[string][]byte{
		"client-certificate-data": []byte(base64.StdEncoding.EncodeToString(
			liCert(t, s510CN, []string{"devs", "ops"}, time.Now().Add(-time.Hour).Truncate(time.Second)))),
	}
	for k, v := range s510ReadDataKeys {
		data[k] = []byte(v)
	}
	data["server-url"] = []byte(serverURL)
	for _, k := range s510UnreadDataKeys {
		data[k] = []byte(sentinel + "/" + k)
	}
	return &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:       s510NS,
			Name:            s510Name,
			ResourceVersion: rv,
			Annotations: map[string]string{
				"krateo.io/keep-me": "annotations are not secret material",
				lastAppliedConfigAnnotation: `{"kind":"Secret","data":{"token":"` +
					sentinel + `"}}`,
			},
			ManagedFields: []metav1.ManagedFieldsEntry{{Manager: "authn", Operation: metav1.ManagedFieldsOperationApply}},
		},
		Type: corev1.SecretTypeOpaque,
		Data: data,
	}
}

// s510AssertReduced checks both directions on one stored object: the sentinel
// is gone, and everything the consumers read is intact.
func s510AssertReduced(t *testing.T, where string, sec *corev1.Secret, wantRV, wantServerURL, sentinel string) {
	t.Helper()
	if sec == nil {
		t.Fatalf("%s: no Secret", where)
	}

	// ── direction 1: nothing unread is resident ───────────────────────────
	for _, k := range s510UnreadDataKeys {
		if v, has := sec.Data[k]; has {
			t.Errorf("%s: data[%q] is resident in the informer cache (%q) — "+
				"#510: an unread Secret key must not survive the transform", where, k, string(v))
		}
	}
	for k, v := range sec.Data {
		if strings.Contains(string(v), sentinel) {
			t.Errorf("%s: data[%q] still carries the #510 sentinel", where, k)
		}
	}
	if got := sec.Annotations[lastAppliedConfigAnnotation]; got != "" {
		t.Errorf("%s: the kubectl last-applied annotation is resident (%q) — it holds a verbatim copy of data", where, got)
	}
	if len(sec.ManagedFields) != 0 {
		t.Errorf("%s: managedFields still resident (%d entries)", where, len(sec.ManagedFields))
	}
	if sec.StringData != nil {
		t.Errorf("%s: StringData still resident (%v)", where, sec.StringData)
	}

	// ── direction 2: everything a consumer reads is still there ───────────
	if sec.Name != s510Name || sec.Namespace != s510NS || sec.ResourceVersion != wantRV {
		t.Errorf("%s: metadata = %s/%s@%s; want %s/%s@%s — rebuildSecretsSnapshot keys on Name and "+
			"syncLearnedFromSecrets diffs on ResourceVersion", where,
			sec.Namespace, sec.Name, sec.ResourceVersion, s510NS, s510Name, wantRV)
	}
	for k, want := range s510ReadDataKeys {
		if k == "server-url" {
			want = wantServerURL
		}
		if got, has := sec.Data[k]; !has || string(got) != want {
			t.Errorf("%s: data[%q] = %q (present=%v); want %q — a consumer reads this key",
				where, k, string(got), has, want)
		}
	}
	if got := string(sec.Data["server-url"]); got != wantServerURL {
		t.Errorf("%s: data[server-url] = %q; want %q", where, got, wantServerURL)
	}
	// The #262 S1 consumer: the x509 material must still parse to CN + O.
	user, groups, _, ok := parseClientconfigCertificate(sec)
	if !ok || user != s510CN || len(groups) != 2 || groups[0] != "devs" || groups[1] != "ops" {
		t.Errorf("%s: parseClientconfigCertificate = (%q, %v, ok=%v); want (%q, [devs ops], ok=true) — "+
			"the clientconfig x509 key must be RETAINED", where, user, groups, ok, s510CN)
	}
	if sec.Annotations["krateo.io/keep-me"] != "annotations are not secret material" {
		t.Errorf("%s: a non-secret annotation was dropped: %v", where, sec.Annotations)
	}
	if sec.Type != corev1.SecretTypeOpaque {
		t.Errorf("%s: Type = %q; want %q", where, sec.Type, corev1.SecretTypeOpaque)
	}
}

// s510FromStore reads the object out of the informer's OWN indexer — the store
// #510 is about — rather than out of the snapshot.
func s510FromStore(t *testing.T, where string) *corev1.Secret {
	t.Helper()
	ind := secretsInformerIndexer()
	if ind == nil {
		t.Fatalf("%s: no informer indexer", where)
	}
	obj, ok, err := ind.GetByKey(s510NS + "/" + s510Name)
	if err != nil || !ok {
		t.Fatalf("%s: indexer.GetByKey(%s/%s) ok=%v err=%v", where, s510NS, s510Name, ok, err)
	}
	sec, ok := obj.(*corev1.Secret)
	if !ok {
		t.Fatalf("%s: indexer entry is %T, not *corev1.Secret", where, obj)
	}
	return sec
}

// s510Start wires the production informer over a fake clientset seeded with
// `seed`, with `fw` as the WATCH so the arm can deliver a deterministic UPDATE.
func s510Start(t *testing.T, seed *corev1.Secret, fw *watch.FakeWatcher) {
	t.Helper()
	t.Setenv("CACHE_ENABLED", "true")
	resetSecretsForTest(t)
	ResetLearnedIdentitiesForTest()
	t.Cleanup(ResetLearnedIdentitiesForTest)

	cli := fake.NewSimpleClientset(seed)
	if fw != nil {
		cli.PrependWatchReactor("secrets", k8stesting.DefaultWatchReactor(fw, nil))
	}
	t.Cleanup(SetSecretsClientForTest(cli))

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	t.Cleanup(cancel)
	if err := StartSecretsInformer(ctx, nil, s510NS); err != nil {
		t.Fatalf("StartSecretsInformer: %v", err)
	}
	t.Cleanup(func() { resetSecretsForTest(t) })
	if !SecretsCacheServable() {
		t.Fatalf("SecretsCacheServable=false post-Start")
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// #510 — the store must not hold a data key no consumer reads
// ─────────────────────────────────────────────────────────────────────────────

func TestS510_InformerStoreHoldsNoUnreadSecretData(t *testing.T) {
	const url = "https://alice.example/k8s"
	s510Start(t, s510Secret(t, "1", url, s510Sentinel), nil)

	s510AssertReduced(t, "informer indexer", s510FromStore(t, "informer indexer"), "1", url, s510Sentinel)

	// The snapshot republishes the indexer's pointers, so it must agree.
	snap := SecretsSnapshotLoad()
	if snap == nil {
		t.Fatalf("SecretsSnapshotLoad = nil after Start")
	}
	s510AssertReduced(t, "published snapshot", snap.ByName[s510Name], "1", url, s510Sentinel)

	// The downstream consumer that the retained x509 key exists for: the
	// learned-identity registry must still have read a class off this Secret.
	if _, fromSecrets, unparseable := LearnedClassCounts(); fromSecrets != 1 || unparseable != 0 {
		t.Errorf("learned classes from_secrets=%d unparseable=%d; want 1 and 0 — "+
			"#262 S1 reads client-certificate-data out of this store", fromSecrets, unparseable)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// #510 — the transform runs on every object, including an UPDATE
// ─────────────────────────────────────────────────────────────────────────────

func TestS510_TransformRunsOnUpdates(t *testing.T) {
	const url1 = "https://alice.example/k8s"
	const url2 = "https://alice.example/k8s-moved"
	// Deliberately NOT a superstring of s510Sentinel, so the "the pre-update
	// sentinel is gone too" check below is a real assertion.
	const sentinel2 = "S510-UPDATE-ONLY-SENTINEL-MUST-NOT-BE-RESIDENT"

	fw := watch.NewFakeWithChanSize(1, false)
	t.Cleanup(fw.Stop)
	s510Start(t, s510Secret(t, "1", url1, s510Sentinel), fw)

	// Deliver a real UPDATE through the reflector's WATCH. Not an
	// indexer.Update: the FIFO's transform hook is what we are testing.
	fw.Modify(s510Secret(t, "2", url2, sentinel2))

	deadline := time.Now().Add(15 * time.Second)
	var sec *corev1.Secret
	for time.Now().Before(deadline) {
		ind := secretsInformerIndexer()
		if ind != nil {
			if obj, ok, _ := ind.GetByKey(s510NS + "/" + s510Name); ok {
				if s, ok := obj.(*corev1.Secret); ok && s.ResourceVersion == "2" {
					sec = s
					break
				}
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	if sec == nil {
		t.Fatalf("the UPDATE never reached the informer store (resourceVersion stayed at 1)")
	}
	s510AssertReduced(t, "informer indexer post-UPDATE", sec, "2", url2, sentinel2)

	// And the pre-update sentinel must not have survived either.
	for k, v := range sec.Data {
		if strings.Contains(string(v), s510Sentinel) {
			t.Errorf("post-UPDATE data[%q] carries the pre-update sentinel", k)
		}
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// #510 — totality: the transform sees every object the factory stores
// ─────────────────────────────────────────────────────────────────────────────

func TestS510_TransformIsTotalAndDefensive(t *testing.T) {
	// (a) a non-Secret passes through untouched, without an error. A non-nil
	//     error here would make the FIFO DROP the object.
	cm := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Namespace: s510NS, Name: "cm"}}
	got, err := transformSecretForCache(cm)
	if err != nil {
		t.Errorf("transform(ConfigMap) err = %v; want nil (an error makes the FIFO drop the object)", err)
	}
	if got != interface{}(cm) {
		t.Errorf("transform(ConfigMap) = %#v; want the input object back unchanged", got)
	}

	// (b) nil and a typed nil must not panic.
	if got, err := transformSecretForCache(nil); got != nil || err != nil {
		t.Errorf("transform(nil) = (%v, %v); want (nil, nil)", got, err)
	}
	var nilSec *corev1.Secret
	if got, err := transformSecretForCache(nilSec); err != nil || got != interface{}(nilSec) {
		t.Errorf("transform((*Secret)(nil)) = (%#v, %v); want the input back, no error", got, err)
	}

	// (c) a DeletedFinalStateUnknown tombstone around a Secret: the Key
	//     survives (a DELETE must still resolve) and the body is reduced.
	full := s510Secret(t, "7", "https://alice.example/k8s", s510Sentinel)
	key := s510NS + "/" + s510Name
	out, err := transformSecretForCache(clientcache.DeletedFinalStateUnknown{Key: key, Obj: full})
	if err != nil {
		t.Fatalf("transform(tombstone) err = %v; want nil", err)
	}
	tomb, ok := out.(clientcache.DeletedFinalStateUnknown)
	if !ok {
		t.Fatalf("transform(tombstone) = %T; want a DeletedFinalStateUnknown back", out)
	}
	if tomb.Key != key {
		t.Errorf("tombstone Key = %q; want %q", tomb.Key, key)
	}
	inner, ok := tomb.Obj.(*corev1.Secret)
	if !ok {
		t.Fatalf("tombstone Obj = %T; want *corev1.Secret", tomb.Obj)
	}
	s510AssertReduced(t, "tombstone body", inner, "7", "https://alice.example/k8s", s510Sentinel)
	// The caller's tombstone must not have been mutated in place.
	if len(full.Data) == len(inner.Data) {
		t.Errorf("the input Secret was reduced in place (%d keys) — the transform must copy", len(full.Data))
	}

	// (d) a tombstone around a non-Secret: passthrough, no panic, no error.
	out, err = transformSecretForCache(clientcache.DeletedFinalStateUnknown{Key: "x/y", Obj: cm})
	if err != nil {
		t.Errorf("transform(tombstone around ConfigMap) err = %v; want nil", err)
	}
	if tomb, ok := out.(clientcache.DeletedFinalStateUnknown); !ok || tomb.Obj != interface{}(cm) {
		t.Errorf("transform(tombstone around ConfigMap) = %#v; want the tombstone back unchanged", out)
	}

	// (e) idempotent — client-go requires it for objects re-fed through
	//     Replace(). Applying it twice must change nothing.
	once, err := transformSecretForCache(s510Secret(t, "9", "https://alice.example/k8s", s510Sentinel))
	if err != nil {
		t.Fatalf("transform err = %v", err)
	}
	twice, err := transformSecretForCache(once)
	if err != nil {
		t.Fatalf("transform(transform(x)) err = %v", err)
	}
	a, b := once.(*corev1.Secret), twice.(*corev1.Secret)
	if len(a.Data) != len(b.Data) {
		t.Errorf("not idempotent: %d data keys then %d", len(a.Data), len(b.Data))
	}
	for k, v := range a.Data {
		if string(b.Data[k]) != string(v) {
			t.Errorf("not idempotent: data[%q] changed on the second pass", k)
		}
	}
	s510AssertReduced(t, "double-transformed", b, "9", "https://alice.example/k8s", s510Sentinel)

	// (f) the retained set is the traced union, not a superset that quietly
	//     keeps everything.
	if _, kept := secretsCacheRetainedDataKeys["kubeconfig"]; kept {
		t.Errorf("secretsCacheRetainedDataKeys must not retain keys no consumer reads")
	}
	if _, kept := secretsCacheRetainedDataKeys[clientCertificateDataKey]; !kept {
		t.Errorf("secretsCacheRetainedDataKeys must retain %q — #262 S1 parses it", clientCertificateDataKey)
	}
}
