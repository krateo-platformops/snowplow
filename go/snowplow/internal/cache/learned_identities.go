// learned_identities.go — #262: the registry of LEARNED identity classes.
//
// THE GAP. The prewarm engine seeds one cell set per binding REPRESENTATIVE
// (prewarm_enumeration.go). Since #423 the identity dimension of a
// resolved-output key is the requester's FULL matching-binding set, so a user
// who holds one extra binding (say a User-subject RoleBinding next to their
// group's) derives a key no representative ever mints. Their first navigation
// after every restart, and after every rotation, is cold.
//
// THE SOURCE. authn already mints, at every login, an x509 client certificate
// from the SAME userinfo it puts in the JWT: CN = username, O = groups
// (verified against the deployed authn 0.27.3, the version installer 0.3.367
// pins: internal/helpers/kube/certs.go NewCertificateRequest, fed by
// config/build.go generateCertAndClusterInfo with the userinfo the login route
// also hands encode.Success). It stores the cert in the `<user>-clientconfig`
// Secret in AUTHN_NAMESPACE, which snowplow already lists and watches
// (secrets_informer.go). So the identity classes are already persisted in the
// cluster; this file only READS them:
//
//   - S1 (survives restarts): every clientconfig Secret's certificate, parsed
//     only when the Secret's ResourceVersion changed. The username is the CN —
//     never the Secret name, which MakeDNS1123Compatible makes lossy.
//   - S2 (in memory): the identities of live customer /call traffic, observed
//     at the dispatcher ServeHTTP entry (never in the key-mint path the seeds
//     also run through). A live-only class expires with the caller's JWT.
//
// A class is (username, the FULL group set). Never a projection: a group that
// is unbound today can gain a binding tomorrow and change the user's key.
//
// PRIVACY (hard rule). The registry holds usernames and groups in memory only
// (nothing new at rest — the Secrets already exist). Nothing here logs a CN, a
// group string or a Secret name (a `-clientconfig` name embeds the username).
// The surfaces are counts (expvar) and LearnedClassLabel, a sha256 tag.

package cache

import (
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/hex"
	"encoding/pem"
	"expvar"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	corev1 "k8s.io/api/core/v1"
)

// clientconfigSecretSuffix / clientCertificateDataKey are authn's storage
// contract (authn internal/helpers/kube/config/storage/storage.go:
// "%s-clientconfig" and ClientCertLabel). The NAME selects a login artifact; a
// clientconfig Secret without a readable certificate is counted unparseable.
const (
	clientconfigSecretSuffix = "-clientconfig"
	clientCertificateDataKey = "client-certificate-data"
)

// LearnedIdentity is one learned class, as the seed engine consumes it.
// Groups is a sorted copy (the FULL set the certificate / JWT carried).
type LearnedIdentity struct {
	Username string
	Groups   []string
	// LastSeen is the newest evidence of the class: the certificate's NotBefore
	// (the login instant, which survives a restart) or the last live /call.
	LastSeen time.Time
}

// Key is the class key (LearnedClassKey).
func (l LearnedIdentity) Key() string { return LearnedClassKey(l.Username, l.Groups) }

type learnedClass struct {
	username   string
	groups     []string // sorted
	secretSeen time.Time
	liveSeen   atomic.Int64 // unix nanos of the last live /call (0 = never)
	liveExp    atomic.Int64 // unix nanos the live evidence expires (JWT exp)
	secrets    map[string]struct{}
}

func (c *learnedClass) lastSeen() time.Time {
	ls := c.secretSeen
	if n := c.liveSeen.Load(); n > 0 {
		if t := time.Unix(0, n); t.After(ls) {
			ls = t
		}
	}
	return ls
}

// liveOnlyExpired reports a class with no Secret behind it whose live evidence
// (the caller's JWT) has expired.
func (c *learnedClass) liveOnlyExpired(now time.Time) bool {
	if len(c.secrets) > 0 {
		return false
	}
	exp := c.liveExp.Load()
	return exp > 0 && now.UnixNano() > exp
}

type secretState struct {
	rv          string
	classKey    string // "" when unparseable or a ServiceAccount
	unparseable bool
}

var learned struct {
	mu      sync.RWMutex
	classes map[string]*learnedClass
	secrets map[string]secretState // Secret name -> last parsed RV + class
	pending map[string]struct{}    // classes new since the last drain
	hooks   []func()
	// everUsers is every username that has been a learned class in this
	// process (append-only, bounded by the user population). The privacy rail
	// keys on it: a cell a learned seed minted keeps that user as its
	// representative after the class is replaced (a re-login) or removed (a
	// Secret delete), and its refreshes must stay redacted.
	everUsers map[string]struct{}

	unparseable atomic.Int64 // clientconfig Secrets whose certificate did not parse
}

// learnedAdmission is the seed engine's last capacity decision (set by the
// dispatchers package; read by the expvars below).
var learnedAdmission struct {
	seeded           atomic.Int64
	unseededCapacity atomic.Int64
	mu               sync.Mutex
	capacity         map[string]any
}

func ensureLearnedLocked() {
	if learned.classes == nil {
		learned.classes = map[string]*learnedClass{}
		learned.secrets = map[string]secretState{}
		learned.pending = map[string]struct{}{}
	}
	if learned.everUsers == nil {
		learned.everUsers = map[string]struct{}{}
	}
}

// WasLearnedUsername reports whether username has been a learned class in this
// process (the privacy rail's predicate; see everUsers).
func WasLearnedUsername(username string) bool {
	if username == "" {
		return false
	}
	learned.mu.RLock()
	defer learned.mu.RUnlock()
	_, ok := learned.everUsers[username]
	return ok
}

// LearnedClassKey is the exact (collision-free) class key: the username and
// the sorted EFFECTIVE group set (WithAuthenticatedGroup — a seed resolves, and
// so records its representative, with system:authenticated added, #424),
// \x00-separated. Never logged.
func LearnedClassKey(username string, groups []string) string {
	g := append([]string(nil), WithAuthenticatedGroup(groups)...)
	sort.Strings(g)
	return username + "\x00" + strings.Join(g, "\x00")
}

// LearnedClassLabel is the only form of a learned class that may reach a log
// line or a debug surface: a sha256 tag of the class key.
func LearnedClassLabel(username string, groups []string) string {
	sum := sha256.Sum256([]byte(LearnedClassKey(username, groups)))
	return "learned:" + hex.EncodeToString(sum[:6])
}

// LearnedIdentitiesSnapshot returns every live class, newest LastSeen first
// (ties by class key, so the order is total and deterministic). A live-only
// class whose JWT expired is dropped here.
func LearnedIdentitiesSnapshot() []LearnedIdentity {
	now := time.Now()
	learned.mu.RLock()
	expired := false
	out := make([]LearnedIdentity, 0, len(learned.classes))
	for _, c := range learned.classes {
		if c.liveOnlyExpired(now) {
			expired = true
			continue
		}
		out = append(out, LearnedIdentity{
			Username: c.username,
			Groups:   append([]string(nil), c.groups...),
			LastSeen: c.lastSeen(),
		})
	}
	learned.mu.RUnlock()
	if expired {
		purgeExpiredLiveOnly(now)
	}
	sort.SliceStable(out, func(i, j int) bool {
		if !out[i].LastSeen.Equal(out[j].LastSeen) {
			return out[i].LastSeen.After(out[j].LastSeen)
		}
		return out[i].Key() < out[j].Key()
	})
	return out
}

func purgeExpiredLiveOnly(now time.Time) {
	learned.mu.Lock()
	defer learned.mu.Unlock()
	for k, c := range learned.classes {
		if c.liveOnlyExpired(now) {
			delete(learned.classes, k)
			delete(learned.pending, k)
		}
	}
}

// RegisterLearnedClassHook adds a callback fired (synchronously, after the
// registry lock is released) whenever a NEW class appears — a login or a
// membership change seen through a clientconfig Secret ADD/UPDATE, or a new
// live identity. The callback must not block (the dispatchers handler only
// enqueues a coalesced, payload-free engine scope); the new classes are read
// with DrainPendingLearnedClasses.
func RegisterLearnedClassHook(fn func()) {
	if fn == nil {
		return
	}
	learned.mu.Lock()
	learned.hooks = append(learned.hooks, fn)
	learned.mu.Unlock()
}

func fireLearnedHooks(hooks []func()) {
	for _, fn := range hooks {
		fn()
	}
}

// DrainPendingLearnedClasses takes and resets the set of classes that appeared
// since the last drain (class keys).
func DrainPendingLearnedClasses() map[string]struct{} {
	learned.mu.Lock()
	defer learned.mu.Unlock()
	ensureLearnedLocked()
	out := learned.pending
	learned.pending = map[string]struct{}{}
	return out
}

// RemergePendingLearnedClasses puts keys back into the pending set (a cut
// class-seed run must not drop a login).
func RemergePendingLearnedClasses(keys map[string]struct{}) {
	if len(keys) == 0 {
		return
	}
	learned.mu.Lock()
	defer learned.mu.Unlock()
	ensureLearnedLocked()
	for k := range keys {
		if _, live := learned.classes[k]; live {
			learned.pending[k] = struct{}{}
		}
	}
}

// parseClientconfigCertificate reads (CN, O, NotBefore) from a clientconfig
// Secret. authn stores the certificate as base64(PEM) through StringData
// (gen.go: base64.StdEncoding.EncodeToString(crt)), so Data holds the base64
// text; a raw PEM value is accepted too.
func parseClientconfigCertificate(sec *corev1.Secret) (username string, groups []string, notBefore time.Time, ok bool) {
	raw, has := sec.Data[clientCertificateDataKey]
	if !has || len(raw) == 0 {
		return "", nil, time.Time{}, false
	}
	pemBytes := raw
	if !strings.HasPrefix(strings.TrimSpace(string(raw)), "-----BEGIN") {
		dec, err := base64.StdEncoding.DecodeString(strings.TrimSpace(string(raw)))
		if err != nil {
			return "", nil, time.Time{}, false
		}
		pemBytes = dec
	}
	block, _ := pem.Decode(pemBytes)
	if block == nil || block.Type != "CERTIFICATE" {
		return "", nil, time.Time{}, false
	}
	cert, err := x509.ParseCertificate(block.Bytes)
	if err != nil || cert.Subject.CommonName == "" {
		return "", nil, time.Time{}, false
	}
	g := append([]string(nil), cert.Subject.Organization...)
	sort.Strings(g)
	return cert.Subject.CommonName, g, cert.NotBefore, true
}

// isClientconfigSecret selects the login artifacts by NAME ONLY. It never
// filters on the certificate key: a clientconfig authn writes in some other
// format (no client-certificate-data) must count as UNPARSEABLE, not be skipped,
// or the format-drift alarm (from_secrets == 0 while clientconfig Secrets
// exist) reads 0/0 — a zero that looks like health.
func isClientconfigSecret(sec *corev1.Secret) bool {
	return sec != nil && strings.HasSuffix(sec.Name, clientconfigSecretSuffix)
}

// detachSecretLocked removes the Secret source from its class, dropping the
// class when nothing else (another Secret or unexpired live evidence) keeps it.
func detachSecretLocked(name string, st secretState, now time.Time) {
	if st.classKey == "" {
		return
	}
	c, ok := learned.classes[st.classKey]
	if !ok {
		return
	}
	delete(c.secrets, name)
	if len(c.secrets) > 0 {
		return
	}
	if exp := c.liveExp.Load(); exp > 0 && now.UnixNano() <= exp {
		return // still live through the caller's JWT
	}
	delete(learned.classes, st.classKey)
	delete(learned.pending, st.classKey)
}

// syncLearnedFromSecrets reconciles S1 with the Secrets informer's current
// item set. Called from rebuildSecretsSnapshot (the single-writer rebuild
// goroutine). A certificate is parsed only when its Secret's ResourceVersion
// changed; a vanished Secret detaches its class.
func syncLearnedFromSecrets(items []*corev1.Secret) {
	now := time.Now()
	learned.mu.Lock()
	ensureLearnedLocked()
	seen := make(map[string]struct{}, len(items))
	added := false
	unparseable := int64(0)
	for _, sec := range items {
		if !isClientconfigSecret(sec) {
			continue
		}
		seen[sec.Name] = struct{}{}
		prev, had := learned.secrets[sec.Name]
		if had && prev.rv == sec.ResourceVersion && sec.ResourceVersion != "" {
			if prev.unparseable {
				unparseable++
			}
			continue
		}
		user, groups, notBefore, ok := parseClientconfigCertificate(sec)
		isSA := false
		if ok {
			// #130 F3: a ServiceAccount never renders the frontend; it is not a
			// seed class (mirrors the SA-only binding exclusion). It is not
			// "unparseable" either.
			_, _, isSA = parseServiceAccountUsername(user)
		}
		if had {
			detachSecretLocked(sec.Name, prev, now)
		}
		if !ok || isSA {
			if !ok {
				unparseable++
			}
			learned.secrets[sec.Name] = secretState{rv: sec.ResourceVersion, unparseable: !ok}
			continue
		}
		key := LearnedClassKey(user, groups)
		c, exists := learned.classes[key]
		if !exists {
			c = &learnedClass{username: user, groups: groups, secrets: map[string]struct{}{}}
			learned.classes[key] = c
			learned.pending[key] = struct{}{}
			learned.everUsers[user] = struct{}{}
			added = true
		}
		c.secrets[sec.Name] = struct{}{}
		if notBefore.After(c.secretSeen) {
			c.secretSeen = notBefore
		}
		learned.secrets[sec.Name] = secretState{rv: sec.ResourceVersion, classKey: key}
	}
	for name, st := range learned.secrets {
		if _, still := seen[name]; still {
			continue
		}
		detachSecretLocked(name, st, now)
		delete(learned.secrets, name)
	}
	learned.unparseable.Store(unparseable)
	var hooks []func()
	if added {
		hooks = append(hooks, learned.hooks...)
	}
	learned.mu.Unlock()
	fireLearnedHooks(hooks)
}

// ObserveLiveIdentity records S2: a live customer /call's identity. exp is the
// caller's JWT expiry; a live-only class is dropped once it passes. A zero exp
// only refreshes an existing class (an identity with no expiry is never
// registered from traffic alone). Cache-off: no-op.
func ObserveLiveIdentity(username string, groups []string, exp time.Time) {
	if Disabled() || username == "" {
		return
	}
	if _, _, isSA := parseServiceAccountUsername(username); isSA {
		return
	}
	key := LearnedClassKey(username, groups)
	now := time.Now()
	learned.mu.RLock()
	c, ok := learned.classes[key]
	learned.mu.RUnlock()
	if ok {
		c.liveSeen.Store(now.UnixNano())
		for !exp.IsZero() {
			cur := c.liveExp.Load()
			if exp.UnixNano() <= cur || c.liveExp.CompareAndSwap(cur, exp.UnixNano()) {
				break
			}
		}
		return
	}
	if exp.IsZero() || !exp.After(now) {
		return
	}
	g := append([]string(nil), groups...)
	sort.Strings(g)
	learned.mu.Lock()
	ensureLearnedLocked()
	var hooks []func()
	if _, raced := learned.classes[key]; !raced {
		nc := &learnedClass{username: username, groups: g, secrets: map[string]struct{}{}}
		nc.liveSeen.Store(now.UnixNano())
		nc.liveExp.Store(exp.UnixNano())
		learned.classes[key] = nc
		learned.pending[key] = struct{}{}
		learned.everUsers[username] = struct{}{}
		hooks = append(hooks, learned.hooks...)
	}
	learned.mu.Unlock()
	fireLearnedHooks(hooks)
}

// SetLearnedAdmissionStats records the seed engine's last capacity decision:
// seeded = classes admitted that had at least one distinct target;
// unseededCapacity = classes with a distinct target left out by the bound.
// capacity carries the measured inputs (numbers and a bound name only — no
// identity) for snowplow_learned_classes_capacity.
func SetLearnedAdmissionStats(seeded, unseededCapacity int, capacity map[string]any) {
	learnedAdmission.seeded.Store(int64(seeded))
	learnedAdmission.unseededCapacity.Store(int64(unseededCapacity))
	learnedAdmission.mu.Lock()
	learnedAdmission.capacity = capacity
	learnedAdmission.mu.Unlock()
}

// LearnedClassCounts returns (registered, fromSecrets, secretsUnparseable).
func LearnedClassCounts() (registered, fromSecrets, unparseable int) {
	learned.mu.RLock()
	defer learned.mu.RUnlock()
	for _, c := range learned.classes {
		registered++
		if len(c.secrets) > 0 {
			fromSecrets++
		}
	}
	return registered, fromSecrets, int(learned.unparseable.Load())
}

// LearnedClientconfigSecrets returns how many `*-clientconfig` Secrets the
// registry sees — the denominator of the format-drift alarm.
func LearnedClientconfigSecrets() int {
	learned.mu.RLock()
	defer learned.mu.RUnlock()
	return len(learned.secrets)
}

// LearnedAdmissionStats returns (seeded, unseededCapacity).
func LearnedAdmissionStats() (seeded, unseededCapacity int) {
	return int(learnedAdmission.seeded.Load()), int(learnedAdmission.unseededCapacity.Load())
}

// init publishes the learned-class surface only when the cache subsystem is
// on (CFG-1: learned classes exist only to seed L1 — e2e/bench/cfg1_probe).
//
//	snowplow_learned_classes_registered          — every class in the registry
//	snowplow_learned_classes_seeded              — admitted classes with ≥1 distinct target
//	snowplow_learned_classes_unseeded_capacity   — distinct classes left out by the bound
//	snowplow_learned_classes_from_secrets        — classes backed by a clientconfig Secret;
//	                                               0 while clientconfig Secrets exist is an ALARM
//	                                               (authn's certificate format moved)
//	snowplow_learned_classes_secrets_unparseable — clientconfig Secrets no class could be read from
//	snowplow_learned_clientconfig_secrets        — every `*-clientconfig` Secret seen (the alarm's denominator)
//	snowplow_learned_classes_capacity            — the measured inputs of the last bound
func init() {
	if Disabled() {
		return
	}
	RegisterLearnedClassesExpvar()
}

var learnedExpvarOnce sync.Once

// RegisterLearnedClassesExpvar publishes the learned-class surface on
// /debug/vars. Idempotent; called from the Disabled()-gated init above (and by
// arms that run with the cache turned on after process start).
func RegisterLearnedClassesExpvar() {
	learnedExpvarOnce.Do(publishLearnedClassesExpvar)
}

func publishLearnedClassesExpvar() {
	expvar.Publish("snowplow_learned_classes_registered", expvar.Func(func() any {
		r, _, _ := LearnedClassCounts()
		return r
	}))
	expvar.Publish("snowplow_learned_classes_seeded", expvar.Func(func() any {
		return learnedAdmission.seeded.Load()
	}))
	expvar.Publish("snowplow_learned_classes_unseeded_capacity", expvar.Func(func() any {
		return learnedAdmission.unseededCapacity.Load()
	}))
	expvar.Publish("snowplow_learned_classes_from_secrets", expvar.Func(func() any {
		_, s, _ := LearnedClassCounts()
		return s
	}))
	expvar.Publish("snowplow_learned_classes_secrets_unparseable", expvar.Func(func() any {
		return learned.unparseable.Load()
	}))
	expvar.Publish("snowplow_learned_clientconfig_secrets", expvar.Func(func() any {
		return LearnedClientconfigSecrets()
	}))
	expvar.Publish("snowplow_learned_classes_capacity", expvar.Func(func() any {
		learnedAdmission.mu.Lock()
		defer learnedAdmission.mu.Unlock()
		out := make(map[string]any, len(learnedAdmission.capacity))
		for k, v := range learnedAdmission.capacity {
			out[k] = v
		}
		return out
	}))
}

// SyncLearnedFromSecretsForTest drives S1 with an explicit item set.
func SyncLearnedFromSecretsForTest(items []*corev1.Secret) { syncLearnedFromSecrets(items) }

// ResetLearnedIdentitiesForTest clears the registry, its hooks and the stats.
func ResetLearnedIdentitiesForTest() {
	learned.mu.Lock()
	learned.classes = nil
	learned.secrets = nil
	learned.pending = nil
	learned.everUsers = nil
	learned.hooks = nil
	learned.unparseable.Store(0)
	learned.mu.Unlock()
	SetLearnedAdmissionStats(0, 0, nil)
}

// LearnedAdmissionCapacity returns a copy of the last bound's measured inputs
// (the snowplow_learned_classes_capacity map), nil before any decision.
func LearnedAdmissionCapacity() map[string]any {
	learnedAdmission.mu.Lock()
	defer learnedAdmission.mu.Unlock()
	if learnedAdmission.capacity == nil {
		return nil
	}
	out := make(map[string]any, len(learnedAdmission.capacity))
	for k, v := range learnedAdmission.capacity {
		out[k] = v
	}
	return out
}
