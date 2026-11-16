// issue1334_authenticated_keys_golden_test.go — #1334: no key, binding-set
// digest, identity class or learned class key moves for an AUTHENTICATED
// identity, so the rollout causes no cache churn.
//
// The world holds a ClusterRoleBinding to Group system:authenticated (the
// binding whose treatment #1334 changes), a group grant and a user grant, so the
// implicit group really is part of every digest below. The want values were
// captured by running this test against main 6a18a554's (pre-#1334) sources,
// with the same world (i1334BuildWatcher) and identities. The learned class
// key is compared as a sha256 so no identity string sits in a golden.
package dispatchers

import (
	"crypto/sha256"
	"encoding/hex"
	"testing"

	"github.com/krateo-platformops/plumbing/jwtutil"
	"github.com/krateo-platformops/snowplow/internal/cache"
	"github.com/krateo-platformops/snowplow/internal/rbac"
)

func TestIssue1334_AuthenticatedIdentities_KeysAndDigestsUnchanged(t *testing.T) {
	i1334BuildWatcher(t)

	type id struct {
		name   string
		user   string
		groups []string
	}
	ids := []id{
		{"alice", i1334Alice, nil},
		{"bob", "bob-1334", []string{"devs"}},
		{"carol", "carol-1334", []string{"devs", "system:authenticated"}},
		{"sa", "system:serviceaccount:ns1:sa1", nil},
		{"public-no-groups", i1334Public, nil},
		{"seed-rep", "", []string{"devs", "system:authenticated"}},
	}
	want := map[string]string{
		"alice/class":                        "7064429a658249a240f89096f629ac38aa70baf2318df9419f6fd9e885803c98/0",
		"alice/digest":                       "7064429a658249a240f89096f629ac38aa70baf2318df9419f6fd9e885803c98",
		"alice/key/catalog-index":            "ef13e2c2247056b8cac554bc7cf4576afc72bfc79c2639627b0a9272210cd29a",
		"alice/key/public-page":              "126353c93975a865524035d5a53454dc6ef5607a6d7f1d827c403749a34c2cc2",
		"alice/learned":                      "b78b335a9b3b9cdb21d5dbcb4717afb4b9cdb933c473858ff4aba196d6afc0a5",
		"bob/class":                          "7064429a658249a240f89096f629ac38aa70baf2318df9419f6fd9e885803c98/0",
		"bob/digest":                         "7064429a658249a240f89096f629ac38aa70baf2318df9419f6fd9e885803c98",
		"bob/key/catalog-index":              "ef13e2c2247056b8cac554bc7cf4576afc72bfc79c2639627b0a9272210cd29a",
		"bob/key/public-page":                "126353c93975a865524035d5a53454dc6ef5607a6d7f1d827c403749a34c2cc2",
		"bob/learned":                        "26a53c3118e08467e2ba5a8c4080666547560ee8a37fecccf341dffbd54a21d4",
		"carol/class":                        "7064429a658249a240f89096f629ac38aa70baf2318df9419f6fd9e885803c98/0",
		"carol/digest":                       "7064429a658249a240f89096f629ac38aa70baf2318df9419f6fd9e885803c98",
		"carol/key/catalog-index":            "ef13e2c2247056b8cac554bc7cf4576afc72bfc79c2639627b0a9272210cd29a",
		"carol/key/public-page":              "126353c93975a865524035d5a53454dc6ef5607a6d7f1d827c403749a34c2cc2",
		"carol/learned":                      "738729e7a52aa6b3dfab7cb7ce1a56e392d92c526b53f3186278755b6b99776d",
		"public-no-groups/class":             "209cc15573a21c52c97d292b8534b5bf0d8e2ab1f73c341b7fe0899603fe7ad5/0",
		"public-no-groups/digest":            "209cc15573a21c52c97d292b8534b5bf0d8e2ab1f73c341b7fe0899603fe7ad5",
		"public-no-groups/key/catalog-index": "1ef84d98ff2dcd2becabc1b2e69b04acce9dbf80c049f759981112a289bacf9f",
		"public-no-groups/key/public-page":   "f29d7848499a40332833d31c5175eb7c2f96a2786c6e869dfbcf0c84f61760e8",
		"public-no-groups/learned":           "196fbe30e25fc1da69357d202cc0a6449dd5ee69877bda677369a59f70422004",
		"sa/class":                           "7064429a658249a240f89096f629ac38aa70baf2318df9419f6fd9e885803c98/0",
		"sa/digest":                          "7064429a658249a240f89096f629ac38aa70baf2318df9419f6fd9e885803c98",
		"sa/key/catalog-index":               "ef13e2c2247056b8cac554bc7cf4576afc72bfc79c2639627b0a9272210cd29a",
		"sa/key/public-page":                 "126353c93975a865524035d5a53454dc6ef5607a6d7f1d827c403749a34c2cc2",
		"sa/learned":                         "2489679f32318fe995fc6e93a36ba4289e752dc5a334499ad922f894dd2bc356",
		"seed-rep/class":                     "7064429a658249a240f89096f629ac38aa70baf2318df9419f6fd9e885803c98/0",
		"seed-rep/digest":                    "7064429a658249a240f89096f629ac38aa70baf2318df9419f6fd9e885803c98",
		"seed-rep/key/catalog-index":         "ef13e2c2247056b8cac554bc7cf4576afc72bfc79c2639627b0a9272210cd29a",
		"seed-rep/key/public-page":           "126353c93975a865524035d5a53454dc6ef5607a6d7f1d827c403749a34c2cc2",
		"seed-rep/learned":                   "c39b65e2f15c6a0ad4f15a1daba649251da3359987de72f6a3bab744e46d3487",
	}
	got := map[string]string{}
	for _, u := range ids {
		ctx := i1334Ctx(u.user, u.groups)
		for _, ra := range []string{i1334AuthOnlyRA, i1334PublicRA} {
			k, _, _ := dispatchCacheLookupKey(ctx, "restactions",
				h1RAGVR.Group, h1RAGVR.Version, h1RAGVR.Resource, h1NS, ra, -1, -1, nil)
			got[u.name+"/key/"+ra] = k
		}
		ui := jwtutil.UserInfo{Username: u.user, Groups: u.groups}
		got[u.name+"/class"] = rbac.IdentityClassOf(ui).String()
		got[u.name+"/digest"] = rbac.SubjectBindingSetDigest(u.user, u.groups)
		sum := sha256.Sum256([]byte(cache.LearnedClassKey(u.user, u.groups)))
		got[u.name+"/learned"] = hex.EncodeToString(sum[:])
	}
	for name, w := range want {
		if got[name] != w {
			t.Errorf("%s moved: got %q want %q", name, got[name], w)
		}
	}
	if len(got) != len(want) {
		t.Errorf("got %d values, want %d", len(got), len(want))
	}
}
