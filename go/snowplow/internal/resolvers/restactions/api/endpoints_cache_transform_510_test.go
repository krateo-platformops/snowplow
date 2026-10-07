// endpoints_cache_transform_510_test.go — #510, the consumer-side half.
//
// The informer transform (internal/cache/secrets_transform.go) reduces every
// Secret to an allow-list of data keys before it enters the store. The
// allow-list is a LITERAL re-declaration of the 14 plumbing endpoint labels
// declared in endpoints_cache.go:49-64, because internal/resolvers/restactions/api
// imports internal/cache and the dependency cannot run the other way.
//
// This arm is the guard that makes that duplication safe: it runs the REAL
// transform over a Secret carrying all 14 labels and requires
// extractEndpointFromSecret to produce a reflect.DeepEqual Endpoint on either
// side of it. A 15th label added to the extractor (or a key dropped from the
// cache-side allow-list) fails here, with the field named.
//
// It also asserts the OTHER direction in this package — the transform does drop
// an unread key — so a no-op transform cannot satisfy it vacuously.
package api

import (
	"reflect"
	"testing"

	"github.com/krateo-platformops/snowplow/internal/cache"
	corev1 "k8s.io/api/core/v1"
)

// s510EveryEndpointLabel carries every one of the 14 labels
// extractEndpointFromSecret reads, each with a DISTINCT non-zero value so no
// field can be silently lost into a zero value.
var s510EveryEndpointLabel = map[string]string{
	clientCertLabel:   "client-cert-510",
	clientKeyLabel:    "client-key-510",
	caLabel:           "ca-510",
	proxyUrlLabel:     "http://proxy-510.example:3128",
	serverUrlLabel:    "https://server-510.example/k8s",
	debugLabel:        "true",
	passwordLabel:     "password-510",
	usernameLabel:     "username-510",
	tokenLabel:        "token-510",
	insecureLabel:     "true",
	awsAccessKeyLabel: "aws-access-510",
	awsSecretKeyLabel: "aws-secret-510",
	awsRegionLabel:    "eu-west-4",
	awsServiceLabel:   "eks",
}

func TestS510_TransformKeepsEveryEndpointField(t *testing.T) {
	const sentinel = "S510-SENTINEL-MUST-NOT-BE-RESIDENT"

	data := make(map[string]string, len(s510EveryEndpointLabel)+2)
	for k, v := range s510EveryEndpointLabel {
		data[k] = v
	}
	// Two keys no consumer reads, both sentinel-bearing.
	data["kubeconfig"] = sentinel + "/kubeconfig"
	data["refresh-token"] = sentinel + "/refresh-token"
	original := mkSecret("krateo-system", "alice-clientconfig", data)

	before, err := extractEndpointFromSecret(original)
	if err != nil {
		t.Fatalf("extractEndpointFromSecret(original): %v", err)
	}
	// Pin that all 14 labels really participate — otherwise a DeepEqual of two
	// mostly-zero Endpoints would prove nothing. (Endpoint.AwsTime is
	// deliberately absent: no label feeds it, upstream marks it testing-only
	// and extractEndpointFromSecret never sets it.)
	bv := reflect.ValueOf(before)
	for _, f := range []string{
		"ServerURL", "ProxyURL", "CertificateAuthorityData", "ClientCertificateData",
		"ClientKeyData", "Token", "Username", "Password", "Debug", "Insecure",
		"AwsAccessKey", "AwsSecretKey", "AwsRegion", "AwsService",
	} {
		if bv.FieldByName(f).IsZero() {
			t.Fatalf("test setup: Endpoint.%s came out zero — the arm cannot discriminate on that field", f)
		}
	}

	out, terr := cache.SecretsCacheTransformForTest(original)
	if terr != nil {
		t.Fatalf("the informer transform returned an error (%v) — the FIFO would DROP the Secret", terr)
	}
	reduced, ok := out.(*corev1.Secret)
	if !ok {
		t.Fatalf("transform returned %T; want *corev1.Secret", out)
	}

	after, err := extractEndpointFromSecret(reduced)
	if err != nil {
		t.Fatalf("extractEndpointFromSecret(reduced): %v — the transform stripped a key the extractor needs", err)
	}
	if !reflect.DeepEqual(before, after) {
		av := reflect.ValueOf(after)
		for i := 0; i < bv.NumField(); i++ {
			if !reflect.DeepEqual(bv.Field(i).Interface(), av.Field(i).Interface()) {
				t.Errorf("#510: Endpoint.%s changed across the informer transform: %v → %v — "+
					"secretsCacheRetainedDataKeys (internal/cache/secrets_transform.go) is missing this field's label",
					bv.Type().Field(i).Name, bv.Field(i).Interface(), av.Field(i).Interface())
			}
		}
		t.FailNow()
	}

	// The other direction: the transform is not a no-op.
	for _, k := range []string{"kubeconfig", "refresh-token"} {
		if _, has := reduced.Data[k]; has {
			t.Errorf("#510: data[%q] survived the transform; no consumer reads it", k)
		}
	}
	// And the original is untouched — the transform must not reduce in place.
	if len(original.Data) != len(s510EveryEndpointLabel)+2 {
		t.Errorf("the transform mutated its input (%d data keys left of %d)",
			len(original.Data), len(s510EveryEndpointLabel)+2)
	}
}
