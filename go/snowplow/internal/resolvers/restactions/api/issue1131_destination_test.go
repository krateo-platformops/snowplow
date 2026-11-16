package api

// issue1131_destination_test.go — #1131, at the resolver's edge: a stage whose
// endpoint names a denied address, through the real resolve, on every builder
// branch. No arm sends a packet to a denied address (the policy refuses in the
// dialer's control hook), and names are answered in-process (fakedns).
//
//   A  every credential kind, read and write: the stage fails with reason
//      DestinationRefused, a 502 under its errorKey, the range named, no token,
//      and the resolve's failure says the policy refused, never that an upstream
//      answered, and classifies as the refresher's destination_refused.
//   B  a write that depends on a refused stage is Skipped.
//   C  a name resolving to a denied address, and a name that rebinds to one on
//      a client kept across fetches (#1088), on both builder branches.
//   D  a stage on a loopback endpoint is still served.
//   E  an upstream cannot claim the reason (#714).
//   F  the reason ranks with snowplow's own refusals.

import (
	"context"
	"net"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/krateo-platformops/plumbing/endpoints"
	httpcall "github.com/krateo-platformops/plumbing/http/request"
	"github.com/krateo-platformops/plumbing/http/response"
	"github.com/krateo-platformops/plumbing/ptr"
	templates "github.com/krateo-platformops/snowplow/apis/templates/v1"
	"github.com/krateo-platformops/snowplow/internal/cache"
	"github.com/krateo-platformops/snowplow/internal/credhttp"
	"github.com/krateo-platformops/snowplow/internal/testsupport/fakedns"
)

// x1131Secrets is one endpoint Secret per chain a named endpoint is sent on.
func x1131Secrets(url string) map[string]map[string]string {
	return map[string]map[string]string{
		"bearer_owned":           x1106Secret(url),
		"no_credential_owned":    {"server-url": url},
		"password_only_plumbing": {"server-url": url, "password": x1106Token},
		"aws_plumbing": {"server-url": url, "aws-access-key": "AKIDX1131", "aws-secret-key": x1106Token,
			"aws-region": "eu-west-1", "aws-service": "execute-api"},
	}
}

func TestIssue1131_A_ARefusedStageNamesItsReason(t *testing.T) {
	for name, secret := range x1131Secrets("http://169.254.169.254") {
		for _, verb := range []string{http.MethodGet, http.MethodPut} {
			t.Run(name+"_"+verb, func(t *testing.T) {
				x1106Env(t, "", map[string]map[string]string{x1106Ref: secret})
				s := x1106Stage("r", verb, "/latest/meta-data")
				o := x1106Resolve(t, x1106Extras, s)

				x1106WantReason(t, o.outcomes, "r", StageReasonDestinationRefused)
				if o.failure == nil || o.failure.Code != http.StatusBadGateway ||
					o.failure.Reason != StageReasonDestinationRefused {
					t.Fatalf("RED: the resolve's failure: %+v", o.failure)
				}
				if !strings.Contains(o.dictJSON, "link-local") || !strings.Contains(o.dictJSON, "destination policy") {
					t.Fatalf("RED: the stage does not say why: %s", o.dictJSON)
				}
				x1106NoSecretIn(t, name, o, x1106Token)

				// What the HTTP caller and refresher.refresh_failed read.
				se := o.failure.StatusError("ns", "ra")
				msg := se.Error()
				if strings.Contains(msg, "upstream answered") || strings.Contains(msg, "/latest/meta-data") {
					t.Fatalf("RED: nothing answered, and the URL is not the caller's: %q", msg)
				}
				for _, want := range []string{`restaction ns/ra stage "r" failed`, "169.254.169.254:80", "link-local", "destination policy"} {
					if !strings.Contains(msg, want) {
						t.Fatalf("RED: the failure does not say %q: %q", want, msg)
					}
				}
				if class, _ := cache.ClassifyRefreshError(se); class != cache.RefreshErrDestinationRefused {
					t.Fatalf("RED: the refresher reads this real failure as %q", class)
				}
				if !cache.IsNonRetryableRefreshError(se) {
					t.Fatal("RED: the refresher would retry this real failure")
				}
			})
		}
	}
}

// A named endpoint's path can look like an apiserver path: the coordinates are
// the stage's own, and it still says nothing answered.
func TestIssue1131_A_AnAPIServerShapedPathKeepsItsCoordinates(t *testing.T) {
	x1106Env(t, "", map[string]map[string]string{x1106Ref: x1106Secret("http://169.254.169.254")})
	o := x1106Resolve(t, x1106Extras, x1106Stage("r", http.MethodGet, "/api/v1/namespaces/team-a/pods"))
	msg := o.failure.StatusError("ns", "ra").Error()
	if strings.Contains(msg, "upstream answered") || !strings.Contains(msg, `pods in namespace "team-a"`) || !strings.Contains(msg, "destination policy") {
		t.Fatalf("RED: %q", msg)
	}
}

func TestIssue1131_B_AWriteThatDependsOnARefusedStageIsNotSent(t *testing.T) {
	x1106Env(t, "", map[string]map[string]string{x1106Ref: x1106Secret("http://169.254.169.254")})
	first := x1106Stage("read", http.MethodGet, "/latest")
	first.ContinueOnError = ptr.To(true)
	second := x1106Write("write", http.MethodPost, "/zd/tickets")
	second.DependsOn = &templates.Dependency{Name: "read"}
	o := x1106Resolve(t, x1106Extras, first, second)
	x1106WantReason(t, o.outcomes, "read", StageReasonDestinationRefused)
	x1106WantReason(t, o.outcomes, "write", StageReasonSkipped)
}

func TestIssue1131_C_NamesAreCheckedOnTheAddressDialled(t *testing.T) {
	srv := newX1106Srv(t, false, x1106OK)
	_, port, _ := net.SplitHostPort(strings.TrimPrefix(srv.srv.URL, "http://"))
	var answer atomic.Value
	fakedns.Install(t, func(name string) []net.IP {
		switch name {
		case "metadata.x1131.test":
			return []net.IP{net.ParseIP("169.254.169.254")}
		case "rebind.x1131.test":
			return []net.IP{net.ParseIP(answer.Load().(string))}
		}
		return nil
	})
	t.Run("resolves_to_denied", func(t *testing.T) {
		for name, secret := range x1131Secrets("http://metadata.x1131.test") {
			t.Run(name, func(t *testing.T) {
				x1106Env(t, "", map[string]map[string]string{x1106Ref: secret})
				o := x1106Resolve(t, x1106Extras, x1106Stage("r", http.MethodGet, "/latest"))
				x1106WantReason(t, o.outcomes, "r", StageReasonDestinationRefused)
			})
		}
	})
	t.Run("rebinds_to_denied", func(t *testing.T) {
		for name, secret := range x1131Secrets("http://rebind.x1131.test:" + port) {
			t.Run(name, func(t *testing.T) {
				x1106Env(t, "", map[string]map[string]string{x1106Ref: secret})
				s := x1106Stage("r", http.MethodGet, "/latest")
				s.Headers = []string{"Connection: close"}

				answer.Store("127.0.0.1")
				before := len(srv.all())
				if o := x1106Resolve(t, x1106Extras, s); o.outcomes != "[]" {
					t.Fatalf("PRE: the allowed answer is served: %s", o.outcomes)
				}
				if len(srv.all()) != before+1 {
					t.Fatal("PRE: the endpoint gets the first request")
				}
				// The kept client (#1088) may hold an idle connection: close it,
				// so the next fetch has to dial.
				srv.srv.CloseClientConnections()

				answer.Store("169.254.169.254")
				o := x1106Resolve(t, x1106Extras, s)
				x1106WantReason(t, o.outcomes, "r", StageReasonDestinationRefused)
				if len(srv.all()) != before+1 {
					t.Fatalf("RED: %d request(s) after the name rebound", len(srv.all())-before-1)
				}
			})
		}
	})
}

// A stage on a loopback endpoint is served on every chain: in-cluster and
// on-prem endpoints live on addresses the policy allows.
func TestIssue1131_D_AnAllowedEndpointIsServed(t *testing.T) {
	srv := newX1106Srv(t, false, x1106OK)
	for name, secret := range x1131Secrets(srv.srv.URL) {
		t.Run(name, func(t *testing.T) {
			x1106Env(t, "", map[string]map[string]string{x1106Ref: secret})
			before := len(srv.all())
			o := x1106Resolve(t, x1106Extras, x1106Stage("r", http.MethodGet, "/ok"))
			if o.outcomes != "[]" || len(srv.all()) != before+1 {
				t.Fatalf("RED: outcomes %s, %d request(s)", o.outcomes, len(srv.all())-before)
			}
		})
	}
}

// #714: the reason is snowplow's. An upstream that answers 502 and claims it reads
// as any other 502.
func TestIssue1131_E_AnUpstreamCannotClaimTheReason(t *testing.T) {
	srv := newX1106Srv(t, false, func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadGateway)
		_, _ = w.Write([]byte(`{"kind":"Status","status":"Failure","code":502,"reason":"DestinationRefused","message":"x"}`))
	})
	x1106Env(t, "", map[string]map[string]string{x1106Ref: x1106Secret(srv.srv.URL)})
	o := x1106Resolve(t, x1106Extras, x1106Stage("r", http.MethodPost, "/x"))
	x1106WantReason(t, o.outcomes, "r", StageReasonError)
	// And it stays an upstream's answer for the HTTP caller and the refresher.
	se := o.failure.StatusError("ns", "ra")
	if !strings.Contains(se.Error(), "upstream answered 502") {
		t.Fatalf("RED: an upstream's 502 lost its message: %q", se.Error())
	}
	if class, _ := cache.ClassifyRefreshError(se); class == cache.RefreshErrDestinationRefused {
		t.Fatal("RED: an upstream claimed the refresher's class")
	}
	if cache.IsNonRetryableRefreshError(se) {
		t.Fatal("RED: an upstream made itself non-retryable")
	}
}

func TestIssue1131_F_TheReasonRanksWithSnowplowsOwnRefusals(t *testing.T) {
	for _, tc := range []struct{ first, second, want string }{
		{StageReasonError, StageReasonDestinationRefused, StageReasonDestinationRefused},
		{StageReasonTimeout, StageReasonDestinationRefused, StageReasonDestinationRefused},
		// #1347 ruling: a cut / failed code outranks a denial (group first).
		{StageReasonDestinationRefused, StageReasonForbidden, StageReasonDestinationRefused},
		{StageReasonForbidden, StageReasonDestinationRefused, StageReasonDestinationRefused},
		{StageReasonDestinationRefused, StageReasonInvalidPath, StageReasonInvalidPath},
	} {
		s := newStageOutcomes()
		s.register([]string{"r"})
		s.fail("r", tc.first)
		s.fail("r", tc.second)
		if got := s.reasonOf("r"); got != tc.want {
			t.Errorf("%s then %s: reason %s, want %s", tc.first, tc.second, got, tc.want)
		}
	}
}

// The fetch itself: the envelope and the go error, on every chain.
func TestIssue1131_G_TheFetchReportsTheRefusal(t *testing.T) {
	for name, ep := range map[string]*endpoints.Endpoint{
		"bearer_owned":  {ServerURL: "http://169.254.169.254/base", Token: x1106Token},
		"plumbing_aws":  {ServerURL: "http://169.254.169.254/base", AwsAccessKey: "AKIDX1131", AwsSecretKey: x1106Token, AwsRegion: "eu-west-1", AwsService: "execute-api"},
		"with_userinfo": {ServerURL: "http://svc:" + x1106Token + "@169.254.169.254/base", Password: "p"},
	} {
		t.Run(name, func(t *testing.T) {
			res, body, _, err := httpFetchAllowingNonJSON(context.Background(), httpcall.RequestOptions{
				RequestInfo: httpcall.RequestInfo{Path: "/x", Verb: ptr.To(http.MethodPost)},
				Endpoint:    ep,
			})
			if err == nil || res == nil || body != nil {
				t.Fatalf("res %+v, body %q, err %v", res, body, err)
			}
			if res.Status != response.StatusFailure || res.Code != http.StatusBadGateway ||
				string(res.Reason) != credhttp.DestinationRefusedReason {
				t.Fatalf("RED: the envelope: %+v", res)
			}
			if !credhttp.DestinationRefused(err) {
				t.Fatalf("RED: the go error lost the refusal: %v", err)
			}
			if strings.Contains(res.Message, x1106Token) || strings.Contains(err.Error(), x1106Token) {
				t.Fatal("RED: a credential in the refusal")
			}
		})
	}
}
