package credhttp

// destination_ipv6_metadata_1481_test.go — #1481 (plumbing#194, snowplow#1216):
// the destination policy refuses the IPv6 cloud metadata addresses, AWS's
// fd00:ec2::254 and GCP's fd20:ce::254, on snowplow's own dial paths: the
// owned chain (EndpointDialHolder, the dialer snowplow builds) and plumbing's
// (credhttp.Do and the plumbing branch of a stage fetch). They sit inside ULA,
// which stays allowed, so the rest of their /64s is NOT refused by this rule.
//
// No arm sends a packet: the refusal happens in the dialer's control hook, and
// the allowed arm stops the dial there too. Names are answered in-process.

import (
	"errors"
	"net"
	"net/http"
	"testing"

	"github.com/krateo-platformops/plumbing/dialguard"
	"github.com/krateo-platformops/snowplow/internal/testsupport/fakedns"
)

// x1481Metadata are the two addresses, each in more than one spelling.
var x1481Metadata = map[string][]string{
	"fd00:ec2::254": {"[fd00:ec2::254]", "[FD00:EC2::254]", "[fd00:0ec2:0000:0000:0000:0000:0000:0254]"},
	"fd20:ce::254":  {"[fd20:ce::254]", "[FD20:CE::254]", "[fd20:00ce:0:0:0:0:0:254]"},
}

// x1481Neighbours are other addresses of the same two /64s, and the next /64s:
// ordinary ULA, which this rule does not refuse.
var x1481Neighbours = []string{
	"fd00:ec2::1", "fd00:ec2::253", "fd00:ec2::255", "fd00:ec2::1:254", "fd00:ec2::ffff:ffff:ffff:ffff",
	"fd00:ec2:0:1::254",
	"fd20:ce::1", "fd20:ce::253", "fd20:ce::255", "fd20:ce::1:254", "fd20:ce::ffff:ffff:ffff:ffff",
	"fd20:ce:0:1::254",
}

func TestIssue1481_IPv6MetadataAddressIsRefusedOnBothChains(t *testing.T) {
	fakedns.Install(t, func(name string) []net.IP {
		switch name {
		case "aws-metadata.x1481.test":
			return []net.IP{net.ParseIP("fd00:ec2::254")}
		case "gcp-metadata.x1481.test":
			return []net.IP{net.ParseIP("fd20:ce::254")}
		}
		return nil
	})
	hosts := []string{"aws-metadata.x1481.test", "gcp-metadata.x1481.test"}
	for _, spellings := range x1481Metadata {
		hosts = append(hosts, spellings...)
	}
	for name, cli := range x1131Clients(t) {
		for _, host := range hosts {
			for _, verb := range []string{http.MethodGet, http.MethodPut} {
				t.Run(name+"/"+verb+"/"+host, func(t *testing.T) {
					req, err := http.NewRequest(verb, "http://"+host+"/x1131-path?x=x1131-query", nil)
					if err != nil {
						t.Fatal(err)
					}
					resp, err := Send(SendSiteStage, cli, req)
					if err == nil {
						_ = resp.Body.Close()
						t.Fatalf("RED: %s answered %d: the metadata address was dialled", host, resp.StatusCode)
					}
					x1131RequireRefused(t, err)
				})
			}
		}
	}
}

// A name resolving to the metadata address and to an allowed one reaches the
// allowed one only: the check is per candidate, so the refusal is the rule's
// and not a failure of the name.
func TestIssue1481_IPv6MetadataCandidateIsSkippedForAnAllowedOne(t *testing.T) {
	seen := x1131StopAtControl(t)
	fakedns.Install(t, func(name string) []net.IP {
		if name == "mixed.x1481.test" {
			return []net.IP{net.ParseIP("fd00:ec2::254"), net.ParseIP("fd20:ce::254"), net.ParseIP("10.0.0.7")}
		}
		return nil
	})
	owned := x1131Clients(t)["owned"]
	resp, err := Send(SendSiteStage, owned, x1131Put(t, "http://mixed.x1481.test"))
	if err == nil {
		_ = resp.Body.Close()
		t.Fatal("PRE: the dial stops at the hook")
	}
	verdicts := seen.Load().(map[string]error)
	for _, a := range []string{"[fd00:ec2::254]:80", "[fd20:ce::254]:80"} {
		var d *dialguard.ErrDenied
		if v, ok := verdicts[a]; !ok || !errors.As(v, &d) {
			t.Errorf("RED: control did not refuse %s (seen=%v): %v", a, ok, v)
		}
	}
	if v, ok := verdicts["10.0.0.7:80"]; !ok || v != nil {
		t.Fatalf("RED: the allowed candidate was not reached with a nil verdict: %v", verdicts)
	}
}

// The rest of both /64s is not refused by this rule: on the owned chain the
// dial reaches the hook with a nil verdict and is stopped there; plumbing's
// chain runs the same function, read directly.
func TestIssue1481_IPv6MetadataNeighboursAreNotRefused(t *testing.T) {
	seen := x1131StopAtControl(t)
	owned := x1131Clients(t)["owned"]
	for _, ip := range x1481Neighbours {
		addr := net.JoinHostPort(ip, "80")
		t.Run(ip, func(t *testing.T) {
			resp, err := Send(SendSiteStage, owned, x1131Put(t, "http://"+addr))
			if err == nil {
				_ = resp.Body.Close()
				t.Fatal("PRE: the dial stops at the hook")
			}
			if DestinationRefused(err) {
				t.Fatalf("RED: the policy refused %s: %v", ip, err)
			}
			if !errors.Is(err, x1131Stopped) {
				t.Fatalf("the dial never reached the hook: %v", err)
			}
			if verdict, ok := seen.Load().(map[string]error)[addr]; !ok || verdict != nil {
				t.Fatalf("control saw %v, want a nil verdict on %s", seen.Load(), addr)
			}
			if err := dialguard.DenyAddr(addr); err != nil {
				t.Errorf("RED: plumbing's chain refuses %s: %v", addr, err)
			}
		})
	}
}
