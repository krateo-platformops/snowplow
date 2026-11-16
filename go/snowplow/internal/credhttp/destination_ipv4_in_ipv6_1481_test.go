package credhttp

// destination_ipv4_in_ipv6_1481_test.go — #1481 (plumbing#198, v1.85.0): the
// destination policy applies the IPv4 rules to the IPv4 address an IPv6 one
// carries by NAT64's well-known prefix (64:ff9b::/96), 6to4 (2002::/16) or the
// IPv4-compatible form (::/96), on snowplow's own dial paths: the owned chain
// (EndpointDialHolder) and plumbing's (credhttp.Do, the plumbing branch of a
// stage fetch).
//
//   R  each form of 169.254.169.254, in several spellings and through a name,
//      is refused on both chains;
//   P  the same forms of a public address (93.184.216.34) are not refused:
//      only translations of refused destinations are;
//   L  NAT64's local-use prefix 64:ff9b:1::/48 is NOT decoded (plumbing
//      documents why: its operator picks the layout), so 64:ff9b:1:: carrying
//      169.254.169.254's bits is ordinary IPv6 to this rule.
//
// No arm sends a packet: the refusal is in the dialer's control hook, and the
// allowed arms stop the dial there.

import (
	"errors"
	"net"
	"net/http"
	"testing"

	"github.com/krateo-platformops/plumbing/dialguard"
	"github.com/krateo-platformops/snowplow/internal/testsupport/fakedns"
)

// x1481Embedded169 are the three forms of 169.254.169.254 (a9fe:a9fe), each
// written more than one way.
var x1481Embedded169 = map[string][]string{
	"NAT64 64:ff9b::/96":    {"[64:ff9b::a9fe:a9fe]", "[64:ff9b::169.254.169.254]", "[0064:FF9B:0000:0000:0000:0000:A9FE:A9FE]"},
	"6to4 2002::/16":        {"[2002:a9fe:a9fe::]", "[2002:a9fe:a9fe::1]", "[2002:A9FE:A9FE:1234::abcd]"},
	"IPv4-compatible ::/96": {"[::a9fe:a9fe]", "[::169.254.169.254]"},
}

func TestIssue1481_R_IPv4InIPv6FormsOfMetadataAreRefusedOnBothChains(t *testing.T) {
	fakedns.Install(t, func(name string) []net.IP {
		switch name {
		case "nat64-metadata.x1481.test":
			return []net.IP{net.ParseIP("64:ff9b::a9fe:a9fe")}
		case "6to4-metadata.x1481.test":
			return []net.IP{net.ParseIP("2002:a9fe:a9fe::1")}
		case "compat-metadata.x1481.test":
			return []net.IP{net.ParseIP("::a9fe:a9fe")}
		}
		return nil
	})
	hosts := []string{"nat64-metadata.x1481.test", "6to4-metadata.x1481.test", "compat-metadata.x1481.test"}
	for _, spellings := range x1481Embedded169 {
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

// x1481RequireNotRefused requires that the policy lets ip through on both
// chains: on the owned chain the dial reaches the control hook with a nil
// verdict and is stopped there; plumbing's chain runs the same function, read
// directly.
func x1481RequireNotRefused(t *testing.T, ip string) {
	t.Helper()
	seen := x1131StopAtControl(t)
	addr := net.JoinHostPort(ip, "80")
	resp, err := Send(SendSiteStage, x1131Clients(t)["owned"], x1131Put(t, "http://"+addr))
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
	canon := net.JoinHostPort(net.ParseIP(ip).String(), "80")
	if verdict, ok := seen.Load().(map[string]error)[canon]; !ok || verdict != nil {
		t.Fatalf("control saw %v, want a nil verdict on %s", seen.Load(), canon)
	}
	if err := dialguard.DenyAddr(addr); err != nil {
		t.Errorf("RED: plumbing's chain refuses %s: %v", ip, err)
	}
}

func TestIssue1481_P_IPv4InIPv6FormsOfAPublicAddressStillDial(t *testing.T) {
	for _, ip := range []string{
		"64:ff9b::5db8:d822",        // NAT64 of 93.184.216.34
		"2002:5db8:d822::1",         // 6to4 of 93.184.216.34
		"2002:5db8:d822:1234::abcd", // 6to4, another host behind the same relay
	} {
		t.Run(ip, func(t *testing.T) { x1481RequireNotRefused(t, ip) })
	}
}

func TestIssue1481_L_NAT64LocalUsePrefixIsNotDecoded(t *testing.T) {
	// 64:ff9b:1::/48 carrying 169.254.169.254's bits in the /96 position and
	// in the /48 position (bits 48-79): neither is read as an IPv4 address.
	for _, ip := range []string{
		"64:ff9b:1::a9fe:a9fe",
		"64:ff9b:1:a9fe:a9:fe00::",
	} {
		t.Run(ip, func(t *testing.T) { x1481RequireNotRefused(t, ip) })
	}
}
