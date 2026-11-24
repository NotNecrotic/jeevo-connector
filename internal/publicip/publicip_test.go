package publicip

import (
	"context"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"testing"
)

func serve(t *testing.T, body string) string {
	t.Helper()
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte(body)) }))
	t.Cleanup(s.Close)
	return s.URL
}

func noIfaces() ([]net.Addr, error) { return nil, nil }

func TestPublicInterfaceWins(t *testing.T) {
	d := &Detector{Interfaces: func() ([]net.Addr, error) {
		return []net.Addr{
			&net.IPNet{IP: net.ParseIP("127.0.0.1"), Mask: net.CIDRMask(8, 32)},
			&net.IPNet{IP: net.ParseIP("192.168.1.20"), Mask: net.CIDRMask(24, 32)},
			&net.IPNet{IP: net.ParseIP("203.0.113.7"), Mask: net.CIDRMask(24, 32)},
		}, nil
	}, Services: []string{"http://127.0.0.1:1"}}
	a, src, err := d.Detect(context.Background())
	if err != nil || a != netip.MustParseAddr("203.0.113.7") || src != FromInterface {
		t.Fatalf("got %v %v %v", a, src, err)
	}
}

func TestLookupNeedsTwoAgreeing(t *testing.T) {
	d := &Detector{Interfaces: noIfaces, Client: http.DefaultClient, Services: []string{
		serve(t, "198.51.100.4\n"), serve(t, "198.51.100.4"), serve(t, "198.51.100.99"),
	}}
	a, src, err := d.Detect(context.Background())
	if err != nil || a != netip.MustParseAddr("198.51.100.4") || src != FromLookup {
		t.Fatalf("got %v %v %v", a, src, err)
	}
}

func TestLookupRefusesDisagreementAndJunk(t *testing.T) {
	for name, bodies := range map[string][]string{
		"all differ":   {"198.51.100.1", "198.51.100.2", "198.51.100.3"},
		"one answer":   {"198.51.100.1", "<html>", "nope"},
		"private":      {"192.168.1.1", "192.168.1.1", "10.0.0.1"},
		"ipv6 / cgnat": {"2001:db8::1", "2001:db8::1", "100.64.1.1"},
	} {
		var urls []string
		for _, b := range bodies {
			urls = append(urls, serve(t, b))
		}
		d := &Detector{Interfaces: noIfaces, Client: http.DefaultClient, Services: urls}
		if _, _, err := d.Detect(context.Background()); !errors.Is(err, ErrUnknown) {
			t.Errorf("%s: want ErrUnknown, got %v", name, err)
		}
	}
}

func TestIsPublic(t *testing.T) {
	for s, want := range map[string]bool{
		"203.0.113.7": true, "8.8.8.8": true, "10.0.0.1": false, "192.168.0.1": false,
		"172.16.5.5": false, "100.100.0.1": false, "127.0.0.1": false, "169.254.1.1": false,
	} {
		if got := IsPublic(netip.MustParseAddr(s)); got != want {
			t.Errorf("IsPublic(%s) = %v", s, got)
		}
	}
}
