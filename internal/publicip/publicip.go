// Package publicip finds the public IPv4; two or more services must agree so one hostile answer cannot move DNS.
package publicip

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/netip"
	"strings"
	"sync"
	"time"
)

// DefaultServices answer a plain-text IPv4 address over HTTPS.
var DefaultServices = []string{
	"https://api.ipify.org",
	"https://checkip.amazonaws.com",
	"https://ipv4.icanhazip.com",
	"https://ifconfig.me/ip",
}

var cgnat = netip.MustParsePrefix("100.64.0.0/10")

// IsPublic reports whether a is an address the internet can reach.
func IsPublic(a netip.Addr) bool {
	a = a.Unmap()
	return a.IsValid() && a.IsGlobalUnicast() && !a.IsPrivate() && !cgnat.Contains(a)
}

// Detector finds the public IPv4. The zero value uses the defaults.
type Detector struct {
	Services   []string
	Client     *http.Client
	Interfaces func() ([]net.Addr, error) // test hook; default net.InterfaceAddrs
}

// Source says where an address came from.
type Source string

const (
	FromInterface Source = "interface"
	FromLookup    Source = "lookup"
)

// ErrUnknown is returned when no trustworthy answer was found.
var ErrUnknown = errors.New("public address unknown")

// Detect returns the host's public IPv4 address and where it came from.
func (d *Detector) Detect(ctx context.Context) (netip.Addr, Source, error) {
	ifaces := d.Interfaces
	if ifaces == nil {
		ifaces = net.InterfaceAddrs
	}
	if addrs, err := ifaces(); err == nil {
		for _, a := range addrs {
			if p, err := netip.ParsePrefix(a.String()); err == nil && p.Addr().Is4() && IsPublic(p.Addr()) {
				return p.Addr(), FromInterface, nil
			}
		}
	}
	a, err := d.lookup(ctx)
	if err != nil {
		return netip.Addr{}, "", err
	}
	return a, FromLookup, nil
}

func (d *Detector) lookup(ctx context.Context) (netip.Addr, error) {
	services := d.Services
	if len(services) == 0 {
		services = DefaultServices
	}
	client := d.Client
	if client == nil {
		client = ipv4Client()
	}
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	var mu sync.Mutex
	votes := map[netip.Addr]int{}
	var wg sync.WaitGroup
	for _, u := range services {
		wg.Add(1)
		go func(u string) {
			defer wg.Done()
			a, err := ask(ctx, client, u)
			if err != nil {
				return
			}
			mu.Lock()
			votes[a]++
			mu.Unlock()
		}(u)
	}
	wg.Wait()
	for a, n := range votes {
		if n >= 2 {
			return a, nil
		}
	}
	return netip.Addr{}, ErrUnknown
}

func ask(ctx context.Context, client *http.Client, url string) (netip.Addr, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return netip.Addr{}, err
	}
	req.Header.Set("User-Agent", "jeevo-connector")
	res, err := client.Do(req)
	if err != nil {
		return netip.Addr{}, err
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		return netip.Addr{}, errors.New(res.Status)
	}
	body, err := io.ReadAll(io.LimitReader(res.Body, 64))
	if err != nil {
		return netip.Addr{}, err
	}
	a, err := netip.ParseAddr(strings.TrimSpace(string(body)))
	if err != nil || !a.Is4() || !IsPublic(a) {
		return netip.Addr{}, errors.New("not a public IPv4 address")
	}
	return a, nil
}

// ipv4Client dials IPv4 only, so services report the address the A record needs.
func ipv4Client() *http.Client {
	dialer := &net.Dialer{Timeout: 5 * time.Second}
	return &http.Client{
		Timeout: 8 * time.Second,
		Transport: &http.Transport{
			Proxy: http.ProxyFromEnvironment,
			DialContext: func(ctx context.Context, _, addr string) (net.Conn, error) {
				return dialer.DialContext(ctx, "tcp4", addr)
			},
			TLSHandshakeTimeout: 5 * time.Second,
		},
	}
}
