package app

import (
	"log/slog"
	"net"
	"net/http"
	"net/netip"
	"sync/atomic"
)

// cgnat is 100.64.0.0/10 (carrier-grade NAT, also used by Tailscale).
var cgnat = netip.MustParsePrefix("100.64.0.0/10")

// isLocalNetwork reports whether ip is loopback, RFC 1918, link-local, IPv6 unique-local, or CGNAT.
func isLocalNetwork(ip netip.Addr) bool {
	ip = ip.Unmap()
	return ip.IsLoopback() || ip.IsPrivate() || ip.IsLinkLocalUnicast() || cgnat.Contains(ip)
}

// localNetworksOnly restricts plain-HTTP development mode to private networks, even if the port is forwarded.
func localNetworksOnly(next http.Handler, log *slog.Logger) http.Handler {
	var refused atomic.Int64
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		host, _, err := net.SplitHostPort(r.RemoteAddr)
		ip, perr := netip.ParseAddr(host)
		if err != nil || perr != nil || !isLocalNetwork(ip) {
			if n := refused.Add(1); n == 1 || n%100 == 0 {
				log.Warn("refused a plain-HTTP request from outside the local network (development mode serves local networks only)", "from", host, "refused", n)
			}
			w.Header().Set("Connection", "close")
			http.Error(w, "Forbidden", http.StatusForbidden)
			return
		}
		next.ServeHTTP(w, r)
	})
}
