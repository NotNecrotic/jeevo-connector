package app

import (
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"testing"
)

func TestIsLocalNetwork(t *testing.T) {
	for addr, want := range map[string]bool{
		"127.0.0.1":          true,
		"::1":                true,
		"10.1.2.3":           true,
		"172.16.0.9":         true,
		"172.32.0.1":         false,
		"192.168.1.20":       true,
		"169.254.10.10":      true,
		"100.100.1.1":        true,
		"100.128.0.1":        false,
		"fd12:3456::1":       true,
		"fe80::1":            true,
		"::ffff:192.168.0.2": true,
		"8.8.8.8":            false,
		"203.0.113.7":        false,
		"2001:4860::8888":    false,
		"::ffff:1.1.1.1":     false,
	} {
		if got := isLocalNetwork(netip.MustParseAddr(addr)); got != want {
			t.Errorf("isLocalNetwork(%s) = %v, want %v", addr, got, want)
		}
	}
}

func TestLocalNetworksOnly(t *testing.T) {
	h := localNetworksOnly(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, "media")
	}), slog.New(slog.NewTextHandler(io.Discard, nil)))
	for remote, want := range map[string]int{
		"192.168.1.30:51000": http.StatusOK,
		"[::1]:51000":        http.StatusOK,
		"203.0.113.7:51000":  http.StatusForbidden,
		"[2001:db8::1]:443":  http.StatusForbidden,
		"garbage":            http.StatusForbidden,
	} {
		req := httptest.NewRequest(http.MethodGet, "/hls/x/master.m3u8", nil)
		req.RemoteAddr = remote
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		if rec.Code != want {
			t.Errorf("%s: status %d, want %d", remote, rec.Code, want)
		}
	}
}
