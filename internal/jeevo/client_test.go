package jeevo

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"testing"
)

func TestNormalizeURL(t *testing.T) {
	for raw, want := range map[string]string{
		"jeevo.net":                 "https://jeevo.net",
		"https://jeevo.net/":        "https://jeevo.net",
		"http://localhost:8080":     "http://localhost:8080",
		"http://127.0.0.1:8080/x":   "http://127.0.0.1:8080",
		"http://[::1]:8080":         "http://[::1]:8080",
		"http://jeevo.net":          "",
		"http://localhost.evil.com": "",
		"http://localhost@evil.com": "",
		"ftp://jeevo.net":           "",
		"wss://jeevo.net":           "",
	} {
		got, err := NormalizeURL(raw)
		if (err == nil) != (want != "") || got != want {
			t.Errorf("NormalizeURL(%q) = %q, %v; want %q", raw, got, err, want)
		}
	}
}

func TestPinReleaseKeys(t *testing.T) {
	raw := make([]byte, 32)
	raw[0] = 1
	other := base64.StdEncoding.EncodeToString(make([]byte, 32))
	key := base64.StdEncoding.EncodeToString(raw)
	sum := sha256.Sum256(raw)
	fp := hex.EncodeToString(sum[:])

	if got, err := PinReleaseKeys([]string{other, key}, ""); err != nil || len(got) != 2 {
		t.Errorf("no pin: %v, %v", got, err)
	}
	if got, err := PinReleaseKeys([]string{other, key}, "  "+fp+"\n"); err != nil || len(got) != 1 || got[0] != key {
		t.Errorf("pin: %v, %v", got, err)
	}
	if _, err := PinReleaseKeys([]string{other}, fp); err == nil {
		t.Error("a pin matching no key must fail")
	}
}
