package token

import (
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"strings"
	"testing"
	"time"
)

func newKey(t *testing.T) (ed25519.PrivateKey, string) {
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	return priv, base64.StdEncoding.EncodeToString(pub)
}

func TestVerify(t *testing.T) {
	priv, pub := newKey(t)
	v, err := NewVerifier("abcdefgh", pub)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	good := Sign(priv, Claims{Connector: "abcdefgh", Media: "m_1", Expires: now.Add(time.Hour).Unix(), MaxHeight: 720})
	c, err := v.Verify(good, "m_1", now)
	if err != nil || c.MaxHeight != 720 {
		t.Fatalf("valid token rejected: %v", err)
	}
	cases := map[string]struct {
		tok   string
		media string
		want  error
	}{
		"other media":     {good, "m_2", ErrScope},
		"other connector": {Sign(priv, Claims{Connector: "zzzzzzzz", Media: "m_1", Expires: now.Add(time.Hour).Unix()}), "m_1", ErrScope},
		"expired":         {Sign(priv, Claims{Connector: "abcdefgh", Media: "m_1", Expires: now.Add(-time.Hour).Unix()}), "m_1", ErrExpired},
		"garbage":         {"nope", "m_1", ErrMalformed},
		"tampered":        {strings.Replace(good, good[5:9], "AAAA", 1), "m_1", nil},
	}
	for name, tc := range cases {
		_, err := v.Verify(tc.tok, tc.media, now)
		if err == nil {
			t.Errorf("%s: accepted", name)
		} else if tc.want != nil && err != tc.want {
			t.Errorf("%s: got %v want %v", name, err, tc.want)
		}
	}
	// A token from another key never verifies.
	other, _ := newKey(t)
	if _, err := v.Verify(Sign(other, Claims{Connector: "abcdefgh", Media: "m_1", Expires: now.Add(time.Hour).Unix()}), "m_1", now); err != ErrSignature {
		t.Errorf("foreign key: %v", err)
	}
}

func TestVerifyLifetimeCap(t *testing.T) {
	priv, pub := newKey(t)
	v, _ := NewVerifier("abcdefgh", pub)
	now := time.Now()
	sign := func(e time.Time) string {
		return Sign(priv, Claims{Connector: "abcdefgh", Media: "m_1", Expires: e.Unix()})
	}
	// The longest lifetime Jeevo can issue is accepted, with the skew as margin.
	if _, err := v.Verify(sign(now.Add(MaxLifetime)), "m_1", now); err != nil {
		t.Fatalf("longest legitimate token: %v", err)
	}
	if _, err := v.Verify(sign(now.Add(MaxLifetime+30*time.Second)), "m_1", now); err != nil {
		t.Fatalf("token within the skew margin: %v", err)
	}
	for name, e := range map[string]time.Time{
		"just past the cap": now.Add(MaxLifetime + 2*time.Minute),
		"a year":            now.Add(365 * 24 * time.Hour),
		"year 3000":         time.Unix(32503680000, 0),
	} {
		if _, err := v.Verify(sign(e), "m_1", now); err != ErrLifetime {
			t.Errorf("%s: got %v, want %v", name, err, ErrLifetime)
		}
	}
}
