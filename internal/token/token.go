// Package token verifies playback tokens signed by Jeevo; they carry no user identity.
package token

import (
	"crypto/ed25519"
	"encoding/base64"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"time"
)

// MaxLifetime bounds expiry (claims have no issued-at): Jeevo's 48h TTL clamp plus an hour of rounding.
const MaxLifetime = 49 * time.Hour

// Claims are a token's contents.
type Claims struct {
	Connector string `json:"c"`
	Media     string `json:"m"`
	Expires   int64  `json:"e"`
	MaxHeight int    `json:"q,omitempty"`
}

// Errors are deliberately coarse: callers answer every one with the same 403.
var (
	ErrMalformed = errors.New("malformed token")
	ErrSignature = errors.New("bad signature")
	ErrExpired   = errors.New("expired")
	ErrLifetime  = errors.New("expires too far ahead")
	ErrScope     = errors.New("token not valid for this resource")
)

// Verifier checks tokens against a (replaceable) public key.
type Verifier struct {
	mu          sync.RWMutex
	key         ed25519.PublicKey
	connectorID string
	skew        time.Duration
}

// NewVerifier builds a verifier from a base64 raw Ed25519 public key.
func NewVerifier(connectorID, publicKeyB64 string) (*Verifier, error) {
	v := &Verifier{connectorID: connectorID, skew: 60 * time.Second}
	if err := v.SetKey(publicKeyB64); err != nil {
		return nil, err
	}
	return v, nil
}

// SetKey replaces the public key (Jeevo rotated it; announced in hello.ack).
func (v *Verifier) SetKey(b64 string) error {
	raw, err := base64.StdEncoding.DecodeString(b64)
	if err != nil || len(raw) != ed25519.PublicKeySize {
		return errors.New("playback public key must be base64 of 32 bytes")
	}
	v.mu.Lock()
	v.key = ed25519.PublicKey(raw)
	v.mu.Unlock()
	return nil
}

// Verify checks signature, expiry (and a sane lifetime), and that the token is for this connector and media.
func (v *Verifier) Verify(tok, mediaID string, now time.Time) (*Claims, error) {
	if len(tok) > 2048 || !strings.HasPrefix(tok, "v1.") {
		return nil, ErrMalformed
	}
	rest := tok[3:]
	body, sigPart, ok := strings.Cut(rest, ".")
	if !ok {
		return nil, ErrMalformed
	}
	sig, err := base64.RawURLEncoding.DecodeString(sigPart)
	if err != nil || len(sig) != ed25519.SignatureSize {
		return nil, ErrMalformed
	}
	v.mu.RLock()
	key := v.key
	v.mu.RUnlock()
	if !ed25519.Verify(key, []byte("v1."+body), sig) {
		return nil, ErrSignature
	}
	raw, err := base64.RawURLEncoding.DecodeString(body)
	if err != nil {
		return nil, ErrMalformed
	}
	var c Claims
	if err := json.Unmarshal(raw, &c); err != nil {
		return nil, ErrMalformed
	}
	if now.Add(-v.skew).Unix() > c.Expires {
		return nil, ErrExpired
	}
	if c.Expires > now.Add(MaxLifetime+v.skew).Unix() {
		return nil, ErrLifetime
	}
	if c.Connector != v.connectorID || c.Media != mediaID {
		return nil, ErrScope
	}
	return &c, nil
}

// Sign creates a token; tests only, the private key lives on the Jeevo server.
func Sign(priv ed25519.PrivateKey, c Claims) string {
	raw, _ := json.Marshal(c)
	body := base64.RawURLEncoding.EncodeToString(raw)
	sig := ed25519.Sign(priv, []byte("v1."+body))
	return "v1." + body + "." + base64.RawURLEncoding.EncodeToString(sig)
}
