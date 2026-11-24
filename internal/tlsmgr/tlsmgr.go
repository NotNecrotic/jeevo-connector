// Package tlsmgr owns the TLS identity; the private key never leaves the machine, Jeevo only sees a CSR.
package tlsmgr

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/NotNecrotic/jeevo-connector/internal/config"
)

// RenewBefore is how long before expiry a renewal is requested.
const RenewBefore = 30 * 24 * time.Hour

// Manager holds the current certificate.
type Manager struct {
	dir    string
	domain string

	mu      sync.RWMutex
	cert    *tls.Certificate
	leaf    *x509.Certificate
	pending *ecdsa.PrivateKey // key behind the CSR in flight
}

// New loads any existing key/cert from dir.
func New(dir, domain string) *Manager {
	m := &Manager{dir: dir, domain: domain}
	_ = m.load()
	return m
}

func (m *Manager) keyPath() string  { return filepath.Join(m.dir, "key.pem") }
func (m *Manager) certPath() string { return filepath.Join(m.dir, "cert.pem") }
func (m *Manager) nextKeyPath() string {
	return filepath.Join(m.dir, "key.next.pem")
}

func (m *Manager) load() error {
	pair, err := tls.LoadX509KeyPair(m.certPath(), m.keyPath())
	if err != nil {
		return err
	}
	leaf, err := x509.ParseCertificate(pair.Certificate[0])
	if err != nil {
		return err
	}
	if leaf.VerifyHostname(m.domain) != nil {
		return fmt.Errorf("stored certificate is not for %s", m.domain)
	}
	pair.Leaf = leaf
	m.mu.Lock()
	m.cert, m.leaf = &pair, leaf
	m.mu.Unlock()
	return nil
}

// Info returns the current certificate's domain and expiry, or nil.
func (m *Manager) Info() (domain string, notAfter time.Time, ok bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.leaf == nil {
		return "", time.Time{}, false
	}
	return m.domain, m.leaf.NotAfter, true
}

// NeedsCertificate reports whether a (new) certificate should be requested.
func (m *Manager) NeedsCertificate(now time.Time) bool {
	_, na, ok := m.Info()
	return !ok || na.Sub(now) < RenewBefore
}

// HasCertificate reports whether any valid certificate is loaded.
func (m *Manager) HasCertificate() bool {
	_, na, ok := m.Info()
	return ok && time.Now().Before(na)
}

// GetCertificate is for tls.Config.
func (m *Manager) GetCertificate(*tls.ClientHelloInfo) (*tls.Certificate, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.cert == nil {
		return nil, errors.New("no certificate yet")
	}
	return m.cert, nil
}

// NewCSR generates a fresh key (kept in memory and key.next.pem) and returns a PEM CSR for the domain.
func (m *Manager) NewCSR() (string, error) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return "", err
	}
	der, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		return "", err
	}
	if err := config.WriteFileAtomic(m.nextKeyPath(), pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: der}), 0o600); err != nil {
		return "", err
	}
	csr, err := x509.CreateCertificateRequest(rand.Reader, &x509.CertificateRequest{
		Subject:  pkix.Name{CommonName: m.domain},
		DNSNames: []string{m.domain},
	}, key)
	if err != nil {
		return "", err
	}
	m.mu.Lock()
	m.pending = key
	m.mu.Unlock()
	return string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE REQUEST", Bytes: csr})), nil
}

// Install verifies the chain against the pending key and domain, then makes it live.
func (m *Manager) Install(chainPEM string) (time.Time, error) {
	m.mu.RLock()
	key := m.pending
	m.mu.RUnlock()
	if key == nil {
		// The process restarted between CSR and issuance: recover the key.
		raw, err := os.ReadFile(m.nextKeyPath())
		if err != nil {
			return time.Time{}, errors.New("no pending key for this certificate")
		}
		b, _ := pem.Decode(raw)
		if b == nil {
			return time.Time{}, errors.New("pending key unreadable")
		}
		if key, err = x509.ParseECPrivateKey(b.Bytes); err != nil {
			return time.Time{}, err
		}
	}
	der, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		return time.Time{}, err
	}
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: der})
	pair, err := tls.X509KeyPair([]byte(chainPEM), keyPEM)
	if err != nil {
		return time.Time{}, fmt.Errorf("certificate does not match our key: %w", err)
	}
	leaf, err := x509.ParseCertificate(pair.Certificate[0])
	if err != nil {
		return time.Time{}, err
	}
	if err := leaf.VerifyHostname(m.domain); err != nil {
		return time.Time{}, fmt.Errorf("certificate is not for %s", m.domain)
	}
	if time.Now().After(leaf.NotAfter) {
		return time.Time{}, errors.New("certificate already expired")
	}
	if err := config.WriteFileAtomic(m.certPath(), []byte(chainPEM), 0o644); err != nil {
		return time.Time{}, err
	}
	if err := config.WriteFileAtomic(m.keyPath(), keyPEM, 0o600); err != nil {
		return time.Time{}, err
	}
	_ = os.Remove(m.nextKeyPath())
	pair.Leaf = leaf
	m.mu.Lock()
	m.cert, m.leaf, m.pending = &pair, leaf, nil
	m.mu.Unlock()
	return leaf.NotAfter, nil
}
