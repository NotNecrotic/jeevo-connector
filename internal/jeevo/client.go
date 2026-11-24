// Package jeevo is the connector's HTTP client for discovery and enrolment.
package jeevo

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"runtime"
	"strings"
	"time"

	"github.com/NotNecrotic/jeevo-connector/internal/protocol"
	"github.com/NotNecrotic/jeevo-connector/internal/version"
)

var httpClient = &http.Client{Timeout: 30 * time.Second}

// secureURL allows https always, and plain http/ws only to the local machine (development).
func secureURL(u *url.URL) bool {
	switch u.Scheme {
	case "https", "wss":
		return true
	case "http", "ws":
		switch u.Hostname() {
		case "localhost", "127.0.0.1", "::1":
			return true
		}
	}
	return false
}

// NormalizeURL turns "jeevo.net" or "https://jeevo.net/" into "https://jeevo.net".
func NormalizeURL(raw string) (string, error) {
	raw = strings.TrimSpace(raw)
	if !strings.Contains(raw, "://") {
		raw = "https://" + raw
	}
	u, err := url.Parse(raw)
	if err != nil || u.Host == "" {
		return "", fmt.Errorf("%q is not a valid URL", raw)
	}
	if u.Scheme != "https" && u.Scheme != "http" || !secureURL(u) {
		return "", errors.New("the Jeevo URL must be https:// (http:// is only accepted for localhost)")
	}
	return u.Scheme + "://" + u.Host, nil
}

// Discover fetches the discovery document.
func Discover(ctx context.Context, base string) (*protocol.Discovery, error) {
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, base+"/.well-known/jeevo-connector.json", nil)
	req.Header.Set("Accept", "application/json")
	resp, err := httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("cannot reach %s: %w", base, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("%s does not look like a Jeevo server (discovery HTTP %d)", base, resp.StatusCode)
	}
	var d protocol.Discovery
	if err := json.NewDecoder(io.LimitReader(resp.Body, 1<<20)).Decode(&d); err != nil {
		return nil, fmt.Errorf("discovery document: %w", err)
	}
	if d.WsUrl == "" || d.ApiBaseUrl == "" || d.ReleasesUrl == "" {
		return nil, errors.New("discovery document is incomplete")
	}
	for _, raw := range []string{d.WsUrl, d.ApiBaseUrl, d.ReleasesUrl} {
		if u, err := url.Parse(raw); err != nil || !secureURL(u) {
			return nil, fmt.Errorf("discovery document points at %q, which is not https", raw)
		}
	}
	if protocol.ProtocolVersion < d.MinProtocolVersion {
		return nil, fmt.Errorf("this connector (protocol %d) is too old for this Jeevo server (needs %d): run `jeevo-connector update`",
			protocol.ProtocolVersion, d.MinProtocolVersion)
	}
	return &d, nil
}

// APIError is an error body from Jeevo.
type APIError struct {
	Status  int
	Code    string
	Message string
}

func (e *APIError) Error() string { return e.Message }

// Enroll exchanges a license key for credentials.
func Enroll(ctx context.Context, apiBase, license string) (*protocol.EnrollResponse, error) {
	host, _ := os.Hostname()
	body, _ := json.Marshal(protocol.EnrollRequest{
		LicenseKey: strings.ToUpper(strings.TrimSpace(license)), Version: version.Version,
		ProtocolVersion: protocol.ProtocolVersion, Os: runtime.GOOS, Arch: runtime.GOARCH, Hostname: host,
	})
	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, apiBase+"/api/connector/v1/enroll", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	resp, err := httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if resp.StatusCode != 200 {
		var e protocol.ErrorBody
		if json.Unmarshal(raw, &e) == nil && e.Error.Message != "" {
			return nil, &APIError{Status: resp.StatusCode, Code: e.Error.Code, Message: e.Error.Message}
		}
		return nil, &APIError{Status: resp.StatusCode, Message: fmt.Sprintf("enrolment failed (HTTP %d)", resp.StatusCode)}
	}
	var out protocol.EnrollResponse
	if err := json.Unmarshal(raw, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// PinReleaseKeys keeps only release keys matching the out-of-band sha256 fingerprint; empty keeps all.
func PinReleaseKeys(keys []string, fingerprint string) ([]string, error) {
	fingerprint = strings.ToLower(strings.ReplaceAll(strings.TrimSpace(fingerprint), ":", ""))
	if fingerprint == "" {
		return keys, nil
	}
	var out []string
	for _, k := range keys {
		raw, err := base64.StdEncoding.DecodeString(k)
		if err != nil {
			continue
		}
		if sum := sha256.Sum256(raw); hex.EncodeToString(sum[:]) == fingerprint {
			out = append(out, k)
		}
	}
	if len(out) == 0 {
		return nil, errors.New("none of this Jeevo server's release signing keys matches JEEVO_RELEASE_KEY_FINGERPRINT")
	}
	return out, nil
}
