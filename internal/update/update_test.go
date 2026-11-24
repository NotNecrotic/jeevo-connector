package update

import (
	"context"
	"crypto/ed25519"
	"encoding/base64"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/NotNecrotic/jeevo-connector/internal/version"
)

func TestNewer(t *testing.T) {
	for _, c := range []struct {
		a, b string
		want bool
	}{
		{"1.2.1", "1.2.0", true},
		{"1.10.0", "1.9.9", true},
		{"2.0.0", "1.99.99", true},
		{"1.2.0", "1.2.0", false},
		{"1.2.0", "1.2.1", false},
		{"1.3.0", "1.3.0-beta.2", true},
		{"1.3.0-beta.2", "1.3.0", false},
		{"1.3.0-beta.2", "1.3.0-beta.1", true},
		{"1.3.0-beta.10", "1.3.0-beta.9", true},
		{"1.3.0-beta.1", "1.3.0-beta.1", false},
		{"1.3.0-rc.1", "1.3.0-beta.5", true},
		{"1.3.0-beta", "1.3.0-alpha.3", true},
		{"1.3.0-beta.1", "1.3.0-beta", true},
		{"1.3.0-1", "1.3.0-beta", false},
		{"1.3.0-beta.1", "1.2.9", true},
		{"1.2.0+build.5", "1.2.0", false},
	} {
		if got := Newer(c.a, c.b); got != c.want {
			t.Errorf("Newer(%q, %q) = %v, want %v", c.a, c.b, got, c.want)
		}
	}
}

func TestValidVersion(t *testing.T) {
	for v, want := range map[string]bool{
		"1.2.3":                          true,
		"0.0.0-dev":                      true,
		"1.3.0-beta.1":                   true,
		"1.2.0+build.5":                  true,
		"":                               false,
		".":                              false,
		"..":                             false,
		"1.2":                            false,
		"v1.2.3":                         false,
		"1.2.3/../x":                     false,
		"1.2.3-.":                        false,
		".1.2.3":                         false,
		"1.2.3-beta..1":                  false,
		strings.Repeat("1", 41) + ".0.0": false,
	} {
		if got := validVersion(v); got != want {
			t.Errorf("validVersion(%q) = %v, want %v", v, got, want)
		}
	}
}

// signed returns a manifest for ver and a key-trusting Updater.
func signed(t *testing.T, ver string) (*Updater, string, string) {
	t.Helper()
	pub, priv, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatal(err)
	}
	manifest := `{"version":"` + ver + `","artifacts":[]}`
	sig := base64.StdEncoding.EncodeToString(ed25519.Sign(priv, []byte(manifest)))
	u := &Updater{InstallDir: t.TempDir(), Keys: []string{base64.StdEncoding.EncodeToString(pub)}}
	return u, manifest, sig
}

func TestInstallRefusesNotNewer(t *testing.T) {
	for _, ver := range []string{version.Version, "0.0.0-alpha", "0.0.0-dev+other"} {
		u, manifest, sig := signed(t, ver)
		_, err := u.Install(context.Background(), manifest, sig, func(string, string, string) {})
		if err == nil || !strings.Contains(err.Error(), "not newer") {
			t.Errorf("Install(%q) = %v, want a not-newer refusal", ver, err)
		}
	}
	// Force skips the version check; this manifest then fails for lacking an artifact for this machine.
	u, manifest, sig := signed(t, version.Version)
	u.Force = true
	_, err := u.Install(context.Background(), manifest, sig, func(string, string, string) {})
	if err == nil || strings.Contains(err.Error(), "not newer") {
		t.Errorf("forced Install = %v, want a failure other than not-newer", err)
	}
}

func TestPruneRemovesStaleDownloads(t *testing.T) {
	u := &Updater{InstallDir: t.TempDir()}
	rel := filepath.Join(u.InstallDir, "releases")
	for _, d := range []string{"1.0.0", "1.1.0", "0.9.0"} {
		if err := os.MkdirAll(filepath.Join(rel, d), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	for _, f := range []string{".download-123", ".install.lock"} {
		if err := os.WriteFile(filepath.Join(rel, f), nil, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	u.prune("1.1.0", "1.0.0")
	var got []string
	ents, _ := os.ReadDir(rel)
	for _, e := range ents {
		got = append(got, e.Name())
	}
	if want := ".install.lock 1.0.0 1.1.0"; strings.Join(got, " ") != want {
		t.Errorf("after prune: %v, want %s", got, want)
	}
}
