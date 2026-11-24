// Package update installs signed connector releases atomically and rolls back after three failed boots.
package update

import (
	"archive/tar"
	"compress/gzip"
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/NotNecrotic/jeevo-connector/internal/config"
	"github.com/NotNecrotic/jeevo-connector/internal/protocol"
	"github.com/NotNecrotic/jeevo-connector/internal/version"
)

// Status is reported back to Jeevo as the update progresses.
type Status func(state, version, errMsg string)

// Updater performs installs.
type Updater struct {
	InstallDir  string
	ReleasesURL string // initial value; change a running updater with SetReleasesURL
	Keys        []string
	HTTP        *http.Client
	Log         *slog.Logger
	// Force allows a downgrade or reinstall; only the CLI's --force sets it, so a replayed signed manifest cannot downgrade.
	Force bool

	mu        sync.RWMutex
	installMu sync.Mutex
}

// SetReleasesURL changes the release server URL of a running updater.
func (u *Updater) SetReleasesURL(url string) {
	u.mu.Lock()
	u.ReleasesURL = url
	u.mu.Unlock()
}

func (u *Updater) releasesURL() string {
	u.mu.RLock()
	defer u.mu.RUnlock()
	return u.ReleasesURL
}

// VerifyEnvelope checks the signature and returns the parsed manifest.
func (u *Updater) VerifyEnvelope(manifest, signature string) (*protocol.ReleaseManifest, error) {
	sig, err := base64.StdEncoding.DecodeString(strings.TrimSpace(signature))
	if err != nil || len(sig) != ed25519.SignatureSize {
		return nil, errors.New("release signature is malformed")
	}
	ok := false
	for _, k := range u.Keys {
		pub, err := base64.StdEncoding.DecodeString(k)
		if err == nil && len(pub) == ed25519.PublicKeySize && ed25519.Verify(ed25519.PublicKey(pub), []byte(manifest), sig) {
			ok = true
			break
		}
	}
	if !ok {
		return nil, errors.New("release manifest is not signed by a trusted key")
	}
	var m protocol.ReleaseManifest
	if err := json.Unmarshal([]byte(manifest), &m); err != nil {
		return nil, fmt.Errorf("release manifest: %w", err)
	}
	if !validVersion(m.Version) {
		return nil, fmt.Errorf("release version %q is not valid", m.Version)
	}
	return &m, nil
}

// Fetch downloads the current release envelope for a channel.
func (u *Updater) Fetch(ctx context.Context, channel string) (manifest, signature string, err error) {
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, u.releasesURL()+"/"+channel, nil)
	resp, err := u.client().Do(req)
	if err != nil {
		return "", "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return "", "", fmt.Errorf("release channel %s: HTTP %d", channel, resp.StatusCode)
	}
	var env protocol.ReleaseEnvelope
	if err := json.NewDecoder(io.LimitReader(resp.Body, 1<<20)).Decode(&env); err != nil {
		return "", "", err
	}
	return env.Manifest, env.Signature, nil
}

func (u *Updater) client() *http.Client {
	if u.HTTP != nil {
		return u.HTTP
	}
	return &http.Client{Timeout: 30 * time.Minute}
}

// Artifact picks the artifact for this machine.
func Artifact(m *protocol.ReleaseManifest) (*protocol.ReleaseArtifact, error) {
	for _, a := range m.Artifacts {
		if string(a.Os) == runtime.GOOS && string(a.Arch) == runtime.GOARCH {
			if strings.ContainsAny(a.File, "/\\") || strings.HasPrefix(a.File, ".") {
				return nil, errors.New("artifact file name is unsafe")
			}
			return &a, nil
		}
	}
	return nil, fmt.Errorf("release %s has no build for %s/%s", m.Version, runtime.GOOS, runtime.GOARCH)
}

// Install downloads, verifies and activates a release; it does not restart the process.
func (u *Updater) Install(ctx context.Context, manifest, signature string, status Status) (string, error) {
	m, err := u.VerifyEnvelope(manifest, signature)
	if err != nil {
		return "", err
	}
	if !u.Force && compareVersions(m.Version, version.Version) <= 0 {
		return "", fmt.Errorf("version %s is not newer than the running %s", m.Version, version.Version)
	}
	// A newer protocol is fine; only guard against a broken manifest.
	if m.ProtocolVersion < 1 || m.ProtocolVersion > 1000 {
		return "", errors.New("release protocol version is implausible")
	}
	art, err := Artifact(m)
	if err != nil {
		return "", err
	}
	if !u.installMu.TryLock() {
		return "", errBusy
	}
	defer u.installMu.Unlock()
	relDir := filepath.Join(u.InstallDir, "releases")
	if err := os.MkdirAll(relDir, 0o755); err != nil {
		return "", err
	}
	// The service and CLI share the staging directory, so they also exclude each other with a lock file.
	lock, err := os.OpenFile(filepath.Join(relDir, ".install.lock"), os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return "", err
	}
	defer lock.Close()
	if err := syscall.Flock(int(lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		return "", errBusy
	}
	status("downloading", m.Version, "")
	tmp, err := os.CreateTemp(relDir, ".download-*")
	if err != nil {
		return "", err
	}
	defer os.Remove(tmp.Name())
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, fmt.Sprintf("%s/files/%s/%s", u.releasesURL(), m.Version, art.File), nil)
	resp, err := u.client().Do(req)
	if err != nil {
		tmp.Close()
		return "", err
	}
	if resp.StatusCode != 200 {
		resp.Body.Close()
		tmp.Close()
		return "", fmt.Errorf("download: HTTP %d", resp.StatusCode)
	}
	h := sha256.New()
	n, err := io.Copy(io.MultiWriter(tmp, h), io.LimitReader(resp.Body, int64(art.Size)+1))
	resp.Body.Close()
	tmp.Close()
	if err != nil {
		return "", err
	}
	status("verifying", m.Version, "")
	if n != int64(art.Size) || hex.EncodeToString(h.Sum(nil)) != art.Sha256 {
		return "", errors.New("downloaded artifact does not match the signed manifest")
	}
	status("installing", m.Version, "")
	dest := filepath.Join(relDir, m.Version)
	stage := dest + ".staging"
	os.RemoveAll(stage)
	if err := extract(tmp.Name(), stage); err != nil {
		os.RemoveAll(stage)
		return "", fmt.Errorf("extract: %w", err)
	}
	bin := filepath.Join(stage, "jeevo-connector")
	if st, err := os.Stat(bin); err != nil || st.Mode()&0o111 == 0 {
		os.RemoveAll(stage)
		return "", errors.New("release does not contain an executable jeevo-connector")
	}
	os.RemoveAll(dest)
	if err := os.Rename(stage, dest); err != nil {
		return "", err
	}
	if err := u.syncWorkerDeps(ctx, dest); err != nil {
		// The old worker deps usually still work; record and continue.
		u.Log.Warn("ai worker dependencies were not updated", "err", err)
	}
	prev, _ := os.Readlink(filepath.Join(u.InstallDir, "current"))
	if err := u.switchTo(m.Version); err != nil {
		return "", err
	}
	_ = config.WriteJSONAtomic(u.statePath(), State{Version: m.Version, Previous: prev, Attempts: 0}, 0o644)
	u.prune(m.Version, filepath.Base(prev))
	return m.Version, nil
}

var errBusy = errors.New("another update is already in progress")

// versionRE is MAJOR.MINOR.PATCH with optional parts; it must start with a digit as it becomes a directory name.
var versionRE = regexp.MustCompile(`^[0-9]+\.[0-9]+\.[0-9]+(-[0-9A-Za-z-]+(\.[0-9A-Za-z-]+)*)?(\+[0-9A-Za-z-]+(\.[0-9A-Za-z-]+)*)?$`)

func validVersion(v string) bool { return len(v) <= 40 && versionRE.MatchString(v) }

// extract unpacks a .tar.gz into dir, refusing anything that could escape it.
func extract(archive, dir string) error {
	f, err := os.Open(archive)
	if err != nil {
		return err
	}
	defer f.Close()
	gz, err := gzip.NewReader(f)
	if err != nil {
		return err
	}
	tr := tar.NewReader(gz)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	var total int64
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		name := filepath.Clean(strings.TrimPrefix(hdr.Name, "./"))
		if name == "." {
			continue
		}
		if filepath.IsAbs(name) || name == ".." || strings.HasPrefix(name, "../") {
			return fmt.Errorf("unsafe path in archive: %s", hdr.Name)
		}
		target := filepath.Join(dir, name)
		switch hdr.Typeflag {
		case tar.TypeDir:
			if err := os.MkdirAll(target, 0o755); err != nil {
				return err
			}
		case tar.TypeReg:
			total += hdr.Size
			if total > 2<<30 {
				return errors.New("archive too large")
			}
			if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
				return err
			}
			mode := os.FileMode(0o644)
			if hdr.Mode&0o111 != 0 {
				mode = 0o755
			}
			out, err := os.OpenFile(target, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, mode)
			if err != nil {
				return err
			}
			if _, err := io.CopyN(out, tr, hdr.Size); err != nil {
				out.Close()
				return err
			}
			out.Close()
		default:
			// Symlinks, devices, etc. are never part of a release.
			return fmt.Errorf("unsupported entry in archive: %s", hdr.Name)
		}
	}
}

func (u *Updater) switchTo(ver string) error {
	link := filepath.Join(u.InstallDir, "current")
	tmp := link + ".new"
	os.Remove(tmp)
	if err := os.Symlink(filepath.Join("releases", ver), tmp); err != nil {
		return err
	}
	return os.Rename(tmp, link)
}

// syncWorkerDeps reinstalls the AI worker's Python deps when requirements changed (cpu/gpu flavour picks the file).
func (u *Updater) syncWorkerDeps(ctx context.Context, releaseDir string) error {
	pip := filepath.Join(u.InstallDir, "venv", "bin", "pip")
	if _, err := os.Stat(pip); err != nil {
		return nil // no worker venv on this host
	}
	reqName := "requirements-cpu.txt"
	if raw, err := os.ReadFile(filepath.Join(u.InstallDir, "venv", ".jeevo-requirements")); err == nil && strings.TrimSpace(string(raw)) == "gpu" {
		reqName = "requirements-gpu.txt"
	}
	req := filepath.Join(releaseDir, "ai", reqName)
	data, err := os.ReadFile(req)
	if err != nil {
		return nil
	}
	sum := sha256.Sum256(data)
	marker := filepath.Join(u.InstallDir, "venv", ".jeevo-requirements.sha256")
	if old, _ := os.ReadFile(marker); strings.TrimSpace(string(old)) == hex.EncodeToString(sum[:]) {
		return nil
	}
	cctx, cancel := context.WithTimeout(ctx, 20*time.Minute)
	defer cancel()
	cmd := exec.CommandContext(cctx, pip, "install", "--disable-pip-version-check", "-q", "-r", req)
	if out, err := cmd.CombinedOutput(); err != nil {
		return fmt.Errorf("pip: %v: %s", err, tail(string(out)))
	}
	return os.WriteFile(marker, []byte(hex.EncodeToString(sum[:])+"\n"), 0o644)
}

func tail(s string) string {
	if len(s) > 500 {
		return s[len(s)-500:]
	}
	return s
}

// prune keeps the active and previous releases and removes leftover downloads (the install lock is held).
func (u *Updater) prune(keep ...string) {
	dir := filepath.Join(u.InstallDir, "releases")
	ents, err := os.ReadDir(dir)
	if err != nil {
		return
	}
	k := map[string]bool{}
	for _, v := range keep {
		k[v] = true
	}
	for _, e := range ents {
		switch {
		case !e.IsDir() && strings.HasPrefix(e.Name(), ".download-"):
			os.Remove(filepath.Join(dir, e.Name()))
		case e.IsDir() && !k[e.Name()] && !strings.HasPrefix(e.Name(), "."):
			os.RemoveAll(filepath.Join(dir, e.Name()))
		}
	}
}

// ---- rollback bookkeeping ---------------------------------------------------

// State tracks an unconfirmed update.
type State struct {
	Version   string `json:"version"`
	Previous  string `json:"previous"`
	Attempts  int    `json:"attempts"`
	Confirmed bool   `json:"confirmed"`
}

func (u *Updater) statePath() string { return filepath.Join(u.InstallDir, "update-state.json") }

// Boot counts boots of an unconfirmed update and rolls back after three; true means exit so systemd restarts.
func (u *Updater) Boot() bool {
	raw, err := os.ReadFile(u.statePath())
	if err != nil {
		return false
	}
	var st State
	if json.Unmarshal(raw, &st) != nil || st.Confirmed || st.Version != version.Version {
		return false
	}
	st.Attempts++
	if st.Attempts > 3 && st.Previous != "" {
		u.Log.Error("new version never reached Jeevo; rolling back", "version", st.Version, "to", st.Previous)
		link := filepath.Join(u.InstallDir, "current")
		tmp := link + ".new"
		os.Remove(tmp)
		if os.Symlink(st.Previous, tmp) == nil && os.Rename(tmp, link) == nil {
			st.Confirmed = true
			_ = config.WriteJSONAtomic(u.statePath(), st, 0o644)
			return true
		}
	}
	_ = config.WriteJSONAtomic(u.statePath(), st, 0o644)
	return false
}

// Confirm marks the running version as good (it reached Jeevo).
func (u *Updater) Confirm() {
	raw, err := os.ReadFile(u.statePath())
	if err != nil {
		return
	}
	var st State
	if json.Unmarshal(raw, &st) != nil || st.Confirmed || st.Version != version.Version {
		return
	}
	st.Confirmed = true
	_ = config.WriteJSONAtomic(u.statePath(), st, 0o644)
}

// Newer reports whether a is higher than b in semver order (pre-releases rank lower, build metadata ignored).
func Newer(a, b string) bool { return compareVersions(a, b) > 0 }

func compareVersions(a, b string) int {
	a, _, _ = strings.Cut(a, "+")
	b, _, _ = strings.Cut(b, "+")
	ca, pa, _ := strings.Cut(a, "-")
	cb, pb, _ := strings.Cut(b, "-")
	na, nb := parts(ca), parts(cb)
	for i := 0; i < len(na) || i < len(nb); i++ {
		var x, y int
		if i < len(na) {
			x = na[i]
		}
		if i < len(nb) {
			y = nb[i]
		}
		if x != y {
			return cmpInt(x, y)
		}
	}
	switch {
	case pa == pb:
		return 0
	case pa == "":
		return 1
	case pb == "":
		return -1
	}
	ia, ib := strings.Split(pa, "."), strings.Split(pb, ".")
	for i := 0; i < len(ia) && i < len(ib); i++ {
		x, errX := strconv.Atoi(ia[i])
		y, errY := strconv.Atoi(ib[i])
		switch {
		case errX == nil && errY == nil:
			if x != y {
				return cmpInt(x, y)
			}
		case errX == nil: // numeric identifiers sort before alphanumeric ones
			return -1
		case errY == nil:
			return 1
		default:
			if c := strings.Compare(ia[i], ib[i]); c != 0 {
				return c
			}
		}
	}
	return cmpInt(len(ia), len(ib))
}

func cmpInt(x, y int) int {
	switch {
	case x < y:
		return -1
	case x > y:
		return 1
	}
	return 0
}

func parts(v string) []int {
	var out []int
	for _, s := range strings.Split(v, ".") {
		n, _ := strconv.Atoi(s)
		out = append(out, n)
	}
	return out
}

// Releases lists installed releases (for `status`).
func (u *Updater) Releases() []string {
	ents, _ := os.ReadDir(filepath.Join(u.InstallDir, "releases"))
	var out []string
	for _, e := range ents {
		if e.IsDir() && !strings.HasPrefix(e.Name(), ".") {
			out = append(out, e.Name())
		}
	}
	sort.Strings(out)
	return out
}
