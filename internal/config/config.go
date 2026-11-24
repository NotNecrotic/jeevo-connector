// Package config holds the Install (written by enroll) and the owner-tunable Settings.
package config

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"regexp"
	"sort"

	"github.com/NotNecrotic/jeevo-connector/internal/protocol"
)

// DefaultConfigPath is where the installer puts the install config.
const DefaultConfigPath = "/etc/jeevo-connector/connector.json"

// Install is the enrolment-time configuration.
type Install struct {
	// JeevoURL is the one URL the installer was given; everything else is rediscovered from it.
	JeevoURL   string `json:"jeevoUrl"`
	InstallDir string `json:"installDir"`
	DataDir    string `json:"dataDir"`

	ConnectorID       string   `json:"connectorId"`
	ConnectorSecret   string   `json:"connectorSecret"`
	Name              string   `json:"name"`
	Domain            string   `json:"domain"`
	PlaybackPublicKey string   `json:"playbackPublicKey"`
	ReleaseKeys       []string `json:"releaseSigningKeys"`

	// Cached discovery results; refreshed at start, used when Jeevo is down.
	APIBaseURL  string `json:"apiBaseUrl"`
	WSURL       string `json:"wsUrl"`
	ReleasesURL string `json:"releasesUrl"`

	// AIDir overrides the directory holding the ai/ package (development); default <installDir>/current.
	AIDir string `json:"aiDir,omitempty"`
	// PythonPath overrides <installDir>/venv/bin/python (development).
	PythonPath string `json:"pythonPath,omitempty"`
	// FFmpegPath / FFprobePath override PATH lookup.
	FFmpegPath  string `json:"ffmpegPath,omitempty"`
	FFprobePath string `json:"ffprobePath,omitempty"`

	path string
}

var idPattern = regexp.MustCompile(`^[a-z2-7]{4}$`)
var secretPattern = regexp.MustCompile(`^[a-f0-9]{64}$`)

// LoadInstall reads and validates the install config.
func LoadInstall(path string) (*Install, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, fmt.Errorf("no connector config at %s: run `jeevo-connector enroll` first", path)
		}
		return nil, err
	}
	var c Install
	if err := json.Unmarshal(raw, &c); err != nil {
		return nil, fmt.Errorf("%s is not valid JSON: %w", path, err)
	}
	c.path = path
	if err := c.Validate(); err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	return &c, nil
}

// Validate checks the fields every running connector needs.
func (c *Install) Validate() error {
	switch {
	case c.JeevoURL == "":
		return errors.New("jeevoUrl is missing")
	case c.DataDir == "" || !filepath.IsAbs(c.DataDir):
		return errors.New("dataDir must be an absolute path")
	case c.InstallDir == "" || !filepath.IsAbs(c.InstallDir):
		return errors.New("installDir must be an absolute path")
	case !idPattern.MatchString(c.ConnectorID):
		return errors.New("connectorId is missing or malformed (re-run enroll)")
	case !secretPattern.MatchString(c.ConnectorSecret):
		return errors.New("connectorSecret is missing or malformed (re-run enroll)")
	case c.PlaybackPublicKey == "":
		return errors.New("playbackPublicKey is missing (re-run enroll)")
	}
	return nil
}

// Path is the file this config was loaded from.
func (c *Install) Path() string { return c.path }

// Identity names the credentials in use, so a revocation binds to them, not the data directory.
func (c *Install) Identity() string {
	sum := sha256.Sum256([]byte(c.ConnectorID + "." + c.ConnectorSecret))
	return hex.EncodeToString(sum[:])
}

// Save writes the config atomically with 0600 permissions.
func (c *Install) Save(path string) error {
	if path == "" {
		path = c.path
	}
	c.path = path
	return WriteJSONAtomic(path, c, 0o600)
}

// Layout of the data directory.
func (c *Install) StateDir() string   { return filepath.Join(c.DataDir, "state") }
func (c *Install) MediaDir() string   { return filepath.Join(c.DataDir, "media") }
func (c *Install) UploadsDir() string { return filepath.Join(c.DataDir, "uploads") }
func (c *Install) TLSDir() string     { return filepath.Join(c.StateDir(), "tls") }
func (c *Install) DBPath() string     { return filepath.Join(c.StateDir(), "connector.db") }
func (c *Install) SettingsPath() string {
	return filepath.Join(c.StateDir(), "settings.json")
}
func (c *Install) ModelCacheDir() string { return filepath.Join(c.DataDir, "models") }

// AIRoot is the directory containing the ai/ package; the worker runs as `python -m ai` from it.
func (c *Install) AIRoot() string {
	if c.AIDir != "" {
		return c.AIDir
	}
	return filepath.Join(c.InstallDir, "current")
}

// Python is the interpreter of the worker's virtualenv.
func (c *Install) Python() string {
	if c.PythonPath != "" {
		return c.PythonPath
	}
	return filepath.Join(c.InstallDir, "venv", "bin", "python")
}

// EnsureDirs creates the data directory layout with private permissions.
func (c *Install) EnsureDirs() error {
	for _, d := range []string{c.DataDir, c.StateDir(), c.MediaDir(), c.UploadsDir(), c.TLSDir(), c.ModelCacheDir()} {
		if err := os.MkdirAll(d, 0o750); err != nil {
			return fmt.Errorf("create %s: %w", d, err)
		}
	}
	return os.Chmod(c.TLSDir(), 0o700)
}

// WriteJSONAtomic writes v as indented JSON via a temp file and rename.
func WriteJSONAtomic(path string, v any, mode os.FileMode) error {
	raw, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}
	return WriteFileAtomic(path, append(raw, '\n'), mode)
}

// WriteFileAtomic writes via temp file, fsync and rename so readers never see a partial file.
func WriteFileAtomic(path string, data []byte, mode os.FileMode) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
		return err
	}
	f, err := os.CreateTemp(filepath.Dir(path), "."+filepath.Base(path)+".tmp-*")
	if err != nil {
		return err
	}
	tmp := f.Name()
	defer os.Remove(tmp)
	if _, err := f.Write(data); err != nil {
		f.Close()
		return err
	}
	if err := f.Chmod(mode); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

// ---- settings -------------------------------------------------------------

// Settings aliases the generated protocol type shared with the server.
type Settings = protocol.ConnectorSettings

// StoredSettings is what settings.json holds.
type StoredSettings struct {
	Revision int      `json:"revision"`
	Settings Settings `json:"settings"`
}

// DefaultListenPort is outside the well-known and ephemeral ranges: no privileges, few collisions.
const DefaultListenPort = 28443

// DefaultSettings must match the Jeevo server's defaults for ConnectorSettings.
func DefaultSettings() Settings {
	return Settings{
		Network: protocol.NetworkSettings{
			Mode:          protocol.NetworkSettingsModeAuto,
			ListenPort:    DefaultListenPort,
			PublicPort:    DefaultListenPort,
			PublicAddress: "",
		},
		Uploads: protocol.UploadSettings{MaxUploadMb: 4096},
		Processing: protocol.ProcessingSettings{
			HlsEncoder:   protocol.ProcessingSettingsHlsEncoderAuto,
			HlsQualities: []int{1080, 720, 480, 360},
		},
		Ai:      protocol.AiSettings{Device: protocol.AiSettingsDeviceAuto},
		Updates: protocol.UpdateSettings{Channel: protocol.Stable, AutoUpdate: false},
		Logging: protocol.LoggingSettings{Level: protocol.LoggingSettingsLevelInfo},
	}
}

func oneOf[T comparable](v T, allowed ...T) bool {
	for _, a := range allowed {
		if v == a {
			return true
		}
	}
	return false
}

// ValidateSettings applies the OpenAPI bounds again rather than trusting the server.
func ValidateSettings(s *Settings) error {
	n := s.Network
	if !oneOf(n.Mode, protocol.NetworkSettingsModeAuto, protocol.NetworkSettingsModeDirect, protocol.NetworkSettingsModeProxy) {
		return fmt.Errorf("network.mode %q is not valid", n.Mode)
	}
	if n.ListenPort < 1 || n.ListenPort > 65535 || n.PublicPort < 1 || n.PublicPort > 65535 {
		return errors.New("network ports must be 1-65535")
	}
	if n.PublicAddress != "" && net.ParseIP(n.PublicAddress) == nil {
		return fmt.Errorf("network.publicAddress %q is not an IP address", n.PublicAddress)
	}
	if s.Uploads.MaxUploadMb < 1 || s.Uploads.MaxUploadMb > 65536 {
		return errors.New("uploads.maxUploadMb out of range")
	}
	p := s.Processing
	if !oneOf(p.HlsEncoder, protocol.ProcessingSettingsHlsEncoderAuto, protocol.ProcessingSettingsHlsEncoderLibx264,
		protocol.ProcessingSettingsHlsEncoderH264Nvenc, protocol.ProcessingSettingsHlsEncoderH264Qsv) {
		return fmt.Errorf("processing.hlsEncoder %q is not valid", p.HlsEncoder)
	}
	if len(p.HlsQualities) < 1 || len(p.HlsQualities) > 8 {
		return errors.New("processing.hlsQualities needs 1-8 entries")
	}
	for _, q := range p.HlsQualities {
		if q < 144 || q > 4320 || q%2 != 0 {
			return fmt.Errorf("processing.hlsQualities: %d must be an even number from 144 to 4320", q)
		}
	}
	if !oneOf(s.Ai.Device, protocol.AiSettingsDeviceAuto, protocol.AiSettingsDeviceCpu, protocol.AiSettingsDeviceCuda) {
		return fmt.Errorf("ai.device %q is not valid", s.Ai.Device)
	}
	if !oneOf(s.Updates.Channel, protocol.Stable, protocol.Beta) {
		return errors.New("updates.channel is not valid")
	}
	if !oneOf(s.Logging.Level, protocol.LoggingSettingsLevelDebug, protocol.LoggingSettingsLevelInfo, protocol.LoggingSettingsLevelWarn, protocol.LoggingSettingsLevelError) {
		return errors.New("logging.level is not valid")
	}
	return nil
}

// NormalizeSettings sorts/dedupes the quality ladder (best first).
func NormalizeSettings(s *Settings) {
	seen := map[int]bool{}
	out := []int{}
	for _, q := range s.Processing.HlsQualities {
		if !seen[q] {
			seen[q] = true
			out = append(out, q)
		}
	}
	sort.Sort(sort.Reverse(sort.IntSlice(out)))
	s.Processing.HlsQualities = out
}

// LoadSettings returns the persisted settings, or defaults at revision 0.
func LoadSettings(path string) StoredSettings {
	raw, err := os.ReadFile(path)
	if err == nil {
		var st StoredSettings
		if json.Unmarshal(raw, &st) == nil && ValidateSettings(&st.Settings) == nil {
			return st
		}
	}
	return StoredSettings{Revision: 0, Settings: DefaultSettings()}
}

// SaveSettings persists settings atomically.
func SaveSettings(path string, st StoredSettings) error {
	return WriteJSONAtomic(path, st, 0o640)
}
