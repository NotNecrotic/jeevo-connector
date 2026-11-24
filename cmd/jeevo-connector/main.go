package main

import (
	"bufio"
	"context"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/NotNecrotic/jeevo-connector/internal/app"
	"github.com/NotNecrotic/jeevo-connector/internal/config"
	"github.com/NotNecrotic/jeevo-connector/internal/jeevo"
	"github.com/NotNecrotic/jeevo-connector/internal/media"
	"github.com/NotNecrotic/jeevo-connector/internal/protocol"
	"github.com/NotNecrotic/jeevo-connector/internal/store"
	"github.com/NotNecrotic/jeevo-connector/internal/sysinfo"
	"github.com/NotNecrotic/jeevo-connector/internal/update"
	"github.com/NotNecrotic/jeevo-connector/internal/version"
)

const serviceName = "jeevo-connector"

func usage() {
	fmt.Fprintf(os.Stderr, `jeevo-connector %s

Usage:
  jeevo-connector run      [--config PATH]
  jeevo-connector enroll   --url URL [--license KEY] [--install-dir DIR] [--data-dir DIR] [--config PATH] [--force]
                           [--release-key-fingerprint SHA256]
  jeevo-connector status   [--config PATH]
  jeevo-connector update   [--config PATH] [--channel stable|beta] [--force] [--no-restart]
  jeevo-connector doctor   [--config PATH]
  jeevo-connector version

The config path defaults to $JEEVO_CONNECTOR_CONFIG or %s.
The license key can also be given in $JEEVO_LICENSE_KEY (it then stays out of the process list).
$JEEVO_RELEASE_KEY_FINGERPRINT pins the release signing key at enrolment.
`, version.Version, config.DefaultConfigPath)
}

func defaultConfigPath() string {
	if p := os.Getenv("JEEVO_CONNECTOR_CONFIG"); p != "" {
		return p
	}
	return config.DefaultConfigPath
}

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}
	cmd, args := os.Args[1], os.Args[2:]
	var err error
	if cmd != "version" && cmd != "--version" && cmd != "-v" && cmd != "help" && cmd != "--help" && cmd != "-h" &&
		os.Geteuid() == 0 && os.Getenv("JEEVO_ALLOW_ROOT") != "1" {
		fmt.Fprintf(os.Stderr, "error: refusing to run %q as root: the install directory belongs to the %s user. Use `sudo jeevo-connector %s` (it runs as that user), or run as that user.\n", cmd, serviceName, cmd)
		os.Exit(1)
	}
	switch cmd {
	case "run":
		err = cmdRun(args)
	case "enroll":
		err = cmdEnroll(args)
	case "status":
		err = cmdStatus(args)
	case "update":
		err = cmdUpdate(args)
	case "doctor":
		err = cmdDoctor(args)
	case "version", "--version", "-v":
		fmt.Println(version.Version)
	case "help", "--help", "-h":
		usage()
	default:
		usage()
		os.Exit(2)
	}
	if err != nil {
		if errors.Is(err, app.ErrRestart) {
			os.Exit(0)
		}
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

func cmdRun(args []string) error {
	fs := flag.NewFlagSet("run", flag.ExitOnError)
	cfgPath := fs.String("config", defaultConfigPath(), "config file")
	_ = fs.Parse(args)
	cfg, err := config.LoadInstall(*cfgPath)
	if err != nil {
		return err
	}
	level := new(slog.LevelVar)
	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: level}))
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	a := &app.App{Cfg: cfg, Log: log, LogLevel: level}
	err = a.Run(ctx)
	if errors.Is(err, app.ErrRestart) {
		log.Info("exiting for restart")
	}
	return err
}

func prompt(q string) string {
	fmt.Fprint(os.Stderr, q)
	r := bufio.NewReader(os.Stdin)
	s, _ := r.ReadString('\n')
	return strings.TrimSpace(s)
}

func cmdEnroll(args []string) error {
	fs := flag.NewFlagSet("enroll", flag.ExitOnError)
	cfgPath := fs.String("config", defaultConfigPath(), "config file to write")
	rawURL := fs.String("url", "", "the Jeevo server URL")
	license := fs.String("license", "", "license key (default $JEEVO_LICENSE_KEY; prompted when omitted)")
	pin := fs.String("release-key-fingerprint", os.Getenv("JEEVO_RELEASE_KEY_FINGERPRINT"), "sha256 (hex) of the release signing key to trust; other keys are ignored")
	installDir := fs.String("install-dir", "/opt/jeevo-connector", "installation directory")
	dataDir := fs.String("data-dir", "/var/lib/jeevo-connector", "data directory (media lives here)")
	force := fs.Bool("force", false, "replace an existing enrolment (re-key)")
	_ = fs.Parse(args)
	if *license == "" {
		*license = os.Getenv("JEEVO_LICENSE_KEY")
	}

	if existing, err := config.LoadInstall(*cfgPath); err == nil && !*force {
		return fmt.Errorf("already enrolled as %s (%s); use --force with a new license key to re-enrol", existing.ConnectorID, existing.Domain)
	}
	if *rawURL == "" {
		return errors.New("--url is required")
	}
	base, err := jeevo.NormalizeURL(*rawURL)
	if err != nil {
		return err
	}
	if !filepath.IsAbs(*installDir) || !filepath.IsAbs(*dataDir) {
		return errors.New("--install-dir and --data-dir must be absolute paths")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	d, err := jeevo.Discover(ctx, base)
	if err != nil {
		return err
	}
	releaseKeys, err := jeevo.PinReleaseKeys(d.ReleaseSigningKeys, *pin)
	if err != nil {
		return err
	}
	key := strings.TrimSpace(*license)
	for attempt := 0; attempt < 3; attempt++ {
		if key == "" {
			key = prompt("License key (Jeevo → Settings → Connectors → Add connector): ")
		}
		res, err := jeevo.Enroll(ctx, d.ApiBaseUrl, key)
		if err != nil {
			var ae *jeevo.APIError
			if errors.As(err, &ae) && (ae.Status == 400 || ae.Status == 401) && *license == "" {
				fmt.Fprintln(os.Stderr, "  ", ae.Message)
				key = ""
				continue
			}
			return err
		}
		cfg := &config.Install{
			JeevoURL: base, InstallDir: filepath.Clean(*installDir), DataDir: filepath.Clean(*dataDir),
			ConnectorID: res.ConnectorId, ConnectorSecret: res.ConnectorSecret, Name: res.Name, Domain: res.Domain,
			PlaybackPublicKey: res.PlaybackPublicKey, ReleaseKeys: releaseKeys,
			APIBaseURL: d.ApiBaseUrl, WSURL: res.WsUrl, ReleasesURL: d.ReleasesUrl,
		}
		if err := cfg.Save(*cfgPath); err != nil {
			return fmt.Errorf("write %s: %w", *cfgPath, err)
		}
		fmt.Printf("Enrolled connector %q\n  id:     %s\n  domain: %s\n  config: %s\n", res.Name, res.ConnectorId, res.Domain, *cfgPath)
		return nil
	}
	return errors.New("enrolment failed: too many invalid license keys")
}

func certExpiry(cfg *config.Install) (time.Time, bool) {
	raw, err := os.ReadFile(filepath.Join(cfg.TLSDir(), "cert.pem"))
	if err != nil {
		return time.Time{}, false
	}
	b, _ := pem.Decode(raw)
	if b == nil {
		return time.Time{}, false
	}
	c, err := x509.ParseCertificate(b.Bytes)
	if err != nil {
		return time.Time{}, false
	}
	return c.NotAfter, true
}

func systemdState() string {
	out, err := exec.Command("systemctl", "is-active", serviceName).Output()
	if err != nil && len(out) == 0 {
		return "unknown"
	}
	return strings.TrimSpace(string(out))
}

func cmdStatus(args []string) error {
	fs := flag.NewFlagSet("status", flag.ExitOnError)
	cfgPath := fs.String("config", defaultConfigPath(), "config file")
	_ = fs.Parse(args)
	cfg, err := config.LoadInstall(*cfgPath)
	if err != nil {
		return err
	}
	fmt.Printf("Connector   %s (%s)\n", cfg.Name, cfg.ConnectorID)
	fmt.Printf("Domain      https://%s\n", cfg.Domain)
	fmt.Printf("Jeevo       %s\n", cfg.JeevoURL)
	fmt.Printf("Version     %s\n", version.Version)
	fmt.Printf("Service     %s\n", systemdState())
	if na, ok := certExpiry(cfg); ok {
		fmt.Printf("Certificate valid until %s (%d days)\n", na.Format("2006-01-02"), int(time.Until(na).Hours()/24))
	} else {
		fmt.Println("Certificate none yet (issued automatically once connected)")
	}
	st := config.LoadSettings(cfg.SettingsPath())
	fmt.Printf("Settings    revision %d, mode %s, listening on %s:%d\n", st.Revision, st.Settings.Network.Mode, config.ListenAddress, st.Settings.Network.ListenPort)
	if total, free, err := sysinfo.Disk(cfg.DataDir); err == nil {
		fmt.Printf("Disk        %.1f GB free of %.1f GB (%s)\n", float64(free)/(1<<30), float64(total)/(1<<30), cfg.DataDir)
	}
	if s, err := store.Open(cfg.DBPath()); err == nil {
		bytes, counts, _ := s.MediaStats()
		jobs, _ := s.JobCounts()
		fmt.Printf("Media       %d done, %d processing, %d failed (%.1f GB of originals)\n",
			counts["done"], counts["processing"]+counts["received"], counts["failed"], float64(bytes)/(1<<30))
		fmt.Printf("AI queue    %d queued, %d scanning\n", jobs["queued"], jobs["processing"])
		fmt.Printf("Outbox      %d report(s) waiting for Jeevo\n", s.OutboxLen())
		// Only a revocation of these credentials counts: re-enrolling clears it.
		if r := s.Get("revoked"); r != "" && s.Get("revoked_for") == cfg.Identity() {
			fmt.Printf("REVOKED     at %s: re-enrol with a new license key\n", r)
		}
		s.Close()
	}
	return nil
}

func cmdUpdate(args []string) error {
	fs := flag.NewFlagSet("update", flag.ExitOnError)
	cfgPath := fs.String("config", defaultConfigPath(), "config file")
	channel := fs.String("channel", "", "release channel (default: from settings)")
	force := fs.Bool("force", false, "install even when not newer (a downgrade or reinstall)")
	noRestart := fs.Bool("no-restart", false, "do not restart the service")
	_ = fs.Parse(args)
	cfg, err := config.LoadInstall(*cfgPath)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	defer cancel()
	if d, err := jeevo.Discover(ctx, cfg.JeevoURL); err == nil {
		cfg.ReleasesURL = d.ReleasesUrl
	}
	if *channel == "" {
		*channel = string(config.LoadSettings(cfg.SettingsPath()).Settings.Updates.Channel)
	}
	log := slog.New(slog.NewTextHandler(os.Stderr, nil))
	u := &update.Updater{InstallDir: cfg.InstallDir, ReleasesURL: cfg.ReleasesURL, Keys: cfg.ReleaseKeys, Log: log, Force: *force}
	manifest, sig, err := u.Fetch(ctx, *channel)
	if err != nil {
		return err
	}
	m, err := u.VerifyEnvelope(manifest, sig)
	if err != nil {
		return err
	}
	if !*force && !update.Newer(m.Version, version.Version) {
		fmt.Printf("Already up to date (%s; latest on %s is %s)\n", version.Version, *channel, m.Version)
		return nil
	}
	fmt.Printf("Updating %s -> %s\n", version.Version, m.Version)
	ver, err := u.Install(ctx, manifest, sig, func(state, v, msg string) { fmt.Printf("  %s\n", state) })
	if err != nil {
		return err
	}
	if *noRestart {
		fmt.Printf("Installed %s. Restart the service to use it: sudo systemctl restart %s\n", ver, serviceName)
		return nil
	}
	if err := restartService(); err != nil {
		return fmt.Errorf("installed %s but restart failed: %v", ver, err)
	}
	fmt.Printf("Updated to %s and restarted.\n", ver)
	return nil
}

// restartService signals the service's main process so systemd restarts it; the service user cannot systemctl.
func restartService() error {
	out, err := exec.Command("systemctl", "show", "--property=MainPID", "--value", serviceName).Output()
	if err != nil {
		return err
	}
	pid, _ := strconv.Atoi(strings.TrimSpace(string(out)))
	if pid <= 0 {
		return fmt.Errorf("the service is not running: start it with sudo systemctl start %s", serviceName)
	}
	return syscall.Kill(pid, syscall.SIGTERM)
}

func cmdDoctor(args []string) error {
	fs := flag.NewFlagSet("doctor", flag.ExitOnError)
	cfgPath := fs.String("config", defaultConfigPath(), "config file")
	_ = fs.Parse(args)
	ok := true
	check := func(name string, good bool, detail string) {
		mark := "ok  "
		if !good {
			mark = "FAIL"
			ok = false
		}
		fmt.Printf("[%s] %s %s\n", mark, name, detail)
	}
	check("platform", runtime.GOOS == "linux", runtime.GOOS+"/"+runtime.GOARCH)
	cfg, err := config.LoadInstall(*cfgPath)
	check("config", err == nil, *cfgPath)
	if err != nil {
		return errors.New("not enrolled")
	}
	t := media.FindTools(cfg.FFmpegPath, cfg.FFprobePath)
	check("ffmpeg", t.FFmpeg != "", t.FFmpeg)
	check("ffprobe", t.FFprobe != "", t.FFprobe)
	_, perr := os.Stat(cfg.Python())
	check("ai worker python", perr == nil, cfg.Python())
	_, werr := os.Stat(filepath.Join(cfg.AIRoot(), "ai", "__main__.py"))
	check("ai worker code", werr == nil, filepath.Join(cfg.AIRoot(), "ai"))
	if _, free, err := sysinfo.Disk(cfg.DataDir); err == nil {
		check("free disk", free > config.DiskReserveBytes, fmt.Sprintf("%.1f GB (at least %.0f GB needed)", float64(free)/(1<<30), float64(config.DiskReserveBytes)/(1<<30)))
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	_, derr := jeevo.Discover(ctx, cfg.JeevoURL)
	detail := cfg.JeevoURL
	if derr != nil {
		detail += " (" + derr.Error() + ")"
	}
	check("jeevo reachable", derr == nil, detail)
	if addrs, err := net.DefaultResolver.LookupHost(ctx, cfg.Domain); err == nil {
		check("dns", true, cfg.Domain+" -> "+strings.Join(addrs, ", "))
	} else {
		check("dns", false, cfg.Domain+" does not resolve yet (set up after the first connection)")
	}
	_, cerr := os.Stat(filepath.Join(cfg.TLSDir(), "cert.pem"))
	check("certificate", cerr == nil, filepath.Join(cfg.TLSDir(), "cert.pem"))
	fmt.Printf("protocol %d, version %s\n", protocol.ProtocolVersion, version.Version)
	if !ok {
		return errors.New("some checks failed")
	}
	return nil
}
