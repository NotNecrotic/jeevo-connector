package app

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"regexp"
	"runtime"
	"strconv"
	"sync"
	"time"

	"github.com/NotNecrotic/jeevo-connector/internal/ai"
	"github.com/NotNecrotic/jeevo-connector/internal/config"
	"github.com/NotNecrotic/jeevo-connector/internal/jeevo"
	"github.com/NotNecrotic/jeevo-connector/internal/link"
	"github.com/NotNecrotic/jeevo-connector/internal/media"
	"github.com/NotNecrotic/jeevo-connector/internal/pipeline"
	"github.com/NotNecrotic/jeevo-connector/internal/protocol"
	"github.com/NotNecrotic/jeevo-connector/internal/proxy"
	"github.com/NotNecrotic/jeevo-connector/internal/server"
	"github.com/NotNecrotic/jeevo-connector/internal/store"
	"github.com/NotNecrotic/jeevo-connector/internal/sysinfo"
	"github.com/NotNecrotic/jeevo-connector/internal/tlsmgr"
	"github.com/NotNecrotic/jeevo-connector/internal/token"
	"github.com/NotNecrotic/jeevo-connector/internal/update"
	"github.com/NotNecrotic/jeevo-connector/internal/version"
)

// ErrRestart asks main to exit cleanly so systemd starts the new version.
var ErrRestart = errors.New("restart requested")

// ErrRolledBack means the update was rolled back; exit non-zero.
var ErrRolledBack = errors.New("rolled back to the previous version")

// App is a running connector.
type App struct {
	Cfg      *config.Install
	Log      *slog.Logger
	LogLevel *slog.LevelVar

	store    *store.Store
	tools    media.Tools
	detector *media.EncoderDetector
	pipe     *pipeline.Pipeline
	public   *server.Public
	verifier *token.Verifier
	certs    *tlsmgr.Manager
	link     *link.Link
	relay    *proxy.Relay
	ai       *ai.Runner
	upd      *update.Updater
	updates  *updateGate

	// cfgMu guards writes to Cfg (discovery, key rotation, rename), Cfg.Save and reads of discovered URLs.
	cfgMu sync.Mutex

	settingsMu sync.RWMutex
	settings   config.StoredSettings

	srvMu   sync.Mutex
	srv     *http.Server
	srvKey  string
	tlsMode string

	startedAt time.Time
	heartbeat time.Duration
	kick      chan struct{}
	hbNow     chan struct{} // send a heartbeat now (public address changed)
	pub       publicAddr
	certBusy  sync.Mutex
	stop      context.CancelCauseFunc
}

// Settings returns the live settings.
func (a *App) Settings() config.Settings {
	a.settingsMu.RLock()
	defer a.settingsMu.RUnlock()
	return a.settings.Settings
}

func (a *App) settingsRevision() int {
	a.settingsMu.RLock()
	defer a.settingsMu.RUnlock()
	return a.settings.Revision
}

// Run starts everything and blocks until ctx ends or a restart is needed.
func (a *App) Run(parent context.Context) error {
	ctx, stop := context.WithCancelCause(parent)
	a.stop = stop
	defer stop(nil)
	a.startedAt = time.Now()
	a.kick = make(chan struct{}, 1)
	a.hbNow = make(chan struct{}, 1)
	a.heartbeat = 30 * time.Second

	a.upd = &update.Updater{InstallDir: a.Cfg.InstallDir, ReleasesURL: a.Cfg.ReleasesURL, Keys: a.Cfg.ReleaseKeys, Log: a.Log}
	if a.upd.Boot() {
		return ErrRolledBack
	}
	if err := a.Cfg.EnsureDirs(); err != nil {
		return err
	}
	st, err := store.Open(a.Cfg.DBPath())
	if err != nil {
		return fmt.Errorf("open state database: %w", err)
	}
	defer st.Close()
	a.store = st

	a.updates = &updateGate{st: st}
	a.settings = config.LoadSettings(a.Cfg.SettingsPath())
	a.applyLogLevel(a.settings.Settings)
	a.tlsMode = st.Get("tls_mode")
	if a.tlsMode == "" {
		a.tlsMode = string(protocol.Acme)
	}

	// With a cached WebSocket URL, link up at once and refresh discovery in the background.
	if a.Cfg.WSURL == "" {
		a.refreshDiscovery(ctx)
	}

	a.verifier, err = token.NewVerifier(a.Cfg.ConnectorID, a.Cfg.PlaybackPublicKey)
	if err != nil {
		return err
	}
	a.certs = tlsmgr.New(a.Cfg.TLSDir(), a.Cfg.Domain)
	a.tools = media.FindTools(a.Cfg.FFmpegPath, a.Cfg.FFprobePath)
	if a.tools.FFmpeg == "" || a.tools.FFprobe == "" {
		a.Log.Error("ffmpeg/ffprobe not found: media cannot be processed until they are installed")
	}
	a.detector = media.NewEncoderDetector(a.tools)
	a.pipe = &pipeline.Pipeline{
		Store: st, Tools: a.tools, Trans: &media.Transcoder{Tools: a.tools, Detector: a.detector},
		Report: a, Log: a.Log.With("component", "pipeline"), Settings: a.Settings, Root: a.Cfg.MediaDir(),
	}
	a.public = &server.Public{
		Store: st, Pipe: a.pipe, Verifier: a.verifier, Log: a.Log.With("component", "http"),
		Settings: a.Settings, UploadDir: a.Cfg.UploadsDir(), Report: a,
	}
	a.loadRevoked()
	a.pipe.Start(ctx)

	// AI worker: a supervised child process speaking JSON lines on stdio.
	a.ai = ai.New(ai.Options{
		Python: a.Cfg.Python(), Dir: a.Cfg.AIRoot(), ModelDir: a.Cfg.ModelCacheDir(),
		FFmpeg: a.tools.FFmpeg, FFprobe: a.tools.FFprobe,
	}, st, pipeline.Events{P: a.pipe}, a.Log.With("component", "ai"))
	a.pipe.AI = a.ai
	a.ai.Apply(a.Settings().Ai, string(a.Settings().Logging.Level))
	go a.ai.Run(ctx)

	// Control channel.
	a.link = link.New(a.cfgWSURL(), a.Cfg.ConnectorID+"."+a.Cfg.ConnectorSecret, a.Log.With("component", "link"))
	a.link.Header = http.Header{"User-Agent": {"jeevo-connector/" + version.Version}}
	a.relay = &proxy.Relay{Link: a.link, Handler: a.public.Handler(), Log: a.Log.With("component", "proxy")}
	a.relay.Register()
	a.link.OnBinary = a.relay.OnBinary
	a.link.OnDisconnect = a.relay.DropAll
	a.link.OnConnect = a.onConnect
	a.registerHandlers()
	go a.link.Run(ctx)
	if a.Cfg.WSURL != "" {
		go a.refreshDiscovery(ctx)
	}

	go a.outboxLoop(ctx)
	go a.maintenance(ctx)
	go a.publicAddrLoop(ctx)
	a.ensureServer()
	defer a.stopServer()

	a.Log.Info("connector started", "version", version.Version, "id", a.Cfg.ConnectorID, "domain", a.Cfg.Domain)
	<-ctx.Done()
	if errors.Is(context.Cause(ctx), ErrRestart) {
		return ErrRestart
	}
	return nil
}

func (a *App) applyLogLevel(s config.Settings) {
	if a.LogLevel == nil {
		return
	}
	switch s.Logging.Level {
	case "debug":
		a.LogLevel.Set(slog.LevelDebug)
	case "warn":
		a.LogLevel.Set(slog.LevelWarn)
	case "error":
		a.LogLevel.Set(slog.LevelError)
	default:
		a.LogLevel.Set(slog.LevelInfo)
	}
}

func (a *App) cfgWSURL() string {
	a.cfgMu.Lock()
	defer a.cfgMu.Unlock()
	return a.Cfg.WSURL
}

// saveCfg persists the install config after mutate changed it.
func (a *App) saveCfg(mutate func()) {
	a.cfgMu.Lock()
	defer a.cfgMu.Unlock()
	mutate()
	if err := a.Cfg.Save(""); err != nil {
		a.Log.Warn("could not save connector config", "err", err)
	}
}

// refreshDiscovery updates cached URLs and hands them to the link and updater; safe in the background.
func (a *App) refreshDiscovery(ctx context.Context) {
	dctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	d, err := jeevo.Discover(dctx, a.Cfg.JeevoURL)
	if err != nil {
		a.Log.Warn("discovery failed; using cached URLs", "err", err)
		return
	}
	a.cfgMu.Lock()
	changed := a.Cfg.WSURL != d.WsUrl || a.Cfg.APIBaseURL != d.ApiBaseUrl || a.Cfg.ReleasesURL != d.ReleasesUrl
	wsChanged := a.Cfg.WSURL != d.WsUrl
	a.Cfg.WSURL, a.Cfg.APIBaseURL, a.Cfg.ReleasesURL = d.WsUrl, d.ApiBaseUrl, d.ReleasesUrl
	if changed {
		if err := a.Cfg.Save(""); err != nil {
			a.Log.Warn("could not save refreshed URLs", "err", err)
		}
	}
	a.cfgMu.Unlock()
	a.upd.SetReleasesURL(d.ReleasesUrl)
	if a.link != nil {
		a.link.SetURL(d.WsUrl)
	}
	if wsChanged {
		a.Log.Info("discovery changed the control channel URL", "url", d.WsUrl)
	}
}

// ---- pipeline.Reporter --------------------------------------------------------

// Emit persists a report in the outbox.
func (a *App) Emit(typ string, payload any) {
	raw, err := json.Marshal(payload)
	if err != nil {
		a.Log.Error("cannot encode report", "type", typ, "err", err)
		return
	}
	if _, err := a.store.Enqueue(typ, string(raw)); err != nil {
		a.Log.Error("cannot queue report", "type", typ, "err", err)
		return
	}
	select {
	case a.kick <- struct{}{}:
	default:
	}
}

// Progress sends a best-effort progress update.
func (a *App) Progress(p protocol.MediaProgressPayload) {
	if a.link != nil && a.link.Connected() {
		_ = a.link.Send(protocol.MsgMediaProgress, p)
	}
}

func (a *App) outboxLoop(ctx context.Context) {
	t := time.NewTicker(15 * time.Second)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-a.kick:
		case <-t.C:
		}
		a.flushOutbox(ctx)
	}
}

// outboxGap spaces replayed reports to stay under Jeevo's 50 messages/s per connector (over closes the link).
const outboxGap = 50 * time.Millisecond

func (a *App) flushOutbox(ctx context.Context) {
	if !a.link.Connected() {
		return
	}
	pace := time.NewTicker(outboxGap)
	defer pace.Stop()
	for ctx.Err() == nil {
		items, err := a.store.Outbox(50)
		if err != nil || len(items) == 0 {
			return
		}
		for _, it := range items {
			select {
			case <-ctx.Done():
				return
			case <-pace.C:
			}
			err := a.link.RequestID(ctx, it.ID, it.Type, json.RawMessage(it.Payload), nil, 30*time.Second)
			if err == nil {
				_ = a.store.Ack(it.ID)
				continue
			}
			var pe *link.Error
			if errors.As(err, &pe) {
				// Refused by the server: retry a few times (may race an upload registration), then drop.
				dropped, _ := a.store.BumpAttempt(it.ID, 10)
				a.Log.Warn("report rejected by jeevo", "type", it.Type, "code", pe.Code, "dropped", dropped)
				continue
			}
			return // transport problem: retry later, in order
		}
	}
}

// ---- control channel -------------------------------------------------------------

func (a *App) storageStats() protocol.StorageStats {
	total, free, _ := sysinfo.Disk(a.Cfg.DataDir)
	used, _, _ := a.store.MediaStats()
	return protocol.StorageStats{TotalBytes: int(total), FreeBytes: int(free), UsedByMediaBytes: int(used)}
}

// jobStats reports media by processing state (see pipeline.Stats).
func (a *App) jobStats() protocol.JobStats {
	queued, active, failed := a.pipe.Stats()
	return protocol.JobStats{Queued: queued, Active: active, Failed: failed}
}

func (a *App) onConnect(ctx context.Context) error {
	host, _ := os.Hostname()
	hello := protocol.HelloPayload{
		Version: version.Version, ProtocolVersion: protocol.ProtocolVersion, Os: runtime.GOOS, Arch: runtime.GOARCH,
		Hostname: host, StartedAt: a.startedAt, SettingsRevision: a.settingsRevision(),
		Storage: a.storageStats(), Jobs: a.jobStats(),
		PublicAddress: a.reportedPublicAddress(),
	}
	if d, na, ok := a.certs.Info(); ok {
		hello.Certificate = &protocol.CertificateInfo{Domain: d, NotAfter: na}
	}
	var ack protocol.HelloAck
	if err := a.link.Request(ctx, protocol.MsgHello, hello, &ack, 30*time.Second); err != nil {
		return fmt.Errorf("hello: %w", err)
	}
	a.upd.Confirm()
	if ack.PlaybackPublicKey != nil && *ack.PlaybackPublicKey != "" && *ack.PlaybackPublicKey != a.Cfg.PlaybackPublicKey {
		if err := a.verifier.SetKey(*ack.PlaybackPublicKey); err == nil {
			a.saveCfg(func() { a.Cfg.PlaybackPublicKey = *ack.PlaybackPublicKey })
			a.Log.Info("playback key rotated")
		}
	}
	if ack.Name != "" && ack.Name != a.Cfg.Name {
		a.saveCfg(func() { a.Cfg.Name = ack.Name })
	}
	if ack.HeartbeatSeconds >= 5 {
		a.heartbeat = time.Duration(ack.HeartbeatSeconds) * time.Second
	}
	if string(ack.Tls) != a.tlsMode {
		a.tlsMode = string(ack.Tls)
		_ = a.store.Set("tls_mode", a.tlsMode)
	}
	if ack.SettingsRevision != a.settingsRevision() {
		if err := a.applySettings(ack.Settings, ack.SettingsRevision); err != nil {
			a.Log.Error("settings from jeevo rejected", "err", err)
		}
	}
	a.ensureServer()
	a.Log.Info("connected to jeevo", "name", ack.Name, "tls", a.tlsMode)
	if a.tlsMode == string(protocol.Acme) && a.certs.NeedsCertificate(time.Now()) {
		go a.requestCertificate(ctx)
	}
	if latest := ack.LatestVersion; latest != nil && a.Settings().Updates.AutoUpdate && update.Newer(*latest, version.Version) && a.updates.begin(*latest, time.Now()) {
		go func() {
			defer a.updates.finish()
			// A connection that dropped under the attempt is no verdict on the release.
			if err := a.selfUpdate(ctx, ""); err != nil && ctx.Err() == nil {
				a.updates.failed(*latest, time.Now())
			}
		}()
	}
	select {
	case a.kick <- struct{}{}:
	default:
	}
	go a.heartbeatLoop(ctx)
	return nil
}

func (a *App) heartbeatLoop(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(a.heartbeat):
		case <-a.hbNow:
		}
		if !a.link.Connected() {
			return
		}
		var ack protocol.HeartbeatAck
		up := int(time.Since(a.startedAt).Seconds())
		err := a.link.Request(ctx, protocol.MsgHeartbeat, protocol.HeartbeatPayload{
			UptimeSeconds: up, Storage: a.storageStats(), Jobs: a.jobStats(),
			PublicAddress: a.reportedPublicAddress(),
		}, &ack, 20*time.Second)
		if err != nil && !errors.Is(err, link.ErrNotConnected) {
			a.Log.Warn("heartbeat failed", "err", err)
		}
	}
}

func decode[T any](raw json.RawMessage) (T, error) {
	var v T
	if err := json.Unmarshal(raw, &v); err != nil {
		return v, link.Errf("BAD_PAYLOAD", "invalid payload")
	}
	return v, nil
}

var reExt = regexp.MustCompile(`^\.[a-z0-9]{1,5}$`)
var reUpload = regexp.MustCompile(`^[A-Za-z0-9_-]{8,64}$`)
var reSHA = regexp.MustCompile(`^[a-f0-9]{64}$`)

func (a *App) registerHandlers() {
	l := a.link
	l.Handle(protocol.MsgUploadExpect, func(ctx context.Context, raw json.RawMessage) (any, error) {
		p, err := decode[protocol.UploadExpectPayload](raw)
		if err != nil {
			return nil, err
		}
		s := a.Settings()
		switch {
		case !reUpload.MatchString(p.UploadId) || !pipeline.ValidID(p.MediaId) || !reSHA.MatchString(p.TokenSha256) || !reExt.MatchString(p.Ext):
			return nil, link.Errf("BAD_PAYLOAD", "invalid upload descriptor")
		case p.Kind == protocol.UploadKindMedia && !media.IsVideoExt(p.Ext) && !media.IsImageExt(p.Ext):
			return nil, link.Errf("UNSUPPORTED_TYPE", "file type %s is not accepted", p.Ext)
		case p.Kind == protocol.UploadKindThumbnail && !media.IsImageExt(p.Ext):
			return nil, link.Errf("UNSUPPORTED_TYPE", "thumbnails must be images")
		case p.Kind != protocol.UploadKindMedia && p.Kind != protocol.UploadKindThumbnail:
			return nil, link.Errf("BAD_PAYLOAD", "unknown upload kind")
		case int64(p.DeclaredSize) > int64(s.Uploads.MaxUploadMb)<<20:
			return nil, link.Errf("TOO_LARGE", "file exceeds this connector's %d MB limit", s.Uploads.MaxUploadMb)
		}
		if p.Kind == protocol.UploadKindMedia {
			if _, err := a.store.GetMedia(p.MediaId); err == nil {
				return nil, link.Errf("CONFLICT", "media id already exists")
			}
		} else if _, err := a.store.GetMedia(p.MediaId); err != nil {
			return nil, link.Errf("NOT_FOUND", "unknown media")
		}
		// Expect checks room for the original, processing output and a fixed reserve, and caps uploads in progress.
		if err := a.public.Expect(store.Upload{ID: p.UploadId, MediaID: p.MediaId, Kind: string(p.Kind), TokenSHA256: p.TokenSha256,
			Ext: p.Ext, DeclaredSize: int64(p.DeclaredSize), ThumbnailAt: p.ThumbnailAt, ExpiresAt: p.ExpiresAt}); err != nil {
			switch {
			case errors.Is(err, server.ErrRevoked):
				return nil, link.Errf("FORBIDDEN", "connector is revoked")
			case errors.Is(err, server.ErrTooManyUploads):
				return nil, link.Errf("BUSY", "too many uploads in progress")
			case errors.Is(err, server.ErrNoRoom):
				return nil, link.Errf("INSUFFICIENT_STORAGE", "connector is low on disk space")
			}
			return nil, err
		}
		return protocol.UploadExpectAck{Path: "/v1/uploads/" + p.UploadId, ChunkSize: config.UploadChunkBytes}, nil
	})
	l.Handle(protocol.MsgUploadCancel, func(ctx context.Context, raw json.RawMessage) (any, error) {
		p, err := decode[protocol.UploadCancelPayload](raw)
		if err != nil {
			return nil, err
		}
		a.public.CancelUpload(p.UploadId)
		return nil, nil
	})
	l.Handle(protocol.MsgMediaDelete, func(ctx context.Context, raw json.RawMessage) (any, error) {
		p, err := decode[protocol.MediaDeletePayload](raw)
		if err != nil {
			return nil, err
		}
		for _, id := range p.MediaIds {
			if err := a.pipe.Delete(id); err != nil {
				a.Log.Warn("delete failed", "media", id, "err", err)
			}
		}
		return nil, nil
	})
	l.Handle(protocol.MsgMediaThumbnailSet, func(ctx context.Context, raw json.RawMessage) (any, error) {
		p, err := decode[protocol.MediaThumbnailSetPayload](raw)
		if err != nil {
			return nil, err
		}
		if !pipeline.ValidID(p.MediaId) {
			return nil, link.Errf("BAD_PAYLOAD", "bad media id")
		}
		if err := a.pipe.SetThumbnailAt(ctx, p.MediaId, p.At); err != nil {
			if errors.Is(err, store.ErrNotFound) {
				return nil, nil // nothing to do: the item is gone here
			}
			return nil, link.Errf("THUMBNAIL_FAILED", "thumbnail could not be generated")
		}
		return nil, nil
	})
	l.Handle(protocol.MsgSettingsApply, func(ctx context.Context, raw json.RawMessage) (any, error) {
		p, err := decode[protocol.SettingsApplyPayload](raw)
		if err != nil {
			return nil, err
		}
		if err := a.applySettings(p.Settings, p.Revision); err != nil {
			return nil, link.Errf("INVALID_SETTINGS", "%s", err.Error())
		}
		return protocol.SettingsApplyAck{Applied: true, RestartRequired: false}, nil
	})
	l.Handle(protocol.MsgReachPrepare, func(ctx context.Context, raw json.RawMessage) (any, error) {
		p, err := decode[protocol.ReachPreparePayload](raw)
		if err != nil {
			return nil, err
		}
		a.public.AddReachNonce(p.Nonce, p.ExpiresAt)
		return nil, nil
	})
	l.Handle(protocol.MsgCertRenew, func(ctx context.Context, raw json.RawMessage) (any, error) {
		go a.requestCertificate(context.WithoutCancel(ctx))
		return nil, nil
	})
	l.Handle(protocol.MsgUpdateApply, func(ctx context.Context, raw json.RawMessage) (any, error) {
		p, err := decode[protocol.UpdateApplyPayload](raw)
		if err != nil {
			return nil, err
		}
		if _, err := a.upd.VerifyEnvelope(p.Manifest, p.Signature); err != nil {
			return nil, link.Errf("BAD_RELEASE", "%s", err.Error())
		}
		// installRelease logs a failure and reports it as update.status.
		go func() { _ = a.installRelease(context.WithoutCancel(ctx), p.Manifest, p.Signature) }()
		return nil, nil
	})
	l.Handle(protocol.MsgConnectorRevoked, func(ctx context.Context, raw json.RawMessage) (any, error) {
		a.revoke()
		return nil, nil
	})
}

// loadRevoked restores a revocation recorded by an earlier run.
func (a *App) loadRevoked() {
	if a.store.Get("revoked_for") == a.Cfg.Identity() {
		a.Log.Error("this connector was revoked by jeevo; it serves nothing until it is enrolled again")
		a.public.Revoke()
	}
}

// revoke remembers the revocation, refuses all media and upload requests and closes the public listener.
func (a *App) revoke() {
	a.Log.Error("jeevo revoked this connector; it will stop serving until it is enrolled again")
	_ = a.store.Set("revoked", time.Now().UTC().Format(time.RFC3339))
	_ = a.store.Set("revoked_for", a.Cfg.Identity())
	a.public.Revoke()
	a.srvMu.Lock()
	defer a.srvMu.Unlock()
	if a.srv != nil {
		_ = a.srv.Close()
		a.srv, a.srvKey = nil, ""
	}
}

func (a *App) applySettings(s config.Settings, revision int) error {
	config.NormalizeSettings(&s)
	if err := config.ValidateSettings(&s); err != nil {
		return err
	}
	a.settingsMu.Lock()
	old := a.settings.Settings
	a.settings = config.StoredSettings{Revision: revision, Settings: s}
	a.settingsMu.Unlock()
	if err := config.SaveSettings(a.Cfg.SettingsPath(), config.StoredSettings{Revision: revision, Settings: s}); err != nil {
		a.Log.Warn("could not persist settings", "err", err)
	}
	a.applyLogLevel(s)
	a.ai.Apply(s.Ai, string(s.Logging.Level))
	a.pipe.SettingsChanged(old, s)
	a.ensureServer()
	a.Log.Info("settings applied", "revision", revision)
	return nil
}

func (a *App) requestCertificate(ctx context.Context) {
	if !a.certBusy.TryLock() {
		return
	}
	defer a.certBusy.Unlock()
	csr, err := a.certs.NewCSR()
	if err != nil {
		a.Log.Error("cannot create CSR", "err", err)
		return
	}
	a.Log.Info("requesting a certificate", "domain", a.Cfg.Domain)
	var issued protocol.CertIssued
	if err := a.link.Request(ctx, protocol.MsgCertRequest, protocol.CertRequestPayload{CsrPem: csr}, &issued, 6*time.Minute); err != nil {
		a.Log.Error("certificate request failed (will retry)", "err", err)
		return
	}
	na, err := a.certs.Install(issued.CertificatePem)
	if err != nil {
		a.Log.Error("issued certificate rejected", "err", err)
		return
	}
	a.Log.Info("certificate installed", "expires", na.Format(time.RFC3339))
	a.ensureServer()
}

func (a *App) selfUpdate(ctx context.Context, channel string) error {
	if channel == "" {
		channel = string(a.Settings().Updates.Channel)
	}
	m, sig, err := a.upd.Fetch(ctx, channel)
	if err != nil {
		a.Log.Warn("update check failed", "err", err)
		return err
	}
	return a.installRelease(ctx, m, sig)
}

// installRelease installs a release and restarts into it; it returns the error on failure.
func (a *App) installRelease(ctx context.Context, manifest, signature string) error {
	status := func(state, ver, msg string) {
		var e *string
		if msg != "" {
			// The protocol caps the error at 2000 characters; a longer one would be rejected.
			if r := []rune(msg); len(r) > 2000 {
				msg = string(r[:1999]) + "…"
			}
			e = &msg
		}
		_ = a.link.Send(protocol.MsgUpdateStatus, protocol.UpdateStatusPayload{State: protocol.UpdateStatusPayloadState(state), Version: ver, Error: e})
	}
	ver, err := a.upd.Install(ctx, manifest, signature, status)
	if err != nil {
		a.Log.Error("update failed", "err", err)
		status("failed", ver, err.Error())
		return err
	}
	a.Log.Info("update installed; restarting", "version", ver)
	status("restarting", ver, "")
	time.Sleep(500 * time.Millisecond)
	a.stop(ErrRestart)
	return nil
}

func (a *App) maintenance(ctx context.Context) {
	t := time.NewTicker(time.Hour)
	defer t.Stop()
	a.public.SweepUploads()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			a.public.SweepUploads()
			if a.tlsMode == string(protocol.Acme) && a.link.Connected() && a.certs.NeedsCertificate(time.Now()) {
				go a.requestCertificate(ctx)
			}
		}
	}
}

// ---- the public listener -------------------------------------------------------

func (a *App) ensureServer() {
	n := a.Settings().Network
	addr := net.JoinHostPort(config.ListenAddress, strconv.Itoa(n.ListenPort))
	mode := a.tlsMode
	key := addr + "|" + mode
	a.srvMu.Lock()
	defer a.srvMu.Unlock()
	if a.public.Revoked() {
		return
	}
	if a.srv != nil && a.srvKey == key {
		return
	}
	if a.srv != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		_ = a.srv.Shutdown(ctx)
		cancel()
		a.srv = nil
	}
	if mode == string(protocol.Acme) && !a.certs.HasCertificate() {
		// Nothing to serve until the certificate arrives; proxy mode keeps working.
		a.srvKey = ""
		return
	}
	handler := a.public.Handler()
	if mode != string(protocol.Acme) {
		handler = localNetworksOnly(handler, a.Log)
	}
	// ReadHeaderTimeout doubles as the TLS handshake deadline; no ReadTimeout, bodies are bounded per request.
	srv := &http.Server{
		Addr:              addr,
		Handler:           handler,
		ReadHeaderTimeout: 15 * time.Second,
		IdleTimeout:       120 * time.Second,
		MaxHeaderBytes:    32 << 10,
		ErrorLog:          slog.NewLogLogger(a.Log.Handler(), slog.LevelDebug),
	}
	tcp, err := net.Listen("tcp", addr)
	if err != nil {
		a.Log.Error("cannot listen", "addr", addr, "err", err)
		a.srvKey = ""
		return
	}
	ln := newLimitListener(tcp, maxConns, maxConnsPerIP)
	if mode == string(protocol.Acme) {
		srv.TLSConfig = &tls.Config{MinVersion: tls.VersionTLS12, GetCertificate: a.certs.GetCertificate, NextProtos: []string{"h2", "http/1.1"}}
		go func() { _ = srv.ServeTLS(ln, "", "") }()
	} else {
		a.Log.Warn("serving plain HTTP to local networks only: development mode (tls=none from jeevo)")
		go func() { _ = srv.Serve(ln) }()
	}
	a.srv, a.srvKey = srv, key
	a.Log.Info("listening", "addr", addr, "tls", mode)
}

func (a *App) stopServer() {
	a.srvMu.Lock()
	defer a.srvMu.Unlock()
	if a.srv != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		_ = a.srv.Shutdown(ctx)
		cancel()
	}
}
