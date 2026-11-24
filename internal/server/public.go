// Package server is the connector's public HTTP surface: token-gated routes that reveal nothing about the host.
package server

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/NotNecrotic/jeevo-connector/internal/config"
	"github.com/NotNecrotic/jeevo-connector/internal/media"
	"github.com/NotNecrotic/jeevo-connector/internal/pipeline"
	"github.com/NotNecrotic/jeevo-connector/internal/protocol"
	"github.com/NotNecrotic/jeevo-connector/internal/store"
	"github.com/NotNecrotic/jeevo-connector/internal/sysinfo"
	"github.com/NotNecrotic/jeevo-connector/internal/token"
)

var (
	reUploadID = regexp.MustCompile(`^[A-Za-z0-9_-]{8,64}$`)
	reVariant  = regexp.MustCompile(`^(p\d{3,4}|audio)$`)
	reHlsFile  = regexp.MustCompile(`^(index\.m3u8|init(_\d{1,2})?\.mp4|seg_\d{5,6}\.m4s)$`)
	reNonce    = regexp.MustCompile(`^[A-Za-z0-9_-]{16,64}$`)
)

// Public serves uploads and media.
type Public struct {
	Store     *store.Store
	Pipe      *pipeline.Pipeline
	Verifier  *token.Verifier
	Log       *slog.Logger
	Settings  func() config.Settings
	UploadDir string
	Report    pipeline.Reporter
	// FreeBytes reports free disk space; nil = the real filesystem (tests replace it).
	FreeBytes func(path string) (int64, error)

	nonceMu sync.Mutex
	nonces  map[string]time.Time

	upMu    sync.Mutex
	upLocks map[string]*uploadLock

	// expectMu makes Expect's checks and its insert one step.
	expectMu sync.Mutex
	// revoked is set when Jeevo revokes the connector; nothing is served after.
	revoked atomic.Bool
}

// Limits on what the public side takes on.
const (
	// maxOpenUploads caps uploads announced and not finished.
	maxOpenUploads = 64
	// transferMinRate is the slowest expected pace (bytes/s); slower transfers time out so a stalled client cannot hold a lock.
	transferMinRate = 32 << 10
)

// Variables so tests can shorten them: lockWait (wait on same upload), transferBase (base transfer time).
var (
	lockWait     = 30 * time.Second
	transferBase = 30 * time.Second
)

// Errors Expect gives for an upload it will not take.
var (
	ErrRevoked        = errors.New("connector is revoked")
	ErrTooManyUploads = errors.New("too many uploads in progress")
	ErrNoRoom         = errors.New("not enough free disk space")
)

// Revoke makes the public side refuse every media, upload and announce request.
func (p *Public) Revoke() { p.revoked.Store(true) }

// Revoked reports whether the connector was revoked.
func (p *Public) Revoked() bool { return p.revoked.Load() }

// transferTimeout is how long moving n bytes may take.
func transferTimeout(n int64) time.Duration {
	return transferBase + time.Duration(n/transferMinRate)*time.Second
}

func (p *Public) freeBytes() func(string) (int64, error) {
	if p.FreeBytes != nil {
		return p.FreeBytes
	}
	return sysinfo.FreeBytes
}

// reserved is the disk that already-accepted uploads and processing will still need.
func (p *Public) reserved() (int64, error) {
	_, declared, received, err := p.Store.OpenUploadStats()
	if err != nil {
		return 0, err
	}
	processing, err := p.Store.UnfinishedMediaBytes()
	if err != nil {
		return 0, err
	}
	return declared*config.UploadSizeFactor - received + processing, nil
}

// hasRoom reports whether free space covers the reserve, reservations and extra; unreadable free space does not refuse.
func (p *Public) hasRoom(extra int64) (bool, error) {
	free, err := p.freeBytes()(p.UploadDir)
	if err != nil {
		p.Log.Warn("cannot read free disk space; not refusing", "err", err)
		return true, nil //nolint:nilerr // fails open by design: unreadable free space must not block uploads
	}
	reserved, err := p.reserved()
	if err != nil {
		return false, err
	}
	return free >= config.DiskReserveBytes+reserved+extra, nil
}

// Expect records an announced upload unless revoked, at maxOpenUploads, or out of room; re-announcing is fine.
func (p *Public) Expect(u store.Upload) error {
	p.expectMu.Lock()
	defer p.expectMu.Unlock()
	if p.revoked.Load() {
		return ErrRevoked
	}
	if cur, err := p.Store.GetUpload(u.ID); err == nil && cur.Status == "open" && time.Now().Before(cur.ExpiresAt) {
		return p.Store.ExpectUpload(u)
	}
	n, _, _, err := p.Store.OpenUploadStats()
	if err != nil {
		return err
	}
	if n >= maxOpenUploads {
		return ErrTooManyUploads
	}
	ok, err := p.hasRoom(u.DeclaredSize * config.UploadSizeFactor)
	if err != nil {
		return err
	}
	if !ok {
		return ErrNoRoom
	}
	return p.Store.ExpectUpload(u)
}

// AddReachNonce arms the reachability probe for one nonce.
func (p *Public) AddReachNonce(nonce string, until time.Time) {
	p.nonceMu.Lock()
	defer p.nonceMu.Unlock()
	if p.nonces == nil {
		p.nonces = map[string]time.Time{}
	}
	now := time.Now()
	for k, v := range p.nonces {
		if now.After(v) {
			delete(p.nonces, k)
		}
	}
	if until.Sub(now) > 2*time.Minute {
		until = now.Add(2 * time.Minute)
	}
	p.nonces[nonce] = until
}

var statusText = map[int][2]string{
	400: {"BAD_REQUEST", "Bad request"},
	401: {"UNAUTHORIZED", "Unauthorized"},
	403: {"FORBIDDEN", "Forbidden"},
	404: {"NOT_FOUND", "Not found"},
	405: {"METHOD_NOT_ALLOWED", "Method not allowed"},
	409: {"CONFLICT", "Conflict"},
	411: {"LENGTH_REQUIRED", "Length required"},
	413: {"TOO_LARGE", "Payload too large"},
	500: {"INTERNAL", "Internal error"},
	503: {"UNAVAILABLE", "Temporarily unavailable"},
	507: {"INSUFFICIENT_STORAGE", "Insufficient storage"},
}

func fail(w http.ResponseWriter, code int) {
	t, ok := statusText[code]
	if !ok {
		t = [2]string{"ERROR", "Error"}
	}
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(map[string]any{"error": map[string]string{"code": t[0], "message": t[1]}})
}

// Handler returns the public router.
func (p *Public) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusNoContent) })
	mux.HandleFunc("GET /.well-known/jeevo-reach/{nonce}", p.reach)
	mux.HandleFunc("HEAD /v1/uploads/{id}", p.uploadHead)
	mux.HandleFunc("PUT /v1/uploads/{id}", p.uploadPut)
	mux.HandleFunc("GET /v1/media/{id}/thumbnail.jpg", p.mediaFile("thumbnail"))
	mux.HandleFunc("GET /v1/media/{id}/original", p.mediaFile("original"))
	mux.HandleFunc("GET /v1/media/{id}/preview.mp4", p.mediaFile("preview"))
	mux.HandleFunc("GET /v1/media/{id}/hls/master.m3u8", p.hlsMaster)
	mux.HandleFunc("GET /v1/media/{id}/hls/{variant}/{file}", p.hlsFile)
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) { fail(w, 404) })
	return p.wrap(mux)
}

// wrap adds the headers every response carries and answers CORS preflights.
func (p *Public) wrap(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		h := w.Header()
		h.Set("X-Content-Type-Options", "nosniff")
		h.Set("Referrer-Policy", "no-referrer")
		h.Set("X-Frame-Options", "DENY")
		h.Set("Cross-Origin-Resource-Policy", "cross-origin")
		// Bearer tokens, never cookies, so "*" is safe for any Jeevo web origin and native apps.
		h.Set("Access-Control-Allow-Origin", "*")
		h.Set("Access-Control-Expose-Headers", "Upload-Offset, Upload-Length, Content-Length, Content-Range")
		if r.Method == http.MethodOptions {
			h.Set("Access-Control-Allow-Methods", "GET, HEAD, PUT, OPTIONS")
			h.Set("Access-Control-Allow-Headers", "Authorization, Content-Type, Upload-Offset, Range")
			h.Set("Access-Control-Max-Age", "86400")
			w.WriteHeader(http.StatusNoContent)
			return
		}
		defer func() {
			if rec := recover(); rec != nil {
				p.Log.Error("panic serving request", "path", r.URL.Path, "panic", rec)
				fail(w, 500)
			}
		}()
		next.ServeHTTP(w, r)
	})
}

func (p *Public) reach(w http.ResponseWriter, r *http.Request) {
	n := r.PathValue("nonce")
	p.nonceMu.Lock()
	until, ok := p.nonces[n]
	p.nonceMu.Unlock()
	if !reNonce.MatchString(n) || !ok || time.Now().After(until) {
		fail(w, 404)
		return
	}
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(http.StatusNoContent)
}

// ---- uploads ----------------------------------------------------------------

// uploadLock serialises one upload's requests; refs counts holder and waiters so an id always meets the same lock.
type uploadLock struct {
	sem  chan struct{}
	refs int
}

// lockUpload takes the upload's lock, giving up (ok = false) after lockWait or when ctx ends.
func (p *Public) lockUpload(ctx context.Context, id string) (unlock func(), ok bool) {
	p.upMu.Lock()
	if p.upLocks == nil {
		p.upLocks = map[string]*uploadLock{}
	}
	l, found := p.upLocks[id]
	if !found {
		l = &uploadLock{sem: make(chan struct{}, 1)}
		p.upLocks[id] = l
	}
	l.refs++
	p.upMu.Unlock()
	drop := func() {
		p.upMu.Lock()
		if l.refs--; l.refs == 0 {
			delete(p.upLocks, id)
		}
		p.upMu.Unlock()
	}
	t := time.NewTimer(lockWait)
	defer t.Stop()
	select {
	case l.sem <- struct{}{}:
		return func() { <-l.sem; drop() }, true
	case <-ctx.Done():
	case <-t.C:
	}
	drop()
	return nil, false
}

func (p *Public) partPath(id string) string { return filepath.Join(p.UploadDir, id+".part") }

// authUpload loads an open upload and checks the bearer token in constant time.
func (p *Public) authUpload(r *http.Request) (*store.Upload, int) {
	if p.revoked.Load() {
		return nil, 403
	}
	id := r.PathValue("id")
	if !reUploadID.MatchString(id) {
		return nil, 404
	}
	auth := r.Header.Get("Authorization")
	tok, ok := strings.CutPrefix(auth, "Bearer ")
	if !ok || len(tok) < 20 || len(tok) > 256 {
		return nil, 401
	}
	u, err := p.Store.GetUpload(id)
	if err != nil {
		return nil, 404
	}
	sum := sha256.Sum256([]byte(tok))
	if subtle.ConstantTimeCompare([]byte(hex.EncodeToString(sum[:])), []byte(u.TokenSHA256)) != 1 {
		return nil, 401
	}
	if u.Status != "open" || time.Now().After(u.ExpiresAt) {
		return nil, 404
	}
	return u, 0
}

func (p *Public) currentOffset(id string) int64 {
	st, err := os.Stat(p.partPath(id))
	if err != nil {
		return 0
	}
	return st.Size()
}

func (p *Public) uploadHead(w http.ResponseWriter, r *http.Request) {
	u, code := p.authUpload(r)
	if code != 0 {
		fail(w, code)
		return
	}
	w.Header().Set("Upload-Offset", strconv.FormatInt(p.currentOffset(u.ID), 10))
	w.Header().Set("Upload-Length", strconv.FormatInt(u.DeclaredSize, 10))
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(http.StatusNoContent)
}

func (p *Public) uploadPut(w http.ResponseWriter, r *http.Request) {
	u, code := p.authUpload(r)
	if code != 0 {
		fail(w, code)
		return
	}
	unlock, ok := p.lockUpload(r.Context(), u.ID)
	if !ok {
		// Another request is still writing: answer like a stale offset so the client re-checks the upload.
		w.Header().Set("Upload-Offset", strconv.FormatInt(p.currentOffset(u.ID), 10))
		fail(w, 409)
		return
	}
	defer unlock()
	// The upload may have been cancelled, finished or revoked while waiting.
	if p.revoked.Load() {
		fail(w, 403)
		return
	}
	if cur, err := p.Store.GetUpload(u.ID); err != nil || cur.Status != "open" {
		fail(w, 404)
		return
	}

	offset, err := strconv.ParseInt(r.Header.Get("Upload-Offset"), 10, 64)
	if err != nil || offset < 0 {
		fail(w, 400)
		return
	}
	cur := p.currentOffset(u.ID)
	if offset != cur {
		w.Header().Set("Upload-Offset", strconv.FormatInt(cur, 10))
		fail(w, 409)
		return
	}
	length := r.ContentLength
	if length < 0 {
		fail(w, 411)
		return
	}
	const maxChunk = int64(config.UploadChunkBytes)
	if length == 0 || length > maxChunk*2 || offset+length > u.DeclaredSize {
		fail(w, 413)
		return
	}
	// Keep room for everything accepted so far plus the fixed reserve.
	room, err := p.hasRoom(0)
	if err != nil {
		p.Log.Error("disk accounting", "err", err)
		fail(w, 500)
		return
	}
	if !room {
		p.Log.Warn("refusing upload chunk: not enough free space", "upload", u.ID)
		fail(w, 507)
		return
	}
	// Bounded chunk time so a stalled client cannot keep the upload lock (no deadline if unsupported).
	_ = http.NewResponseController(w).SetReadDeadline(time.Now().Add(transferTimeout(length)))
	f, err := os.OpenFile(p.partPath(u.ID), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o640)
	if err != nil {
		p.Log.Error("open part file", "err", err)
		fail(w, 500)
		return
	}
	n, cerr := io.Copy(f, io.LimitReader(r.Body, length))
	if cerr == nil && n != length {
		cerr = io.ErrUnexpectedEOF
	}
	if cerr != nil {
		// Roll back a partial chunk so the client can simply resend it.
		_ = f.Truncate(offset)
		f.Close()
		fail(w, 400)
		return
	}
	end := offset + n
	if end == u.DeclaredSize {
		if err := f.Sync(); err != nil {
			f.Close()
			fail(w, 500)
			return
		}
	}
	f.Close()
	_ = p.Store.SetUploadReceived(u.ID, end)
	w.Header().Set("Upload-Offset", strconv.FormatInt(end, 10))
	if end < u.DeclaredSize {
		w.WriteHeader(http.StatusNoContent)
		return
	}
	if err := p.completeUpload(r.Context(), u); err != nil {
		p.Log.Warn("upload finalisation failed", "upload", u.ID, "err", err)
		p.abandonUpload(u, err)
		fail(w, 400)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	_ = json.NewEncoder(w).Encode(protocol.UploadComplete{Status: "received", MediaId: u.MediaID})
}

func fileSHA256(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

// abandonUpload marks a failed finish (retries get 404), removes the partial file and tells Jeevo if it was new.
func (p *Public) abandonUpload(u *store.Upload, cause error) {
	_ = p.Store.SetUploadStatus(u.ID, "failed")
	_ = os.Remove(p.partPath(u.ID))
	if u.Kind == string(protocol.UploadKindMedia) {
		p.Report.Emit(protocol.MsgMediaFailed, protocol.MediaFailedPayload{MediaId: u.MediaID, Stage: protocol.ProcessingStageProbe, Reason: "upload rejected: " + cause.Error()})
	}
}

func (p *Public) completeUpload(ctx context.Context, u *store.Upload) error {
	part := p.partPath(u.ID)
	// The extension is only a claim and ffmpeg goes by content: verify the real type first.
	if err := media.CheckUpload(part, u.Ext); err != nil {
		return err
	}
	sum, err := fileSHA256(part)
	if err != nil {
		return err
	}
	if !pipeline.ValidID(u.MediaID) {
		return errors.New("bad media id")
	}
	switch u.Kind {
	case string(protocol.UploadKindMedia):
		dir := p.Pipe.Dir(u.MediaID)
		if err := os.MkdirAll(dir, 0o750); err != nil {
			return err
		}
		if err := os.Rename(part, p.Pipe.OriginalPath(u.MediaID, u.Ext)); err != nil {
			return err
		}
		if err := p.Store.InsertMedia(store.Media{ID: u.MediaID, Ext: u.Ext, Status: pipeline.StatusReceived, Stage: pipeline.StageReceived, FileSize: u.DeclaredSize, RequestedThumbAt: u.ThumbnailAt}); err != nil {
			return err
		}
		_ = p.Store.SetUploadStatus(u.ID, "complete")
		p.Report.Emit(protocol.MsgUploadReceived, protocol.UploadReceivedPayload{UploadId: u.ID, MediaId: u.MediaID, Kind: protocol.UploadKindMedia, Bytes: int(u.DeclaredSize), Sha256: sum})
		p.Pipe.Kick(u.MediaID)
		return nil
	case string(protocol.UploadKindThumbnail):
		if !media.IsImageExt(u.Ext) {
			os.Remove(part)
			return errors.New("thumbnail must be an image")
		}
		img := part + u.Ext
		if err := os.Rename(part, img); err != nil {
			return err
		}
		_ = p.Store.SetUploadStatus(u.ID, "complete")
		p.Report.Emit(protocol.MsgUploadReceived, protocol.UploadReceivedPayload{UploadId: u.ID, MediaId: u.MediaID, Kind: protocol.UploadKindThumbnail, Bytes: int(u.DeclaredSize), Sha256: sum})
		return p.Pipe.ThumbnailImageReceived(ctx, u.MediaID, img)
	default:
		return fmt.Errorf("unknown upload kind %q", u.Kind)
	}
}

// CancelUpload drops an announced upload and its partial bytes.
func (p *Public) CancelUpload(id string) {
	if !reUploadID.MatchString(id) {
		return
	}
	// Marked first so a waiting request finds the upload closed.
	_ = p.Store.SetUploadStatus(id, "cancelled")
	if unlock, ok := p.lockUpload(context.Background(), id); ok {
		defer unlock()
	}
	_ = os.Remove(p.partPath(id))
}

// SweepUploads removes expired/abandoned partial uploads.
func (p *Public) SweepUploads() {
	stale, err := p.Store.StaleUploads(config.PartialUploadMaxAge)
	if err != nil {
		return
	}
	for _, u := range stale {
		p.CancelUpload(u.ID)
	}
	_ = p.Store.PruneUploads(7 * 24 * time.Hour)
}

// ---- media ------------------------------------------------------------------

// authMedia verifies the playback token for the media id in the path.
func (p *Public) authMedia(r *http.Request) (*store.Media, *token.Claims, string, int) {
	if p.revoked.Load() {
		return nil, nil, "", 403
	}
	id := r.PathValue("id")
	if !pipeline.ValidID(id) {
		return nil, nil, "", 404
	}
	tok := r.URL.Query().Get("t")
	claims, err := p.Verifier.Verify(tok, id, time.Now())
	if err != nil {
		return nil, nil, "", 403
	}
	m, err := p.Store.GetMedia(id)
	if err != nil {
		return nil, nil, "", 404
	}
	return m, claims, tok, 0
}

func (p *Public) serveFile(w http.ResponseWriter, r *http.Request, path, ctype, cache string) {
	f, err := os.Open(path)
	if err != nil {
		fail(w, 404)
		return
	}
	defer f.Close()
	st, err := f.Stat()
	if err != nil || st.IsDir() {
		fail(w, 404)
		return
	}
	// Pace-based deadline so a stalled reader cannot hold the connection; cleared after, as keep-alive would carry it.
	rc := http.NewResponseController(w)
	if rc.SetWriteDeadline(time.Now().Add(transferTimeout(st.Size()))) == nil {
		defer func() { _ = rc.SetWriteDeadline(time.Time{}) }()
	}
	w.Header().Set("Content-Type", ctype)
	w.Header().Set("Cache-Control", cache)
	// ServeContent handles Range/If-Range; the name is only for MIME sniffing, which Content-Type settles.
	http.ServeContent(w, r, "", st.ModTime(), f)
}

var mimeByExt = map[string]string{
	".mp4": "video/mp4", ".m4v": "video/x-m4v", ".webm": "video/webm", ".mov": "video/quicktime",
	".mkv": "video/x-matroska", ".avi": "video/x-msvideo", ".jpg": "image/jpeg", ".jpeg": "image/jpeg",
	".png": "image/png", ".webp": "image/webp", ".gif": "image/gif",
}

const assetCache = "private, max-age=3600"

func (p *Public) mediaFile(which string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		m, _, _, code := p.authMedia(r)
		if code != 0 {
			fail(w, code)
			return
		}
		switch which {
		case "thumbnail":
			p.serveFile(w, r, p.Pipe.ThumbPath(m.ID), "image/jpeg", assetCache)
		case "preview":
			p.serveFile(w, r, media.PreviewPath(p.Pipe.Dir(m.ID)), "video/mp4", assetCache)
		case "original":
			if m.Type != "photo" && m.Type != "gif" {
				fail(w, 404)
				return
			}
			ct := mimeByExt[m.Ext]
			if ct == "" {
				ct = "application/octet-stream"
			}
			p.serveFile(w, r, p.Pipe.OriginalPath(m.ID, m.Ext), ct, assetCache)
		}
	}
}

func (p *Public) hlsMaster(w http.ResponseWriter, r *http.Request) {
	m, claims, tok, code := p.authMedia(r)
	if code != 0 {
		fail(w, code)
		return
	}
	raw, err := os.ReadFile(filepath.Join(media.HlsDir(p.Pipe.Dir(m.ID)), "master.m3u8"))
	if err != nil {
		fail(w, 404)
		return
	}
	w.Header().Set("Content-Type", "application/vnd.apple.mpegurl")
	w.Header().Set("Cache-Control", "private, max-age=60")
	_, _ = io.WriteString(w, media.RewriteMaster(string(raw), tok, claims.MaxHeight))
}

func (p *Public) hlsFile(w http.ResponseWriter, r *http.Request) {
	m, claims, tok, code := p.authMedia(r)
	if code != 0 {
		fail(w, code)
		return
	}
	variant, file := r.PathValue("variant"), r.PathValue("file")
	if !reVariant.MatchString(variant) || !reHlsFile.MatchString(file) {
		fail(w, 404)
		return
	}
	dir := media.HlsDir(p.Pipe.Dir(m.ID))
	if claims.MaxHeight > 0 && variant != "audio" {
		master, err := os.ReadFile(filepath.Join(dir, "master.m3u8"))
		if err != nil || !media.AllowedVariants(string(master), claims.MaxHeight)[variant] {
			fail(w, 403)
			return
		}
	}
	path := filepath.Join(dir, variant, file)
	if file == "index.m3u8" {
		raw, err := os.ReadFile(path)
		if err != nil {
			fail(w, 404)
			return
		}
		w.Header().Set("Content-Type", "application/vnd.apple.mpegurl")
		w.Header().Set("Cache-Control", "private, max-age=60")
		_, _ = io.WriteString(w, media.RewriteMediaPlaylist(string(raw), tok))
		return
	}
	ct := "video/iso.segment"
	if strings.HasSuffix(file, ".mp4") {
		ct = "video/mp4"
	}
	p.serveFile(w, r, path, ct, "private, max-age=86400")
}
