package server

import (
	"bytes"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/NotNecrotic/jeevo-connector/internal/config"
	"github.com/NotNecrotic/jeevo-connector/internal/media"
	"github.com/NotNecrotic/jeevo-connector/internal/pipeline"
	"github.com/NotNecrotic/jeevo-connector/internal/protocol"
	"github.com/NotNecrotic/jeevo-connector/internal/store"
	"github.com/NotNecrotic/jeevo-connector/internal/token"
)

type fakeReporter struct {
	mu     sync.Mutex
	events []string
}

func (f *fakeReporter) Emit(typ string, _ any) {
	f.mu.Lock()
	f.events = append(f.events, typ)
	f.mu.Unlock()
}
func (f *fakeReporter) Progress(protocol.MediaProgressPayload) {}

type rig struct {
	srv  *httptest.Server
	st   *store.Store
	pub  *Public
	priv ed25519.PrivateKey
	rep  *fakeReporter
	pipe *pipeline.Pipeline
}

func newRig(t *testing.T) *rig {
	dir := t.TempDir()
	st, err := store.Open(filepath.Join(dir, "c.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	pubKey, priv, _ := ed25519.GenerateKey(rand.Reader)
	v, _ := token.NewVerifier("abcdefgh", base64.StdEncoding.EncodeToString(pubKey))
	settings := config.DefaultSettings()
	get := func() config.Settings { return settings }
	rep := &fakeReporter{}
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	// No ffmpeg in the pipeline: Kick'd items fail at probe, which is fine here.
	pipe := &pipeline.Pipeline{Store: st, Tools: media.Tools{}, Report: rep, Log: log, Settings: get, Root: filepath.Join(dir, "media")}
	os.MkdirAll(filepath.Join(dir, "uploads"), 0o750)
	pub := &Public{Store: st, Pipe: pipe, Verifier: v, Log: log, Settings: get, UploadDir: filepath.Join(dir, "uploads"), Report: rep,
		FreeBytes: func(string) (int64, error) { return 1 << 40, nil }}
	srv := httptest.NewServer(pub.Handler())
	t.Cleanup(srv.Close)
	return &rig{srv: srv, st: st, pub: pub, priv: priv, rep: rep, pipe: pipe}
}

// mp4Bytes is n bytes that start like an MP4 file, since finishing an upload checks content against extension.
func mp4Bytes(n int) []byte {
	b := bytes.Repeat([]byte("x"), n)
	copy(b, "\x00\x00\x00\x18ftypisom")
	return b
}

func sha(s string) string {
	h := sha256.Sum256([]byte(s))
	return hex.EncodeToString(h[:])
}

func (r *rig) put(t *testing.T, id, tok string, offset int, body []byte) *http.Response {
	req, _ := http.NewRequest(http.MethodPut, r.srv.URL+"/v1/uploads/"+id, bytes.NewReader(body))
	req.Header.Set("Authorization", "Bearer "+tok)
	req.Header.Set("Upload-Offset", strconv.Itoa(offset))
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	return resp
}

func TestUploadFlow(t *testing.T) {
	r := newRig(t)
	tok := "tok_0123456789abcdefghijklmnop"
	data := mp4Bytes(1500 * 1024)
	if err := r.st.ExpectUpload(store.Upload{ID: "up_abcdefgh", MediaID: "m_test1", Kind: "media", TokenSHA256: sha(tok), Ext: ".mp4", DeclaredSize: int64(len(data)), ExpiresAt: time.Now().Add(time.Hour)}); err != nil {
		t.Fatal(err)
	}
	if resp := r.put(t, "up_abcdefgh", "wrong_token_wrong_token_wrong", 0, data[:10]); resp.StatusCode != 401 {
		t.Fatalf("bad token: %d", resp.StatusCode)
	}
	half := 1024 * 1024
	if resp := r.put(t, "up_abcdefgh", tok, 0, data[:half]); resp.StatusCode != 204 || resp.Header.Get("Upload-Offset") != strconv.Itoa(half) {
		t.Fatalf("chunk 1: %d %s", resp.StatusCode, resp.Header.Get("Upload-Offset"))
	}
	// Resending at a stale offset is refused with the real offset.
	if resp := r.put(t, "up_abcdefgh", tok, 0, data[:half]); resp.StatusCode != 409 || resp.Header.Get("Upload-Offset") != strconv.Itoa(half) {
		t.Fatalf("stale offset: %d", resp.StatusCode)
	}
	// HEAD reports progress for resume.
	req, _ := http.NewRequest(http.MethodHead, r.srv.URL+"/v1/uploads/up_abcdefgh", nil)
	req.Header.Set("Authorization", "Bearer "+tok)
	hr, _ := http.DefaultClient.Do(req)
	if hr.Header.Get("Upload-Offset") != strconv.Itoa(half) {
		t.Fatalf("head offset %q", hr.Header.Get("Upload-Offset"))
	}
	if resp := r.put(t, "up_abcdefgh", tok, half, data[half:]); resp.StatusCode != 201 {
		t.Fatalf("final chunk: %d", resp.StatusCode)
	}
	if _, err := os.Stat(r.pipe.OriginalPath("m_test1", ".mp4")); err != nil {
		t.Fatal("original not moved into place")
	}
	if m, err := r.st.GetMedia("m_test1"); err != nil || m.FileSize != int64(len(data)) {
		t.Fatalf("media row: %+v %v", m, err)
	}
	// A finished upload cannot be written again.
	if resp := r.put(t, "up_abcdefgh", tok, len(data), []byte("x")); resp.StatusCode != 404 {
		t.Fatalf("reuse after completion: %d", resp.StatusCode)
	}
	r.rep.mu.Lock()
	defer r.rep.mu.Unlock()
	if len(r.rep.events) == 0 || r.rep.events[0] != protocol.MsgUploadReceived {
		t.Fatalf("events: %v", r.rep.events)
	}
}

// A chunk is refused (507) if it would leave less than the declared size plus the 2 GiB reserve free.
func TestUploadDiskHeadroom(t *testing.T) {
	r := newRig(t)
	tok := "tok_0123456789abcdefghijklmnop"
	data := mp4Bytes(100 * 1024)
	if err := r.st.ExpectUpload(store.Upload{ID: "up_headroom", MediaID: "m_head1", Kind: "media", TokenSHA256: sha(tok), Ext: ".mp4", DeclaredSize: int64(len(data)), ExpiresAt: time.Now().Add(time.Hour)}); err != nil {
		t.Fatal(err)
	}
	need := 2*int64(len(data)) + config.DiskReserveBytes
	r.pub.FreeBytes = func(string) (int64, error) { return need - 1, nil }
	if resp := r.put(t, "up_headroom", tok, 0, data); resp.StatusCode != 507 {
		t.Fatalf("low disk: %d", resp.StatusCode)
	}
	r.pub.FreeBytes = func(string) (int64, error) { return need, nil }
	if resp := r.put(t, "up_headroom", tok, 0, data); resp.StatusCode != 201 {
		t.Fatalf("enough disk: %d", resp.StatusCode)
	}
}

func TestMediaRequiresToken(t *testing.T) {
	r := newRig(t)
	_ = r.st.InsertMedia(store.Media{ID: "m_pic01", Ext: ".jpg", Status: "done", Stage: "done"})
	os.MkdirAll(r.pipe.Dir("m_pic01"), 0o750)
	os.WriteFile(r.pipe.ThumbPath("m_pic01"), []byte("jpegbytes"), 0o640)
	url := r.srv.URL + "/v1/media/m_pic01/thumbnail.jpg"
	resp, _ := http.Get(url)
	if resp.StatusCode != 403 {
		t.Fatalf("no token: %d", resp.StatusCode)
	}
	if resp.Header.Get("Server") != "" {
		t.Fatal("Server header leaked")
	}
	tok := token.Sign(r.priv, token.Claims{Connector: "abcdefgh", Media: "m_pic01", Expires: time.Now().Add(time.Hour).Unix()})
	resp, _ = http.Get(url + "?t=" + tok)
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != 200 || string(body) != "jpegbytes" {
		t.Fatalf("with token: %d %q", resp.StatusCode, body)
	}
	// A token for another item does not open this one.
	other := token.Sign(r.priv, token.Claims{Connector: "abcdefgh", Media: "m_other", Expires: time.Now().Add(time.Hour).Unix()})
	if resp, _ := http.Get(url + "?t=" + other); resp.StatusCode != 403 {
		t.Fatalf("cross-item token: %d", resp.StatusCode)
	}
	// Unknown paths look like every other 404.
	if resp, _ := http.Get(r.srv.URL + "/admin"); resp.StatusCode != 404 {
		t.Fatalf("unknown path: %d", resp.StatusCode)
	}
}

func TestReachNonce(t *testing.T) {
	r := newRig(t)
	n := "abcdefghijklmnop1234"
	if resp, _ := http.Get(r.srv.URL + "/.well-known/jeevo-reach/" + n); resp.StatusCode != 404 {
		t.Fatal("unarmed nonce answered")
	}
	r.pub.AddReachNonce(n, time.Now().Add(time.Minute))
	if resp, _ := http.Get(r.srv.URL + "/.well-known/jeevo-reach/" + n); resp.StatusCode != 204 {
		t.Fatal("armed nonce not answered")
	}
}
