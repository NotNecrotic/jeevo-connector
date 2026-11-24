package server

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/NotNecrotic/jeevo-connector/internal/config"
	"github.com/NotNecrotic/jeevo-connector/internal/protocol"
	"github.com/NotNecrotic/jeevo-connector/internal/store"
	"github.com/NotNecrotic/jeevo-connector/internal/token"
)

const testTok = "tok_0123456789abcdefghijklmnop"

func (r *rig) expect(t *testing.T, id string, size int, ext string) {
	t.Helper()
	if err := r.st.ExpectUpload(store.Upload{ID: id, MediaID: "m_" + id, Kind: "media", TokenSHA256: sha(testTok), Ext: ext, DeclaredSize: int64(size), ExpiresAt: time.Now().Add(time.Hour)}); err != nil {
		t.Fatal(err)
	}
}

func (r *rig) events() []string {
	r.rep.mu.Lock()
	defer r.rep.mu.Unlock()
	return append([]string(nil), r.rep.events...)
}

// Uploads are judged by content, not extension: a playlist named .mp4 never reaches ffmpeg.
func TestUploadRejectsPlaylistNamedMp4(t *testing.T) {
	r := newRig(t)
	body := []byte("#EXTM3U\n#EXTINF:1,\n/etc/passwd\n#EXT-X-ENDLIST\n")
	r.expect(t, "up_playlist", len(body), ".mp4")
	if resp := r.put(t, "up_playlist", testTok, 0, body); resp.StatusCode != 400 {
		t.Fatalf("playlist upload: %d", resp.StatusCode)
	}
	if u, err := r.st.GetUpload("up_playlist"); err != nil || u.Status != "failed" {
		t.Fatalf("upload after rejection: %+v %v", u, err)
	}
	if _, err := os.Stat(r.pub.partPath("up_playlist")); !os.IsNotExist(err) {
		t.Fatal("the rejected file was kept")
	}
	if _, err := r.st.GetMedia("m_up_playlist"); err == nil {
		t.Fatal("a media row was created for it")
	}
	if ev := r.events(); len(ev) != 1 || ev[0] != protocol.MsgMediaFailed {
		t.Fatalf("events: %v", ev)
	}
	// A retry does not meet a full, stuck file: the upload is simply gone.
	if resp := r.put(t, "up_playlist", testTok, 0, body); resp.StatusCode != 404 {
		t.Fatalf("retry: %d", resp.StatusCode)
	}
}

// An image upload whose bytes are a video container (or the reverse) is refused too.
func TestUploadRejectsMismatchedContent(t *testing.T) {
	r := newRig(t)
	r.expect(t, "up_mismatch", 64, ".jpg")
	if resp := r.put(t, "up_mismatch", testTok, 0, mp4Bytes(64)); resp.StatusCode != 400 {
		t.Fatalf("mp4 in a .jpg: %d", resp.StatusCode)
	}
	r.expect(t, "up_garbage1", 64, ".mkv")
	if resp := r.put(t, "up_garbage1", testTok, 0, bytes.Repeat([]byte("z"), 64)); resp.StatusCode != 400 {
		t.Fatalf("garbage in a .mkv: %d", resp.StatusCode)
	}
}

// Several requests racing to deliver the same final chunk finish the upload once.
func TestConcurrentFinalChunk(t *testing.T) {
	r := newRig(t)
	data := mp4Bytes(4096)
	r.expect(t, "up_race0001", len(data), ".mp4")
	codes := make(chan int, 8)
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			req, _ := http.NewRequest(http.MethodPut, r.srv.URL+"/v1/uploads/up_race0001", bytes.NewReader(data))
			req.Header.Set("Authorization", "Bearer "+testTok)
			req.Header.Set("Upload-Offset", "0")
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				codes <- -1
				return
			}
			resp.Body.Close()
			codes <- resp.StatusCode
		}()
	}
	wg.Wait()
	close(codes)
	created := 0
	for c := range codes {
		switch c {
		case 201:
			created++
		case 404, 409:
		default:
			t.Errorf("unexpected status %d", c)
		}
	}
	if created != 1 {
		t.Fatalf("%d requests finished the upload", created)
	}
	received := 0
	for _, e := range r.events() {
		if e == protocol.MsgUploadReceived {
			received++
		}
	}
	if received != 1 {
		t.Fatalf("upload.received reported %d times", received)
	}
}

func TestUploadLockExclusion(t *testing.T) {
	p := &Public{}
	var inside, violations atomic.Int32
	var wg sync.WaitGroup
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 50; j++ {
				unlock, ok := p.lockUpload(context.Background(), "up_locked1")
				if !ok {
					violations.Add(1000)
					return
				}
				if inside.Add(1) != 1 {
					violations.Add(1)
				}
				inside.Add(-1)
				unlock()
			}
		}()
	}
	wg.Wait()
	if v := violations.Load(); v != 0 {
		t.Fatalf("lock failed %d times", v)
	}
	if len(p.upLocks) != 0 {
		t.Fatalf("%d lock entries left behind", len(p.upLocks))
	}
}

// A waiter blocks on the same lock the holder releases, even if the holder is cancelled or finishes first.
func TestUploadLockWaiterKeepsEntry(t *testing.T) {
	old := lockWait
	lockWait = 100 * time.Millisecond
	t.Cleanup(func() { lockWait = old })
	p := &Public{}
	unlock, ok := p.lockUpload(context.Background(), "up_waiter1")
	if !ok {
		t.Fatal("first lock refused")
	}
	got := make(chan bool, 1)
	go func() {
		u2, ok := p.lockUpload(context.Background(), "up_waiter1")
		if ok {
			u2()
		}
		got <- ok
	}()
	time.Sleep(20 * time.Millisecond)
	// A third caller arriving now must queue behind the same lock, not make a new one.
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	if _, ok := p.lockUpload(ctx, "up_waiter1"); ok {
		t.Fatal("a second holder got in")
	}
	unlock()
	if !<-got {
		t.Fatal("the waiter never got the lock")
	}
	if len(p.upLocks) != 0 {
		t.Fatalf("%d lock entries left behind", len(p.upLocks))
	}
	// A lock that cannot be had in time says so.
	unlock, _ = p.lockUpload(context.Background(), "up_waiter2")
	start := time.Now()
	if _, ok := p.lockUpload(context.Background(), "up_waiter2"); ok || time.Since(start) < lockWait/2 {
		t.Fatalf("wait for a held lock: ok=%v after %s", ok, time.Since(start))
	}
	unlock()
	if len(p.upLocks) != 0 {
		t.Fatalf("%d lock entries left behind", len(p.upLocks))
	}
}

// A client that stalls mid-chunk loses the chunk and the upload lock; the upload is usable again after.
func TestSlowBodyReleasesUploadLock(t *testing.T) {
	oldBase, oldWait := transferBase, lockWait
	transferBase, lockWait = 300*time.Millisecond, 10*time.Second
	t.Cleanup(func() { transferBase, lockWait = oldBase, oldWait })
	r := newRig(t)
	data := mp4Bytes(1000)
	r.expect(t, "up_slowbody", len(data), ".mp4")

	pr, pw := io.Pipe()
	defer pr.CloseWithError(io.ErrClosedPipe)
	req, _ := http.NewRequest(http.MethodPut, r.srv.URL+"/v1/uploads/up_slowbody", pr)
	req.ContentLength = int64(len(data))
	req.Header.Set("Authorization", "Bearer "+testTok)
	req.Header.Set("Upload-Offset", "0")
	go func() { _, _ = pw.Write(data[:100]) }() // then nothing more
	done := make(chan struct{})
	start := time.Now()
	go func() {
		if resp, err := http.DefaultClient.Do(req); err == nil {
			resp.Body.Close()
		}
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(8 * time.Second):
		t.Fatal("the stalled request was never ended")
	}
	if time.Since(start) > 5*time.Second {
		t.Fatalf("the stalled request lasted %s", time.Since(start))
	}
	// The partial chunk is gone and the upload takes the chunk again at once.
	if resp := r.put(t, "up_slowbody", testTok, 0, data); resp.StatusCode != 201 {
		t.Fatalf("retry after the stall: %d", resp.StatusCode)
	}
}

func TestRevokedConnectorServesNothing(t *testing.T) {
	r := newRig(t)
	_ = r.st.InsertMedia(store.Media{ID: "m_pic01", Ext: ".jpg", Type: "photo", Status: "done", Stage: "done"})
	os.MkdirAll(r.pipe.Dir("m_pic01"), 0o750)
	os.WriteFile(r.pipe.ThumbPath("m_pic01"), []byte("jpegbytes"), 0o640)
	os.WriteFile(r.pipe.OriginalPath("m_pic01", ".jpg"), []byte("jpegbytes"), 0o640)
	tok := token.Sign(r.priv, token.Claims{Connector: "abcdefgh", Media: "m_pic01", Expires: time.Now().Add(time.Hour).Unix()})
	media := []string{"/thumbnail.jpg", "/original", "/preview.mp4", "/hls/master.m3u8", "/hls/p720/index.m3u8"}
	get := func(path string) int {
		resp, err := http.Get(r.srv.URL + "/v1/media/m_pic01" + path + "?t=" + tok)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		return resp.StatusCode
	}
	if c := get("/thumbnail.jpg"); c != 200 {
		t.Fatalf("before revocation: %d", c)
	}
	r.expect(t, "up_revoked1", 1000, ".mp4")
	head := func() int {
		req, _ := http.NewRequest(http.MethodHead, r.srv.URL+"/v1/uploads/up_revoked1", nil)
		req.Header.Set("Authorization", "Bearer "+testTok)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		return resp.StatusCode
	}
	if c := head(); c != 204 {
		t.Fatalf("upload before revocation: %d", c)
	}

	r.pub.Revoke()
	for _, m := range media {
		if c := get(m); c != 403 {
			t.Errorf("%s after revocation: %d", m, c)
		}
	}
	if c := head(); c != 403 {
		t.Errorf("HEAD upload after revocation: %d", c)
	}
	if resp := r.put(t, "up_revoked1", testTok, 0, mp4Bytes(1000)); resp.StatusCode != 403 {
		t.Errorf("PUT upload after revocation: %d", resp.StatusCode)
	}
	if err := r.pub.Expect(store.Upload{ID: "up_revoked2", MediaID: "m_up_revoked2", Kind: "media", TokenSHA256: sha(testTok), Ext: ".mp4", DeclaredSize: 10, ExpiresAt: time.Now().Add(time.Hour)}); !errors.Is(err, ErrRevoked) {
		t.Errorf("announcing an upload after revocation: %v", err)
	}
}

func TestExpectCapsOpenUploads(t *testing.T) {
	r := newRig(t)
	announce := func(id string) error {
		return r.pub.Expect(store.Upload{ID: id, MediaID: "m_" + id, Kind: "media", TokenSHA256: sha(testTok), Ext: ".mp4", DeclaredSize: 1000, ExpiresAt: time.Now().Add(time.Hour)})
	}
	for i := 0; i < maxOpenUploads; i++ {
		if err := announce("up_cap" + strconv.Itoa(1000+i)); err != nil {
			t.Fatalf("upload %d: %v", i, err)
		}
	}
	if err := announce("up_cap9999"); !errors.Is(err, ErrTooManyUploads) {
		t.Fatalf("one over the cap: %v", err)
	}
	// Announcing an open upload again is a retry, not a new upload.
	if err := announce("up_cap1000"); err != nil {
		t.Fatalf("re-announcing: %v", err)
	}
	// Room appears when one is cancelled.
	r.pub.CancelUpload("up_cap1001")
	if err := announce("up_cap9999"); err != nil {
		t.Fatalf("after a cancel: %v", err)
	}
}

// Free space is checked against everything already accepted, not each upload alone.
func TestExpectReservesDiskAcrossUploads(t *testing.T) {
	r := newRig(t)
	const size = 1000
	free := config.DiskReserveBytes + 2*size*config.UploadSizeFactor + 10
	r.pub.FreeBytes = func(string) (int64, error) { return free, nil }
	announce := func(id string) error {
		return r.pub.Expect(store.Upload{ID: id, MediaID: "m_" + id, Kind: "media", TokenSHA256: sha(testTok), Ext: ".mp4", DeclaredSize: size, ExpiresAt: time.Now().Add(time.Hour)})
	}
	if err := announce("up_disk0001"); err != nil {
		t.Fatalf("first: %v", err)
	}
	if err := announce("up_disk0002"); err != nil {
		t.Fatalf("second: %v", err)
	}
	if err := announce("up_disk0003"); !errors.Is(err, ErrNoRoom) {
		t.Fatalf("third: %v", err)
	}
	// An original still being processed holds its output space too.
	r.pub.CancelUpload("up_disk0002")
	if err := r.st.InsertMedia(store.Media{ID: "m_busy01", Ext: ".mp4", Status: "processing", Stage: "hls_transcode", FileSize: size * 2}); err != nil {
		t.Fatal(err)
	}
	if err := announce("up_disk0004"); !errors.Is(err, ErrNoRoom) {
		t.Fatalf("while a job runs: %v", err)
	}
	_ = r.st.SetMediaStage("m_busy01", "done", "done", 1)
	if err := announce("up_disk0004"); err != nil {
		t.Fatalf("once the job is done: %v", err)
	}
}

// A chunk is refused when all open reservations would not fit, and accepted at equality.
func TestChunkChecksTotalReservation(t *testing.T) {
	r := newRig(t)
	data := mp4Bytes(100 * 1024)
	r.expect(t, "up_total001", len(data), ".mp4")
	r.expect(t, "up_total002", len(data), ".mp4")
	need := config.DiskReserveBytes + 2*int64(len(data))*config.UploadSizeFactor
	r.pub.FreeBytes = func(string) (int64, error) { return need - 1, nil }
	if resp := r.put(t, "up_total001", testTok, 0, data); resp.StatusCode != 507 {
		t.Fatalf("below the total: %d", resp.StatusCode)
	}
	r.pub.FreeBytes = func(string) (int64, error) { return need, nil }
	if resp := r.put(t, "up_total001", testTok, 0, data); resp.StatusCode != 201 {
		t.Fatalf("at the total: %d", resp.StatusCode)
	}
}
