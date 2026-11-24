package pipeline

import (
	"context"
	"io"
	"log/slog"
	"path/filepath"
	"testing"
	"time"

	"github.com/NotNecrotic/jeevo-connector/internal/ai"
	"github.com/NotNecrotic/jeevo-connector/internal/store"
)

func newPipe(t *testing.T) *Pipeline {
	t.Helper()
	st, err := store.Open(filepath.Join(t.TempDir(), "c.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	return &Pipeline{Store: st, Log: slog.New(slog.NewTextHandler(io.Discard, nil))}
}

func add(t *testing.T, p *Pipeline, id, status, stage string) {
	t.Helper()
	if err := p.Store.InsertMedia(store.Media{ID: id, Ext: ".mp4", Status: status, Stage: stage}); err != nil {
		t.Fatal(err)
	}
}

// Stats counts every unfinished item once: queued vs active.
func TestStats(t *testing.T) {
	p := newPipe(t)
	p.running = map[string]context.CancelFunc{}

	add(t, p, "m_done", StatusDone, "done")
	add(t, p, "m_bad", StatusFailed, "hls_transcode")
	add(t, p, "m_recv_idle", StatusReceived, "received")    // nobody picked it up: queued
	add(t, p, "m_recv_busy", StatusReceived, "received")    // being probed: active
	add(t, p, "m_thumb", StatusProcessing, "thumbnail")     // active
	add(t, p, "m_scan_wait", StatusProcessing, "nsfw_scan") // job queued: queued
	add(t, p, "m_scan_run", StatusProcessing, "nsfw_scan")  // job claimed: active
	add(t, p, "m_hls_wait", StatusProcessing, "hls_transcode")
	add(t, p, "m_hls_run", StatusProcessing, "hls_transcode")
	p.running["m_recv_busy"] = func() {}

	// The scanner has claimed m_scan_run's job; m_scan_wait's is still queued.
	if _, err := p.Store.EnqueueJob("m_scan_run", ai.JobType, 3); err != nil {
		t.Fatal(err)
	}
	if j, err := p.Store.ClaimJob(ai.JobType, "ai", time.Minute); err != nil || j == nil || j.MediaID != "m_scan_run" {
		t.Fatalf("claim: %+v %v", j, err)
	}
	if _, err := p.Store.EnqueueJob("m_scan_wait", ai.JobType, 3); err != nil {
		t.Fatal(err)
	}
	p.waiting = map[string]bool{"m_hls_wait": true}

	queued, active, failed := p.Stats()
	// queued: m_recv_idle + m_scan_wait + m_hls_wait
	if queued != 3 || failed != 1 {
		t.Fatalf("queued=%d active=%d failed=%d", queued, active, failed)
	}
	// active: m_recv_busy + m_thumb + m_scan_run + m_hls_run
	if active != 4 {
		t.Fatalf("active=%d (queued=%d)", active, queued)
	}
}

func TestAcquireTracksWaiting(t *testing.T) {
	p := newPipe(t)
	p.slots = make(chan struct{}, 1)
	rel, err := p.acquire(context.Background(), "m_first")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		_, _ = p.acquire(ctx, "m_second")
		close(done)
	}()
	deadline := time.Now().Add(5 * time.Second)
	for {
		p.mu.Lock()
		n := len(p.waiting)
		p.mu.Unlock()
		if n == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("waiting media not tracked")
		}
		time.Sleep(5 * time.Millisecond)
	}
	cancel()
	<-done
	p.mu.Lock()
	n := len(p.waiting)
	p.mu.Unlock()
	if n != 0 {
		t.Fatalf("waiting not cleared: %d", n)
	}
	rel()
}
