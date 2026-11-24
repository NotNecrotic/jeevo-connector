package ai

import (
	"context"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/NotNecrotic/jeevo-connector/internal/protocol"
	"github.com/NotNecrotic/jeevo-connector/internal/store"
)

type events struct {
	mu        sync.Mutex
	paths     map[string]string
	completed map[string]protocol.NsfwResult
	failed    map[string]string
	progress  int
}

func (e *events) ScanProgress(string, float64, *int, *int) { e.mu.Lock(); e.progress++; e.mu.Unlock() }
func (e *events) ScanCompleted(id string, r protocol.NsfwResult) {
	e.mu.Lock()
	e.completed[id] = r
	e.mu.Unlock()
}
func (e *events) ScanFailed(id, reason string) { e.mu.Lock(); e.failed[id] = reason; e.mu.Unlock() }
func (e *events) OriginalPath(id string) (string, string, float64, bool) {
	e.mu.Lock()
	defer e.mu.Unlock()
	p, ok := e.paths[id]
	return p, "video", 10, ok
}

func (e *events) done(id string) bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	_, a := e.completed[id]
	_, b := e.failed[id]
	return a || b
}

func setup(t *testing.T) (*Runner, *store.Store, *events, string) {
	t.Helper()
	py, err := exec.LookPath("python3")
	if err != nil {
		t.Skip("python3 not installed")
	}
	dir := t.TempDir()
	st, err := store.Open(filepath.Join(dir, "c.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	ev := &events{paths: map[string]string{}, completed: map[string]protocol.NsfwResult{}, failed: map[string]string{}}
	wd, _ := os.Getwd()
	r := New(Options{Python: py, Dir: filepath.Join(wd, "testdata"), ModelDir: dir}, st, ev, slog.New(slog.NewTextHandler(io.Discard, nil)))
	r.Apply(protocol.AiSettings{Device: "cpu"}, "info")
	return r, st, ev, dir
}

func queue(t *testing.T, r *Runner, st *store.Store, ev *events, dir, id, name string) {
	t.Helper()
	p := filepath.Join(dir, name)
	if err := os.WriteFile(p, []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	ev.mu.Lock()
	ev.paths[id] = p
	ev.mu.Unlock()
	if _, err := st.EnqueueJob(id, JobType, 3); err != nil {
		t.Fatal(err)
	}
	r.Wake()
}

func wait(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatal("timed out")
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func TestRunnerLifecycle(t *testing.T) {
	r, st, ev, dir := setup(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go r.Run(ctx)

	queue(t, r, st, ev, dir, "m_ok", "ok.mp4")
	wait(t, func() bool { return ev.done("m_ok") })
	if res, ok := ev.completed["m_ok"]; !ok || res.Status != "safe" || res.Threshold != 0.8 {
		t.Fatalf("result: %+v failed=%v", res, ev.failed)
	}
	if r.State() != StateRunning || r.Device() != "cpu" {
		t.Fatalf("state %s device %s", r.State(), r.Device())
	}

	// A crash mid-job is retried on a fresh worker process and then succeeds.
	queue(t, r, st, ev, dir, "m_crash", "crash.mp4")
	wait(t, func() bool { return ev.done("m_crash") })
	if _, ok := ev.completed["m_crash"]; !ok {
		t.Fatalf("crash was not retried: %v", ev.failed)
	}
	j, _ := st.LatestJob("m_crash", JobType)
	if j.Attempts != 2 {
		t.Fatalf("attempts %d", j.Attempts)
	}

	// A non-retryable error fails the item at once.
	queue(t, r, st, ev, dir, "m_bad", "bad.mp4")
	wait(t, func() bool { return ev.done("m_bad") })
	if ev.failed["m_bad"] == "" {
		t.Fatal("bad media should fail")
	}

	// Cancel stops a running job (the item was deleted).
	queue(t, r, st, ev, dir, "m_slow", "slow.mp4")
	wait(t, func() bool {
		r.mu.Lock()
		defer r.mu.Unlock()
		return r.current != nil && r.current.mediaID == "m_slow"
	})
	r.Cancel("m_slow")
	wait(t, func() bool {
		j, err := st.LatestJob("m_slow", JobType)
		return err == nil && j.Status == "failed"
	})
	// Worker keeps serving afterwards.
	queue(t, r, st, ev, dir, "m_after", "after.mp4")
	wait(t, func() bool { return ev.done("m_after") })

	// A device/log level change replaces the process; jobs keep flowing.
	r.Apply(protocol.AiSettings{Device: "cpu"}, "debug")
	queue(t, r, st, ev, dir, "m_restarted", "restarted.mp4")
	wait(t, func() bool { return ev.done("m_restarted") })
	if _, ok := ev.completed["m_restarted"]; !ok || r.State() != StateRunning {
		t.Fatalf("after restart: state %s failed=%v", r.State(), ev.failed)
	}
}
