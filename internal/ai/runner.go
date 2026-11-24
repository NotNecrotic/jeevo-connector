package ai

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/NotNecrotic/jeevo-connector/internal/config"
	"github.com/NotNecrotic/jeevo-connector/internal/protocol"
	"github.com/NotNecrotic/jeevo-connector/internal/store"
)

// JobType is the queue name of NSFW scans.
const JobType = "nsfw_scan"

// Worker states (see Runner.State).
const (
	StateRunning  = "running"
	StateStarting = "starting"
	StateStopped  = "stopped"
	StateMissing  = "missing"
	StateCrashed  = "crashed"
)

// leaseFor is how long a claimed job may run before it is handed out again; progress renews it.
const lease = 2 * time.Hour

// readyTimeout covers the first start, which may download a model.
const readyTimeout = 15 * time.Minute

// Events is how scan outcomes reach the pipeline.
type Events interface {
	ScanProgress(mediaID string, fraction float64, processed, total *int)
	ScanCompleted(mediaID string, result protocol.NsfwResult)
	ScanFailed(mediaID string, reason string)
	OriginalPath(mediaID string) (path string, typ string, duration float64, ok bool)
}

// Options locate the worker.
type Options struct {
	Python   string // interpreter of the worker's virtualenv
	Dir      string // directory that contains the ai/ package
	ModelDir string
	FFmpeg   string
	FFprobe  string
}

// Runner supervises the worker and dispatches jobs to it.
type Runner struct {
	Opts   Options
	Store  *store.Store
	Events Events
	Log    *slog.Logger

	mu       sync.Mutex
	settings protocol.AiSettings
	logLevel string
	state    string
	device   string
	restart  chan struct{}
	wake     chan struct{}
	current  *running
}

type running struct {
	jobID   string
	mediaID string
	cancel  func()
}

// New builds a runner.
func New(opts Options, st *store.Store, ev Events, log *slog.Logger) *Runner {
	return &Runner{Opts: opts, Store: st, Events: ev, Log: log, state: StateStopped,
		restart: make(chan struct{}, 1), wake: make(chan struct{}, 1)}
}

// State is the worker's lifecycle state.
func (r *Runner) State() string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.state
}

// Device is what the worker actually runs on ("cpu", "cuda", or "").
func (r *Runner) Device() string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.device
}

func (r *Runner) setState(s string) {
	r.mu.Lock()
	r.state = s
	if s != StateRunning {
		r.device = ""
	}
	r.mu.Unlock()
}

// Wake tells the dispatcher a job was queued.
func (r *Runner) Wake() {
	select {
	case r.wake <- struct{}{}:
	default:
	}
}

// Apply takes new settings. Device and log level need a new process.
func (r *Runner) Apply(s protocol.AiSettings, logLevel string) {
	r.mu.Lock()
	restart := r.settings.Device != s.Device || r.logLevel != logLevel
	first := r.settings.Device == ""
	r.settings, r.logLevel = s, logLevel
	r.mu.Unlock()
	if restart && !first {
		r.Log.Info("ai worker settings changed; restarting it")
		select {
		case r.restart <- struct{}{}:
		default:
		}
	}
}

// Cancel stops the running job when it belongs to mediaID (item deleted).
func (r *Runner) Cancel(mediaID string) {
	r.mu.Lock()
	cur := r.current
	r.mu.Unlock()
	if cur != nil && cur.mediaID == mediaID {
		cur.cancel()
	}
}

// Available reports whether the worker is installed.
func (r *Runner) Available() bool {
	if _, err := os.Stat(r.Opts.Python); err != nil {
		return false
	}
	_, err := os.Stat(filepath.Join(r.Opts.Dir, "ai", "__main__.py"))
	return err == nil
}

// Run supervises the worker until ctx ends.
func (r *Runner) Run(ctx context.Context) {
	backoff := 2 * time.Second
	for ctx.Err() == nil {
		if !r.Available() {
			r.setState(StateMissing)
			r.Log.Warn("ai worker is not installed; NSFW scans will wait", "python", r.Opts.Python, "dir", r.Opts.Dir)
			r.sleep(ctx, time.Minute)
			continue
		}
		start := time.Now()
		err := r.session(ctx)
		if ctx.Err() != nil {
			r.setState(StateStopped)
			return
		}
		if errors.Is(err, errRestart) {
			backoff = 2 * time.Second
			continue
		}
		if time.Since(start) > 5*time.Minute {
			backoff = 2 * time.Second
		}
		r.setState(StateCrashed)
		r.Log.Warn("ai worker stopped", "err", err, "restart_in", backoff)
		r.sleep(ctx, backoff)
		if backoff *= 2; backoff > 2*time.Minute {
			backoff = 2 * time.Minute
		}
	}
}

func (r *Runner) sleep(ctx context.Context, d time.Duration) {
	select {
	case <-ctx.Done():
	case <-time.After(d):
	case <-r.restart:
	}
}

var errRestart = errors.New("restart requested")

// message is any line the worker writes.
type message struct {
	Type      string            `json:"type"`
	ID        string            `json:"id"`
	Device    string            `json:"device"`
	GPU       *string           `json:"gpu"`
	Tasks     []string          `json:"tasks"`
	Fraction  *float64          `json:"fraction"`
	Processed *int              `json:"processed"`
	Total     *int              `json:"total"`
	Output    json.RawMessage   `json:"output"`
	Code      string            `json:"code"`
	Message   string            `json:"message"`
	Retryable bool              `json:"retryable"`
	Models    map[string]string `json:"models"`
}

type child struct {
	cmd     *exec.Cmd
	stdin   io.WriteCloser
	lines   chan message
	done    chan struct{} // closed once the process has exited
	waitErr error
}

func (c *child) exitErr() error {
	<-c.done
	return c.waitErr
}

// cpuThreads is always explicit: the worker's 0 means ONNX's default, which uses only physical cores.
func cpuThreads() int { return runtime.NumCPU() }

func (r *Runner) spawn() (*child, error) {
	r.mu.Lock()
	s, lvl := r.settings, r.logLevel
	r.mu.Unlock()
	if lvl == "warn" {
		lvl = "warning"
	}
	if lvl == "" {
		lvl = "info"
	}
	device := string(s.Device)
	if device == "" {
		device = "auto"
	}
	args := []string{"-m", "ai", "--model-dir", r.Opts.ModelDir, "--device", device,
		"--cpu-threads", strconv.Itoa(cpuThreads()), "--log-level", lvl}
	if r.Opts.FFmpeg != "" {
		args = append(args, "--ffmpeg", r.Opts.FFmpeg)
	}
	if r.Opts.FFprobe != "" {
		args = append(args, "--ffprobe", r.Opts.FFprobe)
	}
	cmd := exec.Command(r.Opts.Python, args...)
	cmd.Dir = r.Opts.Dir
	cmd.Env = []string{
		"PATH=" + os.Getenv("PATH"), "HOME=" + os.Getenv("HOME"), "LANG=C.UTF-8",
		"PYTHONUNBUFFERED=1", "PYTHONDONTWRITEBYTECODE=1",
	}
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true, Pdeathsig: syscall.SIGKILL}
	stdin, err := cmd.StdinPipe()
	if err != nil {
		return nil, err
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, err
	}
	stderr, err := cmd.StderrPipe()
	if err != nil {
		return nil, err
	}
	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("start: %w", err)
	}
	// Same nice level as the transcode ffmpeg so a scan never starves it.
	_ = syscall.Setpriority(syscall.PRIO_PROCESS, cmd.Process.Pid, config.FFmpegNice)
	c := &child{cmd: cmd, stdin: stdin, lines: make(chan message, 64), done: make(chan struct{})}
	go func() {
		sc := bufio.NewScanner(stderr)
		sc.Buffer(make([]byte, 64*1024), 1<<20)
		for sc.Scan() {
			// The worker logs "LEVEL message" (ai/__main__.py); keep its level.
			line := sc.Text()
			level := slog.LevelInfo
			for prefix, l := range map[string]slog.Level{"DEBUG ": slog.LevelDebug, "INFO ": slog.LevelInfo, "WARNING ": slog.LevelWarn, "ERROR ": slog.LevelError, "CRITICAL ": slog.LevelError} {
				if strings.HasPrefix(line, prefix) {
					level, line = l, strings.TrimPrefix(line, prefix)
					break
				}
			}
			r.Log.Log(context.Background(), level, line)
		}
	}()
	go func() {
		sc := bufio.NewScanner(stdout)
		sc.Buffer(make([]byte, 64*1024), 4<<20)
		for sc.Scan() {
			var m message
			if err := json.Unmarshal(sc.Bytes(), &m); err != nil || m.Type == "" {
				r.Log.Warn("ai worker wrote a malformed line")
				continue
			}
			c.lines <- m
		}
		close(c.lines)
		c.waitErr = cmd.Wait()
		close(c.done)
	}()
	return c, nil
}

func (c *child) send(v any) error {
	raw, err := json.Marshal(v)
	if err != nil {
		return err
	}
	_, err = c.stdin.Write(append(raw, '\n'))
	return err
}

func (c *child) kill() {
	if c.cmd.Process != nil {
		_ = syscall.Kill(-c.cmd.Process.Pid, syscall.SIGKILL)
	}
}

// stop asks the worker to exit and kills it if it does not.
func (c *child) stop() {
	_ = c.send(map[string]string{"type": "shutdown"})
	_ = c.stdin.Close()
	select {
	case <-c.done:
	case <-time.After(10 * time.Second):
		c.kill()
		<-c.done
	}
}

// session runs one worker process: wait for ready, then serve jobs.
func (r *Runner) session(ctx context.Context) error {
	r.setState(StateStarting)
	c, err := r.spawn()
	if err != nil {
		return err
	}
	defer c.stop()

	select {
	case m, ok := <-c.lines:
		switch {
		case !ok:
			return fmt.Errorf("exited before it was ready: %v", c.exitErr())
		case m.Type == "fatal":
			return fmt.Errorf("could not start: %s", m.Message)
		case m.Type != "ready":
			return fmt.Errorf("unexpected first message %q", m.Type)
		}
		r.mu.Lock()
		r.state, r.device = StateRunning, m.Device
		r.mu.Unlock()
		gpu := ""
		if m.GPU != nil {
			gpu = *m.GPU
		}
		r.Log.Info("ai worker ready", "device", m.Device, "gpu", gpu, "tasks", m.Tasks)
	case <-time.After(readyTimeout):
		c.kill()
		return errors.New("did not become ready in time")
	case <-ctx.Done():
		return ctx.Err()
	case <-r.restart:
		return errRestart
	}

	for {
		job, err := r.Store.ClaimJob(JobType, "ai", lease)
		if err != nil {
			return fmt.Errorf("claim: %w", err)
		}
		if job == nil {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-r.restart:
				return errRestart
			case <-r.wake:
			case <-time.After(30 * time.Second):
			case _, ok := <-c.lines:
				if !ok {
					return fmt.Errorf("exited: %v", c.exitErr())
				}
			}
			continue
		}
		if err := r.runJob(ctx, c, job); err != nil {
			return err
		}
	}
}

// runJob sends one job and waits for its outcome; an error means the worker must be replaced.
func (r *Runner) runJob(ctx context.Context, c *child, job *store.Job) error {
	path, typ, duration, ok := r.Events.OriginalPath(job.MediaID)
	if !ok {
		_, _, _ = r.Store.FailJob(job.ID, "ai", "media no longer exists", false)
		return nil
	}
	if _, err := os.Stat(path); err != nil {
		_, _, _ = r.Store.FailJob(job.ID, "ai", "original missing on disk", false)
		r.Events.ScanFailed(job.MediaID, "original missing on disk")
		return nil
	}
	cancelled := make(chan struct{})
	var once sync.Once
	r.mu.Lock()
	r.current = &running{jobID: job.ID, mediaID: job.MediaID, cancel: func() { once.Do(func() { close(cancelled) }) }}
	r.mu.Unlock()
	defer func() {
		r.mu.Lock()
		r.current = nil
		r.mu.Unlock()
	}()

	err := c.send(map[string]any{
		"type": "job", "id": job.ID, "task": JobType,
		"input": map[string]any{
			"path": path, "mediaType": typ, "threshold": config.NsfwThreshold,
			"sampleFps": config.NsfwSampleFps, "batchSize": config.NsfwBatchSize,
		},
	})
	if err != nil {
		r.requeue(job, "worker pipe closed")
		return fmt.Errorf("send job: %w", err)
	}
	// Generous: a feature-length video on a slow CPU is a legitimate wait.
	timeout := time.NewTimer(30*time.Minute + time.Duration(duration*2)*time.Second)
	defer timeout.Stop()
	for {
		select {
		case <-ctx.Done():
			r.requeue(job, "connector stopping")
			return ctx.Err()
		case <-r.restart:
			r.requeue(job, "worker restarting")
			return errRestart
		case <-cancelled:
			_ = c.send(map[string]string{"type": "cancel", "id": job.ID})
			cancelled = nil // wait for the worker's "cancelled" error
		case <-timeout.C:
			c.kill()
			r.fail(job, "scan timed out", true)
			return errors.New("job timed out; worker killed")
		case m, ok := <-c.lines:
			if !ok {
				r.fail(job, "ai worker crashed during the scan", true)
				return fmt.Errorf("exited mid-job: %v", c.exitErr())
			}
			if m.ID != job.ID {
				continue
			}
			switch m.Type {
			case "progress":
				_, _ = r.Store.HeartbeatJob(job.ID, "ai", lease, m.Fraction, m.Processed, m.Total)
				if m.Fraction != nil {
					r.Events.ScanProgress(job.MediaID, *m.Fraction, m.Processed, m.Total)
				}
			case "result":
				var res protocol.NsfwResult
				if err := json.Unmarshal(m.Output, &res); err != nil || (res.Status != "safe" && res.Status != "nsfw") {
					r.fail(job, "ai worker returned an invalid result", false)
					return nil
				}
				if owned, _ := r.Store.CompleteJob(job.ID, "ai", string(m.Output)); owned {
					r.Events.ScanCompleted(job.MediaID, res)
				}
				return nil
			case "error":
				r.fail(job, fmt.Sprintf("%s: %s", m.Code, m.Message), m.Retryable && m.Code != "cancelled")
				return nil
			}
		}
	}
}

// fail records a failure; the media item fails only when no retry is left.
func (r *Runner) fail(job *store.Job, reason string, retryable bool) {
	requeued, _, err := r.Store.FailJob(job.ID, "ai", reason, retryable)
	if err != nil {
		r.Log.Error("could not record ai job failure", "err", err)
		return
	}
	r.Log.Warn("nsfw scan failed", "media", job.MediaID, "reason", reason, "retrying", requeued)
	if !requeued {
		r.Events.ScanFailed(job.MediaID, reason)
	}
}

// requeue puts a job back without counting it as an attempt.
func (r *Runner) requeue(job *store.Job, why string) {
	if err := r.Store.RequeueJob(job.ID, "ai"); err != nil {
		r.Log.Warn("could not requeue ai job", "job", job.ID, "why", why, "err", err)
	}
}
