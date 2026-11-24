// Package pipeline processes a received original into a publishable item, persisting stage state.
package pipeline

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"regexp"
	"sync"
	"time"

	"github.com/NotNecrotic/jeevo-connector/internal/ai"
	"github.com/NotNecrotic/jeevo-connector/internal/config"
	"github.com/NotNecrotic/jeevo-connector/internal/media"
	"github.com/NotNecrotic/jeevo-connector/internal/protocol"
	"github.com/NotNecrotic/jeevo-connector/internal/store"
)

// Reporter delivers events to Jeevo.
type Reporter interface {
	// Emit persists the message in the outbox and delivers it when possible.
	Emit(typ string, payload any)
	// Progress is best-effort (dropped when offline).
	Progress(p protocol.MediaProgressPayload)
}

// Media statuses in the local store.
const (
	StatusReceived   = "received"
	StatusProcessing = "processing"
	StatusDone       = "done"
	StatusFailed     = "failed"
)

// Processing stages as stored in Media.Stage.
const (
	StageReceived     = string(protocol.ProcessingStageReceived)
	StageProbe        = string(protocol.ProcessingStageProbe)
	StageThumbnail    = string(protocol.ProcessingStageThumbnail)
	StageNsfwScan     = string(protocol.ProcessingStageNsfwScan)
	StageHlsTranscode = string(protocol.ProcessingStageHlsTranscode)
	StageSonicGraph   = string(protocol.ProcessingStageSonicGraph)
	StageDone         = string(protocol.ProcessingStageDone)
)

var idRe = regexp.MustCompile(`^[A-Za-z0-9_-]{4,64}$`)

// ValidID reports whether a media id is safe to use as a path component.
func ValidID(id string) bool { return idRe.MatchString(id) }

// Pipeline runs processing.
type Pipeline struct {
	Store    *store.Store
	Tools    media.Tools
	Trans    *media.Transcoder
	Report   Reporter
	Log      *slog.Logger
	Settings func() config.Settings
	Root     string // <dataDir>/media
	// AI is notified when a scan is queued and when an item is deleted.
	AI interface {
		Wake()
		Cancel(mediaID string)
	}

	mu       sync.Mutex
	running  map[string]context.CancelFunc
	slots    chan struct{}
	waiting  map[string]bool // media blocked in acquire, waiting for a heavy slot
	lastProg map[string]time.Time
	baseCtx  context.Context
}

// Start prepares the pipeline and resumes unfinished items.
func (p *Pipeline) Start(ctx context.Context) {
	p.baseCtx = ctx
	p.running = map[string]context.CancelFunc{}
	p.lastProg = map[string]time.Time{}
	p.slots = make(chan struct{}, config.ProcessingConcurrency)
	_ = p.Store.ReleaseAllProcessing()
	ids, err := p.Store.MediaIDsWithStatus(StatusReceived, StatusProcessing)
	if err != nil {
		p.Log.Error("cannot list unfinished media", "err", err)
		return
	}
	for _, id := range ids {
		media.SweepTemp(p.Dir(id))
		p.Kick(id)
	}
	if len(ids) > 0 {
		p.Log.Info("resuming unfinished media", "count", len(ids))
	}
	go p.recoverLoop(ctx)
}

// SettingsChanged rebuilds ladders when the quality ladder changes.
func (p *Pipeline) SettingsChanged(old, cur config.Settings) {
	if hlsOptions(old).Profile() != hlsOptions(cur).Profile() {
		go p.resyncLadders()
	}
}

func (p *Pipeline) recoverLoop(ctx context.Context) {
	t := time.NewTicker(time.Minute)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if rq, f, err := p.Store.RecoverJobs(); err == nil && (rq > 0 || f > 0) {
				p.Log.Warn("reclaimed expired ai jobs", "requeued", rq, "failed", f)
			}
			_ = p.Store.PruneJobs(30 * 24 * time.Hour)
		}
	}
}

// Dir is a media item's directory.
func (p *Pipeline) Dir(id string) string { return filepath.Join(p.Root, id) }

// OriginalPath is the stored original.
func (p *Pipeline) OriginalPath(id, ext string) string {
	return filepath.Join(p.Dir(id), "original"+ext)
}

// ThumbPath is the thumbnail.
func (p *Pipeline) ThumbPath(id string) string { return filepath.Join(p.Dir(id), "thumb.jpg") }

// Kick (re)starts processing an item; a no-op while it is already running.
func (p *Pipeline) Kick(id string) {
	p.mu.Lock()
	if p.running == nil {
		p.running = map[string]context.CancelFunc{}
		p.lastProg = map[string]time.Time{}
	}
	if p.baseCtx == nil {
		p.baseCtx = context.Background()
	}
	if p.slots == nil {
		p.slots = make(chan struct{}, config.ProcessingConcurrency)
	}
	if _, busy := p.running[id]; busy {
		p.mu.Unlock()
		return
	}
	ctx, cancel := context.WithCancel(p.baseCtx)
	p.running[id] = cancel
	p.mu.Unlock()
	go func() {
		defer func() {
			p.mu.Lock()
			delete(p.running, id)
			p.mu.Unlock()
			cancel()
		}()
		p.advance(ctx, id)
	}()
}

// Cancel stops in-flight work for an item (deleted).
func (p *Pipeline) Cancel(id string) {
	p.mu.Lock()
	if c, ok := p.running[id]; ok {
		c()
	}
	p.mu.Unlock()
}

// Delete removes an item and its files.
func (p *Pipeline) Delete(id string) error {
	if !ValidID(id) {
		return errors.New("bad media id")
	}
	p.Cancel(id)
	if p.AI != nil {
		p.AI.Cancel(id)
	}
	if err := p.Store.DeleteMedia(id); err != nil {
		return err
	}
	return os.RemoveAll(p.Dir(id))
}

// Weighted share of the bar for each stage.
var weight = map[string]float64{StageProbe: 0.5, StageThumbnail: 0.5, StageNsfwScan: 3, StageHlsTranscode: 6, StageSonicGraph: 1}

func (p *Pipeline) stagesFor(m *store.Media) []string {
	st := []string{StageProbe, StageThumbnail, StageNsfwScan}
	if media.IsVideoType(m.Type) || m.Type == "" {
		st = append(st, StageHlsTranscode)
		if m.Type == "video" && m.Duration > 60 {
			st = append(st, StageSonicGraph)
		}
	}
	return st
}

func (p *Pipeline) overall(m *store.Media, stage string, frac float64) float64 {
	stages := p.stagesFor(m)
	total, start := 0.0, 0.0
	found := false
	for _, s := range stages {
		if s == stage {
			found = true
			start = total
		}
		total += weight[s]
	}
	if !found || total == 0 {
		return 0
	}
	if frac < 0 {
		frac = 0
	}
	if frac > 1 {
		frac = 1
	}
	return (start + frac*weight[stage]) / total
}

func (p *Pipeline) progress(m *store.Media, stage string, frac float64, processed, total *int, force bool) {
	v := p.overall(m, stage, frac)
	p.mu.Lock()
	last := p.lastProg[m.ID]
	ok := force || time.Since(last) > 2*time.Second
	if ok {
		p.lastProg[m.ID] = time.Now()
	}
	p.mu.Unlock()
	if ok {
		// Stored as often as reported: sonic and the scan can call this many times a second.
		_ = p.Store.SetMediaStage(m.ID, StatusProcessing, stage, v)
		p.Report.Progress(protocol.MediaProgressPayload{MediaId: m.ID, Stage: protocol.ProcessingStage(stage), Progress: v, ProcessedFrames: processed, TotalFrames: total})
	}
}

func (p *Pipeline) fail(id, stage string, err error) {
	reason := err.Error()
	if len(reason) > 2000 {
		reason = reason[:2000]
	}
	p.Log.Warn("media failed", "media", id, "stage", stage, "err", reason)
	_ = p.Store.SetMediaStage(id, StatusFailed, stage, 1)
	_ = p.Store.SetMediaResult(id, "error", reason)
	p.Report.Emit(protocol.MsgMediaFailed, protocol.MediaFailedPayload{MediaId: id, Stage: protocol.ProcessingStage(stage), Reason: reason})
}

func hlsOptions(s config.Settings) media.HlsOptions {
	pr := s.Processing
	return media.HlsOptions{
		Encoder: string(pr.HlsEncoder), Qualities: pr.HlsQualities, X264Preset: config.X264Preset,
		SegmentSeconds: config.SegmentSeconds, Nice: config.FFmpegNice,
	}
}

// acquire takes a heavy-work slot; a non-empty id marks that media as waiting while it blocks.
func (p *Pipeline) acquire(ctx context.Context, id string) (func(), error) {
	p.mu.Lock()
	slots := p.slots
	if id != "" {
		if p.waiting == nil {
			p.waiting = map[string]bool{}
		}
		p.waiting[id] = true
	}
	p.mu.Unlock()
	defer func() {
		if id != "" {
			p.mu.Lock()
			delete(p.waiting, id)
			p.mu.Unlock()
		}
	}()
	select {
	case slots <- struct{}{}:
		return func() { <-slots }, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// Stats counts unfinished media as queued, active or failed; finished media are in none.
func (p *Pipeline) Stats() (queued, active, failed int) {
	_, counts, err := p.Store.MediaStats()
	if err != nil {
		return 0, 0, 0
	}
	received, _ := p.Store.MediaIDsWithStatus(StatusReceived)
	scanWaiting, _ := p.Store.ScanWaiting(ai.JobType)
	p.mu.Lock()
	slotWaiting := len(p.waiting)
	idle := 0 // received but nobody is working on it
	for _, id := range received {
		if _, busy := p.running[id]; !busy {
			idle++
		}
	}
	p.mu.Unlock()

	queued = idle + scanWaiting + slotWaiting
	// Items being probed are still "received" in the store.
	active = counts[StatusProcessing] + len(received) - idle - scanWaiting - slotWaiting
	if active < 0 {
		active = 0
	}
	return queued, active, counts[StatusFailed]
}

// advance runs stages until the item waits on the AI worker or finishes.
func (p *Pipeline) advance(ctx context.Context, id string) {
	for ctx.Err() == nil {
		m, err := p.Store.GetMedia(id)
		if err != nil {
			return
		}
		if m.Status == StatusDone || m.Status == StatusFailed {
			return
		}
		original := p.OriginalPath(id, m.Ext)
		switch m.Stage {
		case StageReceived, StageProbe:
			if err := p.probe(ctx, m, original); err != nil {
				if ctx.Err() == nil {
					p.fail(id, StageProbe, err)
				}
				return
			}
		case StageThumbnail:
			p.thumbnail(ctx, m, original)
			if ctx.Err() != nil {
				return
			}
			p.progress(m, StageNsfwScan, 0, nil, nil, true)
			if _, err := p.Store.EnqueueJob(id, ai.JobType, 3); err != nil {
				p.fail(id, StageNsfwScan, err)
				return
			}
			if p.AI != nil {
				p.AI.Wake()
			}
		case StageNsfwScan:
			// Waiting on the AI worker; make sure a job exists (it may have been lost in a crash).
			j, err := p.Store.LatestJob(id, ai.JobType)
			if err == nil && j.Status == "completed" && m.NsfwJSON == "" {
				var r protocol.NsfwResult
				if json.Unmarshal([]byte(j.ResultJSON), &r) == nil {
					p.ScanCompleted(id, r)
				}
				return
			}
			if err != nil || j.Status == "failed" && m.NsfwJSON == "" {
				if err == nil {
					p.fail(id, StageNsfwScan, fmt.Errorf("scan failed: %s", j.Error))
					return
				}
				_, _ = p.Store.EnqueueJob(id, ai.JobType, 3)
			}
			if m.NsfwJSON != "" {
				p.next(m, StageNsfwScan)
				continue
			}
			return
		case StageHlsTranscode:
			release, err := p.acquire(ctx, id)
			if err != nil {
				return
			}
			err = p.hls(ctx, m, original)
			release()
			if err != nil {
				if ctx.Err() == nil {
					p.fail(id, StageHlsTranscode, err)
				}
				return
			}
		case StageSonicGraph:
			release, err := p.acquire(ctx, id)
			if err != nil {
				return
			}
			p.sonic(ctx, m, original)
			release()
			if ctx.Err() != nil {
				return
			}
			p.next(m, StageSonicGraph)
		case StageDone:
			p.finish(m)
			return
		default:
			p.fail(id, m.Stage, fmt.Errorf("unknown stage %q", m.Stage))
			return
		}
	}
}

// next moves an item to the stage after `completed`.
func (p *Pipeline) next(m *store.Media, completed string) {
	m2, err := p.Store.GetMedia(m.ID)
	if err == nil {
		m = m2
	}
	stages := p.stagesFor(m)
	nextStage := StageDone
	for i, s := range stages {
		if s == completed && i+1 < len(stages) {
			nextStage = stages[i+1]
		}
	}
	prog := 1.0
	if nextStage != StageDone {
		prog = p.overall(m, nextStage, 0)
	}
	_ = p.Store.SetMediaStage(m.ID, StatusProcessing, nextStage, prog)
}

func (p *Pipeline) probe(ctx context.Context, m *store.Media, original string) error {
	st, err := os.Stat(original)
	if err != nil {
		return errors.New("original is missing")
	}
	var pr media.Probe
	if media.IsVideoExt(m.Ext) {
		pr, err = p.Tools.ProbeFile(ctx, original)
		if err != nil {
			return fmt.Errorf("probe: %w", err)
		}
		if !pr.HasVideo {
			return errors.New("file has no video stream")
		}
	} else {
		// Images: ffprobe gives dimensions too; failure is not fatal.
		pr, _ = p.Tools.ProbeFile(ctx, original)
	}
	typ := media.Classify(m.Ext, pr)
	raw, _ := json.Marshal(pr)
	if err := p.Store.UpdateMediaProbe(m.ID, typ, st.Size(), pr.Duration, pr.Width, pr.Height, pr.FPS, pr.HasAudio, string(raw)); err != nil {
		return err
	}
	_ = p.Store.SetMediaStage(m.ID, StatusProcessing, StageThumbnail, 0)
	nz := func(v float64) *float64 {
		if v == 0 {
			return nil
		}
		return &v
	}
	ni := func(v int) *int {
		if v == 0 {
			return nil
		}
		return &v
	}
	ns := func(v string) *string {
		if v == "" {
			return nil
		}
		return &v
	}
	p.Report.Emit(protocol.MsgMediaProbed, protocol.MediaProbedPayload{
		MediaId: m.ID, Type: protocol.MediaType(typ), FileSize: int(st.Size()),
		Probe: protocol.MediaProbe{Duration: nz(pr.Duration), Width: ni(pr.Width), Height: ni(pr.Height), Fps: nz(pr.FPS),
			VideoCodec: ns(pr.VideoCodec), AudioCodec: ns(pr.AudioCodec), Bitrate: ni(pr.Bitrate), Container: ns(pr.Container)},
	})
	return nil
}

func (p *Pipeline) thumbnail(ctx context.Context, m *store.Media, original string) {
	at := media.ThumbSeek(m.Type, m.Duration, config.ThumbPercent)
	if m.RequestedThumbAt != nil && media.IsVideoType(m.Type) {
		at = media.ClampThumb(*m.RequestedThumbAt, m.Duration)
	}
	// A creator-supplied thumbnail image may already be in place.
	if _, err := os.Stat(p.ThumbPath(m.ID)); err == nil && m.ThumbAt != nil {
		_ = p.Store.SetMediaStage(m.ID, StatusProcessing, StageNsfwScan, 0)
		return
	}
	if err := p.Tools.Thumbnail(ctx, original, p.ThumbPath(m.ID), at); err != nil {
		// Not fatal: retry at the start, then carry on without one.
		p.Log.Warn("thumbnail failed", "media", m.ID, "err", err)
		if at > 0 && p.Tools.Thumbnail(ctx, original, p.ThumbPath(m.ID), 0) == nil {
			at = 0
		}
	}
	_ = p.Store.SetMediaResult(m.ID, "thumb_at", at)
	_ = p.Store.SetMediaStage(m.ID, StatusProcessing, StageNsfwScan, 0)
}

// ---- AI worker callbacks (ai.Events) -----------------------------------------

// OriginalPathFor implements ai.Events (via Events).
func (p *Pipeline) OriginalPathFor(id string) (string, string, float64, bool) {
	m, err := p.Store.GetMedia(id)
	if err != nil {
		return "", "", 0, false
	}
	return p.OriginalPath(id, m.Ext), m.Type, m.Duration, true
}

// ScanProgress implements ai.Events.
func (p *Pipeline) ScanProgress(id string, frac float64, processed, total *int) {
	if m, err := p.Store.GetMedia(id); err == nil && m.Stage == StageNsfwScan {
		p.progress(m, StageNsfwScan, frac, processed, total, false)
	}
}

// ScanCompleted implements ai.Events.
func (p *Pipeline) ScanCompleted(id string, r protocol.NsfwResult) {
	m, err := p.Store.GetMedia(id)
	if err != nil || m.Status == StatusDone || m.Status == StatusFailed {
		return
	}
	raw, _ := json.Marshal(r)
	_ = p.Store.SetMediaResult(id, "nsfw_json", string(raw))
	p.next(m, StageNsfwScan)
	p.Kick(id)
}

// ScanFailed implements ai.Events.
func (p *Pipeline) ScanFailed(id, reason string) {
	m, err := p.Store.GetMedia(id)
	if err != nil || m.Status == StatusDone || m.Status == StatusFailed {
		return
	}
	p.fail(id, StageNsfwScan, fmt.Errorf("nsfw scan failed: %s", reason))
}

// ---- heavy stages -----------------------------------------------------------

func (p *Pipeline) hls(ctx context.Context, m *store.Media, original string) error {
	opts := hlsOptions(p.Settings())
	p.progress(m, StageHlsTranscode, 0, nil, nil, true)
	res, err := p.Trans.Transcode(ctx, original, p.Dir(m.ID), opts, func(f float64) {
		p.progress(m, StageHlsTranscode, f*0.9, nil, nil, false)
	})
	if err != nil {
		return err
	}
	p.progress(m, StageHlsTranscode, 0.9, nil, nil, true)
	// The hover preview is always made; a failure never blocks publishing.
	if src, perr := p.Tools.ProbeFile(ctx, original); perr == nil {
		if err := p.Trans.MakePreview(ctx, original, p.Dir(m.ID), src, opts.Nice); err != nil {
			p.Log.Warn("preview failed", "media", m.ID, "err", err)
		} else {
			res.Preview = true
		}
	}
	p.progress(m, StageHlsTranscode, 1, nil, nil, true)
	raw, _ := json.Marshal(res)
	_ = p.Store.SetMediaResult(m.ID, "hls_json", string(raw))
	p.next(m, StageHlsTranscode)
	return nil
}

func (p *Pipeline) sonic(ctx context.Context, m *store.Media, original string) {
	p.progress(m, StageSonicGraph, 0, nil, nil, true)
	r, err := p.Tools.Sonic(ctx, original, config.SonicMinSeconds, 30*time.Minute, func(f float64) {
		p.progress(m, StageSonicGraph, f, nil, nil, false)
	})
	if err != nil {
		// A missing graph never blocks publishing.
		p.Log.Warn("sonic graph failed", "media", m.ID, "err", err)
		r = media.SonicResult{Status: "error"}
	}
	raw, _ := json.Marshal(r)
	_ = p.Store.SetMediaResult(m.ID, "sonic_json", string(raw))
}

func (p *Pipeline) finish(m *store.Media) {
	var nsfw protocol.NsfwResult
	if err := json.Unmarshal([]byte(m.NsfwJSON), &nsfw); err != nil {
		p.fail(m.ID, StageDone, errors.New("no nsfw verdict recorded"))
		return
	}
	payload := protocol.MediaProcessedPayload{MediaId: m.ID, Type: protocol.MediaType(m.Type), Nsfw: nsfw}
	if m.ThumbAt != nil {
		payload.ThumbAt = *m.ThumbAt
	}
	if m.HlsJSON != "" {
		var h media.HlsResult
		if json.Unmarshal([]byte(m.HlsJSON), &h) == nil {
			payload.Hls = toProtoHls(h)
		}
	}
	if m.SonicJSON != "" {
		var s media.SonicResult
		if json.Unmarshal([]byte(m.SonicJSON), &s) == nil {
			ps := &protocol.SonicResult{Status: protocol.SonicResultStatus(s.Status), PeakDb: s.PeakDb}
			if len(s.Graph) > 0 {
				g := base64.StdEncoding.EncodeToString(s.Graph)
				ps.Graph = &g
			}
			payload.Sonic = ps
		}
	}
	_ = p.Store.SetMediaStage(m.ID, StatusDone, StageDone, 1)
	p.Report.Emit(protocol.MsgMediaProcessed, payload)
	p.Log.Info("media processed", "media", m.ID, "type", m.Type, "nsfw", nsfw.Status)
}

func toProtoHls(h media.HlsResult) *protocol.HlsResult {
	out := &protocol.HlsResult{Encoder: h.Encoder, Profile: h.Profile, Preview: h.Preview}
	for _, r := range h.Renditions {
		out.Renditions = append(out.Renditions, protocol.HlsRendition{Height: r.Height, Width: r.Width, FrameHeight: r.FrameHeight, VideoBitrate: r.VideoBitrate, AudioBitrate: r.AudioBitrate})
	}
	return out
}

// ---- thumbnails after publishing ------------------------------------------

// SetThumbnailAt regenerates the thumbnail from a chosen frame.
func (p *Pipeline) SetThumbnailAt(ctx context.Context, id string, at float64) error {
	m, err := p.Store.GetMedia(id)
	if err != nil {
		return err
	}
	_ = p.Store.SetMediaResult(id, "requested_thumb_at", at)
	if m.Status != StatusDone {
		return nil // picked up when the pipeline reaches the thumbnail (or finishes)
	}
	at = media.ClampThumb(at, m.Duration)
	if err := p.Tools.Thumbnail(ctx, p.OriginalPath(id, m.Ext), p.ThumbPath(id), at); err != nil {
		return err
	}
	_ = p.Store.SetMediaResult(id, "thumb_at", at)
	p.Report.Emit(protocol.MsgMediaThumbnailUpdated, protocol.MediaThumbnailUpdatedPayload{MediaId: id, ThumbAt: at})
	return nil
}

// ThumbnailImageReceived installs an uploaded replacement thumbnail.
func (p *Pipeline) ThumbnailImageReceived(ctx context.Context, id, imagePath string) error {
	defer os.Remove(imagePath)
	if _, err := p.Store.GetMedia(id); err != nil {
		return err
	}
	if err := p.Tools.ThumbnailFromImage(ctx, imagePath, p.ThumbPath(id)); err != nil {
		return err
	}
	_ = p.Store.SetMediaResult(id, "thumb_at", 0.0)
	p.Report.Emit(protocol.MsgMediaThumbnailUpdated, protocol.MediaThumbnailUpdatedPayload{MediaId: id, ThumbAt: 0})
	return nil
}

// resyncLadders brings finished videos in line with the current quality ladder, one at a time.
func (p *Pipeline) resyncLadders() {
	ids, err := p.Store.MediaIDsWithStatus(StatusDone)
	if err != nil {
		return
	}
	for _, id := range ids {
		if p.baseCtx.Err() != nil {
			return
		}
		m, err := p.Store.GetMedia(id)
		if err != nil || !media.IsVideoType(m.Type) || m.HlsJSON == "" {
			continue
		}
		var h media.HlsResult
		_ = json.Unmarshal([]byte(m.HlsJSON), &h)
		opts := hlsOptions(p.Settings())
		if h.Profile == opts.Profile() {
			continue
		}
		release, err := p.acquire(p.baseCtx, "")
		if err != nil {
			return
		}
		res, _, err := p.Trans.UpdateLadder(p.baseCtx, p.OriginalPath(id, m.Ext), p.Dir(id), opts)
		release()
		if err != nil {
			p.Log.Warn("ladder update failed", "media", id, "err", err)
			continue
		}
		if res.Encoder == "" {
			res.Encoder = h.Encoder
		}
		res.Preview = h.Preview
		raw, _ := json.Marshal(res)
		_ = p.Store.SetMediaResult(id, "hls_json", string(raw))
		p.Report.Emit(protocol.MsgMediaHlsUpdated, protocol.MediaHlsUpdatedPayload{MediaId: id, Hls: *toProtoHls(*res)})
	}
}

// Events adapts the pipeline to ai.Events.
type Events struct{ P *Pipeline }

func (e Events) ScanProgress(id string, f float64, a, b *int)   { e.P.ScanProgress(id, f, a, b) }
func (e Events) ScanCompleted(id string, r protocol.NsfwResult) { e.P.ScanCompleted(id, r) }
func (e Events) ScanFailed(id, reason string)                   { e.P.ScanFailed(id, reason) }
func (e Events) OriginalPath(id string) (string, string, float64, bool) {
	return e.P.OriginalPathFor(id)
}
