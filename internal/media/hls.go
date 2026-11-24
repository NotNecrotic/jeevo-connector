package media

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

/*
HLS ladder transcoding. One stored original becomes:

	<media dir>/hls/master.m3u8           multivariant playlist
	<media dir>/hls/p720/index.m3u8       one media playlist per rung (video only)
	<media dir>/hls/p720/init.mp4         fMP4 init segment
	<media dir>/hls/p720/seg_00000.m4s    ~segmentSeconds of video
	<media dir>/hls/audio/...             one shared AAC rendition

Every rung is H.264 High + AAC-LC in fMP4, which plays in hls.js everywhere
and natively in Safari. Rungs are measured on the SHORT side, never upscaled,
and the package is written to a temp dir and swapped in only once verified.
*/

const (
	nearRung     = 0.15
	audioBitrate = 128_000
	maxFPS       = 60
	// maxMediaSeconds caps the duration that sizes a job; the file's own metadata can claim anything.
	maxMediaSeconds = 12 * 60 * 60
	// outputSlack is how far output may exceed bitrate x duration before cutoff; outputFloor covers short files.
	outputSlack = 1.5
	outputFloor = 256 << 20
	// maxSegmentBytes caps any one file of a package or preview (RLIMIT_FSIZE).
	maxSegmentBytes = 512 << 20
	maxPreviewBytes = 256 << 20
)

// Encoder names.
const (
	EncX264  = "libx264"
	EncNVENC = "h264_nvenc"
	EncQSV   = "h264_qsv"
)

var hwEncoders = []string{EncNVENC, EncQSV}

// HlsOptions configure a transcode; Encoder and Qualities come from owner settings, the rest are fixed.
type HlsOptions struct {
	Encoder        string // auto | libx264 | h264_nvenc | h264_qsv
	Qualities      []int  // short-side heights, best first
	X264Preset     string
	SegmentSeconds int
	Nice           int
}

// Profile is the ladder recorded with a package.
func (o HlsOptions) Profile() string {
	parts := make([]string, len(o.Qualities))
	for i, q := range o.Qualities {
		parts[i] = strconv.Itoa(q)
	}
	return strings.Join(parts, ",")
}

// Rendition is one rung.
type Rendition struct {
	Height       int `json:"height"`
	Width        int `json:"width"`
	FrameHeight  int `json:"frameHeight"`
	VideoBitrate int `json:"videoBitrate"`
	AudioBitrate int `json:"audioBitrate"`
}

type encProfile struct {
	global     []string
	filterTail string
	codec      []string
}

func profileFor(enc string, o HlsOptions) encProfile {
	switch enc {
	case EncNVENC:
		return encProfile{nil, "format=yuv420p", []string{"-c:v", "h264_nvenc", "-preset", "p5", "-tune", "hq", "-rc", "vbr", "-cq", "23", "-profile:v", "high", "-forced-idr", "1"}}
	case EncQSV:
		return encProfile{[]string{"-init_hw_device", "qsv=hw", "-filter_hw_device", "hw"}, "format=nv12", []string{"-c:v", "h264_qsv", "-preset", "medium", "-profile:v", "high", "-look_ahead", "0"}}
	default:
		preset := o.X264Preset
		if preset == "" {
			preset = "veryfast"
		}
		return encProfile{nil, "format=yuv420p", []string{"-c:v", "libx264", "-preset", preset, "-profile:v", "high", "-crf", "21", "-sc_threshold", "0"}}
	}
}

// EncoderDetector resolves which encoder works on this host, once per configured value.
type EncoderDetector struct {
	mu    sync.Mutex
	cache map[string]string
	tools Tools
	works map[string]bool
}

// NewEncoderDetector builds a detector.
func NewEncoderDetector(t Tools) *EncoderDetector {
	return &EncoderDetector{cache: map[string]string{}, works: map[string]bool{}, tools: t}
}

func (d *EncoderDetector) encoderWorks(ctx context.Context, enc string, o HlsOptions) bool {
	if v, ok := d.works[enc]; ok {
		return v
	}
	p := profileFor(enc, o)
	args := []string{"-nostdin", "-v", "error", "-hide_banner"}
	args = append(args, p.global...)
	args = append(args, "-f", "lavfi", "-i", "testsrc2=size=640x360:rate=30", "-t", "1", "-vf", p.filterTail)
	args = append(args, p.codec...)
	args = append(args, "-f", "null", "-")
	_, err := Run(ctx, d.tools.FFmpeg, args, RunOpts{Timeout: 30 * time.Second})
	d.works[enc] = err == nil
	return err == nil
}

// Resolve returns the encoder: auto tests NVENC then QSV; a failing hardware encoder falls back to libx264.
func (d *EncoderDetector) Resolve(ctx context.Context, o HlsOptions) string {
	d.mu.Lock()
	defer d.mu.Unlock()
	key := o.Encoder
	if v, ok := d.cache[key]; ok {
		return v
	}
	cands := []string{}
	switch o.Encoder {
	case "auto", "":
		cands = hwEncoders
	case EncX264:
	default:
		cands = []string{o.Encoder}
	}
	chosen := EncX264
	for _, enc := range cands {
		if d.encoderWorks(ctx, enc, o) {
			chosen = enc
			break
		}
	}
	d.cache[key] = chosen
	return chosen
}

func even(n float64) int {
	v := int(math.Round(n/2) * 2)
	if v < 2 {
		return 2
	}
	return v
}

func videoBitrateFor(w, h int) int {
	kbps := 5000 * math.Pow(float64(w*h)/(1920*1080), 0.75)
	return int(math.Round(math.Max(300, kbps))) * 1000
}

// PlanLadder returns the rungs for a source, best first.
func PlanLadder(width, height int, hasAudio bool, ladder []int) []Rendition {
	short := width
	if height < short {
		short = height
	}
	landscape := width >= height
	long := width
	if height > long {
		long = height
	}
	aspect := float64(long) / float64(short)

	var shorts []int
	top := -1
	for _, r := range ladder {
		if r <= short {
			top = r
			break
		}
	}
	if top < 0 {
		shorts = append(shorts, even(float64(short)))
	} else {
		if short < ladder[0] && float64(short) > float64(top)*(1+nearRung) {
			shorts = append(shorts, even(float64(short)))
		}
		for _, r := range ladder {
			if r <= short {
				shorts = append(shorts, r)
			}
		}
	}
	out := make([]Rendition, 0, len(shorts))
	for _, s := range shorts {
		l := even(float64(s) * aspect)
		w, fh := s, l
		if landscape {
			w, fh = l, s
		}
		ab := 0
		if hasAudio {
			ab = audioBitrate
		}
		out = append(out, Rendition{Height: s, Width: w, FrameHeight: fh, VideoBitrate: videoBitrateFor(w, fh), AudioBitrate: ab})
	}
	return out
}

// RungDir is a rung's folder name.
func RungDir(h int) string { return fmt.Sprintf("p%d", h) }

// BuildFfmpegArgs builds the ladder command; paths are relative, so run ffmpeg with the package dir as cwd.
func BuildFfmpegArgs(input string, rungs []Rendition, audioStreams int, enc string, fps float64, o HlsOptions) []string {
	hasAudio := audioStreams > 0
	p := profileFor(enc, o)
	seg := o.SegmentSeconds
	if seg <= 0 {
		seg = 4
	}
	kf := seg / 2
	if kf < 1 {
		kf = 1
	}
	n := len(rungs)
	var splits strings.Builder
	for i := range rungs {
		fmt.Fprintf(&splits, "[s%d]", i)
	}
	fpsCap := ""
	if fps > maxFPS+0.5 {
		fpsCap = fmt.Sprintf("fps=%d,", maxFPS)
	}
	chains := make([]string, n)
	for i, r := range rungs {
		s := r.Height
		chains[i] = fmt.Sprintf("[s%d]%sscale=w='if(gte(iw,ih),-2,%d)':h='if(gte(iw,ih),%d,-2)',setsar=1,%s[o%d]", i, fpsCap, s, s, p.filterTail, i)
	}
	filter := fmt.Sprintf("[0:v:0]split=%d%s;%s", n, splits.String(), strings.Join(chains, ";"))
	var audioMap []string
	if hasAudio {
		var mix string
		mix, audioMap = AudioMap(audioStreams)
		if mix != "" {
			filter += ";" + mix
		}
	}

	args := []string{"-nostdin", "-v", "error", "-hide_banner", "-y", "-progress", "pipe:1", "-nostats"}
	args = append(args, p.global...)
	args = append(args, guardInput()...)
	args = append(args, "-i", input, "-filter_complex", filter)
	for i := range rungs {
		args = append(args, "-map", fmt.Sprintf("[o%d]", i))
	}
	args = append(args, audioMap...)
	args = append(args, p.codec...)
	for i, r := range rungs {
		b := int(math.Round(float64(r.VideoBitrate) / 1000))
		if enc != EncX264 {
			args = append(args, fmt.Sprintf("-b:v:%d", i), fmt.Sprintf("%dk", int(math.Round(float64(b)*0.8))))
		}
		args = append(args, fmt.Sprintf("-maxrate:v:%d", i), fmt.Sprintf("%dk", b), fmt.Sprintf("-bufsize:v:%d", i), fmt.Sprintf("%dk", b*2))
	}
	args = append(args, "-force_key_frames", fmt.Sprintf("expr:gte(t,n_forced*%d)", kf))
	if hasAudio {
		args = append(args, "-c:a", "aac", "-ac", "2", "-ar", "48000", "-b:a", fmt.Sprintf("%dk", audioBitrate/1000))
	}
	streamMap := make([]string, 0, n+1)
	for i, r := range rungs {
		if hasAudio {
			streamMap = append(streamMap, fmt.Sprintf("v:%d,agroup:aud,name:%s", i, RungDir(r.Height)))
		} else {
			streamMap = append(streamMap, fmt.Sprintf("v:%d,name:%s", i, RungDir(r.Height)))
		}
	}
	if hasAudio {
		streamMap = append(streamMap, "a:0,agroup:aud,name:audio,default:yes")
	}
	args = append(args,
		"-map_metadata", "-1",
		"-f", "hls",
		"-hls_time", strconv.Itoa(seg),
		"-hls_playlist_type", "vod",
		"-hls_segment_type", "fmp4",
		"-hls_flags", "independent_segments",
		"-hls_fmp4_init_filename", "init.mp4",
		"-hls_segment_filename", "%v/seg_%05d.m4s",
		"-master_pl_name", "master.m3u8",
		"-var_stream_map", strings.Join(streamMap, " "),
		"%v/index.m3u8",
	)
	return args
}

var (
	reMediaURI = regexp.MustCompile(`#EXT-X-MEDIA:[^\n]*URI="([^"]+)"`)
	reMapURI   = regexp.MustCompile(`#EXT-X-MAP:URI="([^"]+)"`)
)

// VerifyPackage checks the master lists every rung and all playlists are finished with existing files.
func VerifyPackage(dir string, rungs int, hasAudio bool) error {
	fail := func(f string, a ...any) error { return fmt.Errorf("verification failed: "+f, a...) }
	raw, err := os.ReadFile(filepath.Join(dir, "master.m3u8"))
	if err != nil {
		return fail("master.m3u8 was not written")
	}
	master := string(raw)
	lines := strings.Split(strings.ReplaceAll(master, "\r\n", "\n"), "\n")
	var variants []string
	for i, l := range lines {
		if strings.TrimSpace(l) != "" && !strings.HasPrefix(l, "#") && i > 0 && strings.HasPrefix(lines[i-1], "#EXT-X-STREAM-INF") {
			variants = append(variants, strings.TrimSpace(l))
		}
	}
	if len(variants) != rungs {
		return fail("master lists %d of %d video renditions", len(variants), rungs)
	}
	var mediaURIs []string
	for _, m := range reMediaURI.FindAllStringSubmatch(master, -1) {
		mediaURIs = append(mediaURIs, m[1])
	}
	if hasAudio && len(mediaURIs) != 1 {
		return fail("expected 1 shared audio rendition, found %d", len(mediaURIs))
	}
	for _, rel := range append(variants, mediaURIs...) {
		if strings.Contains(rel, "..") || strings.Contains(rel, "\\") || filepath.IsAbs(rel) {
			return fail("bad playlist path %s", rel)
		}
		pl, err := os.ReadFile(filepath.Join(dir, filepath.FromSlash(rel)))
		if err != nil {
			return fail("%s is missing", rel)
		}
		text := string(pl)
		if !strings.Contains(text, "#EXT-X-ENDLIST") {
			return fail("%s is incomplete", rel)
		}
		var segs []string
		for _, l := range strings.Split(text, "\n") {
			l = strings.TrimSpace(l)
			if l != "" && !strings.HasPrefix(l, "#") {
				segs = append(segs, l)
			}
		}
		if len(segs) == 0 {
			return fail("%s has no segments", rel)
		}
		m := reMapURI.FindStringSubmatch(text)
		if m == nil {
			return fail("%s has no init segment", rel)
		}
		base := filepath.Dir(filepath.Join(dir, filepath.FromSlash(rel)))
		for _, f := range append(segs, m[1]) {
			if _, err := os.Stat(filepath.Join(base, f)); err != nil {
				return fail("%s/%s is missing", filepath.Dir(rel), f)
			}
		}
	}
	return nil
}

func randHex(n int) string {
	b := make([]byte, n)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

// HlsResult describes a finished package.
type HlsResult struct {
	Encoder    string      `json:"encoder"`
	Profile    string      `json:"profile"`
	Renditions []Rendition `json:"renditions"`
	Preview    bool        `json:"preview"`
}

// Transcoder builds HLS packages.
type Transcoder struct {
	Tools    Tools
	Detector *EncoderDetector
}

// cappedSeconds is the claimed duration limited to maxMediaSeconds; 0 when none or nonsense.
func cappedSeconds(d float64) float64 {
	if !(d > 0) {
		return 0
	}
	return math.Min(d, maxMediaSeconds)
}

// outputBound is the most a ladder may write for duration seconds before it is stopped.
func outputBound(rungs []Rendition, duration float64) int64 {
	d := cappedSeconds(duration)
	if d == 0 {
		d = maxMediaSeconds
	}
	bps := float64(audioBitrate)
	for _, r := range rungs {
		bps += float64(r.VideoBitrate)
	}
	return int64(bps/8*d*outputSlack) + outputFloor
}

func (t *Transcoder) encodeToTemp(ctx context.Context, input, mediaDir string, rungs []Rendition, audioStreams int, src Probe, o HlsOptions, onProgress func(float64)) (string, string, error) {
	preferred := t.Detector.Resolve(ctx, o)
	timeout := time.Duration(math.Max(60*60, cappedSeconds(src.Duration)*30)) * time.Second
	attempt := func(enc string) (string, error) {
		tmp := filepath.Join(mediaDir, "hls.tmp-"+randHex(4))
		if err := os.MkdirAll(tmp, 0o750); err != nil {
			return "", err
		}
		var buf bytes.Buffer
		_, err := Run(ctx, t.Tools.FFmpeg, BuildFfmpegArgs(input, rungs, audioStreams, enc, src.FPS, o), RunOpts{
			Dir: tmp, Timeout: timeout, Nice: o.Nice,
			MaxFileBytes: maxSegmentBytes, MaxDirBytes: outputBound(rungs, src.Duration),
			OnStdout: func(b []byte) {
				if onProgress == nil || !(src.Duration > 0) {
					return
				}
				buf.Write(b)
				for {
					line, rerr := buf.ReadString('\n')
					if rerr != nil {
						buf.Reset()
						buf.WriteString(line)
						return
					}
					if v, ok := strings.CutPrefix(strings.TrimSpace(line), "out_time_us="); ok {
						if us, err := strconv.ParseFloat(v, 64); err == nil {
							onProgress(math.Min(0.99, us/1e6/src.Duration))
						}
					}
				}
			},
		})
		if err == nil {
			err = VerifyPackage(tmp, len(rungs), audioStreams > 0)
		}
		if err != nil {
			os.RemoveAll(tmp)
			return "", err
		}
		return tmp, nil
	}
	tmp, err := attempt(preferred)
	if err == nil {
		return tmp, preferred, nil
	}
	if preferred == EncX264 || ctx.Err() != nil {
		return "", "", err
	}
	tmp, err2 := attempt(EncX264)
	if err2 != nil {
		return "", "", fmt.Errorf("%s failed (%v); libx264 also failed: %w", preferred, err, err2)
	}
	return tmp, EncX264, nil
}

// HlsDir is the package directory of a media dir.
func HlsDir(mediaDir string) string { return filepath.Join(mediaDir, "hls") }

// Transcode builds a fresh package, replacing any previous one.
func (t *Transcoder) Transcode(ctx context.Context, input, mediaDir string, o HlsOptions, onProgress func(float64)) (*HlsResult, error) {
	input, _ = filepath.Abs(input)
	src, err := t.Tools.ProbeFile(ctx, input)
	if err != nil {
		return nil, err
	}
	if !src.HasVideo {
		return nil, errors.New("no video stream")
	}
	rungs := PlanLadder(src.Width, src.Height, src.HasAudio, o.Qualities)
	tmp, enc, err := t.encodeToTemp(ctx, input, mediaDir, rungs, src.AudioStreams, src, o, onProgress)
	if err != nil {
		return nil, err
	}
	defer os.RemoveAll(tmp)
	final := HlsDir(mediaDir)
	old := final + ".old-" + randHex(4)
	if _, err := os.Stat(final); err == nil {
		if err := os.Rename(final, old); err != nil {
			return nil, err
		}
	}
	if err := os.Rename(tmp, final); err != nil {
		return nil, err
	}
	os.RemoveAll(old)
	return &HlsResult{Encoder: enc, Profile: o.Profile(), Renditions: rungs}, nil
}

// ---- updating an existing package to a new ladder --------------------------

type masterEntry struct {
	inf    string
	uri    string
	height int
}

var reResolution = regexp.MustCompile(`RESOLUTION=(\d+)x(\d+)`)

func parseMaster(text string) (audio string, entries []masterEntry) {
	lines := strings.Split(strings.ReplaceAll(text, "\r\n", "\n"), "\n")
	for i := 0; i < len(lines); i++ {
		l := strings.TrimSpace(lines[i])
		if strings.HasPrefix(l, "#EXT-X-MEDIA:") && strings.Contains(l, "TYPE=AUDIO") {
			audio = l
		}
		if !strings.HasPrefix(l, "#EXT-X-STREAM-INF:") {
			continue
		}
		j := i + 1
		for j < len(lines) && (strings.TrimSpace(lines[j]) == "" || strings.HasPrefix(strings.TrimSpace(lines[j]), "#")) {
			j++
		}
		if j < len(lines) {
			if m := reResolution.FindStringSubmatch(l); m != nil {
				w, _ := strconv.Atoi(m[1])
				h, _ := strconv.Atoi(m[2])
				if w < h {
					h = w
				}
				entries = append(entries, masterEntry{inf: l, uri: strings.TrimSpace(lines[j]), height: h})
			}
		}
		i = j
	}
	return
}

func writeMaster(dir, audio string, entries []masterEntry) error {
	sort.Slice(entries, func(a, b int) bool { return entries[a].height > entries[b].height })
	var b strings.Builder
	b.WriteString("#EXTM3U\n#EXT-X-VERSION:7\n")
	if audio != "" {
		b.WriteString(audio + "\n")
	}
	for _, e := range entries {
		b.WriteString(e.inf + "\n" + e.uri + "\n\n")
	}
	tmp := filepath.Join(dir, "master.m3u8.tmp")
	if err := os.WriteFile(tmp, []byte(b.String()), 0o640); err != nil {
		return err
	}
	return os.Rename(tmp, filepath.Join(dir, "master.m3u8"))
}

var (
	reBandwidth = regexp.MustCompile(`BANDWIDTH=(\d+)`)
	reCodecs    = regexp.MustCompile(`CODECS="([^"]*)"`)
	reGroupID   = regexp.MustCompile(`GROUP-ID="([^"]+)"`)
	reAudioDir  = regexp.MustCompile(`URI="([^"/]+)/`)
)

func withSharedAudio(inf, audio string) string {
	g := reGroupID.FindStringSubmatch(audio)
	if g == nil {
		return inf
	}
	out := reBandwidth.ReplaceAllStringFunc(inf, func(m string) string {
		n, _ := strconv.Atoi(strings.TrimPrefix(m, "BANDWIDTH="))
		return fmt.Sprintf("BANDWIDTH=%d", n+audioBitrate)
	})
	out = reCodecs.ReplaceAllStringFunc(out, func(m string) string {
		if strings.Contains(m, "mp4a") {
			return m
		}
		return strings.TrimSuffix(m, `"`) + `,mp4a.40.2"`
	})
	if !strings.Contains(out, `AUDIO="`) {
		out += fmt.Sprintf(`,AUDIO="%s"`, g[1])
	}
	return out
}

// UpdateLadder changes only what differs from o.Qualities; an old-layout package is rebuilt.
func (t *Transcoder) UpdateLadder(ctx context.Context, input, mediaDir string, o HlsOptions) (*HlsResult, bool, error) {
	input, _ = filepath.Abs(input)
	dir := HlsDir(mediaDir)
	src, err := t.Tools.ProbeFile(ctx, input)
	if err != nil {
		return nil, false, err
	}
	desired := PlanLadder(src.Width, src.Height, src.HasAudio, o.Qualities)
	raw, err := os.ReadFile(filepath.Join(dir, "master.m3u8"))
	if err != nil {
		r, err := t.Transcode(ctx, input, mediaDir, o, nil)
		return r, true, err
	}
	audio, entries := parseMaster(string(raw))
	if len(entries) == 0 || src.HasAudio != (audio != "") {
		r, err := t.Transcode(ctx, input, mediaDir, o, nil)
		return r, true, err
	}
	have, want := map[int]bool{}, map[int]bool{}
	for _, e := range entries {
		have[e.height] = true
	}
	var toAdd []Rendition
	for _, r := range desired {
		want[r.Height] = true
		if !have[r.Height] {
			toAdd = append(toAdd, r)
		}
	}
	var next []masterEntry
	removed := 0
	for _, e := range entries {
		if want[e.height] {
			next = append(next, e)
		} else {
			removed++
		}
	}
	if removed == 0 && len(toAdd) == 0 {
		return &HlsResult{Encoder: "", Profile: o.Profile(), Renditions: desired}, false, nil
	}
	enc := ""
	if len(toAdd) > 0 {
		tmp, e, err := t.encodeToTemp(ctx, input, mediaDir, toAdd, 0, src, o, nil)
		if err != nil {
			return nil, false, err
		}
		enc = e
		mraw, _ := os.ReadFile(filepath.Join(tmp, "master.m3u8"))
		_, added := parseMaster(string(mraw))
		for _, a := range added {
			folder := strings.Split(a.uri, "/")[0]
			dest := filepath.Join(dir, folder)
			os.RemoveAll(dest)
			if err := os.Rename(filepath.Join(tmp, folder), dest); err != nil {
				os.RemoveAll(tmp)
				return nil, false, err
			}
			if audio != "" {
				a.inf = withSharedAudio(a.inf, audio)
			}
			next = append(next, a)
		}
		os.RemoveAll(tmp)
	}
	if err := writeMaster(dir, audio, next); err != nil {
		return nil, false, err
	}
	keep := map[string]bool{}
	for _, e := range next {
		keep[strings.Split(e.uri, "/")[0]] = true
	}
	if audio != "" {
		if m := reAudioDir.FindStringSubmatch(audio); m != nil {
			keep[m[1]] = true
		} else {
			keep["audio"] = true
		}
	}
	if ents, err := os.ReadDir(dir); err == nil {
		for _, de := range ents {
			if de.IsDir() && !keep[de.Name()] {
				os.RemoveAll(filepath.Join(dir, de.Name()))
			}
		}
	}
	if err := VerifyPackage(dir, len(next), src.HasAudio); err != nil {
		return nil, false, err
	}
	return &HlsResult{Encoder: enc, Profile: o.Profile(), Renditions: desired}, false, nil
}

// ---- hover preview --------------------------------------------------------

const (
	previewHeight      = 360
	previewClips       = 5
	previewClipSeconds = 2.0
	previewEdge        = 0.05
)

// Clip is one cut of the preview.
type Clip struct{ Start, Length float64 }

// PreviewClips spreads the preview across the video, skipping the edges.
func PreviewClips(duration float64) []Clip {
	total := previewClips * previewClipSeconds
	if !(duration > 0) {
		return []Clip{{0, total}}
	}
	a := duration * previewEdge
	span := duration * (1 - 2*previewEdge)
	step := span / previewClips
	if step < previewClipSeconds {
		return []Clip{{0, math.Max(0.5, math.Min(total, duration))}}
	}
	out := make([]Clip, previewClips)
	for i := range out {
		centre := a + (float64(i)+0.5)*step
		out[i] = Clip{math.Max(0, centre-previewClipSeconds/2), previewClipSeconds}
	}
	return out
}

// PreviewPath is where the preview of a media dir lives.
func PreviewPath(mediaDir string) string { return filepath.Join(mediaDir, "preview.mp4") }

// MakePreview encodes the muted 360p hover-preview clip.
func (t *Transcoder) MakePreview(ctx context.Context, input, mediaDir string, src Probe, nice int) error {
	clips := PreviewClips(src.Duration)
	short := src.Width
	if src.Height < short {
		short = src.Height
	}
	s := previewHeight
	if e := even(float64(short)); e < s {
		s = e
	}
	abs, _ := filepath.Abs(input)
	var args []string
	args = append(args, "-nostdin", "-v", "error", "-hide_banner", "-y")
	for _, c := range clips {
		if c.Start > 0 {
			args = append(args, "-ss", strconv.FormatFloat(c.Start, 'f', 3, 64))
		}
		args = append(args, "-t", strconv.FormatFloat(c.Length, 'f', 3, 64))
		args = append(args, guardInput()...)
		args = append(args, "-i", abs)
	}
	chains := make([]string, len(clips))
	for i := range clips {
		chains[i] = fmt.Sprintf("[%d:v:0]fps=30,scale=w='if(gte(iw,ih),-2,%d)':h='if(gte(iw,ih),%d,-2)',setsar=1,format=yuv420p,setpts=PTS-STARTPTS[c%d]", i, s, s, i)
	}
	var filter string
	if len(clips) == 1 {
		filter = strings.TrimSuffix(chains[0], "[c0]") + "[v]"
	} else {
		var ins strings.Builder
		for i := range clips {
			fmt.Fprintf(&ins, "[c%d]", i)
		}
		filter = strings.Join(chains, ";") + ";" + ins.String() + fmt.Sprintf("concat=n=%d:v=1:a=0[v]", len(clips))
	}
	out := PreviewPath(mediaDir)
	tmp := filepath.Join(mediaDir, "preview.tmp-"+randHex(4)+".mp4")
	defer os.Remove(tmp)
	args = append(args, "-filter_complex", filter, "-map", "[v]", "-an",
		"-c:v", "libx264", "-preset", "slow", "-profile:v", "high", "-crf", "30", "-maxrate", "600k", "-bufsize", "600k",
		"-g", "30", "-movflags", "+faststart", "-map_metadata", "-1", "-f", "mp4", tmp)
	if _, err := Run(ctx, t.Tools.FFmpeg, args, RunOpts{Timeout: 15 * time.Minute, Nice: nice, MaxFileBytes: maxPreviewBytes}); err != nil {
		return err
	}
	if st, err := os.Stat(tmp); err != nil || st.Size() == 0 {
		return errors.New("preview encode produced no output")
	}
	return os.Rename(tmp, out)
}

// SweepTemp removes leftovers from jobs that died mid-transcode.
func SweepTemp(mediaDir string) {
	ents, err := os.ReadDir(mediaDir)
	if err != nil {
		return
	}
	for _, e := range ents {
		n := e.Name()
		if strings.HasPrefix(n, "hls.tmp-") || strings.HasPrefix(n, "hls.old-") || strings.HasPrefix(n, "preview.tmp-") || strings.HasSuffix(n, ".tmp.jpg") {
			os.RemoveAll(filepath.Join(mediaDir, n))
		}
	}
}
