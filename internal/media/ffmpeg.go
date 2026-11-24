// Package media is everything the connector does to bytes: probing, thumbnails, HLS, sonic graph.
package media

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"golang.org/x/sys/unix"
)

// Tools locates ffmpeg and ffprobe.
type Tools struct {
	FFmpeg  string
	FFprobe string
}

// FindTools resolves the binaries from explicit paths or PATH.
func FindTools(ffmpeg, ffprobe string) Tools {
	look := func(explicit, name string) string {
		if explicit != "" {
			return explicit
		}
		if p, err := exec.LookPath(name); err == nil {
			return p
		}
		return ""
	}
	return Tools{FFmpeg: look(ffmpeg, "ffmpeg"), FFprobe: look(ffprobe, "ffprobe")}
}

// inputFormats are the demuxers an upload may need (see CheckUpload); keep in step with ai/media.py.
const inputFormats = "mov,matroska,avi,image2,jpeg_pipe,png_pipe,webp_pipe,gif"

// guardInput allows only the file protocol and known demuxers: blocks playlists reading files or network.
func guardInput() []string {
	return []string{"-protocol_whitelist", "file", "-format_whitelist", inputFormats}
}

// How often a job's output directory is measured against RunOpts.MaxDirBytes.
const dirWatchEvery = 2 * time.Second

// RunOpts controls a child process.
type RunOpts struct {
	Timeout  time.Duration
	Dir      string
	Nice     int
	OnStdout func([]byte)
	// MaxFileBytes caps any one file the child writes (RLIMIT_FSIZE); 0 = no cap.
	MaxFileBytes int64
	// MaxDirBytes kills the child when Dir grows beyond it; 0 = no cap.
	MaxDirBytes int64
}

// dirSize adds up the files under dir.
func dirSize(dir string) int64 {
	var n int64
	_ = filepath.WalkDir(dir, func(_ string, d os.DirEntry, err error) error {
		if err == nil && d.Type().IsRegular() {
			if fi, ierr := d.Info(); ierr == nil {
				n += fi.Size()
			}
		}
		return nil
	})
	return n
}

// Run executes bin with args and returns stdout; killed on timeout or cancel, at the requested nice level.
func Run(ctx context.Context, bin string, args []string, o RunOpts) ([]byte, error) {
	if bin == "" {
		return nil, errors.New("ffmpeg/ffprobe is not installed")
	}
	if o.Timeout <= 0 {
		o.Timeout = 10 * time.Minute
	}
	ctx, cancel := context.WithTimeout(ctx, o.Timeout)
	defer cancel()
	cmd := exec.CommandContext(ctx, bin, args...)
	cmd.Dir = o.Dir
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	var stderr tailBuffer
	cmd.Stderr = &stderr
	var stdout bytes.Buffer
	var pipe io.ReadCloser
	var err error
	if o.OnStdout != nil {
		if pipe, err = cmd.StdoutPipe(); err != nil {
			return nil, err
		}
	} else {
		cmd.Stdout = &limitWriter{w: &stdout, n: 16 << 20}
	}
	if err := cmd.Start(); err != nil {
		return nil, err
	}
	if o.Nice > 0 {
		_ = syscall.Setpriority(syscall.PRIO_PROCESS, cmd.Process.Pid, o.Nice)
	}
	if o.MaxFileBytes > 0 {
		lim := unix.Rlimit{Cur: uint64(o.MaxFileBytes), Max: uint64(o.MaxFileBytes)}
		if err := unix.Prlimit(cmd.Process.Pid, unix.RLIMIT_FSIZE, &lim, nil); err != nil {
			cancel()
			_ = cmd.Wait()
			return nil, fmt.Errorf("limit %s output size: %w", filepath.Base(bin), err)
		}
	}
	var tooBig atomic.Bool
	if o.MaxDirBytes > 0 && o.Dir != "" {
		go func() {
			t := time.NewTicker(dirWatchEvery)
			defer t.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-t.C:
					if dirSize(o.Dir) > o.MaxDirBytes {
						tooBig.Store(true)
						cancel()
						return
					}
				}
			}
		}()
	}
	if pipe != nil {
		buf := make([]byte, 32*1024)
		for {
			n, rerr := pipe.Read(buf)
			if n > 0 {
				o.OnStdout(buf[:n])
			}
			if rerr != nil {
				break
			}
		}
	}
	err = cmd.Wait()
	if tooBig.Load() {
		return nil, fmt.Errorf("%s output grew beyond %d MiB", filepath.Base(bin), o.MaxDirBytes>>20)
	}
	if ctx.Err() == context.DeadlineExceeded {
		return nil, fmt.Errorf("%s timed out after %s", filepath.Base(bin), o.Timeout)
	}
	if ctx.Err() == context.Canceled {
		return nil, context.Canceled
	}
	if err != nil {
		tail := strings.TrimSpace(stderr.String())
		lines := strings.Split(tail, "\n")
		if len(lines) > 4 {
			lines = lines[len(lines)-4:]
		}
		return nil, fmt.Errorf("%s failed: %v: %s", filepath.Base(bin), err, strings.Join(lines, " | "))
	}
	return stdout.Bytes(), nil
}

type tailBuffer struct{ b []byte }

func (t *tailBuffer) Write(p []byte) (int, error) {
	t.b = append(t.b, p...)
	if len(t.b) > 4000 {
		t.b = t.b[len(t.b)-4000:]
	}
	return len(p), nil
}
func (t *tailBuffer) String() string { return string(t.b) }

type limitWriter struct {
	w io.Writer
	n int
}

func (l *limitWriter) Write(p []byte) (int, error) {
	if l.n <= 0 {
		return len(p), nil
	}
	if len(p) > l.n {
		_, _ = l.w.Write(p[:l.n])
		l.n = 0
		return len(p), nil
	}
	l.n -= len(p)
	return l.w.Write(p)
}

// ---- probe ----------------------------------------------------------------

// Probe is what ffprobe tells us about a file (display orientation).
type Probe struct {
	Duration     float64 `json:"duration"`
	Width        int     `json:"width"`
	Height       int     `json:"height"`
	FPS          float64 `json:"fps"`
	VideoCodec   string  `json:"videoCodec"`
	AudioCodec   string  `json:"audioCodec"`
	Bitrate      int     `json:"bitrate"`
	Container    string  `json:"container"`
	HasVideo     bool    `json:"hasVideo"`
	HasAudio     bool    `json:"hasAudio"`
	AudioStreams int     `json:"audioStreams"` // playback mixes them all into one
	NbFrames     int     `json:"nbFrames"`
}

type ffprobeOut struct {
	Format struct {
		Duration   string `json:"duration"`
		BitRate    string `json:"bit_rate"`
		FormatName string `json:"format_name"`
	} `json:"format"`
	Streams []struct {
		CodecType    string `json:"codec_type"`
		CodecName    string `json:"codec_name"`
		Width        int    `json:"width"`
		Height       int    `json:"height"`
		AvgFrameRate string `json:"avg_frame_rate"`
		RFrameRate   string `json:"r_frame_rate"`
		NbFrames     string `json:"nb_frames"`
		Tags         struct {
			Rotate string `json:"rotate"`
		} `json:"tags"`
		SideData []struct {
			Rotation *float64 `json:"rotation"`
		} `json:"side_data_list"`
	} `json:"streams"`
}

func rational(s string) float64 {
	if a, b, ok := strings.Cut(s, "/"); ok {
		x, _ := strconv.ParseFloat(a, 64)
		y, _ := strconv.ParseFloat(b, 64)
		if y != 0 && !math.IsNaN(x/y) && !math.IsInf(x/y, 0) {
			return x / y
		}
		return 0
	}
	v, _ := strconv.ParseFloat(s, 64)
	return v
}

// ProbeFile runs ffprobe; width/height are as displayed (a 90-degree rotation flag reports portrait).
func (t Tools) ProbeFile(ctx context.Context, path string) (Probe, error) {
	args := append([]string{"-v", "quiet"}, guardInput()...)
	out, err := Run(ctx, t.FFprobe, append(args, "-print_format", "json", "-show_format", "-show_streams", path), RunOpts{Timeout: 2 * time.Minute})
	if err != nil {
		return Probe{}, err
	}
	var j ffprobeOut
	if err := json.Unmarshal(out, &j); err != nil {
		return Probe{}, fmt.Errorf("ffprobe output: %w", err)
	}
	var p Probe
	p.Duration, _ = strconv.ParseFloat(j.Format.Duration, 64)
	p.Bitrate, _ = strconv.Atoi(j.Format.BitRate)
	p.Container, _, _ = strings.Cut(j.Format.FormatName, ",")
	for _, s := range j.Streams {
		switch s.CodecType {
		case "video":
			if p.HasVideo || s.Width == 0 || s.Height == 0 {
				continue
			}
			p.HasVideo = true
			p.VideoCodec = s.CodecName
			rot := 0.0
			for _, d := range s.SideData {
				if d.Rotation != nil {
					rot = *d.Rotation
					break
				}
			}
			if rot == 0 && s.Tags.Rotate != "" {
				rot, _ = strconv.ParseFloat(s.Tags.Rotate, 64)
			}
			if int(math.Abs(math.Round(rot)))%180 == 90 {
				p.Width, p.Height = s.Height, s.Width
			} else {
				p.Width, p.Height = s.Width, s.Height
			}
			p.FPS = rational(s.AvgFrameRate)
			if p.FPS == 0 {
				p.FPS = rational(s.RFrameRate)
			}
			p.NbFrames, _ = strconv.Atoi(s.NbFrames)
		case "audio":
			if !p.HasAudio {
				p.HasAudio = true
				p.AudioCodec = s.CodecName
			}
			p.AudioStreams++
		}
	}
	return p, nil
}

// AudioMap mixes all tracks to stereo; volume undoes amix's division since normalize=0 needs ffmpeg 4.4.
func AudioMap(streams int) (mix string, mapArgs []string) {
	if streams <= 1 {
		return "", []string{"-map", "0:a:0"}
	}
	var b strings.Builder
	for i := 0; i < streams; i++ {
		fmt.Fprintf(&b, "[0:a:%d]aformat=sample_fmts=fltp:sample_rates=48000:channel_layouts=stereo[mix%d];", i, i)
	}
	for i := 0; i < streams; i++ {
		fmt.Fprintf(&b, "[mix%d]", i)
	}
	fmt.Fprintf(&b, "amix=inputs=%d:duration=longest:dropout_transition=0,volume=%d[aout]", streams, streams)
	return b.String(), []string{"-map", "[aout]"}
}

// ---- classification -------------------------------------------------------

// ShortMaxSeconds: vertical videos shorter than this are Shorts.
const ShortMaxSeconds = 180

// VideoExts and ImageExts are the accepted upload extensions.
var (
	VideoExts = []string{".mp4", ".mkv", ".webm", ".mov", ".m4v", ".avi"}
	ImageExts = []string{".jpg", ".jpeg", ".png", ".webp", ".gif"}
)

// IsVideoExt reports whether ext is a video extension.
func IsVideoExt(ext string) bool { return contains(VideoExts, ext) }

// IsImageExt reports whether ext is an image extension.
func IsImageExt(ext string) bool { return contains(ImageExts, ext) }

func contains(list []string, v string) bool {
	for _, x := range list {
		if x == v {
			return true
		}
	}
	return false
}

// Classify decides the media type from the file itself. Nobody else gets to.
func Classify(ext string, p Probe) string {
	if ext == ".gif" {
		return "gif"
	}
	if IsImageExt(ext) {
		return "photo"
	}
	if p.Height > p.Width && p.Duration > 0 && p.Duration < ShortMaxSeconds {
		return "short"
	}
	return "video"
}

// IsVideoType reports whether a media type plays in the video player (gets HLS).
func IsVideoType(t string) bool { return t == "video" || t == "short" }

// ---- thumbnails -----------------------------------------------------------

// ThumbSeek picks the default thumbnail frame: a fraction in for video, the first frame for images.
func ThumbSeek(typ string, duration, percent float64) float64 {
	if !IsVideoType(typ) {
		return 0
	}
	if !(duration > 0) {
		return 1
	}
	target := duration * percent
	latest := math.Max(0, duration-0.5)
	return math.Min(math.Max(target, 0.5), latest)
}

// ClampThumb keeps a requested frame inside the file.
func ClampThumb(at, duration float64) float64 {
	limit := duration - 0.1
	if limit <= 0 {
		return 0
	}
	return math.Max(0, math.Min(at, limit))
}

// Thumbnail writes a 640-wide JPEG of the frame at seek seconds.
func (t Tools) Thumbnail(ctx context.Context, input, out string, seek float64) error {
	args := []string{"-y", "-v", "error"}
	if seek > 0 {
		args = append(args, "-ss", strconv.FormatFloat(seek, 'f', 3, 64))
	}
	args = append(args, guardInput()...)
	args = append(args, "-i", input, "-vframes", "1", "-vf", "scale=640:-2", "-q:v", "4", "-f", "image2", "-update", "1")
	return t.writeImage(ctx, args, out)
}

// ThumbnailFromImage turns an uploaded image into a 640x640 cropped JPEG.
func (t Tools) ThumbnailFromImage(ctx context.Context, input, out string) error {
	args := append([]string{"-y", "-v", "error"}, guardInput()...)
	args = append(args, "-i", input, "-frames:v", "1",
		"-vf", "scale=640:640:force_original_aspect_ratio=increase,crop=640:640", "-q:v", "3", "-f", "image2", "-update", "1")
	return t.writeImage(ctx, args, out)
}

// maxImageBytes caps a thumbnail file: a 640 px JPEG is a few dozen KiB.
const maxImageBytes = 64 << 20

func (t Tools) writeImage(ctx context.Context, args []string, out string) error {
	tmp := out + ".tmp.jpg"
	defer os.Remove(tmp)
	if _, err := Run(ctx, t.FFmpeg, append(args, tmp), RunOpts{Timeout: 2 * time.Minute, MaxFileBytes: maxImageBytes}); err != nil {
		return err
	}
	st, err := os.Stat(tmp)
	if err != nil || st.Size() == 0 {
		return errors.New("thumbnail encode produced no output")
	}
	return os.Rename(tmp, out)
}
