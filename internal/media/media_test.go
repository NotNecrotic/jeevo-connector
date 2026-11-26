package media

import (
	"context"
	"errors"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestPlanLadder(t *testing.T) {
	ladder := []int{1080, 720, 480, 360}
	heights := func(r []Rendition) []int {
		out := []int{}
		for _, x := range r {
			out = append(out, x.Height)
		}
		return out
	}
	eq := func(a, b []int) bool {
		if len(a) != len(b) {
			return false
		}
		for i := range a {
			if a[i] != b[i] {
				return false
			}
		}
		return true
	}
	cases := []struct {
		w, h int
		want []int
	}{
		{1920, 1080, []int{1080, 720, 480, 360}},
		{1080, 1920, []int{1080, 720, 480, 360}}, // portrait measured on the short side
		{1776, 1000, []int{1000, 720, 480, 360}}, // between rungs keeps its own size
		{1300, 731, []int{720, 480, 360}},        // barely above a rung: no extra top rung
		{320, 240, []int{240}},                   // below the ladder: source size only
		{3840, 2160, []int{1080, 720, 480, 360}}, // never above the top rung
	}
	for _, c := range cases {
		got := heights(PlanLadder(c.w, c.h, true, ladder))
		if !eq(got, c.want) {
			t.Errorf("%dx%d: got %v want %v", c.w, c.h, got, c.want)
		}
	}
	r := PlanLadder(1080, 1920, false, ladder)[0]
	if r.Width != 1080 || r.FrameHeight != 1920 || r.AudioBitrate != 0 {
		t.Errorf("portrait rung frame: %+v", r)
	}
}

func TestClassify(t *testing.T) {
	if Classify(".gif", Probe{}) != "gif" || Classify(".png", Probe{}) != "photo" {
		t.Fatal("images")
	}
	if Classify(".mp4", Probe{Width: 1080, Height: 1920, Duration: 30}) != "short" {
		t.Fatal("vertical short")
	}
	if Classify(".mp4", Probe{Width: 1080, Height: 1920, Duration: 600}) != "video" {
		t.Fatal("long vertical is a video")
	}
	if Classify(".mp4", Probe{Width: 1920, Height: 1080, Duration: 30}) != "video" {
		t.Fatal("landscape")
	}
}

const master = `#EXTM3U
#EXT-X-VERSION:7
#EXT-X-MEDIA:TYPE=AUDIO,GROUP-ID="aud",NAME="audio",DEFAULT=YES,URI="audio/index.m3u8"
#EXT-X-STREAM-INF:BANDWIDTH=5128000,RESOLUTION=1920x1080,CODECS="avc1.640028,mp4a.40.2",AUDIO="aud"
p1080/index.m3u8

#EXT-X-STREAM-INF:BANDWIDTH=2828000,RESOLUTION=1280x720,CODECS="avc1.64001f,mp4a.40.2",AUDIO="aud"
p720/index.m3u8

#EXT-X-STREAM-INF:BANDWIDTH=1628000,RESOLUTION=854x480,CODECS="avc1.64001e,mp4a.40.2",AUDIO="aud"
p480/index.m3u8
`

func TestRewriteMaster(t *testing.T) {
	out := RewriteMaster(master, "v1.a.b", 720)
	if strings.Contains(out, "p1080") {
		t.Fatal("1080p rung not removed under a 720p cap")
	}
	if !strings.Contains(out, "p720/index.m3u8?t=v1.a.b") || !strings.Contains(out, `URI="audio/index.m3u8?t=v1.a.b"`) {
		t.Fatalf("token not threaded through:\n%s", out)
	}
	if !strings.Contains(RewriteMaster(master, "x", 0), "p1080") {
		t.Fatal("uncapped master lost a rung")
	}
	// Every rung above the cap: the smallest stays so it still plays.
	allowed := AllowedVariants(master, 240)
	if !allowed["p480"] || allowed["p720"] {
		t.Fatalf("fallback rung: %v", allowed)
	}
}

func TestRewriteMediaPlaylist(t *testing.T) {
	pl := "#EXTM3U\n#EXT-X-MAP:URI=\"init.mp4\"\n#EXTINF:4.0,\nseg_00000.m4s\n#EXT-X-ENDLIST\n"
	out := RewriteMediaPlaylist(pl, "tok")
	if !strings.Contains(out, `URI="init.mp4?t=tok"`) || !strings.Contains(out, "seg_00000.m4s?t=tok") {
		t.Fatalf("got:\n%s", out)
	}
}

func TestPreviewClips(t *testing.T) {
	if c := PreviewClips(600); len(c) != 5 || c[0].Start < 30 {
		t.Fatalf("long video: %+v", c)
	}
	if c := PreviewClips(4); len(c) != 1 || c[0].Length != 4 {
		t.Fatalf("short video: %+v", c)
	}
}

// TestTranscode runs the real ffmpeg ladder end to end (skipped without ffmpeg).
func TestTranscode(t *testing.T) {
	tools := FindTools("", "")
	if tools.FFmpeg == "" || tools.FFprobe == "" {
		t.Skip("ffmpeg not installed")
	}
	dir := t.TempDir()
	src := filepath.Join(dir, "original.mp4")
	gen := exec.Command(tools.FFmpeg, "-v", "error", "-f", "lavfi", "-i", "testsrc2=size=640x360:rate=30",
		"-f", "lavfi", "-i", "sine=frequency=440:sample_rate=48000", "-t", "6", "-c:v", "libx264", "-pix_fmt", "yuv420p",
		"-c:a", "aac", "-shortest", src)
	if out, err := gen.CombinedOutput(); err != nil {
		t.Fatalf("make test video: %v %s", err, out)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	p, err := tools.ProbeFile(ctx, src)
	if err != nil || p.Width != 640 || !p.HasAudio {
		t.Fatalf("probe: %+v %v", p, err)
	}
	tr := &Transcoder{Tools: tools, Detector: NewEncoderDetector(tools)}
	opts := HlsOptions{Encoder: "libx264", Qualities: []int{360, 240}, X264Preset: "ultrafast", SegmentSeconds: 2}
	res, err := tr.Transcode(ctx, src, dir, opts, nil)
	if err != nil {
		t.Fatalf("transcode: %v", err)
	}
	if len(res.Renditions) != 2 || res.Profile != "360,240" {
		t.Fatalf("result: %+v", res)
	}
	if err := VerifyPackage(HlsDir(dir), 2, true); err != nil {
		t.Fatal(err)
	}
	// Removing a rung updates in place without re-encoding the rest.
	opts.Qualities = []int{240}
	r2, rebuilt, err := tr.UpdateLadder(ctx, src, dir, opts)
	if err != nil || rebuilt || len(r2.Renditions) != 1 {
		t.Fatalf("update ladder: %+v rebuilt=%v err=%v", r2, rebuilt, err)
	}
	if _, err := os.Stat(filepath.Join(HlsDir(dir), "p360")); !os.IsNotExist(err) {
		t.Fatal("p360 should have been pruned")
	}
	if err := tr.MakePreview(ctx, src, dir, p, 0); err != nil {
		t.Fatalf("preview: %v", err)
	}
	if err := tools.Thumbnail(ctx, src, filepath.Join(dir, "thumb.jpg"), 1); err != nil {
		t.Fatalf("thumbnail: %v", err)
	}
	s, err := tools.Sonic(ctx, src, 1, time.Minute, nil)
	if err != nil || s.Status != "done" || len(s.Graph) < 5 {
		t.Fatalf("sonic: %+v %v", s, err)
	}
}

// Game audio and microphone on separate tracks both play: the first is silent, so only the mix is audible.
func TestTranscodeMixesAudioTracks(t *testing.T) {
	tools := FindTools("", "")
	if tools.FFmpeg == "" || tools.FFprobe == "" {
		t.Skip("ffmpeg not installed")
	}
	dir := t.TempDir()
	src := filepath.Join(dir, "original.mkv")
	gen := exec.Command(tools.FFmpeg, "-v", "error", "-f", "lavfi", "-i", "testsrc2=size=320x180:rate=15",
		"-f", "lavfi", "-i", "anullsrc=channel_layout=stereo:sample_rate=48000",
		"-f", "lavfi", "-i", "sine=frequency=440:sample_rate=44100",
		"-map", "0:v", "-map", "1:a", "-map", "2:a", "-t", "4", "-c:v", "libx264", "-preset", "ultrafast",
		"-pix_fmt", "yuv420p", "-c:a", "aac", src)
	if out, err := gen.CombinedOutput(); err != nil {
		t.Fatalf("make test video: %v %s", err, out)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	p, err := tools.ProbeFile(ctx, src)
	if err != nil || p.AudioStreams != 2 {
		t.Fatalf("probe: %+v %v", p, err)
	}
	tr := &Transcoder{Tools: tools, Detector: NewEncoderDetector(tools)}
	opts := HlsOptions{Encoder: "libx264", Qualities: []int{180}, X264Preset: "ultrafast", SegmentSeconds: 2}
	if _, err := tr.Transcode(ctx, src, dir, opts, nil); err != nil {
		t.Fatalf("transcode: %v", err)
	}
	out, err := exec.Command(tools.FFmpeg, "-v", "info", "-i", filepath.Join(HlsDir(dir), "audio", "index.m3u8"),
		"-af", "volumedetect", "-f", "null", "-").CombinedOutput()
	if err != nil {
		t.Fatalf("read audio rendition: %v %s", err, out)
	}
	m := regexp.MustCompile(`max_volume: (-?[0-9.]+) dB`).FindSubmatch(out)
	if m == nil {
		t.Fatalf("no volume in %s", out)
	}
	if v, _ := strconv.ParseFloat(string(m[1]), 64); v < -30 {
		t.Fatalf("the tone on the second track is missing (max volume %v dB)", v)
	}
	s, err := tools.Sonic(ctx, src, 1, time.Minute, nil)
	if err != nil || s.Status != "done" || s.PeakDb == nil || *s.PeakDb < -30 {
		t.Fatalf("sonic graph of the mix: %+v %v", s, err)
	}
}

func TestSniff(t *testing.T) {
	cases := []struct {
		name string
		head []byte
		want Container
		err  error
	}{
		{"mp4", append([]byte{0, 0, 0, 0x18}, "ftypisom\x00\x00\x02\x00"...), ContainerMov, nil},
		{"quicktime without ftyp", append([]byte{0, 0, 0, 8}, "wide\x00\x00\x00\x00"...), ContainerMov, nil},
		{"matroska", []byte{0x1a, 0x45, 0xdf, 0xa3, 0x9f, 0x42, 0x86, 0x81}, ContainerMatroska, nil},
		{"avi", []byte("RIFF\x00\x00\x00\x00AVI LIST"), ContainerAVI, nil},
		{"webp", []byte("RIFF\x00\x00\x00\x00WEBPVP8 "), ContainerWebP, nil},
		{"jpeg", []byte{0xff, 0xd8, 0xff, 0xe0}, ContainerJPEG, nil},
		{"png", []byte("\x89PNG\r\n\x1a\n\x00\x00"), ContainerPNG, nil},
		{"gif", []byte("GIF89a\x01\x00"), ContainerGIF, nil},
		{"hls playlist", []byte("#EXTM3U\n#EXTINF:1,\n/etc/passwd\n"), "", ErrPlaylist},
		{"hls playlist with bom and blank lines", []byte("\xef\xbb\xbf\r\n \n#extm3u\n"), "", ErrPlaylist},
		{"concat", []byte("ffconcat version 1.0\nfile /etc/passwd\n"), "", ErrPlaylist},
		{"sdp", []byte("v=0\r\no=- 0 0 IN IP4 127.0.0.1\r\n"), "", ErrPlaylist},
		{"empty", nil, "", ErrNotAccepted},
		{"short riff", []byte("RIFF"), "", ErrNotAccepted},
		{"mpegts", []byte{0x47, 0x40, 0x11, 0x10}, "", ErrNotAccepted},
		{"plain text", []byte("hello"), "", ErrNotAccepted},
	}
	for _, c := range cases {
		got, err := Sniff(c.head)
		if got != c.want || err != c.err {
			t.Errorf("%s: got %q %v, want %q %v", c.name, got, err, c.want, c.err)
		}
	}
}

func TestCheckUpload(t *testing.T) {
	dir := t.TempDir()
	write := func(name string, b []byte) string {
		p := filepath.Join(dir, name)
		if err := os.WriteFile(p, b, 0o600); err != nil {
			t.Fatal(err)
		}
		return p
	}
	mp4 := append([]byte{0, 0, 0, 0x18}, "ftypisom\x00\x00\x02\x00"...)
	png := []byte("\x89PNG\r\n\x1a\n\x00\x00")
	playlist := []byte("#EXTM3U\n#EXTINF:1,\n/etc/passwd\n")
	for _, c := range []struct {
		name, ext string
		body      []byte
		err       error
	}{
		{"a.mp4", ".mp4", mp4, nil},
		{"b.mkv", ".mkv", mp4, nil}, // a mislabeled but accepted video container
		{"c.png", ".png", png, nil},
		{"d.mp4", ".mp4", playlist, ErrPlaylist},
		{"e.mov", ".mov", png, ErrExtMismatch},
		{"f.jpg", ".jpg", mp4, ErrExtMismatch},
		{"g.txt", ".txt", mp4, ErrExtMismatch},
		{"h.mp4", ".mp4", []byte("garbage"), ErrNotAccepted},
	} {
		err := CheckUpload(write(c.name, c.body), c.ext)
		if !errors.Is(err, c.err) {
			t.Errorf("%s: got %v, want %v", c.name, err, c.err)
		}
	}
	if err := CheckUpload(filepath.Join(dir, "missing.mp4"), ".mp4"); err == nil {
		t.Error("missing file accepted")
	}
}

func TestRunLimits(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	// One file bigger than MaxFileBytes is cut off by the kernel (SIGXFSZ); the sleep lets Run apply the limit after start.
	_, err := Run(ctx, "/bin/sh", []string{"-c", "sleep 0.2; head -c 3000000 /dev/zero > big"}, RunOpts{Dir: dir, MaxFileBytes: 1 << 20})
	if err == nil {
		t.Fatal("oversized file was written")
	}
	if st, _ := os.Stat(filepath.Join(dir, "big")); st != nil && st.Size() > 1<<20 {
		t.Fatalf("file grew to %d bytes", st.Size())
	}
	// Many small files add up past MaxDirBytes and the process is killed.
	start := time.Now()
	_, err = Run(ctx, "/bin/sh", []string{"-c", "for i in 1 2 3 4; do head -c 400000 /dev/zero > f$i; done; exec sleep 60"}, RunOpts{Dir: dir, MaxDirBytes: 1 << 20})
	if err == nil || !strings.Contains(err.Error(), "output grew beyond") || time.Since(start) > 30*time.Second {
		t.Fatalf("directory bound: %v after %s", err, time.Since(start))
	}
	// Within the limits a command runs normally.
	if _, err := Run(ctx, "/bin/sh", []string{"-c", "head -c 1000 /dev/zero > small"}, RunOpts{Dir: dir, MaxFileBytes: 1 << 20, MaxDirBytes: 8 << 20}); err != nil {
		t.Fatalf("within limits: %v", err)
	}
}

func TestOutputBound(t *testing.T) {
	rungs := []Rendition{{VideoBitrate: 5_000_000}, {VideoBitrate: 1_000_000}}
	short := outputBound(rungs, 10)
	if short <= outputFloor || short > outputFloor+(16<<20) {
		t.Fatalf("10 s bound %d", short)
	}
	// A file claiming an absurd (or no) duration is sized as 12 hours.
	want := outputBound(rungs, maxMediaSeconds)
	for _, d := range []float64{1e12, 0, -5, math.NaN(), math.Inf(1)} {
		if got := outputBound(rungs, d); got != want {
			t.Errorf("duration %v: bound %d, want %d", d, got, want)
		}
	}
	if cappedSeconds(math.NaN()) != 0 || cappedSeconds(1e9) != maxMediaSeconds || cappedSeconds(90) != 90 {
		t.Fatal("cappedSeconds")
	}
}

// ffmpeg and ffprobe must refuse an upload that is really a playlist, even when named like a video.
func TestGuardedInputRefusesPlaylists(t *testing.T) {
	tools := FindTools("", "")
	if tools.FFmpeg == "" || tools.FFprobe == "" {
		t.Skip("ffmpeg not installed")
	}
	dir := t.TempDir()
	secret := filepath.Join(dir, "secret.txt")
	os.WriteFile(secret, []byte("do not read me"), 0o600)
	good := filepath.Join(dir, "good.mp4")
	if out, err := exec.Command(tools.FFmpeg, "-v", "error", "-f", "lavfi", "-i", "testsrc2=size=160x120:rate=10",
		"-t", "1", "-c:v", "libx264", "-pix_fmt", "yuv420p", good).CombinedOutput(); err != nil {
		t.Fatalf("make test video: %v %s", err, out)
	}
	hls := filepath.Join(dir, "hls.mp4")
	os.WriteFile(hls, []byte("#EXTM3U\n#EXT-X-TARGETDURATION:1\n#EXTINF:1,\n"+secret+"\n#EXT-X-ENDLIST\n"), 0o600)
	concat := filepath.Join(dir, "concat.mp4")
	os.WriteFile(concat, []byte("ffconcat version 1.0\nfile "+secret+"\n"), 0o600)
	ctx := context.Background()
	if p, err := tools.ProbeFile(ctx, good); err != nil || !p.HasVideo || p.Container != "mov" {
		t.Fatalf("probe of a real mp4: %+v %v", p, err)
	}
	for _, bad := range []string{hls, concat} {
		if p, err := tools.ProbeFile(ctx, bad); err == nil {
			t.Errorf("ffprobe accepted %s: %+v", filepath.Base(bad), p)
		}
		if err := tools.Thumbnail(ctx, bad, filepath.Join(dir, "t.jpg"), 0); err == nil {
			t.Errorf("ffmpeg thumbnail accepted %s", filepath.Base(bad))
		}
		if _, err := tools.Sonic(ctx, bad, 1, time.Minute, nil); err == nil {
			t.Errorf("sonic accepted %s", filepath.Base(bad))
		}
	}
}
