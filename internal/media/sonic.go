package media

import (
	"context"
	"encoding/binary"
	"errors"
	"math"
	"os/exec"
	"syscall"
	"time"
)

const (
	sonicSampleRate  = 16000
	sonicBytesPerSec = sonicSampleRate * 4 // f32le mono
	silenceDBFS      = -70.0
	normaliseWindow  = 40.0
)

// SonicResult mirrors the protocol's SonicResult.
type SonicResult struct {
	Status string   // done | no_audio | silent | short | error
	PeakDb *float64 // loudest second, dBFS
	Graph  []byte   // one byte per second, 0..255
}

func rmsDb(b []byte) float64 {
	n := len(b) / 4
	if n == 0 {
		return math.Inf(-1)
	}
	var sum float64
	for i := 0; i < n; i++ {
		v := float64(math.Float32frombits(binary.LittleEndian.Uint32(b[i*4:])))
		if math.IsNaN(v) || math.IsInf(v, 0) {
			v = 0
		}
		sum += v * v
	}
	rms := math.Sqrt(sum / float64(n))
	if !(rms > 0) {
		return math.Inf(-1)
	}
	return 20 * math.Log10(rms)
}

// Sonic computes the per-second loudness graph of the first audio track; onProgress is optional.
func (t Tools) Sonic(ctx context.Context, file string, minSeconds float64, timeout time.Duration, onProgress func(float64)) (SonicResult, error) {
	p, err := t.ProbeFile(ctx, file)
	if err != nil {
		return SonicResult{Status: "error"}, err
	}
	if !p.HasAudio {
		return SonicResult{Status: "no_audio"}, nil
	}
	if p.Duration > 0 && p.Duration < minSeconds {
		return SonicResult{Status: "short"}, nil
	}
	if t.FFmpeg == "" {
		return SonicResult{Status: "error"}, errors.New("ffmpeg is not installed")
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	args := append([]string{"-nostdin", "-v", "error"}, guardInput()...)
	args = append(args, "-i", file)
	// The same audio viewers hear: every track mixed.
	mix, audioMap := AudioMap(p.AudioStreams)
	if mix != "" {
		args = append(args, "-filter_complex", mix)
	}
	args = append(args, audioMap...)
	cmd := exec.CommandContext(ctx, t.FFmpeg, append(args, "-vn", "-ac", "1", "-ar", "16000", "-f", "f32le", "pipe:1")...)
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	out, err := cmd.StdoutPipe()
	if err != nil {
		return SonicResult{Status: "error"}, err
	}
	if err := cmd.Start(); err != nil {
		return SonicResult{Status: "error"}, err
	}
	_ = syscall.Setpriority(syscall.PRIO_PROCESS, cmd.Process.Pid, 10)
	var dbs []float64
	var read float64
	buf := make([]byte, 0, sonicBytesPerSec*2)
	chunk := make([]byte, 64*1024)
	for {
		n, rerr := out.Read(chunk)
		buf = append(buf, chunk[:n]...)
		read += float64(n)
		if onProgress != nil && p.Duration > 0 {
			onProgress(math.Min(0.99, read/(p.Duration*sonicBytesPerSec)))
		}
		for len(buf) >= sonicBytesPerSec {
			dbs = append(dbs, rmsDb(buf[:sonicBytesPerSec]))
			buf = buf[sonicBytesPerSec:]
		}
		if rerr != nil {
			break
		}
	}
	werr := cmd.Wait()
	if ctx.Err() == context.DeadlineExceeded {
		return SonicResult{Status: "error"}, errors.New("sonic_graph timed out")
	}
	if werr != nil && len(dbs) == 0 {
		return SonicResult{Status: "error"}, werr
	}
	peak := math.Inf(-1)
	for _, d := range dbs {
		if !math.IsInf(d, 0) && d > peak {
			peak = d
		}
	}
	if math.IsInf(peak, -1) || peak < silenceDBFS {
		var pk *float64
		if !math.IsInf(peak, -1) {
			pk = &peak
		}
		return SonicResult{Status: "silent", PeakDb: pk}, nil
	}
	lo := peak - normaliseWindow
	graph := make([]byte, len(dbs))
	for i, d := range dbs {
		v := 0.0
		if !math.IsInf(d, 0) && !math.IsNaN(d) {
			v = math.Round((d - lo) / normaliseWindow * 255)
		}
		graph[i] = byte(math.Max(0, math.Min(255, v)))
	}
	return SonicResult{Status: "done", PeakDb: &peak, Graph: graph}, nil
}
