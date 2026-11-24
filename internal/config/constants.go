package config

import "time"

// Fixed behaviour: constants the connector decides itself, not owner settings.

// Network and uploads.
const (
	// ListenAddress is the public listener's bind address: all IPv4 interfaces.
	ListenAddress = "0.0.0.0"
	// UploadChunkBytes is the chunk size announced to uploaders; the largest accepted chunk is twice this.
	UploadChunkBytes = 8 << 20
	// PartialUploadMaxAge is how long an unfinished upload is kept before its partial file is removed.
	PartialUploadMaxAge = 24 * time.Hour
)

// Disk headroom: uploads are refused when processing output would not fit next to the original.
const (
	// DiskReserveBytes is the free space always left untouched.
	DiskReserveBytes int64 = 2 << 30
	// UploadSizeFactor multiplies a declared upload size so free space covers original plus output.
	UploadSizeFactor int64 = 2
)

// Processing.
const (
	// X264Preset is the libx264 preset of the HLS ladder.
	X264Preset = "veryfast"
	// SegmentSeconds is the HLS segment length.
	SegmentSeconds = 4
	// ProcessingConcurrency is how many heavy stages (transcode, sonic graph) run at once.
	ProcessingConcurrency = 1
	// FFmpegNice is the nice level of ffmpeg child processes.
	FFmpegNice = 15
	// ThumbPercent is the default thumbnail position as a fraction of duration.
	ThumbPercent = 0.1
	// SonicMinSeconds is the shortest audio a sonic graph is drawn for.
	SonicMinSeconds = 10
)

// NSFW scanning.
const (
	// NsfwThreshold is the score above which a frame counts as NSFW.
	NsfwThreshold = 0.8
	// NsfwSampleFps is how many frames per second of video are scanned.
	NsfwSampleFps = 1.0
	// NsfwBatchSize is how many frames the model scores at once.
	NsfwBatchSize = 16
)
