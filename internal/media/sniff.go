package media

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
)

// Container is the real format of an upload from its first bytes; the extension is only a claim.
type Container string

// The containers an upload may really be.
const (
	ContainerMov      Container = "mov"      // mp4, m4v, mov
	ContainerMatroska Container = "matroska" // mkv, webm
	ContainerAVI      Container = "avi"
	ContainerJPEG     Container = "jpeg"
	ContainerPNG      Container = "png"
	ContainerWebP     Container = "webp"
	ContainerGIF      Container = "gif"
)

// Sniff errors. ErrPlaylist is the dangerous case, kept apart for the log.
var (
	ErrPlaylist    = errors.New("file is a text playlist (m3u8, concat, sdp), not media")
	ErrNotAccepted = errors.New("file content is not an accepted container")
	ErrExtMismatch = errors.New("file content does not match its extension")
)

const sniffBytes = 512

// Sniff decides the container from the first bytes of a file.
func Sniff(head []byte) (Container, error) {
	switch {
	case len(head) >= 12 && isMovBox(head[4:8]):
		return ContainerMov, nil
	case bytes.HasPrefix(head, []byte{0x1a, 0x45, 0xdf, 0xa3}):
		return ContainerMatroska, nil
	case len(head) >= 12 && bytes.HasPrefix(head, []byte("RIFF")) && bytes.Equal(head[8:12], []byte("AVI ")):
		return ContainerAVI, nil
	case len(head) >= 12 && bytes.HasPrefix(head, []byte("RIFF")) && bytes.Equal(head[8:12], []byte("WEBP")):
		return ContainerWebP, nil
	case bytes.HasPrefix(head, []byte{0xff, 0xd8, 0xff}):
		return ContainerJPEG, nil
	case bytes.HasPrefix(head, []byte("\x89PNG\r\n\x1a\n")):
		return ContainerPNG, nil
	case bytes.HasPrefix(head, []byte("GIF87a")), bytes.HasPrefix(head, []byte("GIF89a")):
		return ContainerGIF, nil
	}
	text := bytes.ToLower(bytes.TrimLeft(bytes.TrimPrefix(head, []byte("\xef\xbb\xbf")), " \t\r\n"))
	for _, p := range []string{"#extm3u", "ffconcat", "v=0"} {
		if bytes.HasPrefix(text, []byte(p)) {
			return "", ErrPlaylist
		}
	}
	return "", ErrNotAccepted
}

// isMovBox reports whether b is the type of a first QuickTime/ISO box.
func isMovBox(b []byte) bool {
	switch string(b) {
	case "ftyp", "moov", "mdat", "free", "skip", "wide", "pnot":
		return true
	}
	return false
}

// SniffFile reads the head of a file and sniffs it.
func SniffFile(path string) (Container, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()
	head := make([]byte, sniffBytes)
	n, err := io.ReadFull(f, head)
	if err != nil && err != io.ErrUnexpectedEOF && err != io.EOF {
		return "", err
	}
	return Sniff(head[:n])
}

// CheckUpload verifies the file's real container matches its extension, before any ffmpeg sees it.
func CheckUpload(path, ext string) error {
	c, err := SniffFile(path)
	if err != nil {
		return err
	}
	video := c == ContainerMov || c == ContainerMatroska || c == ContainerAVI
	if (IsVideoExt(ext) && video) || (IsImageExt(ext) && !video) {
		return nil
	}
	return fmt.Errorf("%w: %s content in a %s file", ErrExtMismatch, c, ext)
}
