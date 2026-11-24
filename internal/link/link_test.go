package link

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
)

func TestRetryDelaySchedule(t *testing.T) {
	want := []time.Duration{1, 1, 2, 2, 3, 5, 8, 13, 15, 15, 15}
	for i, w := range want {
		if got := retryDelay(i); got != w*time.Second {
			t.Errorf("retryDelay(%d) = %s, want %s", i, got, w*time.Second)
		}
	}
	if got := retryDelay(10_000); got != 15*time.Second {
		t.Errorf("schedule must cap at 15s, got %s", got)
	}
	if got := retryDelay(-3); got != time.Second {
		t.Errorf("negative attempt: %s", got)
	}
	// Non-decreasing, so a long outage never speeds back up.
	for i := 1; i < 20; i++ {
		if retryDelay(i) < retryDelay(i-1) {
			t.Errorf("schedule decreases at %d", i)
		}
	}
}

func TestJitterBounds(t *testing.T) {
	base := 10 * time.Second
	lo, hi := jittered(base, 0), jittered(base, 0.999999)
	if lo != 8*time.Second {
		t.Errorf("low end %s, want 8s", lo)
	}
	if hi < 11900*time.Millisecond || hi > 12*time.Second {
		t.Errorf("high end %s, want ~12s", hi)
	}
	if mid := jittered(base, 0.5); mid != base {
		t.Errorf("middle %s, want %s", mid, base)
	}
}

// A binary frame is a header plus at most one chunk; a bigger one ends the session.
func TestOversizedBinaryFrameEndsSession(t *testing.T) {
	closed := make(chan websocket.StatusCode, 1)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer c.CloseNow()
		ctx := r.Context()
		_ = c.Write(ctx, websocket.MessageBinary, make([]byte, maxBinaryFrame))
		_ = c.Write(ctx, websocket.MessageBinary, make([]byte, maxBinaryFrame+1))
		_, _, err = c.Read(ctx)
		closed <- websocket.CloseStatus(err)
	}))
	defer srv.Close()

	l := New("ws"+strings.TrimPrefix(srv.URL, "http"), "id.secret", slog.New(slog.NewTextHandler(io.Discard, nil)))
	sizes := make(chan int, 4)
	l.OnBinary = func(_ byte, _ string, data []byte) { sizes <- len(data) }
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := l.session(ctx); err == nil || !strings.Contains(err.Error(), "oversized") {
		t.Fatalf("session ended with %v", err)
	}
	if n := <-sizes; n != FrameChunk {
		t.Fatalf("a maximal frame delivered %d bytes, want %d", n, FrameChunk)
	}
	if len(sizes) != 0 {
		t.Fatal("the oversized frame was delivered")
	}
	if st := <-closed; st != websocket.StatusMessageTooBig {
		t.Fatalf("close status %d, want %d", st, websocket.StatusMessageTooBig)
	}
}
