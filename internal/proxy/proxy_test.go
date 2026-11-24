package proxy

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"os"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/NotNecrotic/jeevo-connector/internal/link"
	"github.com/NotNecrotic/jeevo-connector/internal/protocol"
)

func TestParseContentLength(t *testing.T) {
	cases := map[string]int64{
		"0":                               0,
		"12":                              12,
		"0000012":                         12,
		"9223372036854775807":             9223372036854775807,
		"9223372036854775808":             -1, // one past int64
		"99999999999999999999":            -1, // overflows a naive accumulator
		"1000000000000000000000000000000": -1,
		"-5":                              -1,
		"+5":                              -1,
		"5 ":                              -1,
		" 5":                              -1,
		"1_0":                             -1,
		"0x10":                            -1,
		"1e3":                             -1,
		"٣":                               -1, // a non-ASCII digit
		"":                                -1,
	}
	for in, want := range cases {
		if got := parseContentLength(in); got != want {
			t.Errorf("parseContentLength(%q) = %d, want %d", in, got, want)
		}
	}
}

func newRelay(h http.Handler) *Relay {
	r := &Relay{Link: link.New("", "", nil), Handler: h, Log: slog.New(slog.NewTextHandler(io.Discard, nil))}
	r.Register()
	return r
}

func request(t *testing.T, id string, hasBody bool, headers map[string]string) json.RawMessage {
	raw, err := json.Marshal(protocol.ProxyRequestPayload{StreamId: id, Method: "PUT", Path: "/v1/uploads/x", HasBody: hasBody, Headers: headers})
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func TestRelayContentLengthHeader(t *testing.T) {
	got := make(chan int64, 1)
	r := newRelay(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) { got <- req.ContentLength }))
	for i, c := range []struct {
		header string
		want   int64
	}{
		{"1048576", 1048576},
		{"99999999999999999999999", -1},
		{"-1", -1},
		{"12abc", -1},
	} {
		id := "stream" + strconv.Itoa(i) + "0123456789"[:9]
		_, _ = r.onRequest(context.Background(), request(t, id, false, map[string]string{"Content-Length": c.header}))
		if n := <-got; n != c.want {
			t.Errorf("Content-Length %q reached the handler as %d, want %d", c.header, n, c.want)
		}
	}
}

// start runs a proxied request whose handler waits for its context; returns once the relay knows the stream.
func start(t *testing.T, r *Relay, id string, wg *sync.WaitGroup) {
	t.Helper()
	wg.Add(1)
	go func() {
		defer wg.Done()
		_, _ = r.onRequest(context.Background(), request(t, id, true, nil))
	}()
	for i := 0; i < 500; i++ {
		r.mu.Lock()
		_, ok := r.streams[id]
		r.mu.Unlock()
		if ok {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("stream %s never started", id)
}

func (r *Relay) has(id string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	_, ok := r.streams[id]
	return ok
}

// Queued body frames are bounded across all streams, not only per stream.
func TestRelayBodyBudget(t *testing.T) {
	r := newRelay(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) { <-req.Context().Done() }))
	var wg sync.WaitGroup
	ids := []string{"aaaaaaaaaaaaaaaa", "bbbbbbbbbbbbbbbb", "cccccccccccccccc"}
	for _, id := range ids {
		start(t, r, id, &wg)
	}
	frame := make([]byte, chunk)
	// Two streams use 50 of the 64 MiB.
	for _, id := range ids[:2] {
		for i := 0; i < 100; i++ {
			r.OnBinary(link.FrameData, id, frame)
		}
	}
	if !r.has(ids[0]) || !r.has(ids[1]) {
		t.Fatal("streams within the budget were aborted")
	}
	// The third crosses it and is the one aborted.
	for i := 0; i < 100 && r.has(ids[2]); i++ {
		r.OnBinary(link.FrameData, ids[2], frame)
	}
	if r.has(ids[2]) {
		t.Fatal("a stream past the budget was kept")
	}
	if !r.has(ids[0]) || !r.has(ids[1]) {
		t.Fatal("the other streams were aborted too")
	}
	r.DropAll()
	wg.Wait()
	r.mu.Lock()
	left := r.buffered
	r.mu.Unlock()
	if left != 0 {
		t.Fatalf("%d queued bytes were never given back", left)
	}
}

// An upload chunk sent as many small frames to a late reader arrives whole: only bytes count.
func TestRelaySmallFrames(t *testing.T) {
	gate := make(chan struct{})
	got := make(chan []byte, 1)
	r := newRelay(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		<-gate
		b, _ := io.ReadAll(req.Body)
		got <- b
	}))
	var wg sync.WaitGroup
	const id = "dddddddddddddddd"
	start(t, r, id, &wg)
	var want []byte
	for i := 0; i < 2048; i++ { // 8 MiB in 4 KiB frames
		frame := bytes.Repeat([]byte{byte(i)}, 4096)
		want = append(want, frame...)
		r.OnBinary(link.FrameData, id, frame)
	}
	r.OnBinary(link.FrameEnd, id, nil)
	if !r.has(id) {
		t.Fatal("a stream within the byte budget was aborted")
	}
	close(gate)
	if b := <-got; !bytes.Equal(b, want) {
		t.Fatalf("the handler read %d bytes, want %d in order", len(b), len(want))
	}
	wg.Wait()
	r.mu.Lock()
	left := r.buffered
	r.mu.Unlock()
	if left != 0 {
		t.Fatalf("%d queued bytes were never given back", left)
	}
}

// Frames after the end of the body, or for gone streams, are ignored (once a send on a closed channel).
func TestRelayLateFrames(t *testing.T) {
	// A handler that has not read yet keeps the stream registered.
	gate := make(chan struct{})
	r := newRelay(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		<-gate
		_, _ = io.Copy(io.Discard, req.Body)
	}))
	var wg sync.WaitGroup
	start(t, r, "eeeeeeeeeeeeeeee", &wg)
	r.OnBinary(link.FrameData, "eeeeeeeeeeeeeeee", []byte("abc"))
	r.OnBinary(link.FrameEnd, "eeeeeeeeeeeeeeee", nil)
	r.OnBinary(link.FrameData, "eeeeeeeeeeeeeeee", []byte("late")) // must not panic
	r.OnBinary(link.FrameEnd, "eeeeeeeeeeeeeeee", nil)
	close(gate)
	wg.Wait()
	r.OnBinary(link.FrameData, "eeeeeeeeeeeeeeee", []byte("gone"))
	r.mu.Lock()
	left := r.buffered
	r.mu.Unlock()
	if left != 0 {
		t.Fatalf("%d queued bytes were never given back", left)
	}
}

// A handler can bound its wait for the body with http.ResponseController.
func TestRelayReadDeadline(t *testing.T) {
	res := make(chan error, 1)
	r := newRelay(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if err := http.NewResponseController(w).SetReadDeadline(time.Now().Add(100 * time.Millisecond)); err != nil {
			res <- err
			return
		}
		_, err := io.ReadAll(req.Body)
		res <- err
	}))
	_, _ = r.onRequest(context.Background(), request(t, "ffffffffffffffff", true, nil))
	if err := <-res; !errors.Is(err, os.ErrDeadlineExceeded) {
		t.Fatalf("read of a stalled body ended with %v", err)
	}
	// Without a body there is nothing to bound.
	r = newRelay(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		res <- http.NewResponseController(w).SetReadDeadline(time.Now().Add(time.Second))
	}))
	_, _ = r.onRequest(context.Background(), request(t, "gggggggggggggggg", false, nil))
	if err := <-res; err == nil {
		t.Fatal("a deadline was accepted on a request without a body")
	}
}
