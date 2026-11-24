// Package proxy serves relayed requests through the same http.Handler as direct HTTPS.
package proxy

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/NotNecrotic/jeevo-connector/internal/link"
	"github.com/NotNecrotic/jeevo-connector/internal/protocol"
)

const chunk = link.FrameChunk

// maxBuffered bounds queued request-body bytes across all streams; a stream that cannot keep up is aborted.
const maxBuffered = 64 << 20

// Relay is the connector side of proxy mode.
type Relay struct {
	Link    *link.Link
	Handler http.Handler
	Log     *slog.Logger

	mu       sync.Mutex
	streams  map[string]*stream
	buffered int // body bytes queued in streams, see maxBuffered
}

// stream is one proxied request; body frames are bounded by bytes, not count, as frames can be small.
type stream struct {
	queue  [][]byte      // request body frames from Jeevo, not read yet
	ready  chan struct{} // signalled when queue grows or the body closes
	closed bool          // no more frames will come
	cancel context.CancelFunc
}

// closeBody ends a stream's request body. r.mu must be held.
func (r *Relay) closeBody(s *stream) {
	if !s.closed {
		s.closed = true
		s.signal()
	}
}

func (s *stream) signal() {
	select {
	case s.ready <- struct{}{}:
	default:
	}
}

// Register wires the relay into the link.
func (r *Relay) Register() {
	r.streams = map[string]*stream{}
	r.Link.Handle(protocol.MsgProxyRequest, r.onRequest)
	r.Link.Handle(protocol.MsgProxyCancel, func(ctx context.Context, raw json.RawMessage) (any, error) {
		var p protocol.ProxyCancelPayload
		if json.Unmarshal(raw, &p) == nil {
			r.drop(p.StreamId, true)
		}
		return nil, nil
	})
}

// OnBinary receives request-body frames for proxied uploads.
func (r *Relay) OnBinary(kind byte, id string, data []byte) {
	var buf []byte
	if kind == link.FrameData {
		buf = make([]byte, len(data))
		copy(buf, data)
	}
	r.mu.Lock()
	s := r.streams[id]
	if s == nil {
		r.mu.Unlock()
		return
	}
	overflow, abort := false, false
	switch kind {
	case link.FrameData:
		if s.closed {
			break // data after the end of the body
		}
		s.queue = append(s.queue, buf)
		s.signal()
		r.buffered += len(buf)
		overflow = r.buffered > maxBuffered
	case link.FrameEnd:
		r.closeBody(s)
	case link.FrameAbort:
		abort = true
	}
	r.mu.Unlock()
	if overflow {
		// Jeevo outran the handler: abort rather than queue without bound.
		r.Log.Warn("proxy stream overflow; aborting", "stream", id)
	}
	if overflow || abort {
		r.drop(id, true)
	}
}

// DropAll aborts every stream (the connection went away).
func (r *Relay) DropAll() {
	r.mu.Lock()
	ids := make([]string, 0, len(r.streams))
	for id := range r.streams {
		ids = append(ids, id)
	}
	r.mu.Unlock()
	for _, id := range ids {
		r.drop(id, true)
	}
}

func (r *Relay) drop(id string, cancel bool) {
	r.mu.Lock()
	s := r.streams[id]
	delete(r.streams, id)
	if s != nil {
		r.closeBody(s)
	}
	r.mu.Unlock()
	if s != nil && cancel {
		s.cancel()
	}
}

// take removes the next queued frame; done means none is queued and none will come.
func (r *Relay) take(s *stream) (b []byte, ok, done bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(s.queue) == 0 {
		return nil, false, s.closed
	}
	b, s.queue[0] = s.queue[0], nil
	s.queue = s.queue[1:]
	r.buffered -= len(b)
	return b, true, false
}

// discard drops the frames the handler never read.
func (r *Relay) discard(s *stream) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, b := range s.queue {
		r.buffered -= len(b)
	}
	s.queue = nil
}

type bodyReader struct {
	relay    *Relay
	s        *stream
	cur      []byte
	ctx      context.Context
	deadline atomic.Int64 // unix nanoseconds; 0 = none
}

func (c *bodyReader) Read(p []byte) (int, error) {
	var timeout <-chan time.Time
	if d := c.deadline.Load(); d != 0 {
		t := time.NewTimer(time.Until(time.Unix(0, d)))
		defer t.Stop()
		timeout = t.C
	}
	for len(c.cur) == 0 {
		b, ok, done := c.relay.take(c.s)
		if ok {
			c.cur = b
			continue
		}
		if done {
			return 0, io.EOF
		}
		select {
		case <-c.s.ready:
		case <-c.ctx.Done():
			return 0, c.ctx.Err()
		case <-timeout:
			return 0, os.ErrDeadlineExceeded
		}
	}
	n := copy(p, c.cur)
	c.cur = c.cur[n:]
	return n, nil
}

// Only these request headers are passed to the handler.
var allowedReqHeaders = []string{"Authorization", "Content-Length", "Content-Type", "Upload-Offset", "Range", "If-Range", "If-None-Match", "If-Modified-Since"}

func (r *Relay) onRequest(parent context.Context, raw json.RawMessage) (any, error) {
	var p protocol.ProxyRequestPayload
	if err := json.Unmarshal(raw, &p); err != nil || len(p.StreamId) != 16 {
		return nil, nil
	}
	u, err := url.ParseRequestURI(p.Path)
	if err != nil || !strings.HasPrefix(u.Path, "/") || u.Host != "" {
		r.respondError(p.StreamId, http.StatusBadRequest)
		return nil, nil
	}
	ctx, cancel := context.WithCancel(parent)
	s := &stream{ready: make(chan struct{}, 1), cancel: cancel}
	r.mu.Lock()
	r.streams[p.StreamId] = s
	if !p.HasBody {
		r.closeBody(s)
	}
	r.mu.Unlock()
	defer func() {
		r.drop(p.StreamId, false)
		cancel()
		r.discard(s)
	}()

	var body io.Reader = http.NoBody
	var reader *bodyReader
	if p.HasBody {
		reader = &bodyReader{relay: r, s: s, ctx: ctx}
		body = reader
	}
	req, err := http.NewRequestWithContext(ctx, string(p.Method), p.Path, body)
	if err != nil {
		r.respondError(p.StreamId, http.StatusBadRequest)
		return nil, nil
	}
	for _, h := range allowedReqHeaders {
		if v, ok := p.Headers[h]; ok {
			req.Header.Set(h, v)
		} else if v, ok := p.Headers[strings.ToLower(h)]; ok {
			req.Header.Set(h, v)
		}
	}
	if cl := req.Header.Get("Content-Length"); cl != "" {
		req.ContentLength = parseContentLength(cl)
	}
	req.RemoteAddr = "relay"
	w := &relayWriter{relay: r, id: p.StreamId, header: http.Header{}, body: reader}
	r.Handler.ServeHTTP(w, req)
	w.finish()
	return nil, nil
}

// parseContentLength reads decimal digits fitting an int64, or -1 (unknown, which handlers refuse).
func parseContentLength(s string) int64 {
	n, err := strconv.ParseUint(s, 10, 63)
	if err != nil {
		return -1
	}
	return int64(n)
}

func (r *Relay) respondError(id string, code int) {
	_ = r.Link.Send(protocol.MsgProxyResponse, protocol.ProxyResponsePayload{StreamId: id, Status: code, Headers: map[string]string{}})
	_ = r.Link.SendBinary(link.FrameEnd, id, nil)
}

// relayWriter streams a handler's response back over the link.
type relayWriter struct {
	relay   *Relay
	id      string
	header  http.Header
	wrote   bool
	failed  bool
	pending []byte
	body    *bodyReader // nil when the request has no body
}

func (w *relayWriter) Header() http.Header { return w.header }

// SetReadDeadline bounds the handler's wait for request body, as on a direct connection.
func (w *relayWriter) SetReadDeadline(t time.Time) error {
	if w.body == nil {
		return http.ErrNotSupported
	}
	if t.IsZero() {
		w.body.deadline.Store(0)
	} else {
		w.body.deadline.Store(t.UnixNano())
	}
	return nil
}

func (w *relayWriter) WriteHeader(code int) {
	if w.wrote {
		return
	}
	w.wrote = true
	h := map[string]string{}
	for k, v := range w.header {
		if len(v) > 0 {
			h[k] = v[0]
		}
	}
	if err := w.relay.Link.Send(protocol.MsgProxyResponse, protocol.ProxyResponsePayload{StreamId: w.id, Status: code, Headers: h}); err != nil {
		w.failed = true
	}
}

func (w *relayWriter) Write(b []byte) (int, error) {
	if !w.wrote {
		w.WriteHeader(http.StatusOK)
	}
	if w.failed {
		return 0, link.ErrNotConnected
	}
	w.pending = append(w.pending, b...)
	for len(w.pending) >= chunk {
		if err := w.relay.Link.SendBinary(link.FrameData, w.id, w.pending[:chunk]); err != nil {
			w.failed = true
			return 0, err
		}
		w.pending = w.pending[chunk:]
	}
	return len(b), nil
}

func (w *relayWriter) finish() {
	if !w.wrote {
		w.WriteHeader(http.StatusOK)
	}
	if w.failed {
		return
	}
	if len(w.pending) > 0 {
		_ = w.relay.Link.SendBinary(link.FrameData, w.id, w.pending)
	}
	_ = w.relay.Link.SendBinary(link.FrameEnd, w.id, nil)
}
