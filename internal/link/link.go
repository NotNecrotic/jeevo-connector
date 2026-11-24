// Package link is the connector's control channel: one outbound WebSocket to Jeevo, kept up forever.
package link

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math/rand"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/coder/websocket"

	"github.com/NotNecrotic/jeevo-connector/internal/protocol"
	"github.com/NotNecrotic/jeevo-connector/internal/store"
)

// Binary frame kinds (see ProxyBinaryFrame in the spec).
const (
	FrameData  byte = 1
	FrameEnd   byte = 2
	FrameAbort byte = 3
)

// Frame limits matching Jeevo's hub: a binary frame is a 17-byte header plus at most one chunk.
const (
	FrameChunk     = 256 << 10
	maxBinaryFrame = 17 + FrameChunk
	maxTextFrame   = 1 << 20
)

// Error is a protocol-level error a handler returns to the peer.
type Error struct {
	Code    string
	Message string
}

func (e *Error) Error() string { return e.Code + ": " + e.Message }

// Errf builds a protocol error.
func Errf(code, format string, a ...any) *Error {
	return &Error{Code: code, Message: fmt.Sprintf(format, a...)}
}

// ErrNotConnected is returned when there is no live connection.
var ErrNotConnected = errors.New("not connected to Jeevo")

// Handler answers one request type; handlers run on their own goroutine.
type Handler func(ctx context.Context, payload json.RawMessage) (any, error)

// Message is a decoded frame.
type Message struct {
	ID      string            `json:"id"`
	Type    string            `json:"type"`
	ReplyTo string            `json:"replyTo,omitempty"`
	Ok      *bool             `json:"ok,omitempty"`
	Payload json.RawMessage   `json:"payload,omitempty"`
	Error   *protocol.WsError `json:"error,omitempty"`
}

// Link manages the connection.
type Link struct {
	Auth   string
	Log    *slog.Logger
	Header http.Header

	// OnConnect runs after each dial; returning an error drops the connection and retries.
	OnConnect func(ctx context.Context) error
	// OnBinary receives proxied body frames.
	OnBinary func(kind byte, stream string, data []byte)
	// OnDisconnect runs when a connection ends.
	OnDisconnect func()

	handlers map[string]Handler

	urlMu sync.RWMutex
	url   string

	mu      sync.Mutex
	conn    *websocket.Conn
	pending map[string]chan Message
	writeMu sync.Mutex

	connected atomic.Bool
	connCtx   context.Context
}

// New builds a link.
func New(url, auth string, log *slog.Logger) *Link {
	return &Link{url: url, Auth: auth, Log: log, handlers: map[string]Handler{}, pending: map[string]chan Message{}}
}

// SetURL changes the URL used by the next dial.
func (l *Link) SetURL(url string) {
	l.urlMu.Lock()
	l.url = url
	l.urlMu.Unlock()
}

func (l *Link) wsURL() string {
	l.urlMu.RLock()
	defer l.urlMu.RUnlock()
	return l.url
}

// Handle registers a handler for a server request type.
func (l *Link) Handle(typ string, h Handler) { l.handlers[typ] = h }

// Connected reports whether the link is up.
func (l *Link) Connected() bool { return l.connected.Load() }

// Reconnect timing: back within a second or two of the other end returning, then a modest rate.
const (
	dialTimeout   = 10 * time.Second
	fatalDelay    = 5 * time.Minute // credentials rejected: retrying fast achieves nothing
	stableSession = 30 * time.Second
	retryJitter   = 0.2 // +-20%
)

// retrySchedule is the delay before each reconnect attempt; the last entry repeats.
var retrySchedule = []time.Duration{
	1 * time.Second, 1 * time.Second, 2 * time.Second, 2 * time.Second, 3 * time.Second,
	5 * time.Second, 8 * time.Second, 13 * time.Second, 15 * time.Second,
}

// retryDelay is the un-jittered delay before the attempt-th reconnect since the last good session.
func retryDelay(attempt int) time.Duration {
	if attempt < 0 {
		attempt = 0
	}
	if attempt >= len(retrySchedule) {
		attempt = len(retrySchedule) - 1
	}
	return retrySchedule[attempt]
}

// jittered spreads d by +-retryJitter; unit is a uniform value in [0, 1).
func jittered(d time.Duration, unit float64) time.Duration {
	return time.Duration(float64(d) * (1 + retryJitter*(2*unit-1)))
}

// Run keeps the connection up until ctx ends.
func (l *Link) Run(ctx context.Context) {
	attempt := 0
	for ctx.Err() == nil {
		start := time.Now()
		err := l.session(ctx)
		if ctx.Err() != nil {
			return
		}
		// Only a long-lived session resets the schedule, so a server that drops us at once is not hammered.
		if time.Since(start) > stableSession {
			attempt = 0
		}
		var delay time.Duration
		var fatal *FatalError
		switch {
		case errors.As(err, &fatal):
			l.Log.Error("jeevo refused the connection", "reason", fatal.Reason)
			delay = fatalDelay
		default:
			if err != nil {
				l.Log.Warn("control channel down", "err", err)
			}
			delay = jittered(retryDelay(attempt), rand.Float64()) //nolint:gosec // reconnect jitter, not a secret
			attempt++
		}
		l.Log.Info("reconnecting", "in", delay.Round(100*time.Millisecond))
		select {
		case <-ctx.Done():
			return
		case <-time.After(delay):
		}
	}
}

// FatalError is a refusal retrying will not fix quickly (revoked, 401).
type FatalError struct{ Reason string }

func (f *FatalError) Error() string { return f.Reason }

// session runs one connection until it ends.
func (l *Link) session(ctx context.Context) error {
	dctx, cancel := context.WithTimeout(ctx, dialTimeout)
	h := http.Header{}
	for k, v := range l.Header {
		h[k] = v
	}
	h.Set("Authorization", "Bearer "+l.Auth)
	conn, resp, err := websocket.Dial(dctx, l.wsURL(), &websocket.DialOptions{HTTPHeader: h})
	cancel()
	if resp != nil && resp.Body != nil {
		resp.Body.Close()
	}
	if err != nil {
		if resp != nil && (resp.StatusCode == http.StatusUnauthorized || resp.StatusCode == http.StatusForbidden) {
			return &FatalError{Reason: fmt.Sprintf("HTTP %d: credentials rejected (connector revoked or re-keyed?)", resp.StatusCode)}
		}
		return err
	}
	conn.SetReadLimit(maxTextFrame)
	sctx, scancel := context.WithCancel(ctx)
	defer scancel()

	l.mu.Lock()
	l.conn = conn
	l.connCtx = sctx
	l.mu.Unlock()
	l.connected.Store(true)
	defer func() {
		l.connected.Store(false)
		l.mu.Lock()
		l.conn = nil
		for id, ch := range l.pending {
			close(ch)
			delete(l.pending, id)
		}
		l.mu.Unlock()
		conn.Close(websocket.StatusNormalClosure, "")
		if l.OnDisconnect != nil {
			l.OnDisconnect()
		}
	}()

	readErr := make(chan error, 1)
	go func() { readErr <- l.readLoop(sctx, conn) }()

	// Keepalive: a dead TCP path is noticed within ~1 minute.
	go func() {
		t := time.NewTicker(30 * time.Second)
		defer t.Stop()
		for {
			select {
			case <-sctx.Done():
				return
			case <-t.C:
				pctx, pc := context.WithTimeout(sctx, 20*time.Second)
				err := conn.Ping(pctx)
				pc()
				if err != nil {
					conn.Close(websocket.StatusGoingAway, "ping timeout")
					return
				}
			}
		}
	}()

	if l.OnConnect != nil {
		if err := l.OnConnect(sctx); err != nil {
			return err
		}
	}
	return <-readErr
}

func (l *Link) readLoop(ctx context.Context, conn *websocket.Conn) error {
	for {
		typ, data, err := conn.Read(ctx)
		if err != nil {
			if websocket.CloseStatus(err) == websocket.StatusPolicyViolation {
				return &FatalError{Reason: "server closed the channel: policy violation"}
			}
			return err
		}
		if typ == websocket.MessageBinary {
			if len(data) > maxBinaryFrame {
				conn.Close(websocket.StatusMessageTooBig, "frame too large")
				return errors.New("jeevo sent an oversized binary frame")
			}
			if len(data) < 17 || l.OnBinary == nil {
				continue
			}
			l.OnBinary(data[0], string(data[1:17]), data[17:])
			continue
		}
		var m Message
		if err := json.Unmarshal(data, &m); err != nil || m.ID == "" || m.Type == "" {
			l.Log.Warn("dropping malformed frame")
			continue
		}
		if m.Type == protocol.MsgReply {
			l.mu.Lock()
			ch, ok := l.pending[m.ReplyTo]
			if ok {
				delete(l.pending, m.ReplyTo)
			}
			l.mu.Unlock()
			if ok {
				ch <- m
				close(ch)
			}
			continue
		}
		go l.dispatch(ctx, m)
	}
}

func (l *Link) dispatch(ctx context.Context, m Message) {
	h, ok := l.handlers[m.Type]
	info, known := protocol.Messages[m.Type]
	if !ok {
		l.reply(m.ID, nil, Errf("UNKNOWN_TYPE", "unsupported message type %q", m.Type))
		return
	}
	res, err := h(ctx, m.Payload)
	if known && info.Reply == "" {
		return // fire-and-forget message: no reply expected
	}
	l.reply(m.ID, res, err)
}

func (l *Link) reply(replyTo string, payload any, err error) {
	ok := err == nil
	msg := map[string]any{"id": store.NewID("r_"), "type": protocol.MsgReply, "replyTo": replyTo, "ok": ok}
	if err != nil {
		var pe *Error
		if errors.As(err, &pe) {
			msg["error"] = map[string]string{"code": pe.Code, "message": pe.Message}
		} else {
			msg["error"] = map[string]string{"code": "INTERNAL", "message": err.Error()}
		}
	} else if payload != nil {
		msg["payload"] = payload
	} else {
		msg["payload"] = map[string]any{}
	}
	if werr := l.writeJSON(msg); werr != nil {
		l.Log.Debug("reply not sent", "err", werr)
	}
}

func (l *Link) writeJSON(v any) error {
	raw, err := json.Marshal(v)
	if err != nil {
		return err
	}
	return l.write(websocket.MessageText, raw)
}

func (l *Link) write(typ websocket.MessageType, data []byte) error {
	l.mu.Lock()
	conn, ctx := l.conn, l.connCtx
	l.mu.Unlock()
	if conn == nil {
		return ErrNotConnected
	}
	l.writeMu.Lock()
	defer l.writeMu.Unlock()
	wctx, cancel := context.WithTimeout(ctx, 60*time.Second)
	defer cancel()
	return conn.Write(wctx, typ, data)
}

// Send fires a message without waiting for a reply.
func (l *Link) Send(typ string, payload any) error {
	return l.writeJSON(map[string]any{"id": store.NewID("c_"), "type": typ, "payload": payload})
}

// Request sends a message with a given id and waits for the reply payload in out.
func (l *Link) RequestID(ctx context.Context, id, typ string, payload any, out any, timeout time.Duration) error {
	ch := make(chan Message, 1)
	l.mu.Lock()
	if l.conn == nil {
		l.mu.Unlock()
		return ErrNotConnected
	}
	l.pending[id] = ch
	l.mu.Unlock()
	var raw json.RawMessage
	switch p := payload.(type) {
	case json.RawMessage:
		raw = p
	default:
		b, err := json.Marshal(payload)
		if err != nil {
			return err
		}
		raw = b
	}
	if err := l.writeJSON(map[string]any{"id": id, "type": typ, "payload": raw}); err != nil {
		l.mu.Lock()
		delete(l.pending, id)
		l.mu.Unlock()
		return err
	}
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	select {
	case <-ctx.Done():
		l.mu.Lock()
		delete(l.pending, id)
		l.mu.Unlock()
		return ctx.Err()
	case <-time.After(timeout):
		l.mu.Lock()
		delete(l.pending, id)
		l.mu.Unlock()
		return fmt.Errorf("%s: no reply within %s", typ, timeout)
	case m, ok := <-ch:
		if !ok {
			return ErrNotConnected
		}
		if m.Ok == nil || !*m.Ok {
			if m.Error != nil {
				return &Error{Code: m.Error.Code, Message: m.Error.Message}
			}
			return Errf("FAILED", "%s failed", typ)
		}
		if out != nil && len(m.Payload) > 0 {
			return json.Unmarshal(m.Payload, out)
		}
		return nil
	}
}

// Request sends a message and waits for its reply.
func (l *Link) Request(ctx context.Context, typ string, payload any, out any, timeout time.Duration) error {
	return l.RequestID(ctx, store.NewID("c_"), typ, payload, out, timeout)
}

// SendBinary writes one proxy body frame.
func (l *Link) SendBinary(kind byte, stream string, data []byte) error {
	if len(stream) != 16 {
		return errors.New("stream id must be 16 bytes")
	}
	buf := make([]byte, 0, 17+len(data))
	buf = append(buf, kind)
	buf = append(buf, stream...)
	buf = append(buf, data...)
	return l.write(websocket.MessageBinary, buf)
}
