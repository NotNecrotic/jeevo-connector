package app

import (
	"context"
	"sync"
	"time"

	"github.com/NotNecrotic/jeevo-connector/internal/publicip"
)

// How often the connector re-checks its public address.
const publicAddrInterval = 5 * time.Minute

// publicAddr is the last detected public address ("" = unknown).
type publicAddr struct {
	mu   sync.RWMutex
	addr string
}

func (p *publicAddr) get() string {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.addr
}

// set stores addr and reports whether it changed.
func (p *publicAddr) set(addr string) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.addr == addr {
		return false
	}
	p.addr = addr
	return true
}

// reported is the value for hello and heartbeat payloads.
func (a *App) reportedPublicAddress() *string {
	if v := a.pub.get(); v != "" {
		return &v
	}
	return nil
}

// publicAddrLoop keeps a.pub current and heartbeats on change; it always detects, the override wins for DNS.
func (a *App) publicAddrLoop(ctx context.Context) {
	d := &publicip.Detector{}
	for {
		addr, src, err := d.Detect(ctx)
		switch {
		case err == nil:
			if a.pub.set(addr.String()) {
				a.Log.Info("public address detected", "addr", addr.String(), "source", string(src))
				a.kickHeartbeat()
			}
		case ctx.Err() == nil:
			a.Log.Debug("public address not detected", "err", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(publicAddrInterval):
		}
	}
}

func (a *App) kickHeartbeat() {
	select {
	case a.hbNow <- struct{}{}:
	default:
	}
}
