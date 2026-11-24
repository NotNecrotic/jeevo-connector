package app

import (
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/NotNecrotic/jeevo-connector/internal/store"
)

// A failed version is not retried for updateRetryAfter, nor after updateMaxFailures until a new version is announced.
const (
	updateRetryAfter  = time.Hour
	updateMaxFailures = 3
)

// updateGate decides whether the announced version may install now; failures persist in the store across rollback restarts.
type updateGate struct {
	st *store.Store

	mu      sync.Mutex
	running bool
}

const updateGateKey = "update_failures"

// load reads the failure count and time recorded for version.
func (g *updateGate) load(version string) (failures int, last time.Time) {
	f := strings.Fields(g.st.Get(updateGateKey))
	if len(f) != 3 || f[0] != version {
		return 0, time.Time{}
	}
	n, err1 := strconv.Atoi(f[1])
	at, err2 := strconv.ParseInt(f[2], 10, 64)
	if err1 != nil || err2 != nil {
		return 0, time.Time{}
	}
	return n, time.Unix(at, 0)
}

// begin reports whether an automatic update to version may start at now, marking it running (finish ends it).
func (g *updateGate) begin(version string, now time.Time) bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.running {
		return false
	}
	failures, last := g.load(version)
	if failures >= updateMaxFailures || (failures > 0 && now.Sub(last) < updateRetryAfter) {
		return false
	}
	g.running = true
	return true
}

// failed records a failed attempt to install version.
func (g *updateGate) failed(version string, now time.Time) {
	g.mu.Lock()
	defer g.mu.Unlock()
	failures, _ := g.load(version)
	_ = g.st.Set(updateGateKey, fmt.Sprintf("%s %d %d", version, failures+1, now.Unix()))
}

// finish ends the attempt begin allowed.
func (g *updateGate) finish() {
	g.mu.Lock()
	g.running = false
	g.mu.Unlock()
}
