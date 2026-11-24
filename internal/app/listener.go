package app

import (
	"net"
	"sync"
)

// Connection caps of the public listener; the per-IP cap leaves room for a household or office.
const (
	maxConns      = 1024
	maxConnsPerIP = 64
)

// limitListener caps open connections overall and per address (IPv6 /64 counts as one), closing extras before TLS.
type limitListener struct {
	net.Listener
	maxTotal, maxPerIP int

	mu    sync.Mutex
	total int
	byIP  map[string]int
}

func newLimitListener(l net.Listener, maxTotal, maxPerIP int) *limitListener {
	return &limitListener{Listener: l, maxTotal: maxTotal, maxPerIP: maxPerIP, byIP: map[string]int{}}
}

// addrKey is the address connections are counted under.
func addrKey(a net.Addr) string {
	host, _, err := net.SplitHostPort(a.String())
	if err != nil {
		return a.String()
	}
	ip := net.ParseIP(host)
	if ip == nil {
		return host
	}
	if ip4 := ip.To4(); ip4 != nil {
		return ip4.String()
	}
	return ip.Mask(net.CIDRMask(64, 128)).String()
}

func (l *limitListener) admit(key string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.total >= l.maxTotal || l.byIP[key] >= l.maxPerIP {
		return false
	}
	l.total++
	l.byIP[key]++
	return true
}

func (l *limitListener) release(key string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.total--
	if l.byIP[key]--; l.byIP[key] <= 0 {
		delete(l.byIP, key)
	}
}

// Accept returns the next connection that is within the caps.
func (l *limitListener) Accept() (net.Conn, error) {
	for {
		c, err := l.Listener.Accept()
		if err != nil {
			return nil, err
		}
		key := addrKey(c.RemoteAddr())
		if !l.admit(key) {
			_ = c.Close()
			continue
		}
		return &limitConn{Conn: c, release: func() { l.release(key) }}, nil
	}
}

type limitConn struct {
	net.Conn
	once    sync.Once
	release func()
}

func (c *limitConn) Close() error {
	c.once.Do(c.release)
	return c.Conn.Close()
}
