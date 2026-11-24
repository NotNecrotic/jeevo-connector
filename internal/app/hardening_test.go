package app

import (
	"io"
	"log/slog"
	"net"
	"net/http"
	"path/filepath"
	"testing"
	"time"

	"github.com/NotNecrotic/jeevo-connector/internal/config"
	"github.com/NotNecrotic/jeevo-connector/internal/server"
	"github.com/NotNecrotic/jeevo-connector/internal/store"
)

func testStore(t *testing.T) *store.Store {
	t.Helper()
	st, err := store.Open(filepath.Join(t.TempDir(), "c.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	return st
}

func testApp(st *store.Store, secret string) *App {
	return &App{
		Cfg:    &config.Install{ConnectorID: "abcd", ConnectorSecret: secret},
		Log:    slog.New(slog.NewTextHandler(io.Discard, nil)),
		store:  st,
		public: &server.Public{},
	}
}

func TestRevocationIsEnforcedAndRemembered(t *testing.T) {
	st := testStore(t)
	a := testApp(st, "first-secret")
	a.loadRevoked()
	if a.public.Revoked() {
		t.Fatal("a fresh connector starts revoked")
	}

	// A listener is up when the revocation arrives.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := &http.Server{Handler: http.NotFoundHandler()}
	go func() { _ = srv.Serve(ln) }()
	a.srv, a.srvKey = srv, "127.0.0.1|none"

	a.revoke()
	if !a.public.Revoked() {
		t.Fatal("the public side still serves")
	}
	if a.srv != nil || a.srvKey != "" {
		t.Fatal("the server was not dropped")
	}
	if c, err := net.DialTimeout("tcp", ln.Addr().String(), time.Second); err == nil {
		c.Close()
		t.Fatal("the listener still accepts connections")
	}
	// Nothing brings it back while revoked (a settings change, a certificate).
	a.ensureServer()
	if a.srv != nil {
		t.Fatal("ensureServer restarted the listener of a revoked connector")
	}

	// A restart reads the state back...
	b := testApp(st, "first-secret")
	b.loadRevoked()
	if !b.public.Revoked() {
		t.Fatal("revocation was forgotten across a restart")
	}
	// ...but a connector enrolled again over the same data (new secret) is not revoked.
	c := testApp(st, "second-secret")
	c.loadRevoked()
	if c.public.Revoked() {
		t.Fatal("a re-enrolled connector is still revoked")
	}
	// The status command still shows when it happened.
	if st.Get("revoked") == "" {
		t.Fatal("revocation time not recorded")
	}
}

func TestUpdateGate(t *testing.T) {
	st := testStore(t)
	g := &updateGate{st: st}
	t0 := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	if !g.begin("2.0.0", t0) {
		t.Fatal("first attempt refused")
	}
	// One attempt at a time, however often the connector reconnects.
	if g.begin("2.0.0", t0) {
		t.Fatal("two attempts at once")
	}
	g.failed("2.0.0", t0)
	g.finish()

	if g.begin("2.0.0", t0.Add(time.Second)) || g.begin("2.0.0", t0.Add(59*time.Minute)) {
		t.Fatal("retried a failed version within the hour")
	}
	if !g.begin("2.0.0", t0.Add(time.Hour)) {
		t.Fatal("not retried after the hour")
	}
	g.failed("2.0.0", t0.Add(time.Hour))
	g.finish()

	// The count survives a restart.
	g = &updateGate{st: st}
	if g.begin("2.0.0", t0.Add(time.Hour+time.Minute)) {
		t.Fatal("a restart forgot the failure")
	}
	if !g.begin("2.0.0", t0.Add(2*time.Hour)) {
		t.Fatal("second retry refused")
	}
	g.failed("2.0.0", t0.Add(2*time.Hour))
	g.finish()

	// Three failures end it for this version, however long ago.
	if g.begin("2.0.0", t0.Add(30*24*time.Hour)) {
		t.Fatal("tried a version that failed three times")
	}
	// A newer version starts afresh.
	if !g.begin("2.0.1", t0.Add(2*time.Hour)) {
		t.Fatal("a newer version was held back by the old one")
	}
	g.finish()
	// A connection that dropped under an attempt (finish without failed) costs nothing.
	if !g.begin("2.0.1", t0.Add(2*time.Hour)) {
		t.Fatal("an interrupted attempt counted as a failure")
	}
	g.finish()
}

func accepts(l net.Listener) chan net.Conn {
	ch := make(chan net.Conn, 16)
	go func() {
		for {
			c, err := l.Accept()
			if err != nil {
				return
			}
			ch <- c
		}
	}()
	return ch
}

// closedByServer reports whether the server side closed c.
func closedByServer(c net.Conn) bool {
	_ = c.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
	_, err := c.Read(make([]byte, 1))
	if ne, ok := err.(net.Error); ok && ne.Timeout() {
		return false
	}
	return err != nil
}

func dial(t *testing.T, l net.Listener) net.Conn {
	t.Helper()
	c, err := net.Dial("tcp", l.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Close() })
	return c
}

func TestLimitListenerTotal(t *testing.T) {
	tcp, _ := net.Listen("tcp", "127.0.0.1:0")
	l := newLimitListener(tcp, 2, 10)
	defer l.Close()
	got := accepts(l)
	c1, c2, c3 := dial(t, l), dial(t, l), dial(t, l)
	if !closedByServer(c3) {
		t.Fatal("a connection over the total cap was kept")
	}
	if closedByServer(c1) || closedByServer(c2) {
		t.Fatal("a connection within the cap was closed")
	}
	// Closing one makes room again.
	s1 := <-got
	<-got
	s1.Close()
	s1.Close() // closing twice must not free two places
	c4, c5 := dial(t, l), dial(t, l)
	if closedByServer(c4) {
		t.Fatal("no room after a close")
	}
	if !closedByServer(c5) {
		t.Fatal("a double close freed more than one place")
	}
}

func TestLimitListenerPerAddress(t *testing.T) {
	tcp, _ := net.Listen("tcp", "127.0.0.1:0")
	l := newLimitListener(tcp, 100, 2)
	defer l.Close()
	accepts(l)
	a, b, c := dial(t, l), dial(t, l), dial(t, l)
	if !closedByServer(c) {
		t.Fatal("a third connection from one address was kept")
	}
	if closedByServer(a) || closedByServer(b) {
		t.Fatal("connections within the per-address cap were closed")
	}
}

func TestAddrKey(t *testing.T) {
	addr := func(s string) net.Addr {
		a, err := net.ResolveTCPAddr("tcp", s)
		if err != nil {
			t.Fatal(err)
		}
		return a
	}
	if addrKey(addr("192.0.2.7:5000")) != addrKey(addr("192.0.2.7:6000")) || addrKey(addr("192.0.2.7:1")) == addrKey(addr("192.0.2.8:1")) {
		t.Error("IPv4 addresses are not counted by host")
	}
	if addrKey(addr("[2001:db8:1:2::1]:1")) != addrKey(addr("[2001:db8:1:2:ffff::9]:2")) {
		t.Error("one IPv6 /64 is counted as several addresses")
	}
	if addrKey(addr("[2001:db8:1:2::1]:1")) == addrKey(addr("[2001:db8:1:3::1]:1")) {
		t.Error("two IPv6 /64s are counted as one")
	}
}
