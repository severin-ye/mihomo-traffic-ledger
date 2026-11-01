//go:build windows

package main

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"os"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Microsoft/go-winio"
)

type pipeTestServer struct {
	settings mihomoSettings
	opened   atomic.Int64
	closed   atomic.Int64
	server   *http.Server
}

func startPipeTestServer(t *testing.T) *pipeTestServer {
	t.Helper()
	pipe := fmt.Sprintf(`\\.\pipe\traffic-ledger-test-%d`, time.Now().UnixNano())
	listener, err := winio.ListenPipe(pipe, nil)
	if err != nil {
		t.Fatal(err)
	}
	p := &pipeTestServer{settings: mihomoSettings{URL: "npipe://./pipe/" + pipe[len(`\\.\pipe\`):]}}
	p.server = &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/proxies":
			if r.Method == http.MethodPut {
				w.WriteHeader(http.StatusNoContent)
				return
			}
			fmt.Fprint(w, `{"proxies":{}}`)
		default:
			fmt.Fprint(w, `{"connections":[],"uploadTotal":0,"downloadTotal":0}`)
		}
	}), ConnState: func(_ net.Conn, state http.ConnState) {
		if state == http.StateNew {
			p.opened.Add(1)
		}
		if state == http.StateClosed {
			p.closed.Add(1)
		}
	}}
	go p.server.Serve(listener)
	t.Cleanup(func() { p.server.Close() })
	return p
}

func waitPipeClosed(t *testing.T, p *pipeTestServer) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for p.closed.Load() != p.opened.Load() && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if p.closed.Load() != p.opened.Load() {
		t.Fatalf("pipe connections remain open: opened=%d closed=%d", p.opened.Load(), p.closed.Load())
	}
}

func TestNamedPipePollingReusesConnection(t *testing.T) {
	p := startPipeTestServer(t)
	svc := &service{}
	t.Cleanup(svc.closeMihomoIdleConnections)
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	duration := time.Duration(0)
	if os.Getenv("LEDGER_RESOURCE_OBSERVATION") == "1" {
		duration = 30 * time.Second
	}
	started := time.Now()
	goroutinesBefore := runtime.NumGoroutine()
	polls := 0
	for polls < 100 || time.Since(started) < duration {
		if _, err := svc.fetchConnections(ctx, p.settings); err != nil {
			t.Fatalf("poll %d: %v", polls, err)
		}
		polls++
		if duration > 0 {
			time.Sleep(20 * time.Millisecond)
		}
	}
	if n := p.opened.Load(); n != 1 {
		t.Fatalf("%d sequential polls opened %d pipe connections; want 1 reused connection", polls, n)
	}
	if _, err := svc.listControllableProxyGroups(p.settings); err != nil {
		t.Fatal(err)
	}
	if n := p.opened.Load(); n != 1 {
		t.Fatalf("proxy listing opened another pipe: %d", n)
	}
	t.Logf("polls=%d duration=%s opened=%d live=%d goroutines_before=%d goroutines_after=%d", polls, time.Since(started), p.opened.Load(), p.opened.Load()-p.closed.Load(), goroutinesBefore, runtime.NumGoroutine())
	svc.closeMihomoIdleConnections()
	waitPipeClosed(t, p)
}

func TestNamedPipeEndpointAndSettingsChangesReleaseIdleConnections(t *testing.T) {
	first, second := startPipeTestServer(t), startPipeTestServer(t)
	svc := &service{mihomoSettings: first.settings}
	t.Cleanup(svc.closeMihomoIdleConnections)
	for _, p := range []*pipeTestServer{first, second} {
		if _, err := svc.fetchConnections(context.Background(), p.settings); err != nil {
			t.Fatal(err)
		}
	}
	waitPipeClosed(t, first)
	svc.setMihomoSettings(second.settings)
	waitPipeClosed(t, second)
	// A closed idle transport must be replaceable on the next poll.
	if _, err := svc.fetchConnections(context.Background(), second.settings); err != nil {
		t.Fatal(err)
	}
	svc.closeMihomoIdleConnections()
	waitPipeClosed(t, second)
}
