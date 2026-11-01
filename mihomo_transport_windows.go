//go:build windows

package main

import (
	"context"
	"net"
	"net/http"
	"time"

	"github.com/Microsoft/go-winio"
)

func defaultMihomoEndpoint() string { return defaultWindowsMihomoEndpoint }

func newNamedPipeHTTPClient(pipePath string) (*http.Client, error) {
	transport := &http.Transport{
		MaxIdleConns:        2,
		MaxIdleConnsPerHost: 2,
		IdleConnTimeout:     30 * time.Second,
		DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			return winio.DialPipeContext(ctx, pipePath)
		},
	}
	return &http.Client{Transport: transport, Timeout: 10 * time.Second}, nil
}
