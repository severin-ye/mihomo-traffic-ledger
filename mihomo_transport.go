package main

import (
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"
)

const defaultWindowsMihomoEndpoint = "npipe://./pipe/verge-mihomo"

func resolveMihomoTransport(endpoint string, fallback *http.Client) (string, *http.Client, error) {
	endpoint = strings.TrimSpace(endpoint)
	parsed, err := url.Parse(endpoint)
	if err != nil {
		return "", nil, err
	}
	switch strings.ToLower(parsed.Scheme) {
	case "http", "https":
		if fallback == nil {
			fallback = &http.Client{Timeout: 10 * time.Second}
		}
		return strings.TrimRight(endpoint, "/"), fallback, nil
	case "npipe":
		pipePath := `\\.\pipe\` + strings.TrimPrefix(strings.TrimPrefix(parsed.Path, "/pipe/"), "/")
		if parsed.Host != "." || pipePath == `\\.\pipe\` {
			return "", nil, fmt.Errorf("invalid named pipe endpoint %q", endpoint)
		}
		client, err := newNamedPipeHTTPClient(pipePath)
		if err != nil {
			return "", nil, err
		}
		return "http://mihomo", client, nil
	default:
		return "", nil, fmt.Errorf("unsupported Mihomo endpoint scheme %q", parsed.Scheme)
	}
}

// Keep one owned transport for the current endpoint. In particular, a pipe
// transport must not be abandoned after each poll with a live idle connection.
func (s *service) resolveMihomoTransport(endpoint string) (string, *http.Client, error) {
	endpoint = strings.TrimSpace(endpoint)
	s.transportMu.Lock()
	defer s.transportMu.Unlock()
	if s.mihomoTransportClient != nil && endpoint == s.mihomoTransportEndpoint {
		return s.mihomoTransportBase, s.mihomoTransportClient, nil
	}
	base, client, err := resolveMihomoTransport(endpoint, s.client)
	if err != nil {
		return "", nil, err
	}
	if s.mihomoTransportClient != nil {
		s.mihomoTransportClient.CloseIdleConnections()
	}
	s.mihomoTransportEndpoint = endpoint
	s.mihomoTransportBase = base
	s.mihomoTransportClient = client
	return base, client, nil
}

// Called on settings changes and after collector/server shutdown. Active
// requests keep their contexts; their connections close when returned idle.
func (s *service) closeMihomoIdleConnections() {
	s.transportMu.Lock()
	defer s.transportMu.Unlock()
	if s.mihomoTransportClient != nil {
		s.mihomoTransportClient.CloseIdleConnections()
	}
	s.mihomoTransportClient = nil
	s.mihomoTransportEndpoint = ""
	s.mihomoTransportBase = ""
}
