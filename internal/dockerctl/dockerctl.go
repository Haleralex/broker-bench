// Package dockerctl kills and restarts compose containers through the Docker
// Engine API on its unix socket, so that faults are injected from inside the
// load generator at a precise moment of the traffic.
package dockerctl

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"time"
)

type Client struct {
	project string
	http    *http.Client
}

// New talks to the Docker daemon on socket; project is the compose project
// name, containers are <project>-<service>-1.
func New(socket, project string) *Client {
	transport := &http.Transport{
		DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			var d net.Dialer
			return d.DialContext(ctx, "unix", socket)
		},
	}
	return &Client{project: project, http: &http.Client{Transport: transport, Timeout: 30 * time.Second}}
}

func (c *Client) container(service string) string {
	return c.project + "-" + service + "-1"
}

// Kill sends SIGKILL: no shutdown, no flush; the kernel keeps what the process
// already wrote into the page cache.
func (c *Client) Kill(ctx context.Context, service string) error {
	return c.post(ctx, "/containers/"+url.PathEscape(c.container(service))+"/kill?signal=KILL")
}

// Start starts a stopped container with its volumes, i.e. the broker restarts
// on the data it left behind.
func (c *Client) Start(ctx context.Context, service string) error {
	return c.post(ctx, "/containers/"+url.PathEscape(c.container(service))+"/start")
}

// WaitHealthy waits until the container's healthcheck passes.
func (c *Client) WaitHealthy(ctx context.Context, service string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for {
		status, err := c.health(ctx, service)
		if err == nil && status == "healthy" {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("%s not healthy after %s (status %q, %v)", service, timeout, status, err)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(500 * time.Millisecond):
		}
	}
}

func (c *Client) health(ctx context.Context, service string) (string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://docker/containers/"+url.PathEscape(c.container(service))+"/json", nil)
	if err != nil {
		return "", err
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	var info struct {
		State struct {
			Running bool
			Health  *struct{ Status string }
		}
	}
	if err := json.NewDecoder(resp.Body).Decode(&info); err != nil {
		return "", err
	}
	switch {
	case info.State.Health != nil:
		return info.State.Health.Status, nil
	case info.State.Running:
		return "healthy", nil // no healthcheck: running is the best we know
	default:
		return "stopped", nil
	}
}

func (c *Client) post(ctx context.Context, path string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, "http://docker"+path, nil)
	if err != nil {
		return err
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	// 304: already started or stopped.
	if resp.StatusCode >= 300 && resp.StatusCode != http.StatusNotModified {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("docker %s: HTTP %d: %s", path, resp.StatusCode, body)
	}
	return nil
}
