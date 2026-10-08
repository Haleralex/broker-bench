package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"time"

	"github.com/Haleralex/broker-bench/internal/metrics"
)

// composeProject must match the name in compose.yaml; resource sampling filters on it.
const composeProject = "broker-bench"

type compose struct {
	file string
	env  []string // variables substituted into compose.yaml
}

func (c compose) command(ctx context.Context, extraEnv []string, args ...string) *exec.Cmd {
	cmd := exec.CommandContext(ctx, "docker", append([]string{"compose", "-f", c.file}, args...)...)
	cmd.Env = append(append(os.Environ(), c.env...), extraEnv...)
	return cmd
}

// run streams output to the terminal.
func (c compose) run(ctx context.Context, extraEnv []string, args ...string) error {
	cmd := c.command(ctx, extraEnv, args...)
	cmd.Stdout, cmd.Stderr = os.Stdout, os.Stderr
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("docker compose %s: %w", strings.Join(args, " "), err)
	}
	return nil
}

func (c compose) output(ctx context.Context, args ...string) ([]byte, error) {
	cmd := c.command(ctx, nil, args...)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	out, err := cmd.Output()
	if err != nil {
		return nil, fmt.Errorf("docker compose %s: %w: %s", strings.Join(args, " "), err, strings.TrimSpace(stderr.String()))
	}
	return out, nil
}

func dockerOutput(ctx context.Context, args ...string) ([]byte, error) {
	var stderr bytes.Buffer
	cmd := exec.CommandContext(ctx, "docker", args...)
	cmd.Stderr = &stderr
	out, err := cmd.Output()
	if err != nil {
		return nil, fmt.Errorf("docker %s: %w: %s", strings.Join(args, " "), err, strings.TrimSpace(stderr.String()))
	}
	return out, nil
}

type resourceSample struct {
	At    time.Time       `json:"at"`
	Stats json.RawMessage `json:"stats"`
}

// sampleResources records docker stats for every project container until ctx ends.
// Each docker stats call itself takes about a second, so the interval is approximate.
// publish, if not nil, receives every round for live monitoring.
func sampleResources(ctx context.Context, interval time.Duration, publish func([]metrics.ContainerSample)) []resourceSample {
	var samples []resourceSample
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		ids, err := dockerOutput(ctx, "ps", "-q", "--filter", "label=com.docker.compose.project="+composeProject)
		if err == nil && len(bytes.TrimSpace(ids)) > 0 {
			args := append([]string{"stats", "--no-stream", "--format", "{{json .}}"}, strings.Fields(string(ids))...)
			if out, err := dockerOutput(ctx, args...); err == nil {
				at := time.Now().UTC()
				var round []metrics.ContainerSample
				scanner := bufio.NewScanner(bytes.NewReader(out))
				for scanner.Scan() {
					if line := bytes.TrimSpace(scanner.Bytes()); json.Valid(line) {
						samples = append(samples, resourceSample{At: at, Stats: json.RawMessage(bytes.Clone(line))})
						if s, ok := parseDockerStat(line); ok {
							round = append(round, s)
						}
					}
				}
				if publish != nil {
					publish(round)
				}
			}
		}
		select {
		case <-ctx.Done():
			return samples
		case <-ticker.C:
		}
	}
}

// dockerStat is the subset of docker stats JSON lines used here.
type dockerStat struct {
	Name     string `json:"Name"`
	CPUPerc  string `json:"CPUPerc"`  // "123.45%"
	MemUsage string `json:"MemUsage"` // "1.2GiB / 2GiB"
}

func parseDockerStat(line []byte) (metrics.ContainerSample, bool) {
	var st dockerStat
	if json.Unmarshal(line, &st) != nil {
		return metrics.ContainerSample{}, false
	}
	cpu, err := strconv.ParseFloat(strings.TrimSuffix(st.CPUPerc, "%"), 64)
	if err != nil {
		return metrics.ContainerSample{}, false
	}
	used, _, _ := strings.Cut(st.MemUsage, "/")
	return metrics.ContainerSample{Container: serviceName(st.Name), CPUPercent: cpu, MemoryBytes: parseBytes(strings.TrimSpace(used))}, true
}

// serviceName turns container names into compose service names:
// broker-bench-kafka-2-1 -> kafka-2, broker-bench-loadgen-<run> -> loadgen.
func serviceName(container string) string {
	if strings.HasPrefix(container, composeProject+"-loadgen") {
		return "loadgen"
	}
	name := strings.TrimPrefix(container, composeProject+"-")
	if i := strings.LastIndex(name, "-"); i > 0 {
		if _, err := strconv.Atoi(name[i+1:]); err == nil {
			name = name[:i]
		}
	}
	return name
}

// parseBytes reads docker's human-readable sizes such as 512KiB or 1.5GB.
func parseBytes(s string) float64 {
	units := []struct {
		suffix string
		scale  float64
	}{
		{"KiB", 1 << 10}, {"MiB", 1 << 20}, {"GiB", 1 << 30}, {"TiB", 1 << 40},
		{"kB", 1e3}, {"KB", 1e3}, {"MB", 1e6}, {"GB", 1e9}, {"TB", 1e12}, {"B", 1},
	}
	for _, u := range units {
		if number, ok := strings.CutSuffix(s, u.suffix); ok {
			v, err := strconv.ParseFloat(strings.TrimSpace(number), 64)
			if err != nil {
				return 0
			}
			return v * u.scale
		}
	}
	return 0
}
