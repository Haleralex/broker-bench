// Command loadgen runs one experiment against one profile from inside the
// compose network and writes the result as JSON. cmd/bench starts it.
//
//	loadgen -mode throughput  fill, drain and steady rates (see internal/loadgen)
//	loadgen -mode chaos       steady traffic, a killed node, an integrity check
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/Haleralex/broker-bench/internal/dockerctl"
	"github.com/Haleralex/broker-bench/internal/loadgen"
	"github.com/Haleralex/broker-bench/internal/metrics"
	"github.com/Haleralex/broker-bench/internal/profile"
	"github.com/Haleralex/broker-bench/internal/queue"
)

// Report is the file cmd/bench summarizes; Kind selects which result is set.
type Report struct {
	Kind         string               `json:"kind"` // throughput or chaos
	RunID        string               `json:"run_id"`
	Profile      string               `json:"profile"`
	Description  string               `json:"description"`
	StartedAt    time.Time            `json:"started_at"`
	Config       any                  `json:"config"`
	Result       *loadgen.Result      `json:"result,omitempty"`
	Chaos        *loadgen.ChaosResult `json:"chaos,omitempty"`
	BackendStats map[string]any       `json:"backend_stats,omitempty"`
	Error        string               `json:"error,omitempty"`
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	err := run(ctx)
	stop()
	if err != nil {
		fmt.Fprintln(os.Stderr, "loadgen:", err)
		os.Exit(1)
	}
}

func run(ctx context.Context) error {
	mode := "throughput"
	if len(os.Args) > 2 && os.Args[1] == "-mode" {
		mode = os.Args[2]
		os.Args = append(os.Args[:1], os.Args[3:]...)
	}
	throughput, chaos := loadgen.Defaults(), loadgen.ChaosDefaults()
	fs := flag.NewFlagSet("loadgen", flag.ExitOnError)
	name := fs.String("profile", "kafka", "one of "+strings.Join(profile.Names(), ", "))
	runID := fs.String("run-id", "manual", "identifier stored in the report")
	out := fs.String("out", "", "report path; stdout when empty")
	linger := fs.Duration("kafka-linger", 0, "Kafka producer linger; 0 sends each batch at once")
	metricsAddr := fs.String("metrics-addr", ":9100", "Prometheus endpoint; empty disables it")
	faultKind := fs.String("fault", "kill", "chaos: none, or kill (the node; the queue leader on a cluster)")
	switch mode {
	case "throughput":
		throughput.RegisterFlags(fs)
	case "chaos":
		chaos.RegisterFlags(fs)
	default:
		return fmt.Errorf("-mode must be throughput or chaos, got %q", mode)
	}
	_ = fs.Parse(os.Args[1:])
	p, ok := profile.Lookup(*name)
	if !ok {
		return fmt.Errorf("unknown profile %q", *name)
	}
	publishers := throughput.Producers
	if mode == "chaos" {
		publishers = chaos.Producers
		if err := chaos.Validate(); err != nil {
			return err
		}
	} else if err := throughput.Validate(); err != nil {
		return err
	}

	backend, err := profile.Open(ctx, p, profile.Options{Publishers: publishers, KafkaLinger: *linger})
	if err != nil {
		return err
	}
	defer backend.Close()

	report := Report{Kind: mode, RunID: *runID, Profile: p.Name, Description: p.Description, StartedAt: time.Now().UTC()}
	logf := func(format string, args ...any) {
		fmt.Fprintf(os.Stderr, "%s %s: %s\n", time.Now().Format("15:04:05"), p.Name, fmt.Sprintf(format, args...))
	}
	var observer loadgen.Observer
	if *metricsAddr != "" {
		m := metrics.NewLoadgen(p.Name, *runID)
		metricsCtx, stopMetrics := context.WithCancel(context.WithoutCancel(ctx))
		defer func() {
			// Let Prometheus scrape the final values before the process exits.
			time.Sleep(3 * time.Second)
			stopMetrics()
		}()
		if err := metrics.Serve(metricsCtx, *metricsAddr, m.Handler()); err != nil {
			return err
		}
		go pollBrokerStats(metricsCtx, backend, m)
		observer = m
	}

	switch mode {
	case "throughput":
		report.Config = throughput
		var res loadgen.Result
		res, err = loadgen.Run(ctx, backend, throughput, observer, logf)
		report.Result = &res
	case "chaos":
		report.Config = map[string]any{"chaos": chaos, "fault": *faultKind}
		var fault loadgen.Fault
		if fault, err = newFault(*faultKind, p, backend); err != nil {
			return err
		}
		var res loadgen.ChaosResult
		res, err = loadgen.RunChaos(ctx, backend, chaos, fault, *faultKind, observer, logf)
		report.Chaos = &res
	}
	if err != nil {
		report.Error = err.Error()
	}
	statsCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
	defer cancel()
	if stats, statsErr := backend.Stats(statsCtx); statsErr == nil {
		report.BackendStats = stats
	} else {
		logf("%v", statsErr)
	}
	if writeErr := write(*out, report); writeErr != nil {
		return writeErr
	}
	return err
}

// killFault kills a broker container with SIGKILL and starts it again. On a
// cluster it picks the node leading the queue at the moment of the fault.
type killFault struct {
	docker  *dockerctl.Client
	profile profile.Profile
	backend queue.Backend
	target  string
}

func newFault(kind string, p profile.Profile, backend queue.Backend) (loadgen.Fault, error) {
	switch kind {
	case "none":
		return nil, nil
	case "kill":
		project := os.Getenv("BENCH_PROJECT")
		if project == "" {
			return nil, errors.New("BENCH_PROJECT is not set; see compose.yaml")
		}
		return &killFault{docker: dockerctl.New("/var/run/docker.sock", project), profile: p, backend: backend}, nil
	}
	return nil, fmt.Errorf("-fault must be none or kill, got %q", kind)
}

func (f *killFault) Inject(ctx context.Context) (string, error) {
	f.target = f.profile.Broker
	if f.profile.Cluster {
		clustered, ok := f.backend.(queue.Clustered)
		if !ok {
			return "", fmt.Errorf("%s cannot name its leader", f.profile.Name)
		}
		leader, err := clustered.Leader(ctx)
		if err != nil {
			return "", fmt.Errorf("find leader: %w", err)
		}
		f.target = leader
	}
	return f.target, f.docker.Kill(ctx, f.target)
}

func (f *killFault) Recover(ctx context.Context) error {
	if err := f.docker.Start(ctx, f.target); err != nil {
		return err
	}
	return f.docker.WaitHealthy(ctx, f.target, 3*time.Minute)
}

// pollBrokerStats refreshes broker gauges every two seconds. Errors are
// expected before the queue exists and while a node is down; they are ignored.
func pollBrokerStats(ctx context.Context, backend queue.Backend, m *metrics.Loadgen) {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		statsCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		if stats, err := backend.Stats(statsCtx); err == nil {
			m.SetBrokerStats(stats)
		}
		cancel()
	}
}

func write(path string, report Report) error {
	data, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		return err
	}
	data = append(data, '\n')
	if path == "" {
		_, err = os.Stdout.Write(data)
		return err
	}
	return os.WriteFile(path, data, 0o644)
}
