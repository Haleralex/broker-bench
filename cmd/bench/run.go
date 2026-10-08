package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"github.com/Haleralex/broker-bench/internal/loadgen"
	"github.com/Haleralex/broker-bench/internal/metrics"
	"github.com/Haleralex/broker-bench/internal/profile"
)

// common are the flags of both experiments.
type common struct {
	profiles    []string
	repetitions int
	kafkaLinger time.Duration
	skipBuild   bool
	keepRunning bool
	monitor     bool
	resultDir   string
	composeFile string
}

func (c *common) register(fs *flag.FlagSet, profiles *string, defaultProfiles []string) {
	fs.StringVar(profiles, "profiles", strings.Join(defaultProfiles, ","), "comma-separated profiles: "+strings.Join(profile.Names(), ", "))
	fs.IntVar(&c.repetitions, "repetitions", 1, "full passes over the profiles; even passes run in reverse order")
	fs.DurationVar(&c.kafkaLinger, "kafka-linger", 0, "Kafka producer linger; 0 sends each batch at once")
	fs.BoolVar(&c.skipBuild, "skip-build", false, "reuse the existing loadgen image")
	fs.BoolVar(&c.keepRunning, "keep-running", false, "leave the last profile's brokers running")
	fs.BoolVar(&c.monitor, "monitor", true, "start Prometheus and Grafana (port 3000) and publish container stats")
	fs.StringVar(&c.resultDir, "results", "results", "directory for measurements")
	fs.StringVar(&c.composeFile, "compose", "compose.yaml", "Compose file")
}

func (c *common) validate(fs *flag.FlagSet, profiles string) error {
	if fs.NArg() > 0 {
		return fmt.Errorf("unexpected arguments: %v", fs.Args())
	}
	c.profiles = strings.Split(profiles, ",")
	for _, p := range c.profiles {
		if _, ok := profile.Lookup(p); !ok {
			return fmt.Errorf("unknown profile %q; known: %s", p, strings.Join(profile.Names(), ", "))
		}
	}
	switch {
	case c.kafkaLinger < 0 || c.kafkaLinger > time.Second:
		return errors.New("-kafka-linger must be 0..1s")
	case c.repetitions < 1 || c.repetitions > 10:
		return errors.New("-repetitions must be 1..10")
	}
	return nil
}

// experiment is what one invocation of loadgen measures for one profile.
type experiment struct {
	mode   string   // throughput or chaos
	faults []string // chaos only: one run per fault
	args   []string // workload flags forwarded to loadgen
}

func parseRunFlags(args []string) (common, experiment, error) {
	var c common
	var profiles string
	workload := loadgen.Defaults()
	fs := flag.NewFlagSet("run", flag.ContinueOnError)
	c.register(fs, &profiles, profile.Names())
	workload.RegisterFlags(fs)
	if err := fs.Parse(args); err != nil {
		return c, experiment{}, err
	}
	if err := c.validate(fs, profiles); err != nil {
		return c, experiment{}, err
	}
	if err := workload.Validate(); err != nil {
		return c, experiment{}, err
	}
	return c, experiment{mode: "throughput", args: workload.Args()}, nil
}

func parseChaosFlags(args []string) (common, experiment, error) {
	var c common
	var profiles, faults string
	workload := loadgen.ChaosDefaults()
	fs := flag.NewFlagSet("chaos", flag.ContinueOnError)
	c.register(fs, &profiles, profile.Names())
	fs.StringVar(&faults, "faults", "kill", "comma-separated faults per profile: none (baseline), kill (the node, or the leader of a cluster)")
	workload.RegisterFlags(fs)
	if err := fs.Parse(args); err != nil {
		return c, experiment{}, err
	}
	if err := c.validate(fs, profiles); err != nil {
		return c, experiment{}, err
	}
	if err := workload.Validate(); err != nil {
		return c, experiment{}, err
	}
	list := strings.Split(faults, ",")
	for _, f := range list {
		if f != "none" && f != "kill" {
			return c, experiment{}, fmt.Errorf("unknown fault %q; known: none, kill", f)
		}
	}
	return c, experiment{mode: "chaos", faults: list, args: workload.Args()}, nil
}

type runRecord struct {
	ID         string `json:"id"`
	Profile    string `json:"profile"`
	Fault      string `json:"fault,omitempty"`
	Repetition int    `json:"repetition"`
	Error      string `json:"error,omitempty"`
}

type runner struct {
	cfg        common
	exp        experiment
	compose    compose
	stamp      string
	records    []runRecord
	containers *metrics.Containers // nil without -monitor
}

func runCommand(ctx context.Context, args []string) error {
	c, exp, err := parseRunFlags(args)
	return start(ctx, c, exp, err)
}

func chaosCommand(ctx context.Context, args []string) error {
	c, exp, err := parseChaosFlags(args)
	return start(ctx, c, exp, err)
}

func start(ctx context.Context, c common, exp experiment, err error) error {
	if errors.Is(err, flag.ErrHelp) {
		return nil
	}
	if err != nil {
		return err
	}
	// The loadgen container writes its report into the same directory. Compose
	// reads a bare name such as "results" as a named volume, so pass a path.
	resultDir, err := filepath.Abs(c.resultDir)
	if err != nil {
		return err
	}
	r := &runner{
		cfg: c, exp: exp,
		compose: compose{file: c.composeFile, env: []string{"RESULTS_DIR=" + resultDir}},
		stamp:   time.Now().UTC().Format("20060102-150405"),
	}
	defer r.stopServices()
	return r.run(ctx)
}

// brokerServices are every broker service in compose.yaml.
func brokerServices() []string {
	var all []string
	for _, b := range []string{"kafka", "rabbit", "nats"} {
		all = append(all, b, b+"-1", b+"-2", b+"-3")
	}
	return all
}

func (r *runner) run(ctx context.Context) error {
	if err := os.MkdirAll(r.cfg.resultDir, 0o755); err != nil {
		return err
	}
	info, err := dockerOutput(ctx, "info", "--format", "{{json .}}")
	if err != nil {
		return fmt.Errorf("Docker must be running with Linux containers: %w", err)
	}
	if err := r.save(r.stamp+"-docker-info.json", json.RawMessage(info)); err != nil {
		return err
	}
	if err := r.compose.run(ctx, nil, "--profile", "load", "config", "--quiet"); err != nil {
		return err
	}
	if !r.cfg.skipBuild {
		if err := r.compose.run(ctx, nil, "--profile", "load", "build", "loadgen"); err != nil {
			return err
		}
	}
	var needed []string
	for _, name := range r.cfg.profiles {
		p, _ := profile.Lookup(name)
		needed = append(needed, p.Services()...)
	}
	slices.Sort(needed)
	if err := r.compose.run(ctx, nil, append([]string{"pull", "--quiet"}, slices.Compact(needed)...)...); err != nil {
		return err
	}
	if r.cfg.monitor {
		if err := r.startMonitoring(ctx); err != nil {
			return err
		}
	}
	faults := r.exp.faults
	if r.exp.mode == "throughput" {
		faults = []string{""}
	}
	for repetition := 1; repetition <= r.cfg.repetitions; repetition++ {
		// Reverse alternate passes to expose order and host-cache effects.
		order := slices.Clone(r.cfg.profiles)
		if repetition%2 == 0 {
			slices.Reverse(order)
		}
		for _, p := range order {
			for _, fault := range faults {
				if err := r.runProfile(ctx, repetition, p, fault); err != nil {
					return err
				}
			}
		}
	}
	fmt.Println("Results:", r.cfg.resultDir, "- make summary, make report")
	return nil
}

func (r *runner) runProfile(ctx context.Context, repetition int, name, fault string) error {
	p, _ := profile.Lookup(name)
	id := fmt.Sprintf("%s-r%d-%s", r.stamp, repetition, name)
	if fault != "" {
		id += "-" + fault
	}
	fmt.Printf("\n== %s: %s\n", id, p.Description)

	// Every profile starts from empty storage (brokers use anonymous volumes,
	// which rm --volumes deletes), and only its brokers run.
	if err := r.removeBrokers(ctx); err != nil {
		return err
	}
	if err := r.compose.run(ctx, p.Env, append([]string{"up", "-d", "--wait"}, p.Services()...)...); err != nil {
		return err
	}

	log, err := os.Create(r.path(id + "-loadgen.log"))
	if err != nil {
		return err
	}
	defer log.Close()
	container := "broker-bench-loadgen-" + id
	// --use-aliases makes the container reachable as "loadgen" for Prometheus.
	args := []string{"--profile", "load", "run", "--rm", "--no-deps", "-T", "--use-aliases", "--name", container, "loadgen",
		"-mode", r.exp.mode, "-profile", name, "-run-id", id, "-out", "/results/" + id + "-report.json",
		"-kafka-linger", r.cfg.kafkaLinger.String()}
	if fault != "" {
		args = append(args, "-fault", fault)
	}
	cmd := r.compose.command(ctx, p.Env, append(args, r.exp.args...)...)
	cmd.Stdout = io.MultiWriter(os.Stdout, log)
	cmd.Stderr = io.MultiWriter(os.Stderr, log)

	sampleCtx, stopSampling := context.WithCancel(ctx)
	samples := make(chan []resourceSample, 1)
	var publish func([]metrics.ContainerSample)
	if r.containers != nil {
		publish = r.containers.Set
	}
	go func() { samples <- sampleResources(sampleCtx, 2*time.Second, publish) }()
	runErr := cmd.Run()
	stopSampling()
	resources := <-samples

	if ctx.Err() != nil {
		// Cancellation kills only the compose client; remove the container it started.
		_ = exec.Command("docker", "rm", "-f", container).Run()
		return ctx.Err()
	}
	if err := r.save(id+"-resources.json", resources); err != nil {
		return err
	}
	logs, err := r.compose.output(ctx, append([]string{"logs", "--no-color"}, p.Services()...)...)
	if err != nil {
		return err
	}
	if err := os.WriteFile(r.path(id+"-broker.log"), logs, 0o644); err != nil {
		return err
	}
	record := runRecord{ID: id, Profile: name, Fault: fault, Repetition: repetition}
	if runErr != nil {
		// One profile failing (a broker that would not come back, say) should
		// not cost the rest of a long experiment; the report keeps the error.
		record.Error = runErr.Error()
		fmt.Fprintf(os.Stderr, "bench: %s failed (%v); see %s\n", id, runErr, r.path(id+"-loadgen.log"))
	}
	r.records = append(r.records, record)
	return r.save(r.stamp+"-runs.json", r.records)
}

func (r *runner) stopServices() {
	if r.cfg.keepRunning {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	if err := r.removeBrokers(ctx); err != nil {
		fmt.Fprintln(os.Stderr, "bench: cleanup:", err)
	}
	if r.cfg.monitor {
		fmt.Println("Grafana keeps the history of this run:", grafanaURL(), "- make down removes it")
	}
}

func (r *runner) removeBrokers(ctx context.Context) error {
	return r.compose.run(ctx, nil, append([]string{"rm", "--stop", "--force", "--volumes"}, brokerServices()...)...)
}

// containerMetricsAddr must match the "containers" job in monitoring/prometheus.yml.
const containerMetricsAddr = ":9101"

// startMonitoring starts Prometheus and Grafana and publishes container stats
// on the host for Prometheus, which reaches it as host.docker.internal.
func (r *runner) startMonitoring(ctx context.Context) error {
	if err := r.compose.run(ctx, nil, "--profile", "monitor", "up", "-d", "--wait", "prometheus", "grafana"); err != nil {
		return err
	}
	r.containers = metrics.NewContainers()
	if err := metrics.Serve(ctx, containerMetricsAddr, r.containers.Handler()); err != nil {
		return fmt.Errorf("publish container stats: %w", err)
	}
	fmt.Println("Live dashboard:", grafanaURL())
	return nil
}

func grafanaURL() string { return forwardedURL(":3000") }

// forwardedURL is the browser address of a local port: the forwarded one in
// GitHub Codespaces, localhost elsewhere.
func forwardedURL(addr string) string {
	port := addr[strings.LastIndex(addr, ":")+1:]
	if name, domain := os.Getenv("CODESPACE_NAME"), os.Getenv("GITHUB_CODESPACES_PORT_FORWARDING_DOMAIN"); name != "" && domain != "" {
		return fmt.Sprintf("https://%s-%s.%s", name, port, domain)
	}
	return "http://localhost:" + port
}

func (r *runner) path(name string) string { return filepath.Join(r.cfg.resultDir, name) }

func (r *runner) save(name string, value any) error {
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return fmt.Errorf("encode %s: %w", name, err)
	}
	return os.WriteFile(r.path(name), append(data, '\n'), 0o644)
}
