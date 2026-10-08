package main

import (
	"cmp"
	"context"
	_ "embed"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"html/template"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"time"

	"github.com/Haleralex/broker-bench/internal/loadgen"
	"github.com/Haleralex/broker-bench/internal/profile"
)

//go:embed report.html.tmpl
var reportTemplate string

// reportData is everything report.html.tmpl draws; the template embeds it as JSON.
type reportData struct {
	Experiment      string             `json:"experiment"`
	ChaosExperiment string             `json:"chaos_experiment"`
	Profiles        []profileSummary   `json:"profiles"`
	Steps           []stepSummary      `json:"steps"`
	CPU             map[string]cpuView `json:"cpu"` // per profile: its first throughput run
	Chaos           []chaosSummary     `json:"chaos"`
	ChaosRuns       []chaosRunView     `json:"chaos_runs"`
	Names           []string           `json:"names"`
	Brokers         map[string]string  `json:"brokers"` // profile -> kafka, rabbit, nats
	Generated       string             `json:"generated"`
}

// chaosRunView is one integrity run for the timeline chart.
type chaosRunView struct {
	ID        string              `json:"id"`
	Profile   string              `json:"profile"`
	Fault     loadgen.FaultReport `json:"fault"`
	Integrity loadgen.Integrity   `json:"integrity"`
	Timeline  []loadgen.Second    `json:"timeline"`
	Duration  float64             `json:"duration_s"`
}

// cpuView is docker stats CPU per container over seconds since the run started.
type cpuView struct {
	Run    string                  `json:"run"`
	Series map[string][][2]float64 `json:"series"`
}

func reportCommand(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("report", flag.ContinueOnError)
	dir := fs.String("results", "results", "directory with loadgen reports")
	prefix := fs.String("prefix", "", "only runs whose ID starts with this; default: the latest experiment")
	all := fs.Bool("all", false, "every report in the directory, from all experiments")
	out := fs.String("out", "report.html", "HTML file name inside the results directory")
	serve := fs.String("serve", "", "also serve the results directory on this address, e.g. :8090")
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil
		}
		return err
	}
	throughput, chaos, err := selected(*dir, *prefix, *all)
	if err != nil {
		return err
	}
	data, err := buildReport(*dir, throughput, chaos)
	if err != nil {
		return err
	}
	if *all {
		data.Experiment, data.ChaosExperiment = "all experiments", "all experiments"
	}
	path := filepath.Join(*dir, *out)
	if err := writeReport(path, data); err != nil {
		return err
	}
	fmt.Println("Report:", path)
	if *serve == "" {
		return nil
	}
	return serveResults(ctx, *serve, *dir, *out)
}

func buildReport(dir string, throughput, chaos []report) (reportData, error) {
	profiles, steps := summarize(throughput)
	data := reportData{
		Profiles: profiles, Steps: steps, CPU: map[string]cpuView{},
		Chaos: summarizeChaos(chaos), Names: profile.Names(), Brokers: map[string]string{},
		Generated: time.Now().Format("2006-01-02 15:04"),
	}
	for _, p := range profile.All {
		data.Brokers[p.Name] = p.Broker
	}
	if len(throughput) > 0 {
		data.Experiment = throughput[0].stamp
	}
	if len(chaos) > 0 {
		data.ChaosExperiment = chaos[0].stamp
	}
	// One CPU timeline per profile is enough to see who is busy; repetitions look alike.
	sorted := slices.Clone(throughput)
	slices.SortFunc(sorted, func(a, b report) int { return a.repetition - b.repetition })
	for _, rep := range sorted {
		if _, done := data.CPU[rep.Profile]; done {
			continue
		}
		view, err := loadCPU(filepath.Join(dir, rep.RunID+"-resources.json"))
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			return data, err
		}
		view.Run = rep.RunID
		data.CPU[rep.Profile] = view
	}
	for _, rep := range chaos {
		if rep.Chaos == nil {
			continue
		}
		var cfg struct {
			Chaos loadgen.ChaosConfig `json:"chaos"`
		}
		_ = json.Unmarshal(rep.Config, &cfg)
		data.ChaosRuns = append(data.ChaosRuns, chaosRunView{
			ID: rep.RunID, Profile: rep.Profile, Fault: rep.Chaos.Fault, Integrity: rep.Chaos.Integrity,
			Timeline: rep.Chaos.Timeline, Duration: cfg.Chaos.Duration.Seconds(),
		})
	}
	slices.SortFunc(data.ChaosRuns, func(a, b chaosRunView) int {
		return cmp.Or(cmp.Compare(profileOrder(a.Profile), profileOrder(b.Profile)), cmp.Compare(a.ID, b.ID))
	})
	return data, nil
}

func loadCPU(path string) (cpuView, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return cpuView{}, err
	}
	var samples []resourceSample
	if err := json.Unmarshal(raw, &samples); err != nil {
		return cpuView{}, fmt.Errorf("%s: %w", path, err)
	}
	view := cpuView{Series: map[string][][2]float64{}}
	if len(samples) == 0 {
		return view, nil
	}
	start := samples[0].At
	for _, s := range samples {
		stat, ok := parseDockerStat(s.Stats)
		if !ok || stat.Container == "prometheus" || stat.Container == "grafana" {
			continue
		}
		view.Series[stat.Container] = append(view.Series[stat.Container], [2]float64{s.At.Sub(start).Seconds(), stat.CPUPercent})
	}
	return view, nil
}

func writeReport(path string, data reportData) error {
	tmpl, err := template.New("report").Parse(reportTemplate)
	if err != nil {
		return err
	}
	file, err := os.Create(path)
	if err != nil {
		return err
	}
	defer file.Close()
	if err := tmpl.Execute(file, data); err != nil {
		return err
	}
	return file.Close()
}

// serveResults serves the results directory, so the report opens through the
// port forwarding of GitHub Codespaces; Ctrl+C stops it.
func serveResults(ctx context.Context, addr, dir, page string) error {
	server := &http.Server{Addr: addr, Handler: http.FileServer(http.Dir(dir)), ReadHeaderTimeout: 5 * time.Second}
	go func() {
		<-ctx.Done()
		shutdown, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = server.Shutdown(shutdown)
	}()
	fmt.Println("Serving", forwardedURL(addr)+"/"+page, "- Ctrl+C to stop")
	if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}
