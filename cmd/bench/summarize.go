package main

import (
	"cmp"
	"encoding/csv"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"text/tabwriter"

	"github.com/Haleralex/broker-bench/internal/loadgen"
	"github.com/Haleralex/broker-bench/internal/profile"
)

// runIDPattern splits IDs created by runProfile: <stamp>-r<repetition>-<profile>[-<fault>].
var runIDPattern = regexp.MustCompile(`^(\d{8}-\d{6})-r(\d+)-(.+)$`)

// report is the subset of cmd/loadgen's Report used here.
type report struct {
	Kind    string               `json:"kind"`
	RunID   string               `json:"run_id"`
	Profile string               `json:"profile"`
	Config  json.RawMessage      `json:"config"`
	Result  loadgen.Result       `json:"result"`
	Chaos   *loadgen.ChaosResult `json:"chaos"`
	Error   string               `json:"error"`

	stamp      string // experiment the run belongs to
	repetition int
	fault      string // chaos runs only
}

func loadReports(dir, prefix string) ([]report, error) {
	files, err := filepath.Glob(filepath.Join(dir, prefix+"*-report.json"))
	if err != nil {
		return nil, err
	}
	var reports []report
	for _, file := range files {
		data, err := os.ReadFile(file)
		if err != nil {
			return nil, err
		}
		var rep report
		if err := json.Unmarshal(data, &rep); err != nil {
			return nil, fmt.Errorf("%s: %w", file, err)
		}
		if rep.Kind == "chaos" {
			var cfg struct {
				Fault string `json:"fault"`
			}
			_ = json.Unmarshal(rep.Config, &cfg)
			rep.fault = cfg.Fault
		}
		match := runIDPattern.FindStringSubmatch(rep.RunID)
		if match == nil || strings.TrimSuffix(match[3], "-"+rep.fault) != rep.Profile {
			return nil, fmt.Errorf("%s: unexpected run_id %q for profile %q", file, rep.RunID, rep.Profile)
		}
		rep.stamp = match[1]
		rep.repetition, _ = strconv.Atoi(match[2])
		reports = append(reports, rep)
	}
	return reports, nil
}

// split separates throughput and chaos reports.
func split(reports []report) (throughput, chaos []report) {
	for _, r := range reports {
		if r.Kind == "chaos" {
			chaos = append(chaos, r)
		} else {
			throughput = append(throughput, r)
		}
	}
	return throughput, chaos
}

// latestExperiment keeps the runs of the newest experiment; smoke runs and
// aborted experiments would otherwise mix into the medians.
func latestExperiment(reports []report) []report {
	latest := ""
	for _, r := range reports {
		latest = max(latest, r.stamp)
	}
	return slices.DeleteFunc(reports, func(r report) bool { return r.stamp != latest })
}

// saturated reports a step the queue did not keep up with: workers consumed
// noticeably less than scheduled, or the backlog outlived the drain timeout.
func saturated(s loadgen.Step) bool {
	return s.ConsumedRate < 0.95*float64(s.TargetRate) || !s.Drained
}

// profileSummary holds medians of the closed-loop phases over repetitions.
type profileSummary struct {
	Profile             string
	Runs, Failed        int
	FillRate, DrainRate float64
	SendP99, ReceiveP99 float64
	Duplicates          int64
}

// stepSummary holds medians of one steady rate over repetitions.
type stepSummary struct {
	Profile                  string
	Rate, Runs, Saturated    int
	Consumed, P50, P99, P999 float64
	Backlog                  float64
}

func profileOrder(name string) int { return slices.Index(profile.Names(), name) }

func summarize(reports []report) ([]profileSummary, []stepSummary) {
	byProfile := map[string][]report{}
	for _, rep := range reports {
		byProfile[rep.Profile] = append(byProfile[rep.Profile], rep)
	}
	var profiles []profileSummary
	var steps []stepSummary
	for name, group := range byProfile {
		ok := slices.DeleteFunc(slices.Clone(group), func(r report) bool { return r.Error != "" })
		s := profileSummary{Profile: name, Runs: len(ok), Failed: len(group) - len(ok)}
		s.FillRate = median(ok, func(r report) float64 { return r.Result.Fill.PerSecond })
		s.DrainRate = median(ok, func(r report) float64 { return r.Result.Drain.PerSecond })
		s.SendP99 = median(ok, func(r report) float64 { return r.Result.Fill.CallLatency.P99 })
		s.ReceiveP99 = median(ok, func(r report) float64 { return r.Result.Drain.CallLatency.P99 })
		for _, r := range group {
			s.Duplicates += r.Result.Duplicates
		}
		profiles = append(profiles, s)

		rates := map[int][]loadgen.Step{}
		for _, r := range ok {
			for _, step := range r.Result.Steps {
				rates[step.TargetRate] = append(rates[step.TargetRate], step)
			}
		}
		for rate, group := range rates {
			ss := stepSummary{Profile: name, Rate: rate, Runs: len(group)}
			ss.Consumed = median(group, func(s loadgen.Step) float64 { return s.ConsumedRate })
			ss.P50 = median(group, func(s loadgen.Step) float64 { return s.EndToEnd.P50 })
			ss.P99 = median(group, func(s loadgen.Step) float64 { return s.EndToEnd.P99 })
			ss.P999 = median(group, func(s loadgen.Step) float64 { return s.EndToEnd.P999 })
			ss.Backlog = median(group, func(s loadgen.Step) float64 { return float64(s.BacklogAtEnd) })
			for _, s := range group {
				if saturated(s) {
					ss.Saturated++
				}
			}
			steps = append(steps, ss)
		}
	}
	slices.SortFunc(profiles, func(a, b profileSummary) int { return cmp.Compare(profileOrder(a.Profile), profileOrder(b.Profile)) })
	slices.SortFunc(steps, func(a, b stepSummary) int {
		return cmp.Or(cmp.Compare(a.Rate, b.Rate), cmp.Compare(profileOrder(a.Profile), profileOrder(b.Profile)))
	})
	return profiles, steps
}

// chaosSummary is one profile and fault over repetitions. Violations are
// summed, not averaged: a single lost message in any run matters.
type chaosSummary struct {
	Profile, Fault       string
	Runs, Failed         int
	Confirmed            int64
	Lost                 int64 // confirmed, never received
	RunsWithLoss         int
	DeliveredUnconfirmed int64
	Unconfirmed          int64
	Duplicates           int64
	ProducerStall        float64 // medians
	ConsumerStall        float64
	Settle               float64
	Target               string // node hit in the first run
}

func summarizeChaos(reports []report) []chaosSummary {
	type key struct{ profile, fault string }
	groups := map[key][]report{}
	for _, r := range reports {
		k := key{r.Profile, r.fault}
		groups[k] = append(groups[k], r)
	}
	var out []chaosSummary
	for k, group := range groups {
		s := chaosSummary{Profile: k.profile, Fault: k.fault}
		var ok []loadgen.ChaosResult
		for _, r := range group {
			if r.Chaos == nil || (r.Error != "" && r.Chaos.Integrity.Reserved == 0) {
				s.Failed++
				continue
			}
			ok = append(ok, *r.Chaos)
			in := r.Chaos.Integrity
			s.Confirmed += in.Confirmed
			s.Lost += in.Lost
			s.DeliveredUnconfirmed += in.DeliveredUnconfirmed
			s.Unconfirmed += in.Unconfirmed
			s.Duplicates += in.Duplicates
			if in.Lost > 0 {
				s.RunsWithLoss++
			}
			if s.Target == "" {
				s.Target = r.Chaos.Fault.Target
			}
		}
		s.Runs = len(ok)
		s.ProducerStall = median(ok, func(c loadgen.ChaosResult) float64 { return c.ProducerStall })
		s.ConsumerStall = median(ok, func(c loadgen.ChaosResult) float64 { return c.ConsumerStall })
		s.Settle = median(ok, func(c loadgen.ChaosResult) float64 { return c.SettleSeconds })
		out = append(out, s)
	}
	slices.SortFunc(out, func(a, b chaosSummary) int {
		return cmp.Or(cmp.Compare(profileOrder(a.Profile), profileOrder(b.Profile)), cmp.Compare(a.Fault, b.Fault))
	})
	return out
}

func median[T any](items []T, value func(T) float64) float64 {
	if len(items) == 0 {
		return 0
	}
	values := make([]float64, len(items))
	for i, item := range items {
		values[i] = value(item)
	}
	slices.Sort(values)
	middle := len(values) / 2
	if len(values)%2 == 1 {
		return values[middle]
	}
	return (values[middle-1] + values[middle]) / 2
}

// selected applies -prefix and -all; by default each kind keeps its own
// latest experiment, so a chaos run does not hide the last throughput run.
func selected(dir, prefix string, all bool) (throughput, chaos []report, err error) {
	reports, err := loadReports(dir, prefix)
	if err != nil {
		return nil, nil, err
	}
	if len(reports) == 0 {
		return nil, nil, fmt.Errorf("no reports in %s matching %q", dir, prefix+"*-report.json")
	}
	throughput, chaos = split(reports)
	if prefix == "" && !all {
		throughput, chaos = latestExperiment(throughput), latestExperiment(chaos)
	}
	return throughput, chaos, nil
}

func summarizeCommand(args []string) error {
	fs := flag.NewFlagSet("summarize", flag.ContinueOnError)
	dir := fs.String("results", "results", "directory with loadgen reports")
	prefix := fs.String("prefix", "", "only runs whose ID starts with this, e.g. a run timestamp; default: the latest experiment of each kind")
	all := fs.Bool("all", false, "every report in the directory, from all experiments")
	out := fs.String("out", "comparison.csv", "CSV file name inside the results directory (throughput runs)")
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
	if len(throughput) > 0 {
		fmt.Printf("Throughput, experiment %s\n", throughput[0].stamp)
		csvPath := filepath.Join(*dir, *out)
		if err := writeCSV(csvPath, throughput); err != nil {
			return err
		}
		profiles, steps := summarize(throughput)
		if err := printTables(os.Stdout, profiles, steps); err != nil {
			return err
		}
		fmt.Println("Medians over repetitions; latencies in ms. Per-run rows:", csvPath)
	}
	if len(chaos) > 0 {
		fmt.Printf("\nIntegrity under faults, experiment %s\n", chaos[0].stamp)
		if err := printChaos(os.Stdout, summarizeChaos(chaos)); err != nil {
			return err
		}
		fmt.Println("lost: confirmed by the broker, never received. delivered unconfirmed: Send failed, message arrived anyway.")
	}
	return nil
}

func printTables(w io.Writer, profiles []profileSummary, steps []stepSummary) error {
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', tabwriter.AlignRight)
	fmt.Fprintln(tw, "profile\truns\tfailed\tfill msg/s\tsend p99\tdrain msg/s\treceive p99\tduplicates\t")
	for _, s := range profiles {
		fmt.Fprintf(tw, "%s\t%d\t%d\t%.0f\t%.2f\t%.0f\t%.2f\t%d\t\n", s.Profile, s.Runs, s.Failed, s.FillRate, s.SendP99, s.DrainRate, s.ReceiveP99, s.Duplicates)
	}
	fmt.Fprintln(tw, "\t\t\t\t\t\t\t\t")
	fmt.Fprintln(tw, "rate\tprofile\truns\tconsumed/s\te2e p50\te2e p99\te2e p99.9\tbacklog\tsaturated\t")
	for _, s := range steps {
		fmt.Fprintf(tw, "%d\t%s\t%d\t%.0f\t%.2f\t%.2f\t%.2f\t%.0f\t%d\t\n", s.Rate, s.Profile, s.Runs, s.Consumed, s.P50, s.P99, s.P999, s.Backlog, s.Saturated)
	}
	return tw.Flush()
}

func printChaos(w io.Writer, rows []chaosSummary) error {
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', tabwriter.AlignRight)
	fmt.Fprintln(tw, "profile\tfault\ttarget\truns\tfailed\tconfirmed\tLOST\truns w/ loss\tdelivered unconfirmed\tunconfirmed\tduplicates\tproducer stall s\tconsumer stall s\t")
	for _, s := range rows {
		fmt.Fprintf(tw, "%s\t%s\t%s\t%d\t%d\t%d\t%d\t%d\t%d\t%d\t%d\t%.0f\t%.0f\t\n", s.Profile, s.Fault, s.Target, s.Runs, s.Failed, s.Confirmed, s.Lost, s.RunsWithLoss, s.DeliveredUnconfirmed, s.Unconfirmed, s.Duplicates, s.ProducerStall, s.ConsumerStall)
	}
	return tw.Flush()
}

func writeCSV(path string, reports []report) error {
	file, err := os.Create(path)
	if err != nil {
		return err
	}
	defer file.Close()
	w := csv.NewWriter(file)
	f := func(v float64) string { return strconv.FormatFloat(v, 'f', 3, 64) }
	_ = w.Write([]string{"run", "profile", "repetition", "phase", "target_rate", "msgs_per_s", "p50_ms", "p99_ms", "p999_ms", "max_ms", "backlog_at_end", "completed", "error"})
	for _, rep := range reports {
		base := []string{rep.RunID, rep.Profile, strconv.Itoa(rep.repetition)}
		phase := func(name string, rate int, perSecond float64, l loadgen.Latency, backlog int64, completed bool) {
			_ = w.Write(append(slices.Clone(base), name, strconv.Itoa(rate), f(perSecond), f(l.P50), f(l.P99), f(l.P999), f(l.Max), strconv.FormatInt(backlog, 10), strconv.FormatBool(completed), rep.Error))
		}
		phase("fill", 0, rep.Result.Fill.PerSecond, rep.Result.Fill.CallLatency, 0, rep.Result.Fill.Completed)
		phase("drain", 0, rep.Result.Drain.PerSecond, rep.Result.Drain.CallLatency, 0, rep.Result.Drain.Completed)
		for _, s := range rep.Result.Steps {
			phase("steady", s.TargetRate, s.ConsumedRate, s.EndToEnd, s.BacklogAtEnd, s.Drained)
		}
	}
	w.Flush()
	if err := w.Error(); err != nil {
		return err
	}
	return file.Close()
}
