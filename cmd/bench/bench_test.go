package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/Haleralex/broker-bench/internal/loadgen"
	"github.com/Haleralex/broker-bench/internal/metrics"
	"github.com/Haleralex/broker-bench/internal/profile"
)

func TestParseFlags(t *testing.T) {
	c, exp, err := parseRunFlags(nil)
	if err != nil || len(c.profiles) != len(profile.All) || exp.mode != "throughput" {
		t.Fatalf("run defaults: %+v %+v %v", c, exp, err)
	}
	c, exp, err = parseChaosFlags([]string{"-profiles", "kafka_r3,nats", "-faults", "none,kill", "-rate", "2000"})
	if err != nil || len(c.profiles) != 2 || len(exp.faults) != 2 || exp.mode != "chaos" || !strings.Contains(strings.Join(exp.args, " "), "-rate 2000") {
		t.Fatalf("chaos flags: %+v %+v %v", c, exp, err)
	}
	for _, args := range [][]string{
		{"-profiles", "kafka,postgres"},
		{"-rates", "100,,300"},
		{"-batch", "0"},
		{"-repetitions", "0"},
		{"extra"},
	} {
		if _, _, err := parseRunFlags(args); err == nil {
			t.Errorf("run accepted %v", args)
		}
	}
	for _, args := range [][]string{
		{"-faults", "reboot"},
		{"-fault-at", "2m"}, // after the 60s of traffic
		{"-rate", "0"},
	} {
		if _, _, err := parseChaosFlags(args); err == nil {
			t.Errorf("chaos accepted %v", args)
		}
	}
}

func TestEveryProfileHasItsServices(t *testing.T) {
	compose, err := os.ReadFile("../../compose.yaml")
	if err != nil {
		t.Fatal(err)
	}
	for _, p := range profile.All {
		for _, s := range p.Services() {
			if !strings.Contains(string(compose), "\n  "+s+":\n") {
				t.Fatalf("profile %s needs service %s in compose.yaml", p.Name, s)
			}
		}
	}
	for _, s := range brokerServices() {
		if !strings.Contains(string(compose), "\n  "+s+":\n") {
			t.Fatalf("brokerServices lists %s, which compose.yaml lacks", s)
		}
	}
}

func writeJSON(t *testing.T, path string, v any) {
	t.Helper()
	data, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatal(err)
	}
}

func writeThroughput(t *testing.T, dir, runID, name string, fill, p99 float64, drained bool) {
	t.Helper()
	res := loadgen.Result{
		Fill: loadgen.Throughput{PerSecond: fill}, Drain: loadgen.Throughput{PerSecond: fill * 2},
		Steps: []loadgen.Step{
			{TargetRate: 1000, ConsumedRate: 1000, EndToEnd: loadgen.Latency{P50: 1, P99: p99}, Drained: true},
			{TargetRate: 5000, ConsumedRate: 4000, EndToEnd: loadgen.Latency{P99: p99 * 10}, BacklogAtEnd: 9000, Drained: drained},
		},
		Produced: 1000,
	}
	writeJSON(t, filepath.Join(dir, runID+"-report.json"), map[string]any{"kind": "throughput", "run_id": runID, "profile": name, "result": res})
}

func writeChaos(t *testing.T, dir, runID, name, fault string, lost, unconfirmedDelivered int64) {
	t.Helper()
	res := loadgen.ChaosResult{
		Integrity:     loadgen.Integrity{Reserved: 1000, Confirmed: 900, Consumed: 950 - lost, Lost: lost, DeliveredUnconfirmed: unconfirmedDelivered, Unconfirmed: 100 - unconfirmedDelivered, Duplicates: 3},
		Fault:         loadgen.FaultReport{Kind: fault, Target: "kafka-2", InjectedAt: 20, RecoveredAt: 35},
		ProducerStall: 4, ConsumerStall: 6, SettleSeconds: 15,
		Timeline: []loadgen.Second{{T: 0, Confirmed: 100, Consumed: 100}, {T: 1, SendErrors: 100}},
	}
	cfg := map[string]any{"chaos": loadgen.ChaosDefaults(), "fault": fault}
	writeJSON(t, filepath.Join(dir, runID+"-report.json"), map[string]any{"kind": "chaos", "run_id": runID, "profile": name, "config": cfg, "chaos": res})
}

func TestSummarizeTakesMediansPerProfileAndRate(t *testing.T) {
	dir := t.TempDir()
	for i, fill := range []float64{100, 500, 300} {
		writeThroughput(t, dir, fmt.Sprintf("20261004-120000-r%d-kafka", i+1), "kafka", fill, fill/100, i != 1)
	}
	writeThroughput(t, dir, "20261004-120000-r1-nats", "nats", 9000, 0.5, true)
	// An older experiment, e.g. a smoke run, is left out by default.
	writeThroughput(t, dir, "20261003-090000-r1-nats", "nats", 1, 1, true)
	if err := os.WriteFile(filepath.Join(dir, "20261004-120000-runs.json"), []byte(`[]`), 0o644); err != nil {
		t.Fatal(err)
	}
	throughput, chaos, err := selected(dir, "", false)
	if err != nil || len(throughput) != 4 || len(chaos) != 0 {
		t.Fatalf("throughput=%d chaos=%d err=%v", len(throughput), len(chaos), err)
	}
	profiles, steps := summarize(throughput)
	if len(profiles) != 2 || profiles[0].Profile != "kafka" || profiles[1].Profile != "nats" {
		t.Fatalf("profiles: %+v", profiles)
	}
	if k := profiles[0]; k.Runs != 3 || k.FillRate != 300 || k.DrainRate != 600 {
		t.Fatalf("kafka summary: %+v", k)
	}
	if len(steps) != 4 || steps[0].Rate != 1000 || steps[0].P99 != 3 {
		t.Fatalf("steps: %+v", steps)
	}
	// 4000 of 5000 consumed: saturated in every run.
	if s := steps[2]; s.Rate != 5000 || s.Saturated != 3 || s.Backlog != 9000 {
		t.Fatalf("saturated step: %+v", s)
	}
	var out bytes.Buffer
	if err := printTables(&out, profiles, steps); err != nil || !strings.Contains(out.String(), "nats") {
		t.Fatalf("table: %q %v", out.String(), err)
	}
	if err := writeCSV(filepath.Join(dir, "c.csv"), throughput); err != nil {
		t.Fatal(err)
	}
	csv, _ := os.ReadFile(filepath.Join(dir, "c.csv"))
	if lines := strings.Count(string(csv), "\n"); lines != 1+4*4 {
		t.Fatalf("csv has %d lines", lines)
	}
}

func TestSummarizeChaosSumsViolations(t *testing.T) {
	dir := t.TempDir()
	writeChaos(t, dir, "20261005-100000-r1-kafka_r3_acks1-kill", "kafka_r3_acks1", "kill", 0, 10)
	writeChaos(t, dir, "20261005-100000-r2-kafka_r3_acks1-kill", "kafka_r3_acks1", "kill", 42, 0)
	writeChaos(t, dir, "20261005-100000-r1-kafka_r3_acks1-none", "kafka_r3_acks1", "none", 0, 0)
	writeChaos(t, dir, "20261005-100000-r1-nats_r3-kill", "nats_r3", "kill", 0, 5)
	writeThroughput(t, dir, "20261004-120000-r1-nats", "nats", 9000, 0.5, true)

	throughput, chaos, err := selected(dir, "", false)
	if err != nil || len(throughput) != 1 || len(chaos) != 4 {
		t.Fatalf("throughput=%d chaos=%d err=%v", len(throughput), len(chaos), err)
	}
	rows := summarizeChaos(chaos)
	if len(rows) != 3 {
		t.Fatalf("rows: %+v", rows)
	}
	acks1 := rows[0]
	if acks1.Profile != "kafka_r3_acks1" || acks1.Fault != "kill" || acks1.Runs != 2 || acks1.Lost != 42 || acks1.RunsWithLoss != 1 || acks1.DeliveredUnconfirmed != 10 || acks1.Target != "kafka-2" {
		t.Fatalf("acks1 kill: %+v", acks1)
	}
	if rows[1].Fault != "none" || rows[2].Profile != "nats_r3" || rows[2].Lost != 0 {
		t.Fatalf("order or values: %+v", rows)
	}
	var out bytes.Buffer
	if err := printChaos(&out, rows); err != nil || !strings.Contains(out.String(), "42") {
		t.Fatalf("table: %q %v", out.String(), err)
	}

	data, err := buildReport(dir, throughput, chaos)
	if err != nil {
		t.Fatal(err)
	}
	if len(data.ChaosRuns) != 4 || data.ChaosRuns[0].Duration != 60 || data.ChaosExperiment != "20261005-100000" || data.Experiment != "20261004-120000" {
		t.Fatalf("report data: runs=%d %+v", len(data.ChaosRuns), data.ChaosRuns[0])
	}
}

func TestLoadReportsRejectsForeignIDs(t *testing.T) {
	dir := t.TempDir()
	writeThroughput(t, dir, "20261004-120000-r1-nats", "kafka", 1, 1, true)
	if _, err := loadReports(dir, ""); err == nil {
		t.Fatal("accepted a report whose run id names another profile")
	}
}

func TestParseDockerStat(t *testing.T) {
	for line, want := range map[string]metrics.ContainerSample{
		`{"Name":"broker-bench-kafka-2-1","CPUPerc":"187.25%","MemUsage":"1.5GiB / 2GiB"}`:                      {Container: "kafka-2", CPUPercent: 187.25, MemoryBytes: 1.5 * (1 << 30)},
		`{"Name":"broker-bench-nats-1","CPUPerc":"20.5%","MemUsage":"10MB / 1GB"}`:                              {Container: "nats", CPUPercent: 20.5, MemoryBytes: 10e6},
		`{"Name":"broker-bench-loadgen-20261007-170450-r1-kafka","CPUPerc":"3.00%","MemUsage":"512KiB / 1GiB"}`: {Container: "loadgen", CPUPercent: 3, MemoryBytes: 512 << 10},
	} {
		got, ok := parseDockerStat([]byte(line))
		if !ok || got != want {
			t.Errorf("%s: got %+v %v, want %+v", line, got, ok, want)
		}
	}
	if _, ok := parseDockerStat([]byte(`{"Name":"x","CPUPerc":"--"}`)); ok {
		t.Error("accepted a sample without CPU")
	}
}

func TestReportRenders(t *testing.T) {
	dir := t.TempDir()
	writeThroughput(t, dir, "20261004-120000-r1-rabbit_quorum", "rabbit_quorum", 300, 3, true)
	writeChaos(t, dir, "20261005-100000-r1-nats-kill", "nats", "kill", 7, 0)
	resources := `[
	  {"at": "2026-10-04T12:00:00Z", "stats": {"Name":"broker-bench-rabbit-1","CPUPerc":"150%","MemUsage":"1GiB / 2GiB"}},
	  {"at": "2026-10-04T12:00:02Z", "stats": {"Name":"broker-bench-rabbit-1","CPUPerc":"190%","MemUsage":"1GiB / 2GiB"}},
	  {"at": "2026-10-04T12:00:02Z", "stats": {"Name":"broker-bench-grafana-1","CPUPerc":"1%","MemUsage":"1GiB / 2GiB"}}
	]`
	if err := os.WriteFile(filepath.Join(dir, "20261004-120000-r1-rabbit_quorum-resources.json"), []byte(resources), 0o644); err != nil {
		t.Fatal(err)
	}
	throughput, chaos, err := selected(dir, "", false)
	if err != nil {
		t.Fatal(err)
	}
	data, err := buildReport(dir, throughput, chaos)
	if err != nil {
		t.Fatal(err)
	}
	if cpu := data.CPU["rabbit_quorum"].Series; len(cpu) != 1 || cpu["rabbit"][1] != [2]float64{2, 190} {
		t.Fatalf("cpu series: %+v", data.CPU)
	}
	path := filepath.Join(dir, "report.html")
	if err := writeReport(path, data); err != nil {
		t.Fatal(err)
	}
	html, _ := os.ReadFile(path)
	for _, want := range []string{`"FillRate":300`, `"rabbit":[[0,150],[2,190]]`, `"Lost":7`, `"injected_at_s":20`} {
		if !strings.Contains(string(html), want) {
			t.Errorf("report lacks %s", want)
		}
	}
}
