// Package metrics exposes a running benchmark to Prometheus: loadgen events,
// broker counters and container resource usage. Grafana reads them live; the
// JSON reports in results/ remain the source of truth for comparisons.
package metrics

import (
	"context"
	"net"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Phases are the values loadgen reports; one of them is 1 at a time.
var Phases = []string{"prime", "fill", "drain", "steady", "catch_up", "done"}

// Loadgen implements loadgen.Observer and publishes broker statistics.
type Loadgen struct {
	registry   *prometheus.Registry
	phase      *prometheus.GaugeVec
	targetRate prometheus.Gauge
	produced   prometheus.Counter
	consumed   prometheus.Counter
	duplicates prometheus.Counter
	sendFailed prometheus.Counter
	fault      *prometheus.GaugeVec
	endToEnd   prometheus.Histogram
	send       prometheus.Histogram
	receive    prometheus.Histogram
	broker     *prometheus.GaugeVec
}

// latencyBuckets span 0.5 ms to a minute: saturated steps reach tens of seconds.
var latencyBuckets = prometheus.ExponentialBucketsRange(0.0005, 60, 24)

func NewLoadgen(profile, runID string) *Loadgen {
	m := &Loadgen{registry: prometheus.NewRegistry()}
	// Every series carries the profile, so runs stay apart on one time axis.
	reg := prometheus.WrapRegistererWith(prometheus.Labels{"profile": profile}, m.registry)
	f := func(c prometheus.Collector) { reg.MustRegister(c) }

	info := prometheus.NewGauge(prometheus.GaugeOpts{Name: "queuebench_run_info", Help: "1 while loadgen runs this profile", ConstLabels: prometheus.Labels{"run_id": runID}})
	info.Set(1)
	f(info)
	m.phase = prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "queuebench_phase", Help: "1 for the current phase"}, []string{"phase"})
	m.targetRate = prometheus.NewGauge(prometheus.GaugeOpts{Name: "queuebench_target_rate", Help: "Scheduled messages per second in steady phases"})
	m.produced = prometheus.NewCounter(prometheus.CounterOpts{Name: "queuebench_produced_total", Help: "Messages acknowledged by the broker"})
	m.consumed = prometheus.NewCounter(prometheus.CounterOpts{Name: "queuebench_consumed_total", Help: "Messages handled by workers for the first time"})
	m.duplicates = prometheus.NewCounter(prometheus.CounterOpts{Name: "queuebench_duplicates_total", Help: "Redelivered messages"})
	m.sendFailed = prometheus.NewCounter(prometheus.CounterOpts{Name: "queuebench_send_failed_total", Help: "Messages whose Send failed or timed out"})
	m.fault = prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "queuebench_fault", Help: "1 while the target node is killed"}, []string{"target"})
	m.endToEnd = prometheus.NewHistogram(prometheus.HistogramOpts{Name: "queuebench_e2e_seconds", Help: "Scheduled send time to handling by a worker", Buckets: latencyBuckets})
	m.send = prometheus.NewHistogram(prometheus.HistogramOpts{Name: "queuebench_send_seconds", Help: "One Send of a batch", Buckets: latencyBuckets})
	m.receive = prometheus.NewHistogram(prometheus.HistogramOpts{Name: "queuebench_receive_seconds", Help: "One non-empty Receive of a batch, including its acknowledgement", Buckets: latencyBuckets})
	m.broker = prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "queuebench_broker_stat", Help: "Numeric broker statistics, see backend Stats"}, []string{"stat"})
	for _, c := range []prometheus.Collector{m.phase, m.targetRate, m.produced, m.consumed, m.duplicates, m.sendFailed, m.fault, m.endToEnd, m.send, m.receive, m.broker} {
		f(c)
	}
	return m
}

func (m *Loadgen) Phase(name string, targetRate int) {
	for _, p := range Phases {
		v := 0.0
		if p == name {
			v = 1
		}
		m.phase.WithLabelValues(p).Set(v)
	}
	m.targetRate.Set(float64(targetRate))
}

func (m *Loadgen) Sent(n int, took time.Duration) {
	m.produced.Add(float64(n))
	m.send.Observe(took.Seconds())
}

func (m *Loadgen) Received(_ int, took time.Duration) { m.receive.Observe(took.Seconds()) }
func (m *Loadgen) Consumed(e2e time.Duration)         { m.consumed.Inc(); m.endToEnd.Observe(e2e.Seconds()) }
func (m *Loadgen) Duplicate()                         { m.duplicates.Inc() }
func (m *Loadgen) SendFailed(n int)                   { m.sendFailed.Add(float64(n)) }

func (m *Loadgen) Fault(target string, active bool) {
	v := 0.0
	if active {
		v = 1
	}
	m.fault.WithLabelValues(target).Set(v)
}

// SetBrokerStats publishes the numeric entries of a backend Stats map.
func (m *Loadgen) SetBrokerStats(stats map[string]any) {
	for name, value := range stats {
		switch v := value.(type) {
		case int64:
			m.broker.WithLabelValues(name).Set(float64(v))
		case int:
			m.broker.WithLabelValues(name).Set(float64(v))
		case float64:
			m.broker.WithLabelValues(name).Set(v)
		}
	}
}

func (m *Loadgen) Handler() http.Handler {
	return promhttp.HandlerFor(m.registry, promhttp.HandlerOpts{})
}

// Containers publishes docker stats samples taken by cmd/bench.
type Containers struct {
	registry *prometheus.Registry
	cpu      *prometheus.GaugeVec
	memory   *prometheus.GaugeVec
}

func NewContainers() *Containers {
	c := &Containers{
		registry: prometheus.NewRegistry(),
		cpu:      prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "queuebench_container_cpu_percent", Help: "docker stats CPU; 100 is one core"}, []string{"container"}),
		memory:   prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "queuebench_container_memory_bytes", Help: "docker stats memory usage"}, []string{"container"}),
	}
	c.registry.MustRegister(c.cpu, c.memory)
	return c
}

// Set replaces all samples, so containers that stopped disappear.
func (c *Containers) Set(samples []ContainerSample) {
	c.cpu.Reset()
	c.memory.Reset()
	for _, s := range samples {
		c.cpu.WithLabelValues(s.Container).Set(s.CPUPercent)
		c.memory.WithLabelValues(s.Container).Set(s.MemoryBytes)
	}
}

func (c *Containers) Handler() http.Handler {
	return promhttp.HandlerFor(c.registry, promhttp.HandlerOpts{})
}

type ContainerSample struct {
	Container   string
	CPUPercent  float64
	MemoryBytes float64
}

// Serve exposes handler on addr until ctx ends.
func Serve(ctx context.Context, addr string, handler http.Handler) error {
	mux := http.NewServeMux()
	mux.Handle("/metrics", handler)
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	server := &http.Server{Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	go func() {
		<-ctx.Done()
		shutdown, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = server.Shutdown(shutdown)
	}()
	// Metrics are best effort: the benchmark does not depend on them, so a
	// failing server is not an error of the run.
	go func() { _ = server.Serve(listener) }()
	return nil
}
