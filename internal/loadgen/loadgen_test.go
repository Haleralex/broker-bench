package loadgen

import (
	"context"
	"errors"
	"flag"
	"reflect"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Haleralex/broker-bench/internal/queue"
)

// memory is an in-process queue; redeliver and drop inject broker faults.
type memory struct {
	mu        sync.Mutex
	items     [][]byte
	blocking  bool
	redeliver bool // hand every 10th batch out twice
	drop      bool // lose the first message
	failAt    int  // Receive fails once this many sends happened; 0 never
	sends     int
}

func (m *memory) Reset(context.Context, int) error { m.items = nil; return nil }
func (m *memory) Producer() queue.Producer         { return m }
func (m *memory) Blocking() bool                   { return m.blocking }
func (m *memory) Stats(context.Context) (map[string]any, error) {
	return map[string]any{}, nil
}
func (m *memory) Close() {}
func (m *memory) NewConsumer(context.Context) (queue.Consumer, error) {
	return memConsumer{m}, nil
}

func (m *memory) Send(_ context.Context, payloads [][]byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.drop && m.sends == 0 {
		payloads = payloads[1:]
	}
	m.sends++
	m.items = append(m.items, payloads...)
	return nil
}

type memConsumer struct{ m *memory }

func (c memConsumer) Receive(ctx context.Context, limit int, handle func([]byte)) (int, error) {
	m := c.m
	for {
		m.mu.Lock()
		if m.failAt > 0 && m.sends >= m.failAt {
			m.mu.Unlock()
			return 0, errors.New("broker gone")
		}
		n := min(limit, len(m.items))
		batch := slices.Clone(m.items[:n])
		if !(m.redeliver && m.sends%10 == 0) {
			m.items = m.items[n:]
		}
		m.mu.Unlock()
		if n > 0 || !m.blocking {
			for _, p := range batch {
				handle(p)
			}
			return n, nil
		}
		select {
		case <-ctx.Done():
			return 0, nil
		case <-time.After(time.Millisecond):
		}
	}
}

func (memConsumer) Close() {}

func testConfig() Config {
	cfg := Defaults()
	cfg.Producers, cfg.Consumers, cfg.Batch, cfg.Payload = 2, 3, 20, 64
	cfg.Backlog = 3000
	cfg.Rates = []int{400, 2000}
	cfg.StepWarmup, cfg.Step = 100*time.Millisecond, time.Second
	cfg.DrainTimeout, cfg.Idle = 5*time.Second, time.Millisecond
	return cfg
}

func TestRunAccountsForEveryMessage(t *testing.T) {
	for _, blocking := range []bool{false, true} {
		cfg := testConfig()
		res, err := Run(t.Context(), &memory{blocking: blocking}, cfg, nil, t.Logf)
		if err != nil {
			t.Fatalf("blocking=%v: %v", blocking, err)
		}
		if res.Fill.Messages != int64(cfg.Backlog) || res.Drain.Messages != int64(cfg.Backlog) || !res.Drain.Completed {
			t.Fatalf("fill/drain: %+v %+v", res.Fill, res.Drain)
		}
		if res.Lost != 0 || res.Duplicates != 0 || res.Produced != res.Consumed {
			t.Fatalf("accounting: %+v", res)
		}
		if len(res.Steps) != 2 {
			t.Fatalf("steps: %+v", res.Steps)
		}
		for _, s := range res.Steps {
			// Open loop: the achieved rate follows the schedule, not the broker.
			if s.ProducedRate < 0.9*float64(s.TargetRate) || s.ProducedRate > 1.1*float64(s.TargetRate) || !s.Drained || s.EndToEnd.Count == 0 {
				t.Fatalf("step: %+v", s)
			}
		}
		if res.Steps[0].SendBatch != 2 || res.Steps[1].SendBatch != 10 {
			t.Fatalf("send batches %d, %d", res.Steps[0].SendBatch, res.Steps[1].SendBatch)
		}
	}
}

func TestRunCountsDuplicatesAndLoss(t *testing.T) {
	res, err := Run(t.Context(), &memory{redeliver: true}, testConfig(), nil, t.Logf)
	if err != nil || res.Duplicates == 0 {
		t.Fatalf("redelivery: duplicates=%d err=%v", res.Duplicates, err)
	}
	cfg := testConfig()
	cfg.DrainTimeout = 200 * time.Millisecond
	_, err = Run(t.Context(), &memory{drop: true}, cfg, nil, t.Logf)
	if err == nil || !strings.Contains(err.Error(), "prime") {
		t.Fatalf("loss not reported: %v", err)
	}
}

func TestTracker(t *testing.T) {
	tr := newTracker(100)
	if first, n := tr.reserve(30, 50); first != 0 || n != 30 {
		t.Fatalf("reserve: %d %d", first, n)
	}
	if first, n := tr.reserve(30, 50); first != 30 || n != 20 {
		t.Fatalf("reserve at limit: %d %d", first, n)
	}
	if _, n := tr.reserve(30, 50); n != 0 {
		t.Fatal("reserved past limit")
	}
	if dup, _ := tr.mark(5); dup {
		t.Fatal("first mark is a duplicate")
	}
	if dup, _ := tr.mark(5); !dup || tr.duplicates.Load() != 1 || tr.unique.Load() != 1 {
		t.Fatal("second mark is not a duplicate")
	}
	if _, err := tr.mark(tr.capacity()); err == nil {
		t.Fatal("foreign sequence accepted")
	}
}

func TestSummarizeLatency(t *testing.T) {
	var values []time.Duration
	for i := 100; i >= 1; i-- {
		values = append(values, time.Duration(i)*time.Millisecond)
	}
	l := summarizeLatency(values)
	if l.Count != 100 || l.P50 != 50 || l.P99 != 99 || l.P999 != 100 || l.Max != 100 {
		t.Fatalf("%+v", l)
	}
	if (summarizeLatency(nil) != Latency{}) {
		t.Fatal("empty input")
	}
}

func TestArgsRoundTrip(t *testing.T) {
	want := testConfig()
	got := Defaults()
	fs := flag.NewFlagSet("t", flag.ContinueOnError)
	got.RegisterFlags(fs)
	if err := fs.Parse(want.Args()); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("%+v != %+v", got, want)
	}
	if err := fs.Parse([]string{"-rates", "100,,200"}); err == nil {
		t.Fatal("bad rates accepted")
	}
}

func TestWorkerFailureEndsTheRunWithoutWaitingForDrain(t *testing.T) {
	cfg := testConfig()
	cfg.DrainTimeout = time.Minute
	started := time.Now()
	// Fails during fill: drain must not wait a minute for workers that are gone.
	_, err := Run(t.Context(), &memory{failAt: 50}, cfg, nil, t.Logf)
	if err == nil || !strings.Contains(err.Error(), "broker gone") {
		t.Fatalf("error not reported: %v", err)
	}
	if elapsed := time.Since(started); elapsed > 10*time.Second {
		t.Fatalf("run took %s after the workers failed", elapsed)
	}
}
