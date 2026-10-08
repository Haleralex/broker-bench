// Package loadgen drives one queue backend through the experiment phases:
//
//	prime   a few messages through every worker (Kafka consumers join their group)
//	fill    producers only, as fast as possible, until the backlog is built
//	drain   consumers only, until the backlog is empty
//	steady  producers at fixed rates while consumers keep up (or do not)
//
// RunChaos (chaos.go) is the integrity experiment: steady traffic with a fault.
package loadgen

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Haleralex/broker-bench/internal/queue"
)

type Config struct {
	Producers    int           `json:"producers"`
	Consumers    int           `json:"consumers"`
	Batch        int           `json:"batch"`   // messages per Send and per Receive
	Payload      int           `json:"payload"` // bytes per message
	Backlog      int           `json:"backlog"` // messages produced in fill and consumed in drain
	Rates        []int         `json:"rates"`
	StepWarmup   time.Duration `json:"step_warmup_ns"`
	Step         time.Duration `json:"step_ns"`
	DrainTimeout time.Duration `json:"drain_timeout_ns"`
	Idle         time.Duration `json:"idle_ns"` // pause after an empty poll of a non-blocking backend
}

// Throughput is a closed-loop phase: fill or drain.
type Throughput struct {
	Messages     int64   `json:"messages"`
	Seconds      float64 `json:"seconds"`
	PerSecond    float64 `json:"msgs_per_s"`
	CallLatency  Latency `json:"call_latency"` // one Send (fill) or Receive (drain) of a batch
	Completed    bool    `json:"completed"`
	EmptyPolls   int64   `json:"empty_polls,omitempty"`
	ReceiveCalls int64   `json:"receive_calls,omitempty"`
}

// Step is one open-loop rate.
type Step struct {
	TargetRate   int     `json:"target_rate"`
	SendBatch    int     `json:"send_batch"`
	ProducedRate float64 `json:"produced_per_s"`
	ConsumedRate float64 `json:"consumed_per_s"`
	// EndToEnd runs from the scheduled send time to the moment a worker handles the message.
	EndToEnd Latency `json:"end_to_end"`
	// SendLag is how late producers started a batch against the schedule.
	SendLag      Latency `json:"send_lag"`
	BacklogAtEnd int64   `json:"backlog_at_end"`
	DrainSeconds float64 `json:"drain_seconds"`
	Drained      bool    `json:"drained"`
}

type Result struct {
	Fill       Throughput `json:"fill"`
	Drain      Throughput `json:"drain"`
	Steps      []Step     `json:"steps"`
	Produced   int64      `json:"produced"`
	Consumed   int64      `json:"consumed"`
	Duplicates int64      `json:"duplicates"`
	Lost       int64      `json:"lost"`
	// Undrained is the backlog left by a saturated step that did not empty in
	// time; loss cannot be checked then.
	Undrained int64 `json:"undrained,omitempty"`
}

// steadyBatchesPerSecond keeps send batches small at low rates, so batching does
// not dominate latency there; at high rates the batch reaches Config.Batch.
const steadyBatchesPerSecond = 200

// Observer receives live events for monitoring; Run calls it from many
// goroutines and on the hot path, so implementations must be cheap and safe.
type Observer interface {
	Phase(name string, targetRate int)
	Sent(messages int, took time.Duration)
	Received(messages int, took time.Duration)
	Consumed(endToEnd time.Duration)
	Duplicate()
	SendFailed(messages int)
	// Fault reports a broker node killed (active) and back (not active).
	Fault(target string, active bool)
}

type nopObserver struct{}

func (nopObserver) Phase(string, int)           {}
func (nopObserver) Sent(int, time.Duration)     {}
func (nopObserver) Received(int, time.Duration) {}
func (nopObserver) Consumed(time.Duration)      {}
func (nopObserver) Duplicate()                  {}
func (nopObserver) SendFailed(int)              {}
func (nopObserver) Fault(string, bool)          {}

// Run executes every phase; obs may be nil.
func Run(ctx context.Context, b queue.Backend, cfg Config, obs Observer, log func(string, ...any)) (Result, error) {
	if obs == nil {
		obs = nopObserver{}
	}
	if err := b.Reset(ctx, cfg.Consumers); err != nil {
		return Result{}, err
	}
	r := &runner{b: b, cfg: cfg, obs: obs, log: log, t: newTracker(capacity(cfg)), e2e: newSamples(cfg.Consumers)}
	defer obs.Phase("done", 0)
	for range cfg.Consumers {
		c, err := b.NewConsumer(ctx)
		if err != nil {
			return Result{}, err
		}
		defer c.Close()
		r.consumers = append(r.consumers, c)
	}
	var res Result
	err := r.phases(ctx, &res)
	res.Produced, res.Consumed, res.Duplicates = r.t.acked.Load(), r.t.unique.Load(), r.t.duplicates.Load()
	if res.Undrained == 0 {
		res.Lost = res.Produced - res.Consumed
	}
	if err == nil && res.Lost != 0 {
		err = fmt.Errorf("%d acknowledged messages were never consumed", res.Lost)
	}
	return res, err
}

// capacity bounds the sequence numbers the run can use.
func capacity(cfg Config) uint64 {
	total := float64(cfg.Backlog + primeMessages(cfg))
	for _, rate := range cfg.Rates {
		total += float64(rate) * (cfg.StepWarmup + cfg.Step).Seconds()
	}
	return uint64(total*1.1) + 1<<16
}

func primeMessages(cfg Config) int { return 2 * cfg.Consumers * cfg.Batch }

type runner struct {
	b         queue.Backend
	cfg       Config
	obs       Observer
	log       func(string, ...any)
	t         *tracker
	e2e       *samples
	consumers []queue.Consumer
	// workersFailed is set when a worker error stopped all workers, so nothing
	// waits for a backlog that can no longer drain.
	workersFailed atomic.Bool
	// tolerateErrors keeps workers retrying through broker failures (chaos runs).
	tolerateErrors bool
	timeline       *timeline // per-second counters of chaos runs; nil otherwise
}

func (r *runner) phases(ctx context.Context, res *Result) error {
	r.obs.Phase("prime", 0)
	r.log("prime: %d messages through %d workers", primeMessages(r.cfg), r.cfg.Consumers)
	stop := r.startConsumers(ctx, nil)
	if _, err := r.produceClosed(ctx, uint64(primeMessages(r.cfg))); err != nil {
		return errors.Join(err, stop())
	}
	_, drained := r.waitDrained(ctx, r.cfg.DrainTimeout)
	if err := stop(); err != nil {
		return err
	}
	if !drained {
		return fmt.Errorf("prime: workers did not receive the priming messages within %s", r.cfg.DrainTimeout)
	}

	r.obs.Phase("fill", 0)
	r.log("fill: %d messages, %d producers, batch %d", r.cfg.Backlog, r.cfg.Producers, r.cfg.Batch)
	var err error
	if res.Fill, err = r.produceClosed(ctx, r.t.next.Load()+uint64(r.cfg.Backlog)); err != nil {
		return err
	}
	r.log("fill: %.0f msg/s", res.Fill.PerSecond)

	r.obs.Phase("drain", 0)
	r.log("drain: %d workers", r.cfg.Consumers)
	if res.Drain, err = r.drain(ctx); err != nil {
		return err
	}
	r.log("drain: %.0f msg/s", res.Drain.PerSecond)
	if !res.Drain.Completed {
		return fmt.Errorf("drain did not finish within %s", r.cfg.DrainTimeout)
	}

	stop = r.startConsumers(ctx, nil)
	for _, rate := range r.cfg.Rates {
		step, err := r.steady(ctx, rate)
		if err != nil {
			return errors.Join(err, stop())
		}
		res.Steps = append(res.Steps, step)
		r.log("rate %d: produced %.0f/s consumed %.0f/s e2e p50 %.1f ms p99 %.1f ms backlog %d",
			rate, step.ProducedRate, step.ConsumedRate, step.EndToEnd.P50, step.EndToEnd.P99, step.BacklogAtEnd)
		if !step.Drained {
			r.log("rate %d saturated the queue; higher rates skipped", rate)
			// The backlog of a saturated step is not loss; give it one more chance.
			if _, drained := r.waitDrained(ctx, r.cfg.DrainTimeout); !drained {
				res.Undrained = r.t.backlog()
			}
			break
		}
	}
	return stop()
}

// produceClosed sends batches back to back from every producer until sequence
// number limit is reserved.
func (r *runner) produceClosed(ctx context.Context, limit uint64) (Throughput, error) {
	calls := newSamples(r.cfg.Producers)
	calls.recording.Store(true)
	before := r.t.acked.Load()
	start := time.Now()
	err := r.parallel(ctx, r.cfg.Producers, func(ctx context.Context, p int) error {
		for ctx.Err() == nil {
			first, n := r.t.reserve(r.cfg.Batch, limit)
			if n == 0 {
				return nil
			}
			sent := time.Now()
			if err := r.send(ctx, first, n, sent); err != nil {
				return err
			}
			calls.add(p, time.Since(sent))
		}
		return ctx.Err()
	})
	return throughput(r.t.acked.Load()-before, time.Since(start), calls.take(), err == nil), err
}

func (r *runner) send(ctx context.Context, first uint64, n int, scheduled time.Time) error {
	payloads := make([][]byte, n)
	for i := range payloads {
		payloads[i] = queue.Encode(first+uint64(i), scheduled, r.cfg.Payload)
	}
	started := time.Now()
	if err := r.b.Producer().Send(ctx, payloads); err != nil {
		return fmt.Errorf("send: %w", err)
	}
	r.obs.Sent(n, time.Since(started))
	r.t.confirm(first, n)
	r.timeline.add(func(s *second) { s.Confirmed.Add(int64(n)) })
	return nil
}

func (r *runner) drain(ctx context.Context) (Throughput, error) {
	calls := newSamples(r.cfg.Consumers)
	calls.recording.Store(true)
	before := r.t.unique.Load()
	stop := r.startConsumers(ctx, calls)
	elapsed, drained := r.waitDrained(ctx, r.cfg.DrainTimeout)
	err := stop()
	return throughput(r.t.unique.Load()-before, elapsed, calls.take(), drained), err
}

func throughput(messages int64, elapsed time.Duration, calls []time.Duration, completed bool) Throughput {
	return Throughput{
		Messages: messages, Seconds: elapsed.Seconds(), PerSecond: float64(messages) / elapsed.Seconds(),
		CallLatency: summarizeLatency(calls), Completed: completed,
	}
}

// steady schedules batches at a fixed rate: batch k is due at start + k*interval
// regardless of how long earlier sends took.
func (r *runner) steady(ctx context.Context, rate int) (Step, error) {
	batch := min(r.cfg.Batch, max(1, rate/steadyBatchesPerSecond))
	interval := time.Duration(float64(time.Second) * float64(batch) / float64(rate))
	start := time.Now().Add(10 * time.Millisecond)
	measureFrom := start.Add(r.cfg.StepWarmup)
	end := measureFrom.Add(r.cfg.Step)
	lag := newSamples(r.cfg.Producers)
	r.obs.Phase("steady", rate)

	r.e2e.take()
	var producedAt, consumedAt [2]int64
	measured := make(chan struct{})
	go func() {
		defer close(measured)
		for i, at := range []time.Time{measureFrom, end} {
			select {
			case <-ctx.Done():
				return
			case <-time.After(time.Until(at)):
			}
			producedAt[i], consumedAt[i] = r.t.acked.Load(), r.t.unique.Load()
			r.e2e.recording.Store(i == 0)
			lag.recording.Store(i == 0)
		}
	}()

	err := r.parallel(ctx, r.cfg.Producers, func(ctx context.Context, p int) error {
		for k := p; ; k += r.cfg.Producers {
			due := start.Add(time.Duration(k) * interval)
			if !due.Before(end) {
				return nil
			}
			if wait := time.Until(due); wait > 0 {
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-time.After(wait):
				}
			}
			lag.add(p, time.Since(due))
			first, n := r.t.reserve(batch, r.t.capacity())
			if n < batch {
				return errors.New("sequence capacity exhausted")
			}
			if err := r.send(ctx, first, n, due); err != nil {
				return err
			}
		}
	})
	<-measured
	if err != nil {
		return Step{}, err
	}
	// Producers stopped; workers work off what the step left behind.
	r.obs.Phase("catch_up", rate)
	seconds := r.cfg.Step.Seconds()
	step := Step{
		TargetRate: rate, SendBatch: batch,
		ProducedRate: float64(producedAt[1]-producedAt[0]) / seconds,
		ConsumedRate: float64(consumedAt[1]-consumedAt[0]) / seconds,
		EndToEnd:     summarizeLatency(r.e2e.take()),
		SendLag:      summarizeLatency(lag.take()),
		BacklogAtEnd: max(0, producedAt[1]-consumedAt[1]),
	}
	drainTime, drained := r.waitDrained(ctx, r.cfg.DrainTimeout)
	step.DrainSeconds, step.Drained = drainTime.Seconds(), drained
	return step, ctx.Err()
}

// startConsumers runs one worker goroutine per consumer until the returned
// function is called; it returns the first worker error. calls, if not nil,
// receives the duration of every non-empty Receive.
func (r *runner) startConsumers(ctx context.Context, calls *samples) func() error {
	ctx, cancel := context.WithCancel(ctx)
	var wg sync.WaitGroup
	errs := make([]error, len(r.consumers))
	for i, c := range r.consumers {
		wg.Go(func() {
			handle := func(payload []byte) {
				seq, scheduled, err := queue.Decode(payload)
				if err == nil {
					var duplicate bool
					if duplicate, err = r.t.mark(seq); err == nil && !duplicate {
						e2e := time.Since(scheduled)
						r.e2e.add(i, e2e)
						r.obs.Consumed(e2e)
						r.timeline.add(func(s *second) { s.Consumed.Add(1) })
					} else if duplicate {
						r.obs.Duplicate()
						r.timeline.add(func(s *second) { s.Duplicates.Add(1) })
					}
				}
				if err != nil && errs[i] == nil {
					errs[i] = err
					r.workersFailed.Store(true)
					cancel()
				}
			}
			var lastLogged time.Time
			for ctx.Err() == nil {
				started := time.Now()
				n, err := c.Receive(ctx, r.cfg.Batch, handle)
				if err != nil && r.tolerateErrors {
					// A broker is down on purpose: keep trying, as a real worker would.
					r.timeline.add(func(s *second) { s.ReceiveErrors.Add(1) })
					if time.Since(lastLogged) > 5*time.Second {
						r.log("worker %d: %v (retrying)", i, err)
						lastLogged = time.Now()
					}
					select {
					case <-ctx.Done():
					case <-time.After(200 * time.Millisecond):
					}
					continue
				}
				if err != nil {
					if errs[i] == nil {
						errs[i] = fmt.Errorf("receive: %w", err)
					}
					// Every worker stops; say so now rather than after the drain timeout.
					r.log("worker %d failed, stopping all workers: %v", i, err)
					r.workersFailed.Store(true)
					cancel()
					return
				}
				if n > 0 {
					took := time.Since(started)
					r.obs.Received(n, took)
					if calls != nil {
						calls.add(i, took)
					}
				}
				if n == 0 && !r.b.Blocking() {
					select {
					case <-ctx.Done():
					case <-time.After(r.cfg.Idle):
					}
				}
			}
		})
	}
	return func() error {
		cancel()
		wg.Wait()
		return errors.Join(errs...)
	}
}

// waitDrained waits until every acknowledged message was consumed.
func (r *runner) waitDrained(ctx context.Context, timeout time.Duration) (time.Duration, bool) {
	start := time.Now()
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	for r.t.backlog() > 0 {
		if time.Since(start) > timeout || r.workersFailed.Load() {
			return time.Since(start), false
		}
		select {
		case <-ctx.Done():
			return time.Since(start), false
		case <-ticker.C:
		}
	}
	return time.Since(start), true
}

// parallel runs fn on n goroutines and cancels the rest on the first error.
func (r *runner) parallel(ctx context.Context, n int, fn func(context.Context, int) error) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	var wg sync.WaitGroup
	errs := make([]error, n)
	for i := range n {
		wg.Go(func() {
			if errs[i] = fn(ctx, i); errs[i] != nil {
				cancel()
			}
		})
	}
	wg.Wait()
	return errors.Join(errs...)
}
