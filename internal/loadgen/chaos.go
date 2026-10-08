package loadgen

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Haleralex/broker-bench/internal/queue"
)

// ChaosConfig is an integrity run: steady traffic, one fault in the middle,
// then every sequence number is checked.
type ChaosConfig struct {
	Producers   int           `json:"producers"`
	Consumers   int           `json:"consumers"`
	Batch       int           `json:"batch"`
	Payload     int           `json:"payload"`
	Rate        int           `json:"rate"`            // messages per second
	Duration    time.Duration `json:"duration_ns"`     // traffic
	FaultAt     time.Duration `json:"fault_at_ns"`     // from the start of traffic
	Downtime    time.Duration `json:"downtime_ns"`     // how long the killed node stays down
	SendTimeout time.Duration `json:"send_timeout_ns"` // a Send that takes longer counts as failed
	Settle      time.Duration `json:"settle_ns"`       // longest wait for stragglers after traffic
	Quiet       time.Duration `json:"quiet_ns"`        // settling ends after this long without new messages
	Idle        time.Duration `json:"idle_ns"`         // pause after an empty poll
}

func ChaosDefaults() ChaosConfig {
	return ChaosConfig{
		Producers: 4, Consumers: 4, Batch: 100, Payload: 256, Rate: 5000,
		Duration: 60 * time.Second, FaultAt: 20 * time.Second, Downtime: 15 * time.Second,
		SendTimeout: 5 * time.Second, Settle: 3 * time.Minute, Quiet: 15 * time.Second,
		Idle: 5 * time.Millisecond,
	}
}

func (c ChaosConfig) Validate() error {
	switch {
	case c.Producers < 1 || c.Consumers < 1 || c.Batch < 1 || c.Payload < 16:
		return errors.New("producers, consumers, batch must be positive and payload at least 16")
	case c.Rate < 1 || c.Rate > 1_000_000:
		return errors.New("-rate must be 1..1000000")
	case c.Duration < 5*time.Second || c.FaultAt <= 0 || c.FaultAt >= c.Duration:
		return errors.New("-duration must be at least 5s and -fault-at inside it")
	case c.Downtime < 0 || c.SendTimeout < 100*time.Millisecond || c.Settle < c.Quiet || c.Quiet < time.Second:
		return errors.New("-downtime not negative, -send-timeout at least 100ms, -quiet at least 1s and not above -settle")
	}
	return nil
}

// Fault kills a broker node and brings it back. Inject returns the node it hit.
type Fault interface {
	Inject(ctx context.Context) (target string, err error)
	Recover(ctx context.Context) error
}

type FaultReport struct {
	Kind        string  `json:"kind"`
	Target      string  `json:"target,omitempty"`
	InjectedAt  float64 `json:"injected_at_s,omitempty"`
	RecoveredAt float64 `json:"recovered_at_s,omitempty"`
	Error       string  `json:"error,omitempty"`
}

type ChaosResult struct {
	Integrity Integrity   `json:"integrity"`
	Fault     FaultReport `json:"fault"`
	// Seconds of traffic after the fault in which no Send was confirmed / no
	// message was received: how long producers and workers were stuck.
	ProducerStall float64 `json:"producer_stall_s"`
	ConsumerStall float64 `json:"consumer_stall_s"`
	// SettleSeconds is how long stragglers kept arriving after traffic stopped.
	SettleSeconds float64  `json:"settle_s"`
	EndToEnd      Latency  `json:"end_to_end"`
	Timeline      []Second `json:"timeline"`
}

// Second is one second of a chaos run, counted from the start of traffic.
type Second struct {
	T             int   `json:"t"`
	Confirmed     int64 `json:"confirmed"`
	SendErrors    int64 `json:"send_errors"`
	Consumed      int64 `json:"consumed"`
	Duplicates    int64 `json:"duplicates"`
	ReceiveErrors int64 `json:"receive_errors"`
}

type second struct {
	Confirmed, SendErrors, Consumed, Duplicates, ReceiveErrors atomic.Int64
}

// timeline buckets events by second since start; methods are no-ops on nil.
type timeline struct {
	start   time.Time
	seconds []second
}

func newTimeline(start time.Time, length time.Duration) *timeline {
	return &timeline{start: start, seconds: make([]second, int(length.Seconds())+1)}
}

func (t *timeline) add(f func(*second)) {
	if t == nil {
		return
	}
	i := int(time.Since(t.start).Seconds())
	if i >= 0 && i < len(t.seconds) {
		f(&t.seconds[i])
	}
}

func (t *timeline) snapshot(upTo int) []Second {
	upTo = min(upTo, len(t.seconds))
	out := make([]Second, upTo)
	for i := range upTo {
		s := &t.seconds[i]
		out[i] = Second{T: i, Confirmed: s.Confirmed.Load(), SendErrors: s.SendErrors.Load(), Consumed: s.Consumed.Load(), Duplicates: s.Duplicates.Load(), ReceiveErrors: s.ReceiveErrors.Load()}
	}
	return out
}

// RunChaos sends at a fixed rate, injects the fault at FaultAt, restores the
// node after Downtime, waits for stragglers and checks every message. fault
// may be nil for a baseline run. It returns an error only when the run itself
// could not be carried out; integrity violations are part of the result.
func RunChaos(ctx context.Context, b queue.Backend, cfg ChaosConfig, fault Fault, kind string, obs Observer, log func(string, ...any)) (ChaosResult, error) {
	if obs == nil {
		obs = nopObserver{}
	}
	if err := b.Reset(ctx, cfg.Consumers); err != nil {
		return ChaosResult{}, err
	}
	base := Config{Producers: cfg.Producers, Consumers: cfg.Consumers, Batch: cfg.Batch, Payload: cfg.Payload, DrainTimeout: cfg.Settle, Idle: cfg.Idle}
	capacity := uint64(float64(cfg.Rate)*cfg.Duration.Seconds()*1.2) + uint64(primeMessages(base)) + 1<<16
	r := &runner{b: b, cfg: base, obs: obs, log: log, t: newTracker(capacity), e2e: newSamples(cfg.Consumers), tolerateErrors: true}
	defer obs.Phase("done", 0)
	for range cfg.Consumers {
		c, err := b.NewConsumer(ctx)
		if err != nil {
			return ChaosResult{}, err
		}
		defer c.Close()
		r.consumers = append(r.consumers, c)
	}

	obs.Phase("prime", 0)
	stopPrime := r.startConsumers(ctx, nil)
	_, err := r.produceClosed(ctx, uint64(primeMessages(base)))
	_, drained := r.waitDrained(ctx, time.Minute)
	if err := errors.Join(err, stopPrime()); err != nil {
		return ChaosResult{}, fmt.Errorf("prime: %w", err)
	}
	if !drained {
		return ChaosResult{}, errors.New("prime: workers did not receive the priming messages within a minute")
	}
	// Integrity covers the traffic only.
	primed := r.t.next.Load()

	res := ChaosResult{Fault: FaultReport{Kind: kind}}
	start := time.Now().Add(10 * time.Millisecond)
	r.timeline = newTimeline(start, cfg.Duration+cfg.Settle+time.Minute)
	r.e2e.recording.Store(true)
	stopWorkers := r.startConsumers(ctx, nil)
	obs.Phase("steady", cfg.Rate)
	log("traffic: %d msg/s for %s, fault %q at %s, down for %s", cfg.Rate, cfg.Duration, kind, cfg.FaultAt, cfg.Downtime)

	var faultWG sync.WaitGroup
	recovered := make(chan struct{})
	if fault == nil {
		close(recovered)
	} else {
		faultWG.Go(func() { r.runFault(ctx, fault, start, cfg, &res.Fault, recovered) })
	}

	err = r.chaosTraffic(ctx, cfg, start)
	// A node that is still down comes back before stragglers are awaited.
	<-recovered
	faultWG.Wait()
	if err != nil {
		return res, errors.Join(err, stopWorkers())
	}

	obs.Phase("catch_up", 0)
	settleStart := time.Now()
	r.settle(ctx, cfg)
	res.SettleSeconds = time.Since(settleStart).Seconds()
	if err := stopWorkers(); err != nil {
		return res, err
	}
	r.e2e.recording.Store(false)

	res.Integrity = r.t.integrityFrom(primed)
	res.EndToEnd = summarizeLatency(r.e2e.take())
	res.Timeline = r.timeline.snapshot(int(time.Since(start).Seconds()) + 1)
	res.ProducerStall, res.ConsumerStall = stalls(res.Timeline, res.Fault.InjectedAt, cfg.Duration.Seconds())
	log("integrity: confirmed %d, lost %d, delivered unconfirmed %d, duplicates %d",
		res.Integrity.Confirmed, res.Integrity.Lost, res.Integrity.DeliveredUnconfirmed, res.Integrity.Duplicates)
	return res, ctx.Err()
}

func (r *runner) runFault(ctx context.Context, fault Fault, start time.Time, cfg ChaosConfig, rep *FaultReport, recovered chan<- struct{}) {
	defer close(recovered)
	select {
	case <-ctx.Done():
		return
	case <-time.After(time.Until(start.Add(cfg.FaultAt))):
	}
	target, err := fault.Inject(ctx)
	rep.Target, rep.InjectedAt = target, time.Since(start).Seconds()
	if err != nil {
		rep.Error = err.Error()
		r.log("fault: %v", err)
		return
	}
	r.obs.Fault(target, true)
	r.log("fault: %s killed at %.1fs", target, rep.InjectedAt)
	select {
	case <-ctx.Done():
	case <-time.After(cfg.Downtime):
	}
	if err := fault.Recover(context.WithoutCancel(ctx)); err != nil {
		rep.Error = err.Error()
		r.log("recover: %v", err)
		return
	}
	rep.RecoveredAt = time.Since(start).Seconds()
	r.obs.Fault(target, false)
	r.log("fault: %s healthy again at %.1fs", target, rep.RecoveredAt)
}

// chaosTraffic is steady's schedule without the measurement window: a failed
// or slow Send is counted and the producer moves on to its next slot. Slots
// still due when the traffic ends are never sent and not counted.
func (r *runner) chaosTraffic(ctx context.Context, cfg ChaosConfig, start time.Time) error {
	batch := min(cfg.Batch, max(1, cfg.Rate/steadyBatchesPerSecond))
	interval := time.Duration(float64(time.Second) * float64(batch) / float64(cfg.Rate))
	end := start.Add(cfg.Duration)
	return r.parallel(ctx, cfg.Producers, func(ctx context.Context, p int) error {
		for k := p; ; k += cfg.Producers {
			due := start.Add(time.Duration(k) * interval)
			// A producer that fell behind (every Send timing out while the
			// broker is gone) stops with the traffic instead of working
			// through the missed slots afterwards: at one timeout per slot
			// that would take hours.
			if !due.Before(end) || !time.Now().Before(end) {
				return nil
			}
			if wait := time.Until(due); wait > 0 {
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-time.After(wait):
				}
			}
			first, n := r.t.reserve(batch, r.t.capacity())
			if n < batch {
				return errors.New("sequence capacity exhausted")
			}
			sendCtx, cancel := context.WithTimeout(ctx, cfg.SendTimeout)
			err := r.send(sendCtx, first, n, due)
			cancel()
			if err != nil {
				if ctx.Err() != nil {
					return ctx.Err()
				}
				r.obs.SendFailed(n)
				r.timeline.add(func(s *second) { s.SendErrors.Add(int64(n)) })
			}
		}
	})
}

// settle waits for stragglers: redeliveries and messages a recovering broker
// still holds. It ends when nothing new arrived for Quiet, or after Settle.
func (r *runner) settle(ctx context.Context, cfg ChaosConfig) {
	deadline := time.Now().Add(cfg.Settle)
	last, lastChange := r.t.unique.Load(), time.Now()
	for time.Now().Before(deadline) && time.Since(lastChange) < cfg.Quiet {
		select {
		case <-ctx.Done():
			return
		case <-time.After(100 * time.Millisecond):
		}
		if u := r.t.unique.Load(); u != last {
			last, lastChange = u, time.Now()
		}
	}
}

// stalls counts seconds of traffic, from the fault on, with nothing confirmed
// and with nothing consumed.
func stalls(seconds []Second, faultAt, duration float64) (producer, consumer float64) {
	if faultAt <= 0 {
		return 0, 0
	}
	for _, s := range seconds {
		if float64(s.T) < faultAt || float64(s.T+1) > duration {
			continue
		}
		if s.Confirmed == 0 {
			producer++
		}
		if s.Consumed == 0 {
			consumer++
		}
	}
	return producer, consumer
}

// RegisterFlags binds the chaos flags; cmd/bench registers the same flags and
// forwards them with Args.
func (c *ChaosConfig) RegisterFlags(fs *flag.FlagSet) {
	fs.IntVar(&c.Producers, "producers", c.Producers, "producer goroutines")
	fs.IntVar(&c.Consumers, "consumers", c.Consumers, "worker goroutines; Kafka gets one partition per worker")
	fs.IntVar(&c.Batch, "batch", c.Batch, "largest batch per send and per receive")
	fs.IntVar(&c.Payload, "payload", c.Payload, "bytes per message")
	fs.IntVar(&c.Rate, "rate", c.Rate, "messages per second during the whole run")
	fs.DurationVar(&c.Duration, "duration", c.Duration, "traffic duration")
	fs.DurationVar(&c.FaultAt, "fault-at", c.FaultAt, "when the node is killed, from the start of traffic")
	fs.DurationVar(&c.Downtime, "downtime", c.Downtime, "how long the killed node stays down")
	fs.DurationVar(&c.SendTimeout, "send-timeout", c.SendTimeout, "a Send that takes longer counts as unconfirmed")
	fs.DurationVar(&c.Settle, "settle", c.Settle, "longest wait for stragglers after traffic")
	fs.DurationVar(&c.Quiet, "quiet", c.Quiet, "settling ends after this long without a new message")
	fs.DurationVar(&c.Idle, "idle", c.Idle, "pause after an empty poll of a non-blocking backend")
}

func (c ChaosConfig) Args() []string {
	return []string{
		"-producers", strconv.Itoa(c.Producers), "-consumers", strconv.Itoa(c.Consumers),
		"-batch", strconv.Itoa(c.Batch), "-payload", strconv.Itoa(c.Payload), "-rate", strconv.Itoa(c.Rate),
		"-duration", c.Duration.String(), "-fault-at", c.FaultAt.String(), "-downtime", c.Downtime.String(),
		"-send-timeout", c.SendTimeout.String(), "-settle", c.Settle.String(), "-quiet", c.Quiet.String(),
		"-idle", c.Idle.String(),
	}
}
