package loadgen

import (
	"context"
	"errors"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/Haleralex/broker-bench/internal/queue"
)

// crashable is an in-memory broker that a fault can take down. How it
// misbehaves on the way down is chosen per test.
type crashable struct {
	mu    sync.Mutex
	items [][]byte
	down  bool
	// loseOnCrash drops everything stored when the broker goes down, like a
	// broker that acknowledged from memory.
	loseOnCrash bool
	// lostAcks stores the batch but reports failure, like an acknowledgement
	// lost on a dying connection.
	lostAcks bool
	// receiveDelay slows workers down so that a backlog builds up.
	receiveDelay time.Duration
	// hang makes Send wait for its context while the broker is down, like a
	// client waiting for an acknowledgement that never comes.
	hang bool
	// stayDown keeps the broker down after Recover, like a stream that did
	// not survive the restart.
	stayDown bool
}

var errDown = errors.New("broker down")

func (m *crashable) Reset(context.Context, int) error { return nil }
func (m *crashable) Producer() queue.Producer         { return m }
func (m *crashable) Blocking() bool                   { return false }
func (m *crashable) Close()                           {}
func (m *crashable) Stats(context.Context) (map[string]any, error) {
	return map[string]any{}, nil
}
func (m *crashable) NewConsumer(context.Context) (queue.Consumer, error) {
	return crashConsumer{m}, nil
}

func (m *crashable) Send(ctx context.Context, payloads [][]byte) error {
	m.mu.Lock()
	if m.down && m.hang {
		m.mu.Unlock()
		<-ctx.Done()
		return ctx.Err()
	}
	defer m.mu.Unlock()
	if m.down {
		if m.lostAcks {
			m.items = append(m.items, payloads...)
		}
		return errDown
	}
	m.items = append(m.items, payloads...)
	return nil
}

type crashConsumer struct{ m *crashable }

func (c crashConsumer) Receive(_ context.Context, limit int, handle func([]byte)) (int, error) {
	c.m.mu.Lock()
	if c.m.down {
		c.m.mu.Unlock()
		return 0, errDown
	}
	n := min(limit, len(c.m.items))
	batch := slices.Clone(c.m.items[:n])
	c.m.items = c.m.items[n:]
	c.m.mu.Unlock()
	for _, p := range batch {
		handle(p)
	}
	time.Sleep(c.m.receiveDelay)
	return n, nil
}

func (crashConsumer) Close() {}

// crash is the Fault for crashable.
type crash struct{ m *crashable }

func (f crash) Inject(context.Context) (string, error) {
	f.m.mu.Lock()
	defer f.m.mu.Unlock()
	f.m.down = true
	if f.m.loseOnCrash {
		f.m.items = nil
	}
	return "memory", nil
}

func (f crash) Recover(context.Context) error {
	f.m.mu.Lock()
	defer f.m.mu.Unlock()
	f.m.down = f.m.stayDown
	return nil
}

func chaosTestConfig() ChaosConfig {
	cfg := ChaosDefaults()
	cfg.Producers, cfg.Consumers, cfg.Batch, cfg.Payload = 2, 2, 20, 32
	// Two seconds down leave at least one whole second without traffic.
	cfg.Rate, cfg.Duration, cfg.FaultAt, cfg.Downtime = 2000, 6*time.Second, 2*time.Second, 2*time.Second
	cfg.SendTimeout, cfg.Settle, cfg.Quiet, cfg.Idle = 200*time.Millisecond, 10*time.Second, time.Second, time.Millisecond
	return cfg
}

func TestChaosCleanCrashLosesNothing(t *testing.T) {
	m := &crashable{}
	res, err := RunChaos(t.Context(), m, chaosTestConfig(), crash{m}, "kill", nil, t.Logf)
	if err != nil {
		t.Fatal(err)
	}
	in := res.Integrity
	if in.Lost != 0 || in.DeliveredUnconfirmed != 0 || in.Duplicates != 0 {
		t.Fatalf("integrity: %+v", in)
	}
	// About two seconds of sends failed while the broker was down.
	if in.Unconfirmed < 3000 || in.Unconfirmed > 5000 || in.Confirmed+in.Unconfirmed != in.Reserved {
		t.Fatalf("unconfirmed: %+v", in)
	}
	if res.Fault.Target != "memory" || res.Fault.InjectedAt < 1.9 || res.Fault.RecoveredAt < res.Fault.InjectedAt+1.9 {
		t.Fatalf("fault: %+v", res.Fault)
	}
	if res.ProducerStall < 1 || res.ConsumerStall < 1 {
		t.Fatalf("stalls: producer %.0f consumer %.0f", res.ProducerStall, res.ConsumerStall)
	}
	var sent, failed int64
	for _, s := range res.Timeline {
		sent += s.Confirmed
		failed += s.SendErrors
	}
	if sent != in.Confirmed || failed != in.Unconfirmed {
		t.Fatalf("timeline sums %d/%d, integrity %d/%d", sent, failed, in.Confirmed, in.Unconfirmed)
	}
}

func TestChaosDetectsLossOfConfirmedMessages(t *testing.T) {
	// One worker takes at most 20 messages per 5 ms: 4000/s against 6000/s
	// sent, so a backlog exists when the broker crashes.
	m := &crashable{loseOnCrash: true, receiveDelay: 5 * time.Millisecond}
	cfg := chaosTestConfig()
	cfg.Consumers = 1
	cfg.Rate = 6000
	res, err := RunChaos(t.Context(), m, cfg, crash{m}, "kill", nil, t.Logf)
	if err != nil {
		t.Fatal(err)
	}
	if res.Integrity.Lost == 0 || len(res.Integrity.LostSample) == 0 {
		t.Fatalf("loss not detected: %+v", res.Integrity)
	}
}

func TestChaosCountsDeliveredButUnconfirmed(t *testing.T) {
	m := &crashable{lostAcks: true}
	res, err := RunChaos(t.Context(), m, chaosTestConfig(), crash{m}, "kill", nil, t.Logf)
	if err != nil {
		t.Fatal(err)
	}
	if in := res.Integrity; in.Lost != 0 || in.DeliveredUnconfirmed < 3000 || in.Unconfirmed != 0 {
		t.Fatalf("integrity: %+v", in)
	}
}

func TestChaosWithoutFault(t *testing.T) {
	cfg := chaosTestConfig()
	res, err := RunChaos(t.Context(), &crashable{}, cfg, nil, "none", nil, t.Logf)
	if err != nil {
		t.Fatal(err)
	}
	if in := res.Integrity; in.Lost != 0 || in.Unconfirmed != 0 || in.Confirmed != in.Reserved || in.Confirmed < 11000 {
		t.Fatalf("integrity: %+v", in)
	}
	if res.ProducerStall != 0 {
		t.Fatalf("stall without fault: %+v", res)
	}
}

func TestChaosStopsWhenTrafficEndsEvenIfSendsHang(t *testing.T) {
	m := &crashable{hang: true, stayDown: true}
	cfg := chaosTestConfig()
	cfg.SendTimeout = time.Second
	started := time.Now()
	res, err := RunChaos(t.Context(), m, cfg, crash{m}, "kill", nil, t.Logf)
	if err != nil {
		t.Fatal(err)
	}
	// Traffic 6s, at most one hanging Send past its end, then the quiet period.
	if elapsed := time.Since(started); elapsed > cfg.Duration+cfg.SendTimeout+cfg.Quiet+3*time.Second {
		t.Fatalf("run took %s: producers kept working through missed slots", elapsed)
	}
	// The broker never comes back, so a batch confirmed just before the crash
	// may count as lost; this test is about the run ending, not about loss.
	if in := res.Integrity; in.Unconfirmed == 0 || in.Reserved >= int64(cfg.Rate)*int64(cfg.Duration.Seconds()) {
		t.Fatalf("integrity: %+v", in)
	}
}
