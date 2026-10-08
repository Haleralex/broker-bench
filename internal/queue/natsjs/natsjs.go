// Package natsjs is the queue as a NATS JetStream stream with work-queue
// retention (a message is removed once acknowledged) and one durable pull
// consumer shared by all workers.
package natsjs

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/Haleralex/broker-bench/internal/queue"
)

// Config is one JetStream profile. How often the file store fsyncs is a server
// setting (jetstream.sync_interval), chosen by the compose configuration.
type Config struct {
	URLs     []string
	Memory   bool // memory storage: nothing survives a server restart
	Replicas int  // 1 on the single server, 3 on the cluster
}

type Backend struct {
	cfg    Config
	nc     *nats.Conn
	js     jetstream.JetStream
	stream atomic.Pointer[streamRef]
}

type streamRef struct {
	stream   jetstream.Stream
	subject  string
	consumer jetstream.Consumer
}

func New(ctx context.Context, cfg Config) (*Backend, error) {
	nc, err := nats.Connect(strings.Join(cfg.URLs, ","),
		nats.MaxReconnects(-1), nats.ReconnectWait(100*time.Millisecond),
		nats.RetryOnFailedConnect(false), nats.Timeout(3*time.Second),
	)
	if err != nil {
		return nil, fmt.Errorf("nats is not reachable: %w", err)
	}
	// Bounded so that producers block instead of buffering without limit while
	// the server is down. The timeout releases acknowledgements that will never
	// come (publishes lost with a dead server); otherwise they would hold the
	// pending slots forever and stall every later publish.
	js, err := jetstream.New(nc, jetstream.WithPublishAsyncMaxPending(8192), jetstream.WithPublishAsyncTimeout(10*time.Second))
	if err != nil {
		nc.Close()
		return nil, err
	}
	if _, err := js.AccountInfo(ctx); err != nil {
		nc.Close()
		return nil, fmt.Errorf("jetstream is not enabled: %w", err)
	}
	return &Backend{cfg: cfg, nc: nc, js: js}, nil
}

func (b *Backend) Reset(ctx context.Context, consumers int) error {
	name := "Q" + strconv.FormatInt(time.Now().UnixNano(), 36)
	storage := jetstream.FileStorage
	if b.cfg.Memory {
		storage = jetstream.MemoryStorage
	}
	var stream jetstream.Stream
	err := queue.Retry(ctx, time.Minute, func() (err error) {
		stream, err = b.js.CreateStream(ctx, jetstream.StreamConfig{
			Name: name, Subjects: []string{"q." + name}, Storage: storage,
			Replicas: max(1, b.cfg.Replicas), Retention: jetstream.WorkQueuePolicy,
		})
		return err
	})
	if err != nil {
		return fmt.Errorf("create stream %s: %w", name, err)
	}
	consumer, err := stream.CreateOrUpdateConsumer(ctx, jetstream.ConsumerConfig{
		Durable: "workers", AckPolicy: jetstream.AckExplicitPolicy,
		// Unacknowledged messages of a worker that disappeared come back after this.
		AckWait:       10 * time.Second,
		MaxAckPending: 100_000,
	})
	if err != nil {
		return fmt.Errorf("create consumer: %w", err)
	}
	b.stream.Store(&streamRef{stream: stream, subject: "q." + name, consumer: consumer})
	return nil
}

func (b *Backend) Producer() queue.Producer { return b }

// Send publishes asynchronously and waits for every PubAck: the stream leader
// stored the message (and, with replicas, a quorum did).
func (b *Backend) Send(ctx context.Context, payloads [][]byte) error {
	ref := b.stream.Load()
	futures := make([]jetstream.PubAckFuture, 0, len(payloads))
	for _, p := range payloads {
		f, err := b.js.PublishAsync(ref.subject, p)
		if err != nil {
			return fmt.Errorf("publish: %w", err)
		}
		futures = append(futures, f)
	}
	for _, f := range futures {
		select {
		case <-f.Ok():
		case err := <-f.Err():
			return fmt.Errorf("pub ack: %w", err)
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}

func (b *Backend) NewConsumer(context.Context) (queue.Consumer, error) {
	return consumer{c: b.stream.Load().consumer}, nil
}

// Blocking is false: workers use FetchNoWait and pause after an empty fetch,
// so a stopped worker never holds messages it was not given yet.
func (b *Backend) Blocking() bool { return false }

func (b *Backend) Leader(ctx context.Context) (string, error) {
	info, err := b.stream.Load().stream.Info(ctx)
	if err != nil {
		return "", err
	}
	if info.Cluster == nil || info.Cluster.Leader == "" {
		return "", errors.New("nats leader: stream has no leader")
	}
	// server_name is the compose service name (nats-1, nats-2, ...).
	return info.Cluster.Leader, nil
}

func (b *Backend) Stats(ctx context.Context) (map[string]any, error) {
	ref := b.stream.Load()
	if ref == nil {
		return nil, errors.New("nats stats: no stream before Reset")
	}
	info, err := ref.stream.Info(ctx)
	if err != nil {
		return nil, err
	}
	stats := map[string]any{"messages": int64(info.State.Msgs), "stored_bytes": int64(info.State.Bytes)}
	if info.Cluster != nil {
		current := int64(0)
		for _, r := range info.Cluster.Replicas {
			if r.Current {
				current++
			}
		}
		stats["current_replicas"] = current
	}
	if ci, err := ref.consumer.Info(ctx); err == nil {
		stats["ack_pending"] = int64(ci.NumAckPending)
		stats["redelivered"] = int64(ci.NumRedelivered)
	}
	return stats, nil
}

func (b *Backend) Close() { b.nc.Close() }

type consumer struct {
	c jetstream.Consumer
}

func (c consumer) Receive(ctx context.Context, limit int, handle func([]byte)) (int, error) {
	if ctx.Err() != nil {
		return 0, nil
	}
	batch, err := c.c.FetchNoWait(limit)
	if err != nil {
		return 0, err
	}
	var last jetstream.Msg
	n := 0
	for msg := range batch.Messages() {
		handle(msg.Data())
		// Every message but the last is acknowledged without waiting.
		if last != nil {
			if err := last.Ack(); err != nil {
				return n, fmt.Errorf("ack: %w", err)
			}
		}
		last = msg
		n++
	}
	if err := batch.Error(); err != nil && !errors.Is(err, jetstream.ErrNoMessages) {
		return n, err
	}
	if last == nil {
		return 0, nil
	}
	// The last ack waits for the server; acks of one connection arrive in order.
	ackCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
	defer cancel()
	if err := last.DoubleAck(ackCtx); err != nil {
		return n, fmt.Errorf("double ack: %w", err)
	}
	return n, nil
}

func (consumer) Close() {}
