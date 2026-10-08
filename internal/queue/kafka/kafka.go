// Package kafka is the queue as a Kafka topic read by one consumer group: one
// partition per worker, offsets committed after each batch.
package kafka

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/Haleralex/broker-bench/internal/queue"
)

// Config is one Kafka profile.
type Config struct {
	Brokers []string
	// Replicas is the topic's replication factor: 1 on the single broker, 3 on the cluster.
	Replicas int16
	// MinInsync is min.insync.replicas: with acks=all, how many replicas must
	// have a record before the leader acknowledges it.
	MinInsync int
	// LeaderAcks uses acks=1: the leader acknowledges after its own append,
	// before followers copy the record. Idempotence requires acks=all, so it is off.
	LeaderAcks bool
	// Fsync sets flush.messages=1: every append is fsynced before the acknowledgement.
	Fsync  bool
	Linger time.Duration
}

type Backend struct {
	cfg      Config
	admin    *kadm.Client
	producer *kgo.Client
	// topic is set by Reset and read by Stats and Leader, which monitoring and
	// fault injection call concurrently.
	topic atomic.Pointer[string]
}

func New(ctx context.Context, cfg Config) (*Backend, error) {
	cl, err := kgo.NewClient(kgo.SeedBrokers(cfg.Brokers...))
	if err != nil {
		return nil, err
	}
	admin := kadm.NewClient(cl)
	if _, err := admin.ApiVersions(ctx); err != nil {
		cl.Close()
		return nil, fmt.Errorf("kafka is not reachable: %w", err)
	}
	return &Backend{cfg: cfg, admin: admin}, nil
}

// Reset creates a fresh topic and consumer group instead of deleting the old
// ones: topic deletion in Kafka is asynchronous.
func (b *Backend) Reset(ctx context.Context, consumers int) error {
	topic := "queue-" + strconv.FormatInt(time.Now().UnixNano(), 36)
	configs := map[string]*string{"min.insync.replicas": kadm.StringPtr(strconv.Itoa(max(1, b.cfg.MinInsync)))}
	if b.cfg.Fsync {
		configs["flush.messages"] = kadm.StringPtr("1")
	}
	err := queue.Retry(ctx, time.Minute, func() error {
		resp, err := b.admin.CreateTopic(ctx, int32(consumers), max(1, b.cfg.Replicas), configs, topic)
		if err == nil {
			err = resp.Err
		}
		// An earlier attempt may have succeeded after its response was lost.
		if errors.Is(err, kerr.TopicAlreadyExists) {
			return nil
		}
		return err
	})
	if err != nil {
		return fmt.Errorf("create topic %s: %w", topic, err)
	}
	b.topic.Store(&topic)
	if b.producer != nil {
		b.producer.Close()
	}
	opts := []kgo.Opt{
		kgo.SeedBrokers(b.cfg.Brokers...),
		kgo.DefaultProduceTopic(topic),
		kgo.ProducerLinger(b.cfg.Linger),
		// Payloads are random; compression would only cost CPU.
		kgo.ProducerBatchCompression(kgo.NoCompression()),
	}
	if b.cfg.LeaderAcks {
		opts = append(opts, kgo.RequiredAcks(kgo.LeaderAck()), kgo.DisableIdempotentWrite())
	} else {
		opts = append(opts, kgo.RequiredAcks(kgo.AllISRAcks()))
	}
	b.producer, err = kgo.NewClient(opts...)
	return err
}

func (b *Backend) Producer() queue.Producer { return b }

// Send waits until every record of the batch is acknowledged, or ctx ends.
// The client keeps retrying in the background, so a record whose Send failed
// may still be written: the integrity check counts it as delivered but unconfirmed.
func (b *Backend) Send(ctx context.Context, payloads [][]byte) error {
	records := make([]*kgo.Record, len(payloads))
	for i, p := range payloads {
		records[i] = &kgo.Record{Value: p}
	}
	return b.producer.ProduceSync(ctx, records...).FirstErr()
}

func (b *Backend) NewConsumer(context.Context) (queue.Consumer, error) {
	topic := *b.topic.Load()
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(b.cfg.Brokers...),
		kgo.ConsumeTopics(topic),
		kgo.ConsumerGroup(topic),
		kgo.DisableAutoCommit(),
		// Partitions are not reassigned between poll and commit, which would
		// hand already processed records to another worker.
		kgo.BlockRebalanceOnPoll(),
	)
	if err != nil {
		return nil, err
	}
	return consumer{cl: cl}, nil
}

func (b *Backend) Blocking() bool { return true }

// Leader is the broker leading partition 0. Brokers are named kafka-<node id>
// in compose.yaml.
func (b *Backend) Leader(ctx context.Context) (string, error) {
	topic := b.topic.Load()
	if topic == nil {
		return "", errors.New("kafka leader: no topic before Reset")
	}
	md, err := b.admin.Metadata(ctx, *topic)
	if err != nil {
		return "", err
	}
	p, ok := md.Topics[*topic].Partitions[0]
	if !ok || p.Leader < 0 {
		return "", fmt.Errorf("kafka leader: partition 0 of %s has no leader", *topic)
	}
	return "kafka-" + strconv.Itoa(int(p.Leader)), nil
}

// Stats reports the topic's log size and the group's lag, in total and per partition.
func (b *Backend) Stats(ctx context.Context) (map[string]any, error) {
	topic := b.topic.Load()
	if topic == nil {
		return nil, errors.New("kafka stats: no topic before Reset")
	}
	// A TopicsSet entry with no partitions selects none; nil describes everything.
	dirs, err := b.admin.DescribeAllLogDirs(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("kafka stats: %w", err)
	}
	stats := map[string]any{"topic": *topic}
	var size int64
	for _, broker := range dirs {
		broker.EachPartition(func(p kadm.DescribedLogDirPartition) {
			if p.Topic == *topic {
				// With replicas every copy counts: this is what the cluster stores.
				size += p.Size
				key := fmt.Sprintf("partition_%d_log_bytes", p.Partition)
				stats[key] = max(p.Size, int64Of(stats[key]))
			}
		})
	}
	stats["log_bytes"] = size
	if lags, err := b.admin.Lag(ctx, *topic); err == nil {
		var total int64
		for _, l := range lags[*topic].Lag[*topic] {
			if l.Err == nil && l.Lag >= 0 {
				total += l.Lag
				stats[fmt.Sprintf("partition_%d_lag", l.Partition)] = l.Lag
			}
		}
		stats["lag"] = total
	}
	if md, err := b.admin.Metadata(ctx, *topic); err == nil {
		var isr []int
		for _, p := range md.Topics[*topic].Partitions {
			isr = append(isr, len(p.ISR))
		}
		if len(isr) > 0 {
			stats["min_isr_size"] = int64(slices.Min(isr))
		}
	}
	return stats, nil
}

func int64Of(v any) int64 {
	i, _ := v.(int64)
	return i
}

func (b *Backend) Close() {
	if b.producer != nil {
		b.producer.Close()
	}
	b.admin.Close()
}

type consumer struct {
	cl *kgo.Client
}

func (c consumer) Receive(ctx context.Context, limit int, handle func([]byte)) (int, error) {
	defer c.cl.AllowRebalance()
	fetches := c.cl.PollRecords(ctx, limit)
	for _, fe := range fetches.Errors() {
		if !errors.Is(fe.Err, context.Canceled) && !errors.Is(fe.Err, context.DeadlineExceeded) {
			return 0, fe.Err
		}
	}
	n := 0
	fetches.EachRecord(func(r *kgo.Record) {
		handle(r.Value)
		n++
	})
	if n == 0 {
		return 0, nil
	}
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
	defer cancel()
	return n, c.cl.CommitUncommittedOffsets(ctx)
}

func (c consumer) Close() { c.cl.Close() }
