// Package rabbit is the queue as one RabbitMQ queue shared by all workers:
// publisher confirms on the producing side, manual acknowledgements with a
// prefetch window on the consuming side. Connections are redialled after a
// broker restart, round-robin over the configured nodes.
package rabbit

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/Haleralex/broker-bench/internal/queue"
)

// Config is one RabbitMQ profile.
type Config struct {
	URLs []string // amqp://user:pass@host:5672/ per node
	// Management is the HTTP API base per node, used to find the queue leader.
	Management []string
	// Quorum declares a quorum queue (Raft, replicated, fsynced before confirm);
	// otherwise a classic queue.
	Quorum bool
	// Transient publishes with delivery mode 1: the broker keeps messages in
	// memory and drops them on restart even on a durable queue.
	Transient bool
	// NoConfirm publishes without publisher confirms: Send returns once the
	// frame is written to the socket, before the broker has the message.
	NoConfirm bool
	// Publishers is the number of confirm channels Send may use in parallel.
	Publishers int
	// Members is how many nodes must host the quorum queue before traffic starts.
	Members int
}

type Backend struct {
	cfg        Config
	dialer     *dialer
	queue      atomic.Pointer[string]
	publishers chan *publisher
	http       *http.Client
}

func New(ctx context.Context, cfg Config) (*Backend, error) {
	d := &dialer{urls: cfg.URLs}
	if _, err := d.channel(ctx); err != nil {
		return nil, fmt.Errorf("rabbitmq is not reachable: %w", err)
	}
	b := &Backend{cfg: cfg, dialer: d, publishers: make(chan *publisher, max(1, cfg.Publishers)), http: &http.Client{Timeout: 5 * time.Second}}
	for range cap(b.publishers) {
		b.publishers <- &publisher{}
	}
	return b, nil
}

// Reset declares a fresh queue. Classic and quorum queues are both durable;
// only the delivery mode decides whether messages survive a restart.
func (b *Backend) Reset(ctx context.Context, _ int) error {
	name := "queue-" + strconv.FormatInt(time.Now().UnixNano(), 36)
	ch, err := b.dialer.channel(ctx)
	if err != nil {
		return err
	}
	defer ch.Close()
	args := amqp.Table{"x-queue-type": "classic"}
	if b.cfg.Quorum {
		args["x-queue-type"] = "quorum"
	}
	if b.cfg.Members > 1 {
		args["x-quorum-initial-group-size"] = b.cfg.Members
	}
	if _, err := ch.QueueDeclare(name, true, false, false, false, args); err != nil {
		return fmt.Errorf("declare %s: %w", name, err)
	}
	b.queue.Store(&name)
	if b.cfg.Members <= 1 {
		return nil
	}
	// A node that joined the cluster late is not a member yet.
	return queue.Retry(ctx, time.Minute, func() error {
		info, err := b.queueInfo(ctx)
		if err == nil && len(info.Online) < b.cfg.Members {
			err = fmt.Errorf("quorum queue has %d of %d members online", len(info.Online), b.cfg.Members)
		}
		return err
	})
}

func (b *Backend) Producer() queue.Producer { return b }

// Send publishes the batch on one channel and, with confirms, waits for the
// broker to confirm every message: for persistent messages on a durable queue
// that means written to disk (classic) or to a majority of Raft logs (quorum).
func (b *Backend) Send(ctx context.Context, payloads [][]byte) error {
	var p *publisher
	select {
	case p = <-b.publishers:
	case <-ctx.Done():
		return ctx.Err()
	}
	defer func() { b.publishers <- p }()
	ch, err := p.get(ctx, b.dialer, !b.cfg.NoConfirm)
	if err != nil {
		return err
	}
	mode := amqp.Persistent
	if b.cfg.Transient {
		mode = amqp.Transient
	}
	queueName := *b.queue.Load()
	confirms := make([]*amqp.DeferredConfirmation, 0, len(payloads))
	for _, payload := range payloads {
		dc, err := ch.PublishWithDeferredConfirmWithContext(ctx, "", queueName, false, false, amqp.Publishing{DeliveryMode: mode, Body: payload})
		if err != nil {
			p.reset()
			return fmt.Errorf("publish: %w", err)
		}
		confirms = append(confirms, dc)
	}
	if b.cfg.NoConfirm {
		return nil
	}
	for _, dc := range confirms {
		ok, err := dc.WaitContext(ctx)
		if err != nil {
			// The channel's confirm sequence is now unknown; start a new one.
			p.reset()
			return fmt.Errorf("confirm: %w", err)
		}
		if !ok {
			return errors.New("broker nacked a message")
		}
	}
	return nil
}

func (b *Backend) NewConsumer(context.Context) (queue.Consumer, error) {
	return &consumer{dialer: b.dialer, queue: *b.queue.Load()}, nil
}

func (b *Backend) Blocking() bool { return true }

// Leader asks the management API which node leads the quorum queue
// ("rabbit@rabbit-2" -> "rabbit-2"); classic queues report their node.
func (b *Backend) Leader(ctx context.Context) (string, error) {
	info, err := b.queueInfo(ctx)
	if err != nil {
		return "", err
	}
	node := info.Leader
	if node == "" {
		node = info.Node
	}
	_, host, ok := strings.Cut(node, "@")
	if !ok {
		return "", fmt.Errorf("rabbit leader: unexpected node %q", node)
	}
	return host, nil
}

type queueInfo struct {
	Leader   string   `json:"leader"`
	Node     string   `json:"node"`
	Members  []string `json:"members"`
	Online   []string `json:"online"`
	Messages int64    `json:"messages"`
	Ready    int64    `json:"messages_ready"`
	Unacked  int64    `json:"messages_unacknowledged"`
}

// queueInfo tries every node: the one that just died does not answer.
func (b *Backend) queueInfo(ctx context.Context) (queueInfo, error) {
	name := b.queue.Load()
	if name == nil {
		return queueInfo{}, errors.New("rabbit: no queue before Reset")
	}
	var errs []error
	for _, base := range b.cfg.Management {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, base+"/api/queues/%2F/"+url.PathEscape(*name), nil)
		if err != nil {
			return queueInfo{}, err
		}
		resp, err := b.http.Do(req)
		if err != nil {
			errs = append(errs, err)
			continue
		}
		var info queueInfo
		err = json.NewDecoder(resp.Body).Decode(&info)
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK || err != nil {
			errs = append(errs, fmt.Errorf("%s: HTTP %d: %v", base, resp.StatusCode, err))
			continue
		}
		return info, nil
	}
	return queueInfo{}, errors.Join(errs...)
}

func (b *Backend) Stats(ctx context.Context) (map[string]any, error) {
	info, err := b.queueInfo(ctx)
	if err != nil {
		return nil, err
	}
	return map[string]any{
		"messages": info.Messages, "messages_ready": info.Ready, "messages_unacked": info.Unacked,
		"members": int64(len(info.Members)), "online_members": int64(len(info.Online)),
	}, nil
}

func (b *Backend) Close() { b.dialer.close() }

// dialer holds one connection and redials it, trying the nodes in turn.
type dialer struct {
	urls []string
	mu   sync.Mutex
	conn *amqp.Connection
	next int
}

func (d *dialer) channel(ctx context.Context) (*amqp.Channel, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.conn == nil || d.conn.IsClosed() {
		var errs []error
		for range d.urls {
			u := d.urls[d.next%len(d.urls)]
			d.next++
			conn, err := amqp.DialConfig(u, amqp.Config{Dial: amqp.DefaultDial(3 * time.Second), Heartbeat: 5 * time.Second})
			if err == nil {
				d.conn = conn
				break
			}
			errs = append(errs, err)
			if ctx.Err() != nil {
				break
			}
		}
		if d.conn == nil || d.conn.IsClosed() {
			return nil, fmt.Errorf("dial rabbitmq: %w", errors.Join(errs...))
		}
	}
	return d.conn.Channel()
}

func (d *dialer) close() {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.conn != nil {
		d.conn.Close()
	}
}

// publisher is one channel in confirm mode, reopened after any error.
type publisher struct {
	ch *amqp.Channel
}

func (p *publisher) get(ctx context.Context, d *dialer, confirm bool) (*amqp.Channel, error) {
	if p.ch != nil && !p.ch.IsClosed() {
		return p.ch, nil
	}
	ch, err := d.channel(ctx)
	if err != nil {
		return nil, err
	}
	if confirm {
		if err := ch.Confirm(false); err != nil {
			ch.Close()
			return nil, err
		}
	}
	p.ch = ch
	return ch, nil
}

func (p *publisher) reset() {
	if p.ch != nil {
		p.ch.Close()
		p.ch = nil
	}
}

type consumer struct {
	dialer     *dialer
	queue      string
	ch         *amqp.Channel
	deliveries <-chan amqp.Delivery
}

// prefetch lets the broker push a few batches ahead of the worker.
const prefetchBatches = 2

func (c *consumer) Receive(ctx context.Context, limit int, handle func([]byte)) (int, error) {
	if c.ch == nil || c.ch.IsClosed() {
		if err := c.subscribe(ctx, limit); err != nil {
			return 0, err
		}
	}
	var batch []amqp.Delivery
	select {
	case <-ctx.Done():
		return 0, nil
	case d, ok := <-c.deliveries:
		if !ok {
			c.ch = nil
			return 0, errors.New("delivery channel closed")
		}
		batch = append(batch, d)
	}
	// Take what has already arrived, without waiting for a full batch.
more:
	for len(batch) < limit {
		select {
		case d, ok := <-c.deliveries:
			if !ok {
				break more
			}
			batch = append(batch, d)
		default:
			break more
		}
	}
	for _, d := range batch {
		handle(d.Body)
	}
	// One ack with multiple=true covers every earlier delivery on this channel.
	if err := batch[len(batch)-1].Ack(true); err != nil {
		c.ch = nil
		return len(batch), fmt.Errorf("ack: %w", err)
	}
	return len(batch), nil
}

func (c *consumer) subscribe(ctx context.Context, limit int) error {
	ch, err := c.dialer.channel(ctx)
	if err != nil {
		return err
	}
	if err := ch.Qos(limit*prefetchBatches, 0, false); err != nil {
		ch.Close()
		return err
	}
	deliveries, err := ch.ConsumeWithContext(context.WithoutCancel(ctx), c.queue, "", false, false, false, false, nil)
	if err != nil {
		ch.Close()
		return err
	}
	c.ch, c.deliveries = ch, deliveries
	return nil
}

func (c *consumer) Close() {
	if c.ch != nil {
		c.ch.Close()
	}
}
