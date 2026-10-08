// Package profile lists the compared broker configurations. Each runs either on
// a single node or on a three-node cluster, and differs in what the broker has
// done before it acknowledges a message.
package profile

import (
	"context"
	"fmt"
	"os"
	"slices"
	"strings"
	"time"

	"github.com/Haleralex/broker-bench/internal/queue"
	"github.com/Haleralex/broker-bench/internal/queue/kafka"
	"github.com/Haleralex/broker-bench/internal/queue/natsjs"
	"github.com/Haleralex/broker-bench/internal/queue/rabbit"
)

type Profile struct {
	Name        string
	Broker      string // kafka, rabbit, nats
	Cluster     bool
	Description string
	// Env is passed to docker compose when the brokers start (server-side settings).
	Env []string
}

// Services are the compose services the profile needs.
func (p Profile) Services() []string {
	if !p.Cluster {
		return []string{p.Broker}
	}
	return []string{p.Broker + "-1", p.Broker + "-2", p.Broker + "-3"}
}

var All = []Profile{
	{Name: "kafka", Broker: "kafka", Description: "1 broker, acks=all (= leader only), log in the page cache"},
	{Name: "kafka_fsync", Broker: "kafka", Description: "1 broker, acks=all, flush.messages=1: fsync before every ack"},
	{Name: "kafka_r3", Broker: "kafka", Cluster: true, Description: "3 brokers, RF=3, min.insync.replicas=2, acks=all"},
	{Name: "kafka_r3_acks1", Broker: "kafka", Cluster: true, Description: "3 brokers, RF=3, acks=1: the leader acks before followers copy"},

	{Name: "rabbit_classic", Broker: "rabbit", Description: "1 node, durable classic queue, persistent messages, publisher confirms"},
	{Name: "rabbit_transient", Broker: "rabbit", Description: "1 node, durable classic queue, transient messages, publisher confirms"},
	{Name: "rabbit_noconfirm", Broker: "rabbit", Description: "1 node, durable classic queue, persistent messages, no publisher confirms"},
	{Name: "rabbit_quorum", Broker: "rabbit", Description: "1 node, quorum queue (Raft log fsynced before confirm)"},
	{Name: "rabbit_quorum_r3", Broker: "rabbit", Cluster: true, Description: "3 nodes, quorum queue replicated to all, confirm after a majority"},

	{Name: "nats", Broker: "nats", Description: "1 server, file storage, default sync_interval (fsync every 2 minutes)"},
	{Name: "nats_sync", Broker: "nats", Description: "1 server, file storage, sync_interval: always (fsync every write)", Env: []string{"NATS_CONF=nats-sync.conf"}},
	{Name: "nats_memory", Broker: "nats", Description: "1 server, memory storage"},
	{Name: "nats_r3", Broker: "nats", Cluster: true, Description: "3 servers, file storage, R3: ack after a quorum stored it"},
}

func Lookup(name string) (Profile, bool) {
	i := slices.IndexFunc(All, func(p Profile) bool { return p.Name == name })
	if i < 0 {
		return Profile{}, false
	}
	return All[i], true
}

func Names() []string {
	names := make([]string, len(All))
	for i, p := range All {
		names[i] = p.Name
	}
	return names
}

// Options tune the clients, not the profile's durability.
type Options struct {
	Publishers  int
	KafkaLinger time.Duration
}

// Open connects to the profile's brokers. Addresses come from the loadgen
// container's environment, see compose.yaml.
func Open(ctx context.Context, p Profile, o Options) (queue.Backend, error) {
	scope := "SINGLE"
	if p.Cluster {
		scope = "CLUSTER"
	}
	addrs := func(broker string) []string {
		return strings.Split(os.Getenv(strings.ToUpper(broker)+"_"+scope), ",")
	}
	switch p.Broker {
	case "kafka":
		cfg := kafka.Config{Brokers: addrs("kafka"), Linger: o.KafkaLinger, Replicas: 1, MinInsync: 1}
		if p.Cluster {
			cfg.Replicas, cfg.MinInsync = 3, 2
		}
		cfg.Fsync = p.Name == "kafka_fsync"
		cfg.LeaderAcks = p.Name == "kafka_r3_acks1"
		return kafka.New(ctx, cfg)
	case "rabbit":
		return rabbit.New(ctx, rabbit.Config{
			URLs: addrs("rabbit"), Management: addrs("rabbit_http"), Publishers: o.Publishers,
			Quorum:    strings.HasPrefix(p.Name, "rabbit_quorum"),
			Transient: p.Name == "rabbit_transient",
			NoConfirm: p.Name == "rabbit_noconfirm",
			Members:   map[bool]int{true: 3, false: 1}[p.Cluster],
		})
	case "nats":
		cfg := natsjs.Config{URLs: addrs("nats"), Memory: p.Name == "nats_memory", Replicas: 1}
		if p.Cluster {
			cfg.Replicas = 3
		}
		return natsjs.New(ctx, cfg)
	}
	return nil, fmt.Errorf("unknown broker %q", p.Broker)
}
