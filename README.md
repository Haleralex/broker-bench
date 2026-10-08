# Kafka / RabbitMQ / NATS JetStream benchmark

Two questions about three message brokers, each in several configurations:

1. **Throughput** — how many messages per second, at what latency, before the
   queue falls behind.
2. **Integrity under faults** — a node is killed with `SIGKILL` while traffic
   flows; afterwards every message is checked: was anything the broker
   *acknowledged* lost?

The same Go load generator drives every broker with the same message size,
batch size, worker count and at-least-once semantics: a producer counts a
message as sent only when the broker acknowledged it, a worker acknowledges a
message only after handling it.

## Profiles

| Profile | Nodes | Acknowledged when |
| --- | --- | --- |
| `kafka` | 1 | appended to the leader's log (page cache); `acks=all` = leader only |
| `kafka_fsync` | 1 | appended and fsynced (`flush.messages=1`) |
| `kafka_r3` | 3 | replicated to `min.insync.replicas=2` of 3 (`acks=all`, RF=3) |
| `kafka_r3_acks1` | 3 | appended by the leader, before followers copy it (`acks=1`) |
| `rabbit_classic` | 1 | persistent message on a durable classic queue, publisher confirm |
| `rabbit_transient` | 1 | as above, but transient messages: dropped on restart |
| `rabbit_noconfirm` | 1 | no publisher confirms: "sent" means written to the socket |
| `rabbit_quorum` | 1 | quorum queue: Raft log fsynced before the confirm |
| `rabbit_quorum_r3` | 3 | quorum queue on 3 nodes: a majority has it |
| `nats` | 1 | stored by JetStream; file store fsyncs every 2 minutes (default `sync_interval`) |
| `nats_sync` | 1 | stored and fsynced (`sync_interval: always`) |
| `nats_memory` | 1 | stored in memory |
| `nats_r3` | 3 | stored by a quorum of 3 servers (R3) |

Queues: a Kafka topic with one partition per worker and one consumer group; a
single RabbitMQ queue with prefetch; a JetStream stream with work-queue
retention and one durable pull consumer.

## Run

Requires Docker with Linux containers and Go 1.25. In GitHub Codespaces use
an 8-core machine (`.devcontainer/` asks for one): a cluster profile runs three
broker containers next to the load generator.

```bash
make test         # vet and unit tests, no Docker
make smoke        # throughput, one profile per broker, ~5 minutes
make chaos-smoke  # one kill per broker, ~5 minutes
make bench        # throughput of every profile
make chaos        # kill under traffic for every profile, ~30 minutes
make summary      # tables of the latest throughput and chaos experiments
make report       # results/report.html with charts, served on port 8090
make down         # remove containers, volumes and the Prometheus history
```

`ARGS` passes flags, `go run ./cmd/bench run -h` and `go run ./cmd/bench chaos -h` list them:

```bash
make chaos ARGS="-profiles kafka_r3,kafka_r3_acks1 -repetitions 3"
make chaos ARGS="-profiles nats_memory -faults none,kill"   # baseline and fault
make chaos ARGS="-rate 20000 -downtime 30s"
make bench ARGS="-profiles kafka,nats -rates 50000,100000,150000"
```

`make bench` and `make chaos` start Prometheus and Grafana and print the
dashboard address (port 3000 in Codespaces' *Ports* tab). The dashboard shows
the current profile and phase, the killed node, confirmed and consumed
messages per second, failed sends, duplicates, latency, CPU per container and
the broker's own view: queue depth and live replicas.

## Integrity run

```
 0s ─────────── 20s ──── 35s ───────────── 60s ── settle ──
    5000 msg/s   kill -9   node back up     traffic stops   stragglers
```

1. Producers send at a fixed rate. A Send that fails or does not get its
   acknowledgement within `-send-timeout` (5s) counts as unconfirmed; the
   producer moves on, as a real service would after a timeout.
2. At `-fault-at` the load generator kills the broker container through the
   Docker socket. On a cluster it first asks the broker which node leads the
   queue (partition 0 for Kafka, the quorum queue leader for RabbitMQ, the
   stream leader for NATS) and kills that one.
3. After `-downtime` the container starts again on its own data.
4. After the traffic, workers keep reading until nothing new arrives for
   `-quiet`.

Every sequence number then falls into one class:

| Class | Meaning |
| --- | --- |
| confirmed and received | fine |
| **lost** | confirmed by the broker, never received: **the broker broke its promise** |
| delivered unconfirmed | Send failed or timed out, the message arrived anyway: no violation, but a producer that retries creates a duplicate |
| unconfirmed | Send failed and the message never arrived: no violation, the producer knew |
| duplicates | received more than once: expected with at-least-once after a failure |

`producer stall` and `consumer stall` count seconds after the fault in which
nothing was confirmed or received: how long the outage was felt.

### What to expect, and what this setup cannot show

`kill -9` kills the broker **process**. Whatever it had already written into
files is in the kernel's page cache, survives, and reaches the disk later. So
under `kill -9` the profiles that do not fsync (`kafka`, `nats`) normally lose
nothing; their weakness is a power or kernel failure, which takes the page
cache with it. Simulating that needs a file system that can drop unsynced data
(LazyFS) — the next stage of this benchmark.

| Profile | `kill -9` of the single node / the leader | Power loss (not simulated yet) |
| --- | --- | --- |
| `kafka`, `nats` | expected no loss | up to ~30 s (`kafka`) or 2 min (`nats`) of acknowledged data |
| `kafka_fsync`, `nats_sync`, `rabbit_quorum`, `rabbit_classic` | expected no loss | expected no loss |
| `rabbit_transient`, `nats_memory` | **loss**: messages kept in memory | loss |
| `rabbit_noconfirm` | **loss** possible: messages in flight were never the broker's | loss |
| `kafka_r3`, `rabbit_quorum_r3`, `nats_r3` | expected no loss: another replica has every acknowledged message | expected no loss unless all replicas lose power |
| `kafka_r3_acks1` | **loss** possible: the leader acknowledged before followers copied; the new leader never had those records | loss |

Loss in `kafka_r3_acks1` depends on how many records were acknowledged but not
yet replicated at the moment of the kill — a few milliseconds of traffic. Raise
`-rate` and add `-repetitions` to see it reliably.

## Throughput run

As in the PostgreSQL/Kafka benchmark this one grew from: fill (producers
only, `-backlog` messages), drain (workers only), then steady rates (`-rates`)
with end-to-end latency measured from the scheduled send time, so a producer
that falls behind does not hide the delay.

## Layout

| Path | Responsibility |
| --- | --- |
| `cmd/bench` | Host side: Compose lifecycle, resource sampling, summaries, HTML report |
| `cmd/loadgen` | Runs inside Compose: one profile, `-mode throughput` or `-mode chaos`, writes `*-report.json` |
| `internal/loadgen` | Phases, open-loop scheduler, sequence tracking, integrity check, timeline |
| `internal/queue` | `Backend`/`Producer`/`Consumer` contract and message layout |
| `internal/queue/{kafka,rabbit,natsjs}` | The brokers; reconnects and leader lookup for faults |
| `internal/dockerctl` | Kills and starts containers through the Docker socket |
| `internal/profile` | Profile list, services and client settings |
| `internal/metrics` | Prometheus metrics for Grafana |
| `config/` | NATS server and RabbitMQ cluster configuration |
| `monitoring/` | Prometheus scrape config, Grafana provisioning and dashboard (`gen_dashboard.py`) |

The load generator container mounts `/var/run/docker.sock` to kill brokers.
That gives it full control of the Docker daemon — acceptable for a local lab,
not for anything shared.
