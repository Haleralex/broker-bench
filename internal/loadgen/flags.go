package loadgen

import (
	"flag"
	"fmt"
	"strconv"
	"strings"
	"time"
)

// Defaults are sized for compose.yaml: 2 CPUs per single broker, 3 for the load generator.
func Defaults() Config {
	return Config{
		Producers: 4, Consumers: 4, Batch: 100, Payload: 256, Backlog: 500_000,
		Rates:      []int{10_000, 30_000, 60_000, 100_000},
		StepWarmup: 5 * time.Second, Step: 30 * time.Second,
		DrainTimeout: 3 * time.Minute, Idle: 5 * time.Millisecond,
	}
}

// RegisterFlags binds the workload flags; the host runner registers the same
// flags and forwards them with Args.
func (c *Config) RegisterFlags(fs *flag.FlagSet) {
	fs.IntVar(&c.Producers, "producers", c.Producers, "producer goroutines")
	fs.IntVar(&c.Consumers, "consumers", c.Consumers, "worker goroutines; Kafka gets one partition per worker")
	fs.IntVar(&c.Batch, "batch", c.Batch, "messages per send and per receive, 1..1000")
	fs.IntVar(&c.Payload, "payload", c.Payload, "bytes per message, 16..16384")
	fs.IntVar(&c.Backlog, "backlog", c.Backlog, "messages produced in fill and consumed in drain")
	fs.Var((*rates)(&c.Rates), "rates", "comma-separated steady rates in messages/s; a saturated rate ends the list")
	fs.DurationVar(&c.StepWarmup, "step-warmup", c.StepWarmup, "unmeasured start of each rate")
	fs.DurationVar(&c.Step, "step", c.Step, "measured duration of each rate")
	fs.DurationVar(&c.DrainTimeout, "drain-timeout", c.DrainTimeout, "longest wait for the backlog to empty")
	fs.DurationVar(&c.Idle, "idle", c.Idle, "pause after an empty poll of a non-blocking backend (NATS)")
}

func (c Config) Validate() error {
	switch {
	case c.Producers < 1 || c.Producers > 64, c.Consumers < 1 || c.Consumers > 64:
		return fmt.Errorf("-producers and -consumers must be 1..64")
	case c.Batch < 1 || c.Batch > 1000:
		return fmt.Errorf("-batch must be 1..1000")
	case c.Payload < 16 || c.Payload > 16384:
		return fmt.Errorf("-payload must be 16..16384")
	case c.Backlog < 1000 || c.Backlog > 20_000_000:
		return fmt.Errorf("-backlog must be 1000..20000000")
	case len(c.Rates) == 0:
		return fmt.Errorf("-rates must list at least one rate")
	case c.Step < time.Second || c.StepWarmup < 0 || c.DrainTimeout < time.Second || c.Idle < 0:
		return fmt.Errorf("-step and -drain-timeout must be at least 1s; -step-warmup and -idle not negative")
	}
	return nil
}

// Args renders the config as flags understood by RegisterFlags.
func (c Config) Args() []string {
	return []string{
		"-producers", strconv.Itoa(c.Producers), "-consumers", strconv.Itoa(c.Consumers),
		"-batch", strconv.Itoa(c.Batch), "-payload", strconv.Itoa(c.Payload), "-backlog", strconv.Itoa(c.Backlog),
		"-rates", (*rates)(&c.Rates).String(),
		"-step-warmup", c.StepWarmup.String(), "-step", c.Step.String(),
		"-drain-timeout", c.DrainTimeout.String(), "-idle", c.Idle.String(),
	}
}

type rates []int

func (r *rates) String() string {
	parts := make([]string, len(*r))
	for i, v := range *r {
		parts[i] = strconv.Itoa(v)
	}
	return strings.Join(parts, ",")
}

func (r *rates) Set(s string) error {
	var parsed []int
	for _, part := range strings.Split(s, ",") {
		v, err := strconv.Atoi(part)
		if err != nil || v < 1 || v > 1_000_000 {
			return fmt.Errorf("rate %q must be 1..1000000", part)
		}
		parsed = append(parsed, v)
	}
	*r = parsed
	return nil
}
