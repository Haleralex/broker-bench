// Package queue defines the contract every broker implements and the message
// layout shared by producers and consumers.
package queue

import (
	"context"
	"encoding/binary"
	"fmt"
	"math/rand/v2"
	"time"
)

// HeaderSize is the fixed prefix of every payload: sequence number and the time
// the message was scheduled to be sent. The rest is incompressible filler.
const HeaderSize = 16

// filler is random so that no broker gains from compression.
var filler = func() []byte {
	b := make([]byte, 1<<16)
	r := rand.NewChaCha8([32]byte{'q', 'u', 'e', 'u', 'e'})
	_, _ = r.Read(b)
	return b
}()

// Encode returns a payload of size bytes carrying seq and the scheduled send time.
// Scheduling time, not actual send time, keeps end-to-end latency honest when
// producers fall behind (coordinated omission).
func Encode(seq uint64, scheduled time.Time, size int) []byte {
	b := make([]byte, max(size, HeaderSize))
	binary.BigEndian.PutUint64(b, seq)
	binary.BigEndian.PutUint64(b[8:], uint64(scheduled.UnixNano()))
	offset := int(seq*131) % (len(filler) - len(b))
	copy(b[HeaderSize:], filler[offset:])
	return b
}

// Decode reads the header written by Encode.
func Decode(b []byte) (seq uint64, scheduled time.Time, err error) {
	if len(b) < HeaderSize {
		return 0, time.Time{}, fmt.Errorf("payload of %d bytes is shorter than the %d-byte header", len(b), HeaderSize)
	}
	return binary.BigEndian.Uint64(b), time.Unix(0, int64(binary.BigEndian.Uint64(b[8:]))), nil
}

// Producer appends batches. Send returns after the broker acknowledged the whole
// batch with the durability the profile configures. It is safe for concurrent use.
type Producer interface {
	Send(ctx context.Context, payloads [][]byte) error
}

// Consumer claims and acknowledges messages for one worker; it is not shared
// between goroutines.
type Consumer interface {
	// Receive claims up to limit messages, calls handle for each, then
	// acknowledges them (at-least-once). ctx only bounds waiting for messages:
	// once a message reaches handle, its acknowledgement is completed even if
	// ctx is cancelled, so stopping a phase does not cause redelivery.
	// It returns 0 without error when nothing was available.
	Receive(ctx context.Context, limit int, handle func(payload []byte)) (int, error)
	Close()
}

// Backend is one broker configured by one profile.
type Backend interface {
	// Reset creates an empty queue with room for the given number of consumers.
	Reset(ctx context.Context, consumers int) error
	Producer() Producer
	NewConsumer(ctx context.Context) (Consumer, error)
	// Blocking reports whether Receive waits on the broker for new messages;
	// otherwise the worker sleeps between empty polls.
	Blocking() bool
	// Stats returns broker-side counters such as storage size, recorded after a run.
	Stats(ctx context.Context) (map[string]any, error)
	Close()
}

// Retry calls f until it succeeds or timeout passes: a cluster accepts
// replicated queues only after all its nodes have joined.
func Retry(ctx context.Context, timeout time.Duration, f func() error) error {
	deadline := time.Now().Add(timeout)
	for {
		err := f()
		if err == nil || time.Now().After(deadline) {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Second):
		}
	}
}

// Clustered is implemented by backends that replicate the queue over several
// nodes. Leader names the compose service that currently leads the queue, the
// node a leader-kill fault targets.
type Clustered interface {
	Leader(ctx context.Context) (string, error)
}
