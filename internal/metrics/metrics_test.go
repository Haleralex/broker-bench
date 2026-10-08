package metrics

import (
	"io"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestLoadgenExposition(t *testing.T) {
	m := NewLoadgen("pg_sync", "run-1")
	m.Phase("steady", 10000)
	m.Sent(100, 2*time.Millisecond)
	m.Consumed(3 * time.Millisecond)
	m.Duplicate()
	m.SetBrokerStats(map[string]any{"dead_tuples": int64(42), "topic": "ignored", "flush_messages_1": true})

	rec := httptest.NewRecorder()
	m.Handler().ServeHTTP(rec, httptest.NewRequest("GET", "/metrics", nil))
	body, _ := io.ReadAll(rec.Body)
	text := string(body)
	for _, want := range []string{
		`queuebench_phase{phase="steady",profile="pg_sync"} 1`,
		`queuebench_phase{phase="fill",profile="pg_sync"} 0`,
		`queuebench_target_rate{profile="pg_sync"} 10000`,
		`queuebench_produced_total{profile="pg_sync"} 100`,
		`queuebench_consumed_total{profile="pg_sync"} 1`,
		`queuebench_duplicates_total{profile="pg_sync"} 1`,
		`queuebench_broker_stat{profile="pg_sync",stat="dead_tuples"} 42`,
		`queuebench_run_info{profile="pg_sync",run_id="run-1"} 1`,
		`queuebench_e2e_seconds_count{profile="pg_sync"} 1`,
	} {
		if !strings.Contains(text, want) {
			t.Errorf("exposition lacks %s", want)
		}
	}
	if strings.Contains(text, `stat="topic"`) {
		t.Error("non-numeric stat exported")
	}
}

func TestContainersDropStoppedOnes(t *testing.T) {
	c := NewContainers()
	c.Set([]ContainerSample{{Container: "rabbit", CPUPercent: 150}, {Container: "loadgen", CPUPercent: 80}})
	c.Set([]ContainerSample{{Container: "kafka", CPUPercent: 120}})
	rec := httptest.NewRecorder()
	c.Handler().ServeHTTP(rec, httptest.NewRequest("GET", "/metrics", nil))
	text := rec.Body.String()
	if !strings.Contains(text, `queuebench_container_cpu_percent{container="kafka"} 120`) || strings.Contains(text, `container="rabbit"`) {
		t.Fatalf("unexpected exposition:\n%s", text)
	}
}
