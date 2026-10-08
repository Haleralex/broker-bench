BENCH := go run ./cmd/bench

.PHONY: test smoke bench chaos-smoke chaos summary report monitor down

## test: static checks and unit tests; no Docker required
test:
	go vet ./...
	go test ./...

## smoke: quick throughput check of one profile per broker
smoke:
	$(BENCH) run -profiles kafka,rabbit_classic,nats -backlog 20000 -rates 2000,5000 -step-warmup 2s -step 5s $(ARGS)

## bench: throughput of every profile
bench:
	$(BENCH) run $(ARGS)

## chaos-smoke: one kill per broker on a single node, short traffic
chaos-smoke:
	$(BENCH) chaos -profiles kafka,rabbit_classic,nats -rate 2000 -duration 30s -fault-at 10s -downtime 10s -quiet 10s $(ARGS)

## chaos: kill a node (the leader on clusters) under traffic for every profile
chaos:
	$(BENCH) chaos $(ARGS)

## summary: median tables of the latest throughput and chaos experiments
summary:
	$(BENCH) summarize $(ARGS)

## report: results/report.html with charts, served on port 8090 (Ctrl+C stops)
report:
	$(BENCH) report -serve :8090 $(ARGS)

## monitor: start Prometheus and Grafana alone, e.g. to look at a past run
monitor:
	docker compose --profile monitor up -d --wait prometheus grafana

## down: remove every container, volume and the Prometheus history
down:
	docker compose --profile load --profile monitor down -v --remove-orphans
