.PHONY: up rebuild down test web-build bench-suite bench-charts metrics-api metrics-worker compose-config compose-config-benchmark

RESULT_DATE ?= $(shell date +%F)
RESULT_DIR ?= benchmarks/results/$(RESULT_DATE)

up:
	docker compose up -d

rebuild:
	docker compose up -d --build

down:
	docker compose down

test:
	go test ./...

web-build:
	npm --prefix apps/web ci
	npm --prefix apps/web run build

bench-suite:
	./scripts/run_bench_suite.sh

bench-charts:
	./scripts/generate_benchmark_charts.sh $(RESULT_DIR)

metrics-api:
	curl -fsS http://localhost:8080/metrics

metrics-worker:
	curl -fsS http://localhost:8081/metrics

compose-config:
	docker compose config --services

compose-config-benchmark:
	docker compose --profile benchmark config --services
