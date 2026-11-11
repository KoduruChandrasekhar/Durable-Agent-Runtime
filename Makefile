# keel: common tasks. (On Windows without make, run the commands directly.)
COMPOSE = docker compose -f deploy/docker-compose.yml
PY = PYTHONPATH=. python
TESTDB ?= postgres://keel:keel@localhost:55432/keel_test?sslmode=disable

.PHONY: up down proto build test test-go test-py sim evals injection conformance chaos-demo naive zombie bench plot kind lint

up:            ## the whole system + the refund demo, one command
	$(COMPOSE) up -d --build

down:
	$(COMPOSE) down -v

proto:
	./scripts/gen_proto.sh

build:
	go build -o bin/ ./server/cmd/... ./gateway/cmd/... ./sandbox/cmd/...

lint:
	go vet ./...
	ruff check sdk/python/keel demos sim evals chaos bench
	$(PY) -m keel.cli lint demos bench

test: test-go test-py

test-go:
	KEEL_TEST_DATABASE_URL=$(TESTDB) go test -race -p 1 -count=1 ./...

test-py:
	$(PY) -m pytest -q sdk/python/tests

sim:           ## 1,000 seeded fault schedules
	$(PY) -m sim.run_seeds --seeds 1000 --json bench/results/sim.json

evals:
	$(PY) -m evals.runner --versions 1 2 3 --trials 8 --json bench/results/evals.json
	$(PY) -m evals.gate --report bench/results/evals.json --candidate 2

injection:
	$(PY) -m evals.injection_suite --versions 1 2 --json bench/results/injection.json

chaos-demo:    ## 1,000 refund agents, a worker killed every 3 s, 10% LLM errors, 20% fail-after-commit
	$(PY) -m chaos.demo --runs 1000

naive:         ## the same agent as a plain loop, under the same chaos
	$(COMPOSE) --profile naive up -d naive-worker
	$(PY) -m chaos.naive_baseline run --runs 1000
	$(COMPOSE) --profile naive stop naive-worker

zombie:
	$(PY) -m chaos.zombie

bench:
	./bench/run_all.sh

plot:
	$(PY) -m bench.plot

kind:
	./deploy/k8s/kind_up.sh
