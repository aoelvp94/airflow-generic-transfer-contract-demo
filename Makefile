# Mirror of justfile targets for reviewers without `just`.
.PHONY: install up down status test test-integration test-all bench bench-emulators e2e check reproduce

install:
	uv venv .venv
	uv pip install -e '.[dev,iceberg,emulators]'

up:
	./scripts/emulators.sh up

down:
	./scripts/emulators.sh down

status:
	./scripts/emulators.sh status

test:
	.venv/bin/pytest -v -m 'not integration'

test-integration:
	.venv/bin/pytest -v -m integration

test-all:
	.venv/bin/pytest -v

bench:
	.venv/bin/python scripts/bench.py

bench-emulators:
	.venv/bin/python scripts/bench_emulators.py --rounds 5

e2e: up test-all bench bench-emulators

check: test bench

reproduce: install e2e
	@echo "Results in bench-out/BENCH.md and bench-out/latest.json"
