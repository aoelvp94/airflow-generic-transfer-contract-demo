install:
    uv venv .venv
    uv pip install -e '.[dev,iceberg]'

test:
    .venv/bin/pytest -v

bench:
    .venv/bin/python scripts/bench.py

check: test bench
