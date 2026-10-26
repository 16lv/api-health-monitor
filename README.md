# API Health & SLA Monitor

An asynchronous HTTP endpoint availability and SLA latency monitoring tool built with **Python 3.12**, **FastAPI**, **httpx**, and **SQLite**.

## Overview

`api-health-monitor` checks HTTP endpoints against configurable status code contracts, latency ceilings, and required JSON response keys. Every check is persisted to SQLite so operators or automated pipelines can query availability percentages and `p50` / `p95` / `p99` response-time percentiles via a FastAPI REST interface.

## Key Features

- **Async HTTP Probing with Retries:** Built on `httpx.AsyncClient` with per-target timeout enforcement and configurable retry policies before marking an endpoint unhealthy.
- **Contract Validation:** Verifies expected HTTP status codes, maximum SLA response latency (`max_latency_ms`), and required top-level JSON keys in API responses.
- **Historical Telemetry & Percentile Rollups:** Stores probe outcomes in an indexed SQLite database (`probe_history`) and computes availability percentage, `p50`, `p95`, and `p99` latency metrics.
- **FastAPI Control Plane:** Exposes `/targets`, `/targets/{name}/probe`, and `/targets/{name}/summary` endpoints with automatic OpenAPI documentation.

## Tech Stack

- **Language:** Python 3.12 (strict type annotations)
- **Framework:** FastAPI, Pydantic v2, Uvicorn
- **HTTP Client:** `httpx` (`AsyncClient` + `MockTransport` for deterministic testing)
- **Database:** SQLite (`sqlite3`)
- **Testing & Quality:** `pytest`, `pytest-asyncio`, `ruff`

## Setup & Usage

```bash
pip install -e ".[dev]"
```

### Run the FastAPI Server

```bash
uvicorn src.api_monitor.api:app --reload --port 8000
```

### Run Tests & Linter

```bash
ruff check .
pytest -v
```
