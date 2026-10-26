from __future__ import annotations

from fastapi import FastAPI, HTTPException

from src.api_monitor.engine import HealthStore, TargetConfig, execute_probe


def create_app(store: HealthStore | None = None) -> FastAPI:
    active_store = store or HealthStore("api_monitor.db")
    app = FastAPI(
        title="API Health & SLA Monitor",
        version="1.0.0",
        description="Endpoint availability monitor tracking HTTP status contracts, JSON schema keys, and p50/p95/p99 latency percentiles.",
    )

    @app.get("/health")
    def service_health() -> dict[str, str]:
        return {"service": "api-health-monitor", "status": "ok"}

    @app.post("/targets", status_code=201)
    def register_target(target: TargetConfig) -> TargetConfig:
        active_store.upsert_target(target)
        return target

    @app.get("/targets")
    def list_targets() -> list[TargetConfig]:
        return active_store.list_targets()

    @app.post("/targets/{name}/probe")
    async def trigger_probe(name: str) -> dict[str, object]:
        targets = {t.name: t for t in active_store.list_targets()}
        if name not in targets:
            raise HTTPException(status_code=404, detail=f"Target '{name}' not found")
        outcome = await execute_probe(targets[name])
        active_store.record_outcome(outcome)
        return {
            "target_name": outcome.target_name,
            "checked_at": outcome.checked_at,
            "status_code": outcome.status_code,
            "latency_ms": outcome.latency_ms,
            "healthy": outcome.healthy,
            "failure_reason": outcome.failure_reason,
            "attempt_count": outcome.attempt_count,
        }

    @app.get("/targets/{name}/summary")
    def get_summary(name: str) -> dict[str, object]:
        targets = {t.name: t for t in active_store.list_targets()}
        if name not in targets:
            raise HTTPException(status_code=404, detail=f"Target '{name}' not found")
        return active_store.compute_target_summary(name)

    return app


app = create_app()
