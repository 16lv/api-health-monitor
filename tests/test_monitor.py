from __future__ import annotations

import httpx
import pytest
from fastapi.testclient import TestClient

from src.api_monitor.api import create_app
from src.api_monitor.engine import HealthStore, TargetConfig, execute_probe


@pytest.mark.asyncio
async def test_execute_probe_passes_when_status_latency_and_json_key_match() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"status": "healthy", "region": "eu-west-2"})

    transport = httpx.MockTransport(handler)
    async with httpx.AsyncClient(transport=transport) as client:
        target = TargetConfig(
            name="auth-service",
            url="https://api.example.com/health",
            expected_status=200,
            max_latency_ms=500.0,
            required_json_key="status",
            retries=1,
        )
        outcome = await execute_probe(target, client=client)
        assert outcome.healthy is True
        assert outcome.status_code == 200
        assert outcome.failure_reason is None
        assert outcome.attempt_count == 1


@pytest.mark.asyncio
async def test_execute_probe_retries_and_reports_missing_json_key() -> None:
    calls = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return httpx.Response(200, json={"wrong_field": True})

    transport = httpx.MockTransport(handler)
    async with httpx.AsyncClient(transport=transport) as client:
        target = TargetConfig(
            name="billing-api",
            url="https://api.example.com/v1/status",
            expected_status=200,
            required_json_key="healthy",
            retries=2,
        )
        outcome = await execute_probe(target, client=client)
        assert outcome.healthy is False
        assert outcome.attempt_count == 3
        assert calls == 3
        assert outcome.failure_reason is not None
        assert "Missing required JSON key 'healthy'" in outcome.failure_reason


def test_fastapi_target_registration_and_sla_percentile_summary() -> None:
    store = HealthStore(":memory:")
    client = TestClient(create_app(store))

    resp = client.post(
        "/targets",
        json={
            "name": "payments-gateway",
            "url": "https://payments.example.org/ping",
            "method": "GET",
            "expected_status": 200,
            "max_latency_ms": 300.0,
            "retries": 1,
        },
    )
    assert resp.status_code == 201

    # Seed deterministic probe history to verify p50/p95/p99 and availability rollup
    from src.api_monitor.engine import ProbeOutcome

    sample_latencies = [12.0, 15.0, 18.0, 22.0, 25.0, 30.0, 42.0, 95.0, 140.0, 210.0]
    for idx, lat in enumerate(sample_latencies):
        store.record_outcome(
            ProbeOutcome(
                target_name="payments-gateway",
                url="https://payments.example.org/ping",
                checked_at=f"2026-10-26T12:00:{idx:02d}Z",
                status_code=200 if idx < 9 else 503,
                latency_ms=lat,
                healthy=(idx < 9),
                failure_reason=None if idx < 9 else "Expected HTTP 200, received 503",
                attempt_count=1,
            )
        )

    summary_resp = client.get("/targets/payments-gateway/summary")
    assert summary_resp.status_code == 200
    body = summary_resp.json()
    assert body["total_checks"] == 10
    assert body["availability_pct"] == 90.0
    assert body["p50_latency_ms"] == 27.5
    assert body["p95_latency_ms"] > 140.0
