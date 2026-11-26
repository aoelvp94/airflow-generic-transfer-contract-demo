"""Run the example Dag on a real Airflow 3.4 scheduler.

``just aip132-e2e`` starts the emulators and the compose stack, then runs
this module. The tests talk to the API on the host. They skip when the stack
is down unless ``AIP132_E2E_REQUIRE`` is ``1``.
"""

from __future__ import annotations

import json
import os
import subprocess
import time
import uuid
from pathlib import Path

import pytest

pytest.importorskip("airflow.sdk")

import requests

from examples.aip132_s3_prefix_to_gcs import PREFIX

pytestmark = [pytest.mark.e2e, pytest.mark.aip132]

_ROOT = Path(__file__).resolve().parents[2]
_API = os.environ.get("AIP132_AIRFLOW_URL", "http://127.0.0.1:18080").rstrip("/")
_DAG_ID = "aip132_s3_prefix_to_gcs"
_TASK_ID = "copy_s3_prefix_to_gcs"
_EXPECTED = [f"{PREFIX}/a.json", f"{PREFIX}/nested/b.json"]
_REQUIRE = os.environ.get("AIP132_E2E_REQUIRE") == "1"


def _absent(reason: str) -> None:
    if _REQUIRE:
        pytest.fail(reason)
    pytest.skip(reason)


def _api_up() -> bool:
    try:
        response = requests.get(f"{_API}/api/v2/monitor/health", timeout=3)
    except requests.RequestException:
        return False
    return response.status_code == 200


class _Airflow:
    def __init__(self) -> None:
        response = requests.post(
            f"{_API}/auth/token",
            json={"username": "airflow", "password": "airflow"},
            timeout=30,
        )
        response.raise_for_status()
        body = response.json()
        token = body.get("access_token") or body.get("token")
        if not token:
            raise AssertionError(f"auth response has no token: {body}")
        self._session = requests.Session()
        self._session.headers["Authorization"] = f"Bearer {token}"

    def request(self, method: str, path: str, **kwargs):
        response = self._session.request(method, _API + path, timeout=60, **kwargs)
        return response


def _seed() -> None:
    from generic_transfer.adapters.emulators import (
        clear_prefix,
        ensure_bucket,
        load_emulator_env,
        make_fs,
        seed_tree_with_markers,
    )

    load_emulator_env()
    os.environ.setdefault("STORAGE_EMULATOR_HOST", "http://127.0.0.1:14443")
    src = make_fs("s3", bucket="src-bucket")
    dst = make_fs("gcs", bucket="dst-bucket")
    ensure_bucket(src)
    ensure_bucket(dst)
    clear_prefix(src, PREFIX)
    clear_prefix(dst, PREFIX)
    seed_tree_with_markers(src, PREFIX)


def _unwrap_xcom(payload):
    value = payload
    if isinstance(value, dict) and "value" in value:
        value = value["value"]
    if isinstance(value, str):
        value = json.loads(value)
    return value


def _log_text(payload) -> str:
    if isinstance(payload, str):
        return payload
    if isinstance(payload, dict):
        content = payload.get("content", payload)
        if isinstance(content, str):
            return content
        if isinstance(content, list):
            parts: list[str] = []
            for item in content:
                if isinstance(item, str):
                    parts.append(item)
                elif isinstance(item, dict):
                    parts.append(
                        str(item.get("event") or item.get("message") or json.dumps(item))
                    )
                else:
                    parts.append(str(item))
            return "\n".join(parts)
        return json.dumps(payload)
    return json.dumps(payload)


def _events_in(text: str) -> list[dict]:
    decoder = json.JSONDecoder()
    found: list[dict] = []
    idx = 0
    while True:
        start = text.find('"eventType"', idx)
        if start < 0:
            break
        brace = text.rfind("{", 0, start)
        if brace < 0:
            idx = start + 1
            continue
        try:
            obj, end = decoder.raw_decode(text[brace:])
        except json.JSONDecodeError:
            idx = start + 1
            continue
        if isinstance(obj, dict) and "eventType" in obj and ("inputs" in obj or "outputs" in obj):
            found.append(obj)
            idx = brace + end
            continue
        idx = start + 1
    return found


def _collect_logs(api: _Airflow, run_id: str) -> str:
    chunks: list[str] = []
    for path in (
        f"/api/v2/dags/{_DAG_ID}/dagRuns/{run_id}/taskInstances/{_TASK_ID}/logs/1",
        f"/api/v2/dags/{_DAG_ID}/dagRuns/{run_id}/taskInstances/{_TASK_ID}/logs/1?full_content=true",
    ):
        response = api.request("GET", path)
        if response.status_code == 200:
            chunks.append(_log_text(response.json() if response.headers.get("content-type", "").startswith("application/json") else response.text))
    log_dir = _ROOT / "aip132" / "docker" / "logs"
    if log_dir.is_dir():
        for path in log_dir.rglob("*"):
            if path.is_file() and path.stat().st_size < 5_000_000:
                try:
                    chunks.append(path.read_text(errors="replace"))
                except OSError:
                    continue
    compose = _ROOT / "aip132" / "docker" / "docker-compose.yml"
    if compose.is_file():
        proc = subprocess.run(
            ["docker", "compose", "-f", str(compose), "logs", "--no-color"],
            check=False,
            capture_output=True,
            text=True,
            timeout=60,
        )
        chunks.append(proc.stdout)
        chunks.append(proc.stderr)
    return "\n".join(chunks)


def _trigger(api: _Airflow) -> dict:
    deadline = time.time() + 180
    dag = None
    while time.time() < deadline:
        response = api.request("GET", f"/api/v2/dags/{_DAG_ID}")
        if response.status_code == 200:
            dag = response.json()
            break
        time.sleep(2)
    if dag is None:
        pytest.fail(f"dag {_DAG_ID} was not parsed; last status {response.status_code} {response.text[:500]}")
    paused = api.request("PATCH", f"/api/v2/dags/{_DAG_ID}", json={"is_paused": False})
    if paused.status_code not in {200, 204}:
        pytest.fail(f"unpause failed: {paused.status_code} {paused.text[:500]}")
    run_id = "aip132_e2e_" + uuid.uuid4().hex[:8]
    body = {
        "dag_run_id": run_id,
        "logical_date": "2026-11-25T00:00:00+00:00",
        "conf": {},
    }
    created = api.request("POST", f"/api/v2/dags/{_DAG_ID}/dagRuns", json=body)
    if created.status_code == 422:
        body["data_interval_start"] = body["logical_date"]
        body["data_interval_end"] = "2026-11-25T00:01:00+00:00"
        created = api.request("POST", f"/api/v2/dags/{_DAG_ID}/dagRuns", json=body)
    if created.status_code not in {200, 201}:
        pytest.fail(f"trigger failed: {created.status_code} {created.text[:800]}")
    state = None
    run_body: dict = {}
    deadline = time.time() + 180
    while time.time() < deadline:
        polled = api.request("GET", f"/api/v2/dags/{_DAG_ID}/dagRuns/{run_id}")
        if polled.status_code == 200:
            run_body = polled.json()
            state = run_body.get("state")
            if state in {"success", "failed"}:
                break
        time.sleep(2)
    logs = _collect_logs(api, run_id)
    if state != "success":
        pytest.fail(f"dag run state={state}\n{logs[-4000:]}")
    xcom = api.request(
        "GET",
        f"/api/v2/dags/{_DAG_ID}/dagRuns/{run_id}/taskInstances/{_TASK_ID}/xcomEntries/return_value",
    )
    if xcom.status_code != 200:
        pytest.fail(f"xcom read failed: {xcom.status_code} {xcom.text[:800]}\n{logs[-2000:]}")
    return {"xcom": _unwrap_xcom(xcom.json()), "logs": logs, "run_id": run_id}


@pytest.fixture(scope="module")
def completed_run():
    if not _api_up():
        _absent(f"Airflow API is not up at {_API}")
    _seed()
    api = _Airflow()
    return _trigger(api)


def test_scheduler_xcom_is_the_copied_keys_without_folder_markers(completed_run):
    value = completed_run["xcom"]
    print("AIP132_E2E_XCOM", json.dumps(value))
    assert value == _EXPECTED, value
    assert not any(str(key).endswith("/") for key in value)


def test_openlineage_console_event_names_the_prefix_and_the_files(completed_run):
    events = _events_in(completed_run["logs"])
    sample_dir = _ROOT / "aip132" / "docker" / "logs"
    sample_dir.mkdir(parents=True, exist_ok=True)
    task_complete = [
        event
        for event in events
        if event.get("eventType") == "COMPLETE"
        and any(
            (dataset.get("name") or "").endswith("/a.json")
            for dataset in event.get("outputs") or []
        )
    ]
    sample = task_complete[-1] if task_complete else (events[-1] if events else None)
    if sample is not None:
        (sample_dir / "openlineage-event.json").write_text(json.dumps(sample, indent=2))
        print("AIP132_E2E_OL_EVENT", json.dumps(sample)[:4000])
    assert events, completed_run["logs"][-3000:]
    start = [event for event in events if event.get("eventType") in {"START", "RUNNING"}]
    complete = [event for event in events if event.get("eventType") == "COMPLETE"]
    assert start, [event.get("eventType") for event in events]
    assert complete, [event.get("eventType") for event in events]

    def pairs(selected: list[dict], side: str) -> set[tuple[str, str]]:
        found: set[tuple[str, str]] = set()
        for event in selected:
            for dataset in event.get(side) or []:
                found.add((dataset.get("namespace"), dataset.get("name")))
        return found

    assert ("s3://src-bucket", f"{PREFIX}/") in pairs(start, "inputs"), pairs(start, "inputs")
    assert ("gs://dst-bucket", f"{PREFIX}/") in pairs(start, "outputs"), pairs(start, "outputs")
    outputs = pairs(complete, "outputs")
    inputs = pairs(complete, "inputs")
    assert ("gs://dst-bucket", f"{PREFIX}/a.json") in outputs, outputs
    assert ("gs://dst-bucket", f"{PREFIX}/nested/b.json") in outputs, outputs
    assert ("s3://src-bucket", f"{PREFIX}/a.json") in inputs, inputs
    assert ("s3://src-bucket", f"{PREFIX}/nested/b.json") in inputs, inputs
    assert not any((name or "").endswith("nested/") or (name or "").endswith("/nested") for _ns, name in outputs)
